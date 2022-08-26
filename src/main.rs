// Copyright (c) 2022 Lev Kokotov <hi@levthe.dev>

// Permission is hereby granted, free of charge, to any person obtaining
// a copy of this software and associated documentation files (the
// "Software"), to deal in the Software without restriction, including
// without limitation the rights to use, copy, modify, merge, publish,
// distribute, sublicense, and/or sell copies of the Software, and to
// permit persons to whom the Software is furnished to do so, subject to
// the following conditions:

// The above copyright notice and this permission notice shall be
// included in all copies or substantial portions of the Software.

// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
// EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
// MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
// NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE
// LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION
// OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION
// WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

extern crate arc_swap;
extern crate async_trait;
extern crate bb8;
extern crate bytes;
extern crate env_logger;
extern crate exitcode;
extern crate log;
extern crate md5;
extern crate num_cpus;
extern crate once_cell;
extern crate rustls_pemfile;
extern crate serde;
extern crate serde_derive;
extern crate sqlparser;
extern crate tokio;
extern crate tokio_rustls;
extern crate toml;

use log::{debug, error, info};
use parking_lot::Mutex;
use tokio::net::TcpListener;
use tokio::{
    signal::unix::{signal as unix_signal, SignalKind},
    sync::mpsc,
};

use std::collections::HashMap;
use std::net::SocketAddr;
use std::str::FromStr;
use std::sync::Arc;
use tokio::sync::broadcast;

mod admin;
mod client;
mod config;
mod constants;
mod errors;
mod messages;
mod pool;
mod prometheus;
mod query_router;
mod scram;
mod server;
mod sharding;
mod stats;
mod tls;

use crate::config::{get_config, reload_config, VERSION};
use crate::pool::{ClientServerMap, ConnectionPool};
use crate::prometheus::start_metric_server;
use crate::stats::{Collector, Reporter, REPORTER};

#[tokio::main(worker_threads = 4)]
async fn main() {
    env_logger::init();
    info!("Welcome to PgCat! Meow. (Version {})", VERSION);

    if !query_router::QueryRouter::setup() {
        error!("Could not setup query router");
        std::process::exit(exitcode::CONFIG);
    }

    let args = std::env::args().collect::<Vec<String>>();

    let config_file = if args.len() == 2 {
        args[1].to_string()
    } else {
        String::from("pgcat.toml")
    };

    match config::parse(&config_file).await {
        Ok(_) => (),
        Err(err) => {
            error!("Config parse error: {:?}", err);
            std::process::exit(exitcode::CONFIG);
        }
    };

    let config = get_config();

    if let Some(true) = config.general.enable_prometheus_exporter {
        let http_addr_str = format!(
            "{}:{}",
            config.general.host, config.general.prometheus_exporter_port
        );
        let http_addr = match SocketAddr::from_str(&http_addr_str) {
            Ok(addr) => addr,
            Err(err) => {
                error!("Invalid http address: {}", err);
                std::process::exit(exitcode::CONFIG);
            }
        };
        tokio::task::spawn(async move {
            start_metric_server(http_addr).await;
        });
    }

    let addr = format!("{}:{}", config.general.host, config.general.port);

    let listener = match TcpListener::bind(&addr).await {
        Ok(sock) => sock,
        Err(err) => {
            error!("Listener socket error: {:?}", err);
            std::process::exit(exitcode::CONFIG);
        }
    };

    info!("Running on {}", addr);

    config.show();

    // Tracks which client is connected to which server for query cancellation.
    let client_server_map: ClientServerMap = Arc::new(Mutex::new(HashMap::new()));

    // Statistics reporting.
    let (tx, rx) = mpsc::channel(100_000);
    REPORTER.store(Arc::new(Reporter::new(tx.clone())));

    // Connection pool that allows to query all shards and replicas.
    match ConnectionPool::from_config(client_server_map.clone()).await {
        Ok(_) => (),
        Err(err) => {
            error!("Pool error: {:?}", err);
            std::process::exit(exitcode::CONFIG);
        }
    };

    // Statistics collector task.
    let collector_tx = tx.clone();

    // Save these for reloading
    let reload_client_server_map = client_server_map.clone();
    let autoreload_client_server_map = client_server_map.clone();

    tokio::task::spawn(async move {
        let mut stats_collector = Collector::new(rx, collector_tx);
        stats_collector.collect().await;
    });

    info!("Waiting for clients");

    let (shutdown_event_tx, mut shutdown_event_rx) = broadcast::channel::<()>(1);

    let shutdown_event_tx_clone = shutdown_event_tx.clone();

    // Client connection loop.
    tokio::task::spawn(async move {
        // Creates event subscriber for shutdown event, this is dropped when shutdown event is broadcast
        let mut listener_shutdown_event_rx = shutdown_event_tx_clone.subscribe();
        loop {
            let client_server_map = client_server_map.clone();

            // Listen for shutdown event and client connection at the same time
            let (socket, addr) = tokio::select! {
                _ = listener_shutdown_event_rx.recv() => {
                    // Exits client connection loop which drops listener, listener_shutdown_event_rx and shutdown_event_tx_clone
                    break;
                }

                listener_response = listener.accept() => {
                    match listener_response {
                        Ok((socket, addr)) => (socket, addr),
                        Err(err) => {
                            error!("{:?}", err);
                            continue;
                        }
                    }
                }
            };

            // Used to signal shutdown
            let client_shutdown_handler_rx = shutdown_event_tx_clone.subscribe();

            // Used to signal that the task has completed
            let dummy_tx = shutdown_event_tx_clone.clone();

            // Handle client.
            tokio::task::spawn(async move {
                let start = chrono::offset::Utc::now().naive_utc();

                match client::client_entrypoint(
                    socket,
                    client_server_map,
                    client_shutdown_handler_rx,
                )
                .await
                {
                    Ok(_) => {
                        let duration = chrono::offset::Utc::now().naive_utc() - start;

                        info!(
                            "Client {:?} disconnected, session duration: {}",
                            addr,
                            format_duration(&duration)
                        );
                    }

                    Err(err) => {
                        debug!("Client disconnected with error {:?}", err);
                    }
                };
                // Drop this transmitter so receiver knows that the task is completed
                drop(dummy_tx);
            });
        }
    });

    // Reload config:
    // kill -SIGHUP $(pgrep pgcat)
    tokio::task::spawn(async move {
        let mut stream = unix_signal(SignalKind::hangup()).unwrap();

        loop {
            stream.recv().await;

            info!("Reloading config");

            match reload_config(reload_client_server_map.clone()).await {
                Ok(_) => (),
                Err(_) => continue,
            };

            get_config().show();
        }
    });

    if config.general.autoreload {
        let mut interval = tokio::time::interval(tokio::time::Duration::from_millis(15_000));

        tokio::task::spawn(async move {
            info!("Config autoreloader started");

            loop {
                interval.tick().await;
                match reload_config(autoreload_client_server_map.clone()).await {
                    Ok(changed) => {
                        if changed {
                            get_config().show()
                        }
                    }
                    Err(_) => (),
                };
            }
        });
    }

    let mut term_signal = unix_signal(SignalKind::terminate()).unwrap();
    let mut interrupt_signal = unix_signal(SignalKind::interrupt()).unwrap();

    tokio::select! {
        // Initiate graceful shutdown sequence on sig int
        _ = interrupt_signal.recv() => {
            info!("Got SIGINT, waiting for client connection drain now");

            // Broadcast that client tasks need to finish
            shutdown_event_tx.send(()).unwrap();
            // Closes transmitter
            drop(shutdown_event_tx);

            // This is in a loop because the first event that the receiver receives will be the shutdown event
            // This is not what we are waiting for instead, we want the receiver to send an error once all senders are closed which is reached after the shutdown event is received
            loop {
                match tokio::time::timeout(
                    tokio::time::Duration::from_millis(config.general.shutdown_timeout),
                    shutdown_event_rx.recv(),
                )
                .await
                {
                    Ok(res) => match res {
                        Ok(_) => {}
                        Err(_) => break,
                    },
                    Err(_) => {
                        info!("Timed out while waiting for clients to shutdown");
                        break;
                    }
                }
            }
        },
        _ = term_signal.recv() => (),
    }

    info!("Shutting down...");
}

/// Format chrono::Duration to be more human-friendly.
///
/// # Arguments
///
/// * `duration` - A duration of time
fn format_duration(duration: &chrono::Duration) -> String {
    let seconds = {
        let seconds = duration.num_seconds() % 60;
        if seconds < 10 {
            format!("0{}", seconds)
        } else {
            format!("{}", seconds)
        }
    };

    let minutes = {
        let minutes = duration.num_minutes() % 60;
        if minutes < 10 {
            format!("0{}", minutes)
        } else {
            format!("{}", minutes)
        }
    };

    let hours = {
        let hours = duration.num_hours() % 24;
        if hours < 10 {
            format!("0{}", hours)
        } else {
            format!("{}", hours)
        }
    };

    let days = duration.num_days().to_string();

    format!("{}d {}:{}:{}", days, hours, minutes, seconds)
}
