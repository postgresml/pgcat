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
use crate::errors::Error;
use crate::pool::{ClientServerMap, ConnectionPool};
use crate::prometheus::start_metric_server;
use crate::stats::{Collector, Reporter, REPORTER};

#[tokio::main(worker_threads = 4)]
async fn main() {
    env_logger::init();
    info!("Welcome to PgCat! Meow. (Version {})", VERSION);

    if !query_router::QueryRouter::setup() {
        error!("Could not setup query router");
        return;
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
            return;
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
                return;
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
            return;
        }
    };

    info!("Running on {}", addr);

    config.show();

    // Tracks which client is connected to which server for query cancellation.
    let client_server_map: ClientServerMap = Arc::new(Mutex::new(HashMap::new()));

    // Statistics reporting.
    let (stats_tx, stats_rx) = mpsc::channel(100_000);
    REPORTER.store(Arc::new(Reporter::new(stats_tx.clone())));

    // Connection pool that allows to query all shards and replicas.
    match ConnectionPool::from_config(client_server_map.clone()).await {
        Ok(_) => (),
        Err(err) => {
            error!("Pool error: {:?}", err);
            return;
        }
    };

    tokio::task::spawn(async move {
        let mut stats_collector = Collector::new(stats_rx, stats_tx.clone());
        stats_collector.collect().await;
    });

    info!("Config autoreloader: {}", config.general.autoreload);

    let mut term_signal = unix_signal(SignalKind::terminate()).unwrap();
    let mut interrupt_signal = unix_signal(SignalKind::interrupt()).unwrap();
    let mut sighup_signal = unix_signal(SignalKind::hangup()).unwrap();
    let mut autoreload_interval = tokio::time::interval(tokio::time::Duration::from_millis(15_000));
    let (shutdown_tx, _) = broadcast::channel::<()>(1);
    let (drain_tx, mut drain_rx) = mpsc::channel::<usize>(1);
    let (exit_tx, mut exit_rx) = mpsc::channel::<()>(1);

    info!("Waiting for clients");

    let mut admin_only = false;
    let mut total_clients = 0;

    loop {
        tokio::select! {
            // Reload config:
            // kill -SIGHUP $(pgrep pgcat)
            _ = sighup_signal.recv() => {
                info!("Reloading config");

                match reload_config(client_server_map.clone()).await {
                    Ok(_) => (),
                    Err(_) => (),
                };

                get_config().show();
            },

            _ = autoreload_interval.tick() => {
                if config.general.autoreload {
                    info!("Automatically reloading config");

                    match reload_config(client_server_map.clone()).await {
                        Ok(changed) => {
                            if changed {
                                get_config().show()
                            }
                        }
                        Err(_) => (),
                    };
                }
            },

            // Initiate graceful shutdown sequence on sig int
            _ = interrupt_signal.recv() => {
                info!("Got SIGINT, waiting for client connection drain now");
                admin_only = true;

                // Broadcast that client tasks need to finish
                shutdown_tx.send(()).unwrap();
                drain_tx.send(total_clients).await.unwrap();

                let drain_tx = drain_tx.clone();

                tokio::task::spawn(async move {
                    let mut interval = tokio::time::interval(tokio::time::Duration::from_millis(15_000));

                    // First tick fires immediately.
                    interval.tick().await;

                    // Second one in the interval time.
                    interval.tick().await;

                    // We're done waiting.
                    error!("Timed out waiting for clients");

                    drain_tx.send(0).await.unwrap();
                });
            },

            _ = term_signal.recv() => break,

            new_client = listener.accept() => {
                let (socket, addr) = match new_client {
                    Ok((socket, addr)) => (socket, addr),
                    Err(err) => {
                        error!("{:?}", err);
                        continue;
                    }
                };

                let shutdown_rx = shutdown_tx.subscribe();
                let drain_tx = drain_tx.clone();
                let client_server_map = client_server_map.clone();

                if !admin_only {
                    total_clients += 1;
                }

                tokio::task::spawn(async move {
                    let start = chrono::offset::Utc::now().naive_utc();

                    match client::client_entrypoint(
                        socket,
                        client_server_map,
                        shutdown_rx,
                        drain_tx.clone(),
                        admin_only,
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

                            total_clients -= 1;
                        }

                        Err(err) => {
                            match err {
                                // Don't count the clients we rejected.
                                Error::ShuttingDown => (),
                                _ => {
                                    total_clients -= 1;
                                }
                            }

                            debug!("Client disconnected with error {:?}", err);
                        }
                    };

                    let _ = drain_tx.send(total_clients).await;
                });
            }

            _ = exit_rx.recv() => {
                break;
            }

            remaining_clients = drain_rx.recv() => {
                let remaining_clients = remaining_clients.unwrap();

                if remaining_clients == 0 {
                    exit_tx.send(()).await.unwrap();
                }
            }
        }
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
