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

#[cfg(not(target_env = "msvc"))]
use jemallocator::Jemalloc;

#[cfg(not(target_env = "msvc"))]
#[global_allocator]
static GLOBAL: Jemalloc = Jemalloc;

use log::{error, info, warn};
use parking_lot::Mutex;
use pgcat::format_duration;
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
    env_logger::builder().format_timestamp_micros().init();

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
    let (stats_tx, stats_rx) = mpsc::channel(100_000);
    REPORTER.store(Arc::new(Reporter::new(stats_tx.clone())));

    // Connection pool that allows to query all shards and replicas.
    match ConnectionPool::from_config(client_server_map.clone()).await {
        Ok(_) => (),
        Err(err) => {
            error!("Pool error: {:?}", err);
            std::process::exit(exitcode::CONFIG);
        }
    };

    tokio::task::spawn(async move {
        let mut stats_collector = Collector::new(stats_rx, stats_tx.clone());
        stats_collector.collect().await;
    });

    info!("Config autoreloader: {}", config.general.autoreload);

    let mut autoreload_interval = tokio::time::interval(tokio::time::Duration::from_millis(15_000));
    let autoreload_client_server_map = client_server_map.clone();
    tokio::task::spawn(async move {
        loop {
            autoreload_interval.tick().await;
            if config.general.autoreload {
                info!("Automatically reloading config");

                if let Ok(changed) = reload_config(autoreload_client_server_map.clone()).await {
                    if changed {
                        get_config().show()
                    }
                };
            }
        }
    });

    let mut term_signal = unix_signal(SignalKind::terminate()).unwrap();
    let mut interrupt_signal = unix_signal(SignalKind::interrupt()).unwrap();
    let mut sighup_signal = unix_signal(SignalKind::hangup()).unwrap();
    let (shutdown_tx, _) = broadcast::channel::<()>(1);
    let (drain_tx, mut drain_rx) = mpsc::channel::<i32>(2048);
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

                _ = reload_config(client_server_map.clone()).await;

                get_config().show();
            },

            // Initiate graceful shutdown sequence on sig int
            _ = interrupt_signal.recv() => {
                info!("Got SIGINT, waiting for client connection drain now");
                admin_only = true;

                // Broadcast that client tasks need to finish
                let _ = shutdown_tx.send(());
                let exit_tx = exit_tx.clone();
                let _ = drain_tx.send(0).await;

                tokio::task::spawn(async move {
                    let mut interval = tokio::time::interval(tokio::time::Duration::from_millis(config.general.shutdown_timeout));

                    // First tick fires immediately.
                    interval.tick().await;

                    // Second one in the interval time.
                    interval.tick().await;

                    // We're done waiting.
                    error!("Graceful shutdown timed out. {} active clients being closed", total_clients);

                    let _ = exit_tx.send(()).await;
                });
            },

            _ = term_signal.recv() => {
                info!("Got SIGTERM, closing with {} clients active", total_clients);
                break;
            },

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

                tokio::task::spawn(async move {
                    let start = chrono::offset::Utc::now().naive_utc();

                    match client::client_entrypoint(
                        socket,
                        client_server_map,
                        shutdown_rx,
                        drain_tx,
                        admin_only,
                    )
                    .await
                    {
                        Ok(()) => {

                            let duration = chrono::offset::Utc::now().naive_utc() - start;

                            info!(
                                "Client {:?} disconnected, session duration: {}",
                                addr,
                                format_duration(&duration)
                            );
                        }

                        Err(err) => {
                            warn!("Client disconnected with error {:?}", err);
                        }
                    };
                });
            }

            _ = exit_rx.recv() => {
                break;
            }

            client_ping = drain_rx.recv() => {
                let client_ping = client_ping.unwrap();
                total_clients += client_ping;

                if total_clients == 0 && admin_only {
                    let _ = exit_tx.send(()).await;
                }
            }
        }
    }

    info!("Shutting down...");
}
