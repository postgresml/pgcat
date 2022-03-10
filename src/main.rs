// Copyright (c) 2022 Lev Kokotov <lev@levthe.dev>

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
extern crate serde;
extern crate serde_derive;
extern crate sqlparser;
extern crate tokio;
extern crate toml;

use log::{error, info};
use parking_lot::Mutex;
use tokio::net::TcpListener;
use tokio::{
    signal,
    signal::unix::{signal as unix_signal, SignalKind},
    sync::mpsc,
};

use std::collections::HashMap;
use std::sync::Arc;

mod admin;
mod client;
mod config;
mod constants;
mod errors;
mod messages;
mod pool;
mod query_router;
mod server;
mod sharding;
mod stats;

use config::get_config;
use pool::{ClientServerMap, ConnectionPool};
use stats::{Collector, Reporter};

#[tokio::main(worker_threads = 4)]
async fn main() {
    env_logger::init();
    info!("Welcome to PgCat! Meow.");

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
    let (tx, rx) = mpsc::channel(100);

    // Connection pool that allows to query all shards and replicas.
    let mut pool =
        ConnectionPool::from_config(client_server_map.clone(), Reporter::new(tx.clone())).await;

    // Statistics collector task.
    let collector_tx = tx.clone();
    let addresses = pool.databases();
    tokio::task::spawn(async move {
        let mut stats_collector = Collector::new(rx, collector_tx);
        stats_collector.collect(addresses).await;
    });

    // Connect to all servers and validate their versions.
    let server_info = match pool.validate().await {
        Ok(info) => info,
        Err(err) => {
            error!("Could not validate connection pool: {:?}", err);
            return;
        }
    };

    info!("Waiting for clients");

    // Client connection loop.
    tokio::task::spawn(async move {
        loop {
            let pool = pool.clone();
            let client_server_map = client_server_map.clone();
            let server_info = server_info.clone();
            let reporter = Reporter::new(tx.clone());

            let (socket, addr) = match listener.accept().await {
                Ok((socket, addr)) => (socket, addr),
                Err(err) => {
                    error!("{:?}", err);
                    continue;
                }
            };

            // Handle client.
            tokio::task::spawn(async move {
                let start = chrono::offset::Utc::now().naive_utc();
                match client::Client::startup(socket, client_server_map, server_info, reporter)
                    .await
                {
                    Ok(mut client) => {
                        info!("Client {:?} connected", addr);
                        match client.handle(pool).await {
                            Ok(()) => {
                                let duration = chrono::offset::Utc::now().naive_utc() - start;

                                info!(
                                    "Client {:?} disconnected, session duration: {}",
                                    addr,
                                    format_duration(&duration)
                                );
                            }

                            Err(err) => {
                                error!("Client disconnected with error: {:?}", err);
                                client.release();
                            }
                        }
                    }

                    Err(err) => {
                        error!("Client failed to login: {:?}", err);
                    }
                };
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
            match config::parse("pgcat.toml").await {
                Ok(_) => {
                    get_config().show();
                }
                Err(err) => {
                    error!("{:?}", err);
                    return;
                }
            };
        }
    });

    // Exit on Ctrl-C (SIGINT) and SIGTERM.
    let mut term_signal = unix_signal(SignalKind::terminate()).unwrap();

    tokio::select! {
        _ = signal::ctrl_c() => (),
        _ = term_signal.recv() => (),
    };

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
