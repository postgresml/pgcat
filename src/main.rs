// PgCat, a PostgreSQL pooler with load balancing, failover, and sharding support.
// Copyright (C) 2022  Lev Kokotov <lev@levthe.dev>

// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.

// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.

// You should have received a copy of the GNU General Public License
// along with this program.  If not, see <http://www.gnu.org/licenses/>.
extern crate arc_swap;
extern crate async_trait;
extern crate bb8;
extern crate bytes;
extern crate log;
extern crate md5;
extern crate num_cpus;
extern crate once_cell;
extern crate serde;
extern crate serde_derive;
extern crate sqlparser;
extern crate statsd;
extern crate tokio;
extern crate toml;

use tokio::net::TcpListener;
use tokio::{
    signal,
    signal::unix::{signal as unix_signal, SignalKind},
};

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use tokio::sync::mpsc;

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

// Support for query cancellation: this maps our process_ids and
// secret keys to the backend's.
use config::get_config;
use pool::{ClientServerMap, ConnectionPool};
use stats::{Collector, Reporter};

/// Main!
#[tokio::main(worker_threads = 4)]
async fn main() {
    println!("> Welcome to PgCat! Meow.");

    // Prepare regexes
    if !query_router::QueryRouter::setup() {
        println!("> Could not setup query router.");
        return;
    }

    // Prepare the config
    match config::parse("pgcat.toml").await {
        Ok(_) => (),
        Err(err) => {
            println!("> Config parse error: {:?}", err);
            return;
        }
    };

    let config = get_config();

    let addr = format!("{}:{}", config.general.host, config.general.port);
    let listener = match TcpListener::bind(&addr).await {
        Ok(sock) => sock,
        Err(err) => {
            println!("> Error: {:?}", err);
            return;
        }
    };

    println!("> Running on {}", addr);
    config.show();

    // Tracks which client is connected to which server for query cancellation.
    let client_server_map: ClientServerMap = Arc::new(Mutex::new(HashMap::new()));

    // Collect statistics and send them to StatsD
    let (tx, rx) = mpsc::channel(100);

    tokio::task::spawn(async move {
        println!("> Statistics reporter started");

        let mut stats_collector = Collector::new(rx);
        stats_collector.collect().await;
    });

    let mut pool =
        ConnectionPool::from_config(client_server_map.clone(), Reporter::new(tx.clone())).await;

    let server_info = match pool.validate().await {
        Ok(info) => info,
        Err(err) => {
            println!("> Could not validate connection pool: {:?}", err);
            return;
        }
    };

    println!("> Waiting for clients...");

    // Main app runs here.
    tokio::task::spawn(async move {
        loop {
            let pool = pool.clone();
            let client_server_map = client_server_map.clone();
            let server_info = server_info.clone();
            let reporter = Reporter::new(tx.clone());

            let (socket, addr) = match listener.accept().await {
                Ok((socket, addr)) => (socket, addr),
                Err(err) => {
                    println!("> Listener: {:?}", err);
                    continue;
                }
            };

            // Client goes to another thread, bye.
            tokio::task::spawn(async move {
                let start = chrono::offset::Utc::now().naive_utc();

                println!(">> Client {:?} connected", addr);

                match client::Client::startup(socket, client_server_map, server_info, reporter)
                    .await
                {
                    Ok(mut client) => {
                        println!(">> Client {:?} authenticated successfully!", addr);

                        match client.handle(pool).await {
                            Ok(()) => {
                                let duration = chrono::offset::Utc::now().naive_utc() - start;

                                println!(
                                    ">> Client {:?} disconnected, session duration: {}",
                                    addr,
                                    format_duration(&duration)
                                );
                            }

                            Err(err) => {
                                println!(">> Client disconnected with error: {:?}", err);
                                client.release();
                            }
                        }
                    }

                    Err(err) => {
                        println!(">> Error: {:?}", err);
                    }
                };
            });
        }
    });

    // Reload config
    // kill -SIGHUP $(pgrep pgcat)
    tokio::task::spawn(async move {
        let mut stream = unix_signal(SignalKind::hangup()).unwrap();

        loop {
            stream.recv().await;
            println!("> Reloading config");
            match config::parse("pgcat.toml").await {
                Ok(_) => {
                    get_config().show();
                }
                Err(err) => {
                    println!("> Config parse error: {:?}", err);
                    return;
                }
            };
        }
    });

    // Setup shut down sequence
    match signal::ctrl_c().await {
        Ok(()) => {
            println!("> Shutting down...");
        }

        Err(err) => {
            eprintln!("Unable to listen for shutdown signal: {}", err);
        }
    };
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
