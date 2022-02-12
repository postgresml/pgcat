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
extern crate async_trait;
extern crate bb8;
extern crate bytes;
extern crate md5;
extern crate num_cpus;
extern crate once_cell;
extern crate serde;
extern crate serde_derive;
extern crate tokio;
extern crate toml;

use regex::Regex;
use tokio::net::TcpListener;

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

mod client;
mod config;
mod errors;
mod messages;
mod pool;
mod server;
mod sharding;

// Support for query cancellation: this maps our process_ids and
// secret keys to the backend's.
use config::Role;
use pool::{ClientServerMap, ConnectionPool};

/// Main!
#[tokio::main(worker_threads = 4)]
async fn main() {
    println!("> Welcome to PgCat! Meow.");

    client::SHARDING_REGEX_RE
        .set(Regex::new(client::SHARDING_REGEX).unwrap())
        .unwrap();
    client::ROLE_REGEX_RE
        .set(Regex::new(client::ROLE_REGEX).unwrap())
        .unwrap();

    let config = match config::parse("pgcat.toml").await {
        Ok(config) => config,
        Err(err) => {
            println!("> Config parse error: {:?}", err);
            return;
        }
    };

    let addr = format!("{}:{}", config.general.host, config.general.port);
    let listener = match TcpListener::bind(&addr).await {
        Ok(sock) => sock,
        Err(err) => {
            println!("> Error: {:?}", err);
            return;
        }
    };

    println!("> Running on {}", addr);

    // Tracks which client is connected to which server for query cancellation.
    let client_server_map: ClientServerMap = Arc::new(Mutex::new(HashMap::new()));

    println!("> Pool size: {}", config.general.pool_size);
    println!("> Pool mode: {}", config.general.pool_mode);
    println!("> Ban time: {}s", config.general.ban_time);
    println!(
        "> Healthcheck timeout: {}ms",
        config.general.healthcheck_timeout
    );
    println!("> Connection timeout: {}ms", config.general.connect_timeout);

    let mut pool = ConnectionPool::from_config(config.clone(), client_server_map.clone()).await;
    let transaction_mode = config.general.pool_mode == "transaction";
    let default_server_role = match config.query_router.default_role.as_ref() {
        "any" => None,
        "primary" => Some(Role::Primary),
        "replica" => Some(Role::Replica),
        _ => {
            println!("> Config error, got unexpected query_router.default_role.");
            return;
        }
    };

    let server_info = match pool.validate().await {
        Ok(info) => info,
        Err(err) => {
            println!("> Could not validate connection pool: {:?}", err);
            return;
        }
    };

    println!("> Waiting for clients...");

    loop {
        let pool = pool.clone();
        let client_server_map = client_server_map.clone();
        let server_info = server_info.clone();

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

            match client::Client::startup(
                socket,
                client_server_map,
                transaction_mode,
                default_server_role,
                server_info,
            )
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
