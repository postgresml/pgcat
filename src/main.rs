//
// Copyright 2022 Lev Kokotov <lev@levthe.dev>
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
extern crate async_trait;
extern crate bb8;
extern crate bytes;
extern crate md5;
extern crate serde;
extern crate serde_derive;
extern crate tokio;
extern crate toml;

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
use config::{Address, User};
use pool::{ClientServerMap, ConnectionPool};

/// Main!
#[tokio::main]
async fn main() {
    println!("> Welcome to PgCat! Meow.");

    let config = match config::parse("pgcat.toml").await {
        Ok(config) => config,
        Err(err) => {
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

    let pool = ConnectionPool::from_config(config.clone(), client_server_map.clone()).await;
    let transaction_mode = config.general.pool_mode == "transaction";

    println!("> Waiting for clients...");

    loop {
        let pool = pool.clone();
        let client_server_map = client_server_map.clone();

        let (socket, addr) = match listener.accept().await {
            Ok((socket, addr)) => (socket, addr),
            Err(err) => {
                println!("> Listener: {:?}", err);
                continue;
            }
        };

        // Client goes to another thread, bye.
        tokio::task::spawn(async move {
            println!(
                ">> Client {:?} connected, transaction pooling: {}",
                addr, transaction_mode
            );

            match client::Client::startup(socket, client_server_map, transaction_mode).await {
                Ok(mut client) => {
                    println!(">> Client {:?} authenticated successfully!", addr);

                    match client.handle(pool).await {
                        Ok(()) => {
                            println!(">> Client {:?} disconnected.", addr);
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
