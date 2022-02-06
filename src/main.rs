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
extern crate tokio;

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

    let addr = "0.0.0.0:6432";
    let listener = match TcpListener::bind(addr).await {
        Ok(sock) => sock,
        Err(err) => {
            println!("> Error: {:?}", err);
            return;
        }
    };

    println!("> Running on {}", addr);

    let client_server_map: ClientServerMap = Arc::new(Mutex::new(HashMap::new()));

    // Replica pool.
    let addresses = vec![
        Address {
            host: "127.0.0.1".to_string(),
            port: "5432".to_string(),
        },
        Address {
            host: "localhost".to_string(),
            port: "5432".to_string(),
        },
    ];

    let user = User {
        name: "lev".to_string(),
        password: "lev".to_string(),
    };

    let database = "lev";

    let pool = ConnectionPool::new(addresses, user, database, client_server_map.clone()).await;

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
            println!(">> Client {:?} connected.", addr);

            match client::Client::startup(socket, client_server_map).await {
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
