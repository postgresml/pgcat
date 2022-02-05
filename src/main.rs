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

use bb8::Pool;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

mod client;
mod config;
mod errors;
mod messages;
mod pool;
mod server;

// Support for query cancellation: this maps our process_ids and
// secret keys to the backend's.
use config::{Address, User};
use pool::{ClientServerMap, ReplicaPool, ServerPool};

//
// Poor man's config
//
const POOL_SIZE: u32 = 15;

/// Main!
#[tokio::main]
async fn main() {
    println!("> Welcome to PgCat! Meow.");

    let addr = "0.0.0.0:5433";
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
    let num_addresses = addresses.len() as u32;

    let user = User {
        name: "lev".to_string(),
        password: "lev".to_string(),
    };

    let database = "lev";

    let replica_pool = ReplicaPool::new(addresses).await;
    let manager = ServerPool::new(replica_pool, user, database, client_server_map.clone());

    // We are round-robining, so ideally the replicas will be equally loaded.
    // Therefore, we are allocating number of replicas * pool size of connections.
    // However, if a replica dies, the remaining replicas will share the burden,
    // also equally.
    //
    // Note that failover in this case could bring down the remaining replicas, so
    // in certain situations, e.g. when replicas are running hot already, failover
    // is not at all desirable!!
    let pool = Pool::builder()
        .max_size(POOL_SIZE * num_addresses)
        .build(manager)
        .await
        .unwrap();

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
