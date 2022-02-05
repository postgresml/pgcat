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

// Support for query cancellation: this maps our process_ids and
// secret keys to the backend's.
use config::{Address, User};
use pool::{ClientServerMap, ReplicaPool};

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

    // Note in the logs that it will fetch two connections!
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

    let replica_pool = ReplicaPool::new(addresses, user, "lev", client_server_map.clone()).await;

    loop {
        let client_server_map = client_server_map.clone();
        let replica_pool = replica_pool.clone();

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

                    match client.handle(replica_pool).await {
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
