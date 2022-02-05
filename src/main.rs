extern crate async_trait;
extern crate bb8;
extern crate bytes;
extern crate md5;
extern crate tokio;

use bb8::Pool;
use tokio::net::TcpListener;

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

mod client;
mod errors;
mod messages;
mod pool;
mod server;

type ClientServerMap = Arc<Mutex<HashMap<(i32, i32), (i32, i32)>>>;

#[tokio::main]
async fn main() {
    println!("> Welcome to PgRabbit");

    let listener = match TcpListener::bind("0.0.0.0:5433").await {
        Ok(sock) => sock,
        Err(err) => {
            println!("> Error: {:?}", err);
            return;
        }
    };

    let client_server_map: ClientServerMap = Arc::new(Mutex::new(HashMap::new()));
    let manager = pool::ServerPool::new(
        "127.0.0.1",
        "5432",
        "lev",
        "lev",
        "lev",
        client_server_map.clone(),
    );
    let pool = Pool::builder().max_size(15).build(manager).await.unwrap();

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

            let pool = pool.clone();
            let client_server_map = client_server_map.clone();

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
