extern crate bytes;
extern crate md5;
extern crate tokio;

use tokio::net::TcpListener;

mod client;
mod errors;
mod messages;
mod server;

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

    loop {
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

            match client::Client::startup(socket).await {
                Ok(mut client) => {
                    println!(">> Client {:?} authenticated successfully!", addr);
                    let server =
                        match server::Server::startup("127.0.0.1", "5432", "lev", "lev", "lev")
                            .await
                        {
                            Ok(server) => server,
                            Err(_) => return,
                        };

                    match client.handle(server).await {
                        Ok(()) => {
                            println!(">> Client {:?} disconnected.", addr);
                        }

                        Err(err) => {
                            println!(">> Client disconnected with error: {:?}", err);
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
