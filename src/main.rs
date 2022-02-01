#![allow(dead_code)]

mod client;
mod communication;
mod error;
mod messages;
mod server;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let sock = tokio::net::TcpListener::bind("0.0.0.0:5433").await?;

    loop {
        let (mut stream, addr) = sock.accept().await?;
        tokio::task::spawn(async move {
            let mut client = client::Client::new(stream).await;
            match client.handle().await {
                Ok(()) => println!("INFO: Client {} disconnected", addr),
                Err(err) => println!("ERROR: {}", err),
            };
        });
    }
}
