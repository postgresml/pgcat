use tokio::io::{AsyncReadExt, AsyncWriteExt};

mod client;
mod communication;
mod messages;
mod server;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let sock = tokio::net::TcpListener::bind("0.0.0.0:5433").await?;

    loop {
        let (mut stream, addr) = sock.accept().await?;
        tokio::task::spawn(async move {
            let mut client = client::Client::new(stream);
            match client.handle().await {
                Ok(()) => println!("INFO: Client {} disconnected", addr),
                Err(err) => println!("ERROR: {}", err),
            };
        });
    }

    Ok(())

    // loop {
    //     let (mut stream, addr) = sock.accept().await?;

    //     tokio::task::spawn(async move {
    //         println!("INFO: Connection from {}", addr);
    //         let mut buf = vec![0u8; 1024];

    //         loop {
    //             let n = match stream.read(&mut buf).await {
    //                 Ok(n) => {
    //                     println!("DEBUG: Read {} bytes, {:?}", n, &buf[0..n]);
    //                     n
    //                 }
    //                 Err(err) => {
    //                     println!("Error {}, closing", err);
    //                     0
    //                 }
    //             };

    //             if n == 0 {
    //                 return;
    //             }

    //             let message = Message::parse(&buf[0..n]);

    //             let _ = match message {
    //                 None => {
    //                     println!("Unknown message");
    //                 }
    //                 Some(msg) => {
    //                     match msg.name {
    //                         MessageName::SslRequest => {
    //                             println!("SSL request");
    //                             let r = SslRequest {};
    //                             r.handle(&mut stream).await;
    //                         }

    //                         MessageName::StartupMessage => {
    //                             let r = StartupMessage::parse(&msg.payload.unwrap()).unwrap();
    //                             let ok: Vec<u8> = AuthenticationOk {}.into();
    //                             let rfq: Vec<u8> =
    //                                 ReadyForQuery::new(ccccccccccc).into();
    //                             stream.write(&ok).await;
    //                             stream.write(&rfq).await;
    //                             let mut client = client::Client::new(stream, &r);
    //                             client.handle().await;
    //                             return;
    //                             // r.handle(&mut stream).await;
    //                         }

    //                         MessageName::Termination => {
    //                             // Return backend connection into pool
    //                             println!("Client closed: {:?}", stream);
    //                             break;
    //                         }

    //                         _ => (),
    //                     }
    //                 }
    //             };
    //         }

            // let mbuf = bytes::Bytes::from(buf);

            // if mbuf.len() < 5 {
            //     println!("Message cannot be shorter than 5 bytes");
            //     return;
            // }

            // // Process startup message
            // let len = vec![0u8; 4];

            // let len = i32::from_be_bytes(mbuf[0..4].try_into().unwrap());
            // let protocol = i32::from_be_bytes(mbuf[4..8].try_into().unwrap());
            // println!("Len: {}, protocol: {}", len, protocol);

            // let mut sbuf = Vec::with_capacity(n);

            // for c in &mbuf[8..n] {
            //     if *c != 0 {
            //         sbuf.push(*c);
            //     }
            //     else {
            //         println!("{:?}", bytes::Bytes::from(sbuf.clone()));
            //         sbuf.clear()
            //     }
            // }
        // })
        // .await?
    // }
}
