/// psql client
use crate::messages::startup_message::StartupMessage;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use crate::messages::Message;

pub struct Client {
    stream: tokio::net::TcpStream,
    username: String,
    database: String,

    // Settings
    client_idle_timeout: f64,
}

impl Client {
    pub fn new(stream: tokio::net::TcpStream, startup_message: &StartupMessage) -> Client {
        Client {
            stream: stream,
            username: startup_message.username(),
            database: startup_message.database(),
            client_idle_timeout: 0.0,
        }
    }

    pub async fn handle(&mut self) {
        let mut buf = vec![0u8; 1024];
        let mut n = 0;

        loop {
            n = n + match self.client_idle_timeout {
                0.0 => {
                    match self.stream.read(&mut buf[n..]).await {
                        Ok(n) => n,
                        Err(err) => {
                            println!("ERROR: Client closed connection or died: {}", err);
                            return;
                        }
                    }
                },

                // Timeout
                _ => {
                    match tokio::time::timeout(
                        tokio::time::Duration::from_millis((self.client_idle_timeout * 1000.0) as u64),
                        self.stream.read(&mut buf[n..])
                    ).await {
                        Ok(res) => match res {
                            Ok(n) => n,
                            Err(err) => {
                                println!("ERROR: Client clsoed connection or died: {}", err);
                                return;
                            },
                        },
                        Err(_) => {
                            println!("ERROR: Timed out waiting for client");
                            return;
                        },
                    }
                }
            };

            match n {
                0 => {
                    println!("INFO: Client closed connection");
                },
                _ => {
                    if self.is_message_complete(&buf[0..n]) {
                        self.handle_message(&buf[0..n]).await;
                        n = 0;
                    }
                },
            };
        }
    }

    fn is_message_complete(&self, buf: &[u8]) -> bool {
        if buf.len() < 5 {
            false
        }

        else {
            let len = i32::from_be_bytes(buf[1..5].try_into().unwrap());
            if buf.len() == (len + 1) as usize {
                println!("DEBUG: Packet complete");
                true
            }

            else {
                println!("DEBUG: Packet incomplete, len: {}, expected: {}", len, buf.len());
                false
            }
        }
    }

    async fn handle_query(&self, query: &crate::messages::query::Query) {

    }

    async fn handle_message(&self, buf: &[u8]) {

        match crate::messages::parse(&buf) {
            Some((len, message_name)) => {
                match message_name {
                    crate::messages::MessageName::Query => {
                        let mut server = crate::server::Server::connect("127.0.0.1:5432", "lev", Some("lev"), Some("lev")).await.unwrap();

                        // Parse / rewrite query here is possible.
                        let query = crate::messages::query::Query::parse(&buf, len as i32).unwrap();
                        let data: Vec<u8> = query.into();

                        println!("after: {:?}\nbefore: {:?}", data, buf);
                        server.send(&data).await;

                        let mut buf = vec![0u8; 1024];
                        let n = server.stream.read(&mut buf).await.unwrap();
                        println!("Result: {:?}", String::from_utf8_lossy(&buf[0..n]));
                    },
                    _ => (),
                }
            },

            None => {
                println!("ERROR: Unknown message");
            }
        };

        println!("OK DONE");

        // let (m, t) = crate::messages::parse(&buf);

        // match crate::messages::parse(&buf) {
        //     Some((m, t)) => async move {
        //         match t {
        //             crate::messages::MessageName::Query => {
        //                 let mut server = crate::server::Server::new("127.0.0.1:5432", "lev", Some("lev"), Some("lev")).await.unwrap();
        //                 let query = m.to_vec();
        //                 server.stream.write(&query).await;
        //                 let mut buf = vec![0u8; 1024];
        //                 let n = server.stream.read(&mut buf).await.unwrap();
        //                 println!("Result: {:?}", String::from_utf8_lossy(&buf[0..n]));
        //             },
        //             _ => (),
        //         };
        //     },

        //     None => (),
        // };

        // let c = buf[0] as char;

        // match c {
        //     // Client is sending a query
        //     'Q' => {
        //         // TODO: find backend server and send it the query
        //         let query = String::from_utf8_lossy(&buf[5..]);
        //         println!("{}", query);

        //         let mut server = crate::server::Server::new("127.0.0.1:5432", "lev", Some("lev"), Some("lev")).await.unwrap();
        //         server.stream.write(&buf).await;
        //         let mut buf = vec![0u8; 1024];
        //         let n = server.stream.read(&mut buf).await.unwrap();
        //         println!("Result: {:?}", String::from_utf8_lossy(&buf[0..n]));
        //     },

        //     _ => {
        //         println!("Unknown packet: {}", c);
        //     }
        // }
    }
}