/// psql client
use crate::messages::startup_message::StartupMessage;
// use crate::messages::{parse, Message};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use crate::communication::{read_all, write_all};
use crate::messages::*;
use crate::messages::authentication_ok::AuthenticationOk;
use crate::messages::ready_for_query::*;
use crate::messages::query::*;
use crate::server::Server;

pub struct Client {
    stream: tokio::net::TcpStream,

    username: Option<String>,
    database: Option<String>,

    buffer: Vec<u8>,

    state: ClientState,
    
    server: Option<Server>,

    // Settings
    client_idle_timeout: f64,
}

#[derive(Debug, PartialEq)]
pub enum ClientState {
    Connecting,
    SslRequest,
    LoginRequest,
    LoggingIn,
    Idle,
    WaitingForServer,
    Active,
    InTransaction,
}

impl Client {
    pub fn new(stream: tokio::net::TcpStream) -> Client {
        Client {
            stream: stream,
            username: None,
            database: None,
            client_idle_timeout: 0.0,
            server: None,
            state: ClientState::Connecting,
            buffer: vec![0u8; 8196],
        }
    }

    pub async fn handle(&mut self) -> Result<(), &'static str> {
        loop {
            println!("DEBUG: client state {:?}", self.state);

            match self.state {
                // Client just connected
                ClientState::Connecting => {
                    let mut n = 0;
                    n = match self.stream.read(&mut self.buffer[n..]).await {
                        Ok(n) => n,
                        Err(_err) => return Err("ERROR: client disconnected during startup"),
                    };

                    let len = i32::from_be_bytes(self.buffer[0..4].try_into().unwrap());

                    // Read what's remaining
                    if n < len as usize {
                        self.stream.read_exact(&mut self.buffer[n..(n - len as usize)]);
                    }

                    let code = i32::from_be_bytes(self.buffer[4..8].try_into().unwrap());

                    match code {
                        80877103 => {
                            self.state = ClientState::SslRequest;
                        },

                        196608 => {
                            self.state = ClientState::LoginRequest;
                        },

                        _ => return Err("ERROR: unsupported startup message"),
                    };
                },

                ClientState::SslRequest => {
                    // No SSL for now
                    let res = vec![b'N'];
                    write_all(&mut self.stream, &res[..]).await?;
                    self.state = ClientState::Connecting;
                },

                ClientState::LoginRequest => {
                    let msg = startup_message::StartupMessage::parse(&self.buffer[8..]).unwrap();
                    self.username = Some(msg.username());
                    self.database = Some(msg.database());
                    self.state = ClientState::LoggingIn;
                },

                ClientState::LoggingIn => {
                    // TODO: Challenge for password
                    // Let the client in and tell it we are ready for queries.
                    let auth_ok: Vec<u8> = AuthenticationOk{}.into();
                    let ready_for_query: Vec<u8> = ready_for_query::ReadyForQuery::new(TransactionStatusIndicator::Idle).into();

                    write_all(&mut self.stream, &auth_ok).await?;
                    write_all(&mut self.stream, &ready_for_query).await?;

                    self.state = ClientState::Idle;
                },

                ClientState::Idle => {
                    read_all(&mut self.stream, &mut self.buffer).await?;
                    let (len, message_name) = parse(&self.buffer)?;

                    match message_name {
                        MessageName::Query => {
                            let query = Query::parse(&self.buffer, len as i32).unwrap();
                            println!("Got query: {:?}", query);
                            self.state = ClientState::WaitingForServer;
                        },

                        MessageName::Termination => return Ok(()),

                        _ => return Err("ERROR: unexpected message while idle"),
                    }
                },

                ClientState::WaitingForServer => {
                    let server = match Server::connect("127.0.0.1:5432", "lev", Some("lev"), Some("lev")).await {
                        Ok(server) => {
                            self.server = Some(server);
                            self.state = ClientState::Active;
                        },
                        Err(err) => return Err("ERROR: could not connect to server"),
                    };
                },

                _ => return Err("ERROR: unexpected message"),
            };
        }

        Ok(())
        // let mut buf = vec![0u8; 1024];
        // let mut n = 0;

        // loop {
        //     n = n + match self.client_idle_timeout {
        //         0.0 => match self.stream.read(&mut buf[n..]).await {
        //             Ok(n) => n,
        //             Err(err) => {
        //                 println!("ERROR: Client closed connection or died: {}", err);
        //                 return;
        //             }
        //         },

        //         // Timeout
        //         _ => {
        //             match tokio::time::timeout(
        //                 tokio::time::Duration::from_millis(
        //                     (self.client_idle_timeout * 1000.0) as u64,
        //                 ),
        //                 self.stream.read(&mut buf[n..]),
        //             )
        //             .await
        //             {
        //                 Ok(res) => match res {
        //                     Ok(n) => n,
        //                     Err(err) => {
        //                         println!("ERROR: Client clsoed connection or died: {}", err);
        //                         return;
        //                     }
        //                 },
        //                 Err(_) => {
        //                     println!("ERROR: Timed out waiting for client");
        //                     return;
        //                 }
        //             }
        //         }
        //     };

        //     match n {
        //         0 => {
        //             println!("INFO: Client closed connection");
        //         }
        //         _ => {
        //             if self.is_message_complete(&buf[0..n]) {
        //                 self.handle_message(&buf[0..n]).await;
        //                 n = 0;
        //             }
        //         }
        //     };
        // }
    }

    // fn is_message_complete(&self, buf: &[u8]) -> bool {
    //     if buf.len() < 5 {
    //         false
    //     } else {
    //         let len = i32::from_be_bytes(buf[1..5].try_into().unwrap());
    //         if buf.len() == (len + 1) as usize {
    //             println!("DEBUG: Packet complete");
    //             true
    //         } else {
    //             println!(
    //                 "DEBUG: Packet incomplete, len: {}, expected: {}",
    //                 len,
    //                 buf.len()
    //             );
    //             false
    //         }
    //     }
    // }

    // async fn handle_query(&self, query: &crate::messages::query::Query) {}

    // async fn handle_message(&self, buf: &[u8]) {
    //     match crate::messages::parse(&buf) {
    //         Some((len, message_name)) => {
    //             match message_name {
    //                 crate::messages::MessageName::Query => {
    //                     let mut server = crate::server::Server::connect(
    //                         "127.0.0.1:5432",
    //                         "lev",
    //                         Some("lev"),
    //                         Some("lev"),
    //                     )
    //                     .await
    //                     .unwrap();

    //                     // Parse / rewrite query here is possible.
    //                     let query = crate::messages::query::Query::parse(&buf, len as i32).unwrap();
    //                     let data: Vec<u8> = query.into();

    //                     println!("after: {:?}\nbefore: {:?}", data, buf);
    //                     server.send(&data).await;

    //                     let mut buf = vec![0u8; 1024];
    //                     let n = server.recv(&mut buf).await;
    //                     println!("Result: {:?}", String::from_utf8_lossy(&buf[0..n]));
    //                 }
    //                 _ => (),
    //             }
    //         }

    //         None => {
    //             println!("ERROR: Unknown message");
    //         }
    //     };

    //     println!("OK DONE");
    // }
}
// 