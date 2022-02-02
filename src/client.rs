/// psql client
use crate::messages::startup_message::*;
use crate::messages::{parse, MessageName};
use crate::communication::{write_all, read_messages, read_message, Protocol};
use crate::messages::authentication_ok::*;
use crate::messages::query::*;
use crate::messages::ready_for_query::*;
use crate::messages::*;
use crate::server::Server;
use crate::error::{Error, ErrorCode};
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::Mutex;


use bytes::Buf;

pub struct Client {
    stream: tokio::io::BufReader<tokio::net::TcpStream>,

    username: Option<String>,
    database: Option<String>,

    buffer: bytes::BytesMut,
    state: ClientState,

    server: Server,
    protocol: Protocol,

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
    ShuttingDown,
    Disconnected,
}

impl Client {
    pub async fn new(stream: tokio::net::TcpStream) -> Client {
        let mut server = Server::connect("127.0.0.1:5432", "lev", "lev", "lev")
            .await
            .unwrap();
        server.handle().await.unwrap();
        Client {
            stream: tokio::io::BufReader::new(stream),
            username: None,
            database: None,
            client_idle_timeout: 0.0,
            server: server,
            protocol: Protocol::Simple,
            buffer: bytes::BytesMut::with_capacity(8196),
            state: ClientState::Connecting,
        }
    }

    pub async fn handle(&mut self) -> Result<(), &'static str> {
        loop {
            // println!("Client state: {:?}", self.state);

            match self.state {
                // Client just connected
                ClientState::Connecting => {
                    let mut n = 0;
                    n = match self.stream.read_buf(&mut self.buffer).await {
                        Ok(n) => n,
                        Err(_err) => return Err("ERROR: client disconnected during startup"),
                    };

                    let _len = self.buffer.get_i32();

                    // Read what's remaining
                    // if n < len as usize {
                    //     self.stream.read_exact(&mut self.buffer[n..(n - len as usize)]);
                    // }

                    let code = self.buffer.get_i32();

                    match code {
                        80877103 => {
                            self.state = ClientState::SslRequest;
                        }

                        196608 => {
                            self.state = ClientState::LoginRequest;
                        }

                        _ => return Err("ERROR: unsupported startup message"),
                    };
                }

                ClientState::SslRequest => {
                    // No SSL for now
                    let res = vec![b'N'];
                    write_all(&mut self.stream, &res[..]).await?;
                    self.state = ClientState::Connecting;
                }

                ClientState::LoginRequest => {
                    let msg = StartupMessage::parse(&self.buffer).unwrap();
                    self.username = Some(msg.username());
                    self.database = Some(msg.database());
                    self.state = ClientState::LoggingIn;
                    self.buffer.clear();
                }

                ClientState::LoggingIn => {
                    // TODO: Challenge for password
                    // Let the client in and tell it we are ready for queries.
                    let auth_ok: Vec<u8> = AuthenticationOk {}.into();
                    let ready_for_query: Vec<u8> =
                        ReadyForQuery::new(TransactionStatusIndicator::Idle).into();

                    write_all(&mut self.stream, &auth_ok).await?;
                    write_all(&mut self.stream, &ready_for_query).await?;

                    self.state = ClientState::Idle;
                }

                ClientState::Idle => {
                    // Read some messages pending
                    // match read_messages(&mut self.stream, &mut self.buffer, self.protocol).await {
                    //     Ok(()) => (),
                    //     Err(err) => {
                    //         match err.code {
                    //             ErrorCode::ClientDisconnected => {
                    //                 self.state = ClientState::Disconnected;
                    //                 return Ok(());
                    //             },

                    //             _ => {
                    //                 println!("DEBUG: Client error {:?}", err.code);
                    //                 return Err("Client error");
                    //             }
                    //         }
                    //     }
                    // }
                    // // let n = match self.stream.read_buf(&mut self.buffer).await {
                    // //     Ok(n) => n,
                    // //     Err(_err) => 0,
                    // // };

                    // // if n == 0 {
                    // //     self.state = ClientState::Disconnected;
                    // //     return Ok(());
                    // // }

                    // // if self.buffer.len() < 6 {
                    // //     continue;
                    // // }

                    // // read_all(&mut self.stream, &mut self.buffer).await?;
                    // let (len, message_name) = parse(&self.buffer)?;

                    // println!("Client message: {:?}", message_name);

                    // match message_name {
                    //     MessageName::Query => {
                    //         // Buffer the whole query if it's not here yet
                    //         // if len + 1 > self.buffer.len() {
                    //         //     continue;
                    //         // }

                    //         self.state = ClientState::WaitingForServer;
                    //         self.protocol = Protocol::Simple;
                    //     },

                    //     MessageName::Prepare => {
                    //         self.state = ClientState::WaitingForServer;
                    //         self.protocol = Protocol::Extended;
                    //     },

                    //     MessageName::Bind => {
                    //         self.state = ClientState::WaitingForServer;
                    //     },

                    //     MessageName::Execute => {
                    //         self.state = ClientState::WaitingForServer;
                    //     },

                    //     MessageName::Sync => {
                    //         self.state = ClientState::WaitingForServer;
                    //     },

                    //     MessageName::Termination => return Ok(()),

                    //     // Move offset in case we want to read more messages later
                    //     // self.offset += len;
                    //     _ => return Err("ERROR: unexpected message while idle"),
                    // }
                    match read_message(&mut self.stream, &mut self.buffer).await {
                        Ok(()) => {
                            self.state = ClientState::WaitingForServer;
                        },
                        Err(_err) => {
                            self.state = ClientState::Disconnected;
                        }
                    }
                }

                ClientState::WaitingForServer => {
                    // TODO: handle multiple statements
                    // self.state = ClientState::Active;
                    // println!("Buf: {:?}", self.buffer);
                    let (len, message_name) = parse(&self.buffer)?;

                    match message_name {
                        MessageName::Query => {
                            self.protocol = Protocol::Simple;
                            self.state = ClientState::Active;
                        },

                        MessageName::Prepare => {
                            self.protocol = Protocol::Extended;
                            self.state = ClientState::Idle;
                        },

                        MessageName::Execute => {
                            self.state = ClientState::Active;
                        },

                        MessageName::Sync => {
                            self.state = ClientState::Active;
                        },

                        MessageName::ParameterStatus => {
                            self.state = ClientState::Active;
                        },

                        _ => {
                            self.state = ClientState::Idle; // More messages to follow
                        },
                    };
                    // println!("client buf: {:?}", self.buffer);
                    self.server.forward(&mut self.buffer, self.protocol, len + 1).await?;
                    // println!("client buf: {:?}", self.buffer);
                    // self.buffer.clear();
                }

                ClientState::Active => {
                    let done = self.server.receive(&mut self.buffer).await?;

                    while self.buffer.has_remaining() {
                        self.stream.write_buf(&mut self.buffer).await;
                    }
                    self.buffer.clear();

                    // tokio::time::sleep(tokio::time::Duration::from_millis(1000)).await;

                    if done {
                        self.state = ClientState::Idle;
                    }
                },

                ClientState::Disconnected => {
                    return Ok(());
                },

                _ => return Err("ERROR: unexpected message"),
            };
        }
    }
}
