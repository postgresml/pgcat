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
use std::sync::{Arc};
use tokio::sync::Mutex;

use bytes::{BufMut, Buf};
use std::io::BufRead;

pub struct Client {
    stream: tokio::net::TcpStream,

    username: Option<String>,
    database: Option<String>,

    buffer: bytes::BytesMut,
    offset: usize,
    state: ClientState,
    
    server: Option<Arc<Mutex<Server>>>,

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
            buffer: bytes::BytesMut::with_capacity(8196),
            offset: 0,
        }
    }

    pub async fn handle(&mut self) -> Result<(), &'static str> {
        loop {
            println!("DEBUG: client state {:?}", self.state);

            match self.state {
                // Client just connected
                ClientState::Connecting => {
                    let mut n = 0;
                    n = match self.stream.read_buf(&mut self.buffer).await {
                        Ok(n) => n,
                        Err(_err) => return Err("ERROR: client disconnected during startup"),
                    };

                    let len = self.buffer.get_i32();

                    // Read what's remaining
                    // if n < len as usize {
                    //     self.stream.read_exact(&mut self.buffer[n..(n - len as usize)]);
                    // }

                    let code = self.buffer.get_i32();

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
                    let msg = startup_message::StartupMessage::parse(&self.buffer).unwrap();
                    self.username = Some(msg.username());
                    self.database = Some(msg.database());
                    self.state = ClientState::LoggingIn;
                    self.buffer.clear();
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
                    // Reset buffer offset
                    self.offset = 0;

                    // Read all messages pending
                    self.stream.read_buf(&mut self.buffer).await;
                    // read_all(&mut self.stream, &mut self.buffer).await?;
                    let (len, message_name) = parse(&self.buffer)?;

                    match message_name {
                        MessageName::Query => {
                            let _query = Query::parse(&self.buffer, len as i32).unwrap();
                            self.state = ClientState::WaitingForServer;
                        },

                        MessageName::Termination => return Ok(()),

                        // Move offset in case we want to read more messages later
                        // self.offset += len;

                        _ => return Err("ERROR: unexpected message while idle"),
                    }
                },

                ClientState::WaitingForServer => {
                    let server = match Server::connect("127.0.0.1:5432", "lev", "lev", "lev").await {
                        Ok(server) => {
                            self.server = Some(Arc::new(Mutex::new(server)));

                            // Perform the connection authentication
                            let mut server = self.server.as_ref().unwrap().lock().await;

                            server.handle().await?;

                            // TODO: handle multiple statements
                            self.state = ClientState::Active;

                            server.forward(&self.buffer).await?;
                            self.buffer.clear();
                        },
                        Err(err) => return Err("ERROR: could not connect to server"),
                    };
                },

                ClientState::Active => {
                    let mut server = self.server.as_ref().unwrap().lock().await;
                    server.receive(&mut self.buffer).await?;
                    self.stream.write_buf(&mut self.buffer).await;
                    self.buffer.clear();
                    self.state = ClientState::Idle;
                },

                _ => return Err("ERROR: unexpected message"),
            };
        }
    }
}