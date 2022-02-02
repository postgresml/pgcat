/// The PostgreSQL backend.
use async_trait::async_trait;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

use crate::messages::authentication_md5_password::*;
use crate::messages::authentication_ok::*;
use crate::messages::backend_key_data::*;
use crate::messages::error_response::*;
use crate::messages::parameter_status::*;
use crate::messages::password_message::*;
use crate::messages::ready_for_query::*;
use crate::messages::startup_message::*;
use crate::messages::terminate::*;
use crate::communication::{Protocol, read_messages, read_message};

use crate::messages::{parse, Message, MessageName};
use bytes::{Buf, BufMut};

#[derive(Debug, PartialEq)]
pub enum ServerState {
    Connecting,
    WaitingForChallenge,
    LoginSuccessful,
    LoginRequest,
    Idle,
    Active,
    InTransaction,
    WaitingForBackend,
    DataReady,
    Error,
    Terminatation,
}

pub struct Server {
    host: String,
    username: String,
    password: String,
    database: String,
    stream: tokio::io::BufReader<tokio::net::TcpStream>,
    state: ServerState,
    buffer: bytes::BytesMut,
    protocol: Protocol,
    data_available: usize,
}

#[derive(Debug)]
pub enum Error {
    BadServer,
    UsernamePasswordIncorrect,
}

impl Server {
    pub async fn connect(
        host: &str,
        username: &str,
        password: &str,
        database: &str,
    ) -> Result<Server, &'static str> {
        // TCP connection
        let (read, write) = match tokio::net::TcpStream::connect(host).await {
            Ok(stream) => (tokio::io::BufReader::new(stream), tokio::io::BufWriter::new(stream)),
            Err(err) => return Err("ERROR: cannot connect to server"),
        };

        Ok(Server {
            host: host.to_string(),
            username: username.to_string(),
            password: password.to_string(),
            database: database.to_string(),
            stream: read,
            state: ServerState::Connecting,
            buffer: bytes::BytesMut::with_capacity(8196),
            protocol: Protocol::Simple,
            data_available: 0,
        })
    }

    pub async fn handle(&mut self) -> Result<(), &'static str> {
        loop {
            // println!("Server state: {:?}", self.state);
            match self.state {
                ServerState::Connecting => {
                    let startup: Vec<u8> =
                        StartupMessage::new(&self.username, &self.database).into();
                    match self.stream.write_all(&startup).await {
                        Ok(_) => (),
                        Err(_err) => return Err("ERROR: server socket died"),
                    };

                    self.state = ServerState::WaitingForChallenge;
                }

                ServerState::WaitingForChallenge => {
                    read_message(&mut self.stream, &mut self.buffer).await.unwrap(); // Panic for now

                    let (len, message_name) = parse(&mut self.buffer)?;

                    match message_name {
                        MessageName::AuthenticationOk => {
                            AuthenticationOk::parse(&mut self.buffer, len as i32);
                        }

                        MessageName::AuthenticationMD5Password => {
                            let challenge =
                                AuthenticationMD5Password::parse(&mut self.buffer, len as i32)
                                    .unwrap();
                            let password_message: Vec<u8> = PasswordMessage::new(
                                &self.username,
                                &self.password,
                                &challenge.salt,
                            )
                            .into();

                            self.stream.write_all(&password_message).await;
                            self.buffer.clear();
                        }

                        MessageName::ErrorResponse => {
                            let _error =
                                ErrorResponse::parse(&mut self.buffer, len as i32).unwrap();
                            self.state = ServerState::Terminatation;
                        }

                        MessageName::ParameterStatus => {
                            ParameterStatus::parse(&mut self.buffer, len as i32);
                        }

                        MessageName::ReadyForQuery => {
                            ReadyForQuery::parse(&mut self.buffer, len as i32);
                            self.state = ServerState::LoginSuccessful;
                        }

                        MessageName::BackendKeyData => {
                            BackendKeyData::parse(&mut self.buffer, len as i32);
                        }

                        _ => return Err("ERROR: bad message from server"),
                    }
                }

                ServerState::LoginSuccessful => {
                    self.state = ServerState::Idle;
                    break;
                }

                ServerState::Active => {
                    // Data in buffer?
                    if self.buffer.has_remaining() {
                        while self.buffer.has_remaining() {
                            self.stream.write_buf(&mut self.buffer).await;
                        }

                        // Server will acklnowledge every query sent
                        // with a result or an ok message.
                        self.state = ServerState::WaitingForBackend;
                    } else {
                        self.state = ServerState::Idle;
                    }
                }

                ServerState::WaitingForBackend => {
                    // read_messages(&mut self.stream, &mut self.buffer, self.protocol).await;
                    read_message(&mut self.stream, &mut self.buffer).await.unwrap();

                    let (len, message_name) = parse(&mut self.buffer)?;
                    self.data_available += len + 1;

                    // println!("Backend msg: {:?}", message_name);

                    match message_name {
                        MessageName::ReadyForQuery => {
                            self.state = ServerState::Idle;
                        },

                        MessageName::CommandComplete => {
                            if self.protocol == Protocol::Extended {
                                self.state = ServerState::DataReady;
                            }

                            else {
                                self.state = ServerState::DataReady;
                            }
                        },

                        _ => {
                            self.state = ServerState::DataReady;
                        },
                    };
                }

                ServerState::Error => {
                    break;
                }

                ServerState::Terminatation => {
                    let terminate: Vec<u8> = Terminate {}.into();
                    self.buffer.put_slice(&terminate);
                    self.state = ServerState::Error;
                }

                ServerState::DataReady => {
                    break;
                }

                ServerState::Idle => {
                    break;
                }

                _ => (),
            };
        }

        Ok(())
    }

    pub async fn forward(&mut self, buf: &mut bytes::BytesMut, protocol: Protocol, len: usize) -> Result<(), &'static str> {
        match self.state {
            ServerState::Idle => (),
            ServerState::Active => (),
            ServerState::InTransaction => (),
            ServerState::Error => {
                self.state = ServerState::DataReady;
                return Ok(());
            }
            _ => {
                return Err("ERROR: client and server out of sync");
            }
        };

        self.protocol = protocol;

        for _ in 0..len {
            self.buffer.put_u8(buf.get_u8());
        }

        self.state = ServerState::Active;

        Ok(())
    }

    pub async fn receive(&mut self, buf: &mut bytes::BytesMut) -> Result<bool, &'static str> {
        self.handle().await?;

        // Two acceptable states to return to the client.
        let mut done = false;
        match self.state {
            ServerState::DataReady => (),
            ServerState::Idle => { done = true; },
            _ => return Err("ERROR: server and client out of sync with bad state"),
        };

        for _ in 0..self.data_available {
            buf.put_u8(self.buffer.get_u8());
        }

        self.data_available = 0;

        if !done {
            self.state = ServerState::WaitingForBackend;
        }

        Ok(done)
    }
}
