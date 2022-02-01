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
    stream: tokio::net::TcpStream,
    state: ServerState,
    buffer: bytes::BytesMut,
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
        let stream = match tokio::net::TcpStream::connect(host).await {
            Ok(stream) => stream,
            Err(err) => return Err("ERROR: cannot connect to server"),
        };

        Ok(Server {
            host: host.to_string(),
            username: username.to_string(),
            password: password.to_string(),
            database: database.to_string(),
            stream: stream,
            state: ServerState::Connecting,
            buffer: bytes::BytesMut::with_capacity(8196),
        })
    }

    pub async fn handle(&mut self) -> Result<(), &'static str> {
        loop {
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
                    // let b = tokio::io::ReadBuf::new(&mut self.buffer);
                    if !self.buffer.has_remaining() {
                        self.stream.read_buf(&mut self.buffer).await;
                    }

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
                        // self.buffer.clear();

                        // TODO: handle multiple statements in a transaction
                        self.state = ServerState::WaitingForBackend;
                    } else {
                        self.state = ServerState::Idle;
                    }
                }

                ServerState::WaitingForBackend => {
                    let n = self.stream.read_buf(&mut self.buffer).await.unwrap();

                    // Not enough data yet
                    if self.buffer.len() < 5 {
                        continue;
                    } else if self.buffer.len() == 5 {
                        let c = self.buffer[0] as char;

                        match c {
                            'C' => {
                                continue;
                            }
                            'X' => {
                                self.state = ServerState::DataReady;
                                break;
                            }
                            _ => (),
                        };
                    }

                    let len = self.buffer.len();

                    // We have the whole thing
                    if self.buffer[len - 6] == b'Z' {
                        self.state = ServerState::DataReady;
                    }
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

    pub async fn forward(&mut self, buf: &[u8]) -> Result<(), &'static str> {
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

        self.buffer.put(buf);
        self.state = ServerState::Active;

        Ok(())
    }

    pub async fn receive(&mut self, buf: &mut bytes::BytesMut) -> Result<(), &'static str> {
        self.handle().await?;

        match self.state {
            ServerState::DataReady => {
                while self.buffer.has_remaining() {
                    buf.put_u8(self.buffer.get_u8());
                }
                self.buffer.clear();
                self.state = ServerState::Idle;
                Ok(())
            }

            _ => Err("ERROR: server and client out of sync with bad state"),
        }
    }
}
