use md5::{Digest, Md5};
/// The PostgreSQL backend.
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::oneshot::{Sender, Receiver};

use crate::messages::authentication_md5_password::*;
use crate::messages::error_response::*;
use crate::messages::parameter_status::*;
use crate::messages::password_message::*;
use crate::messages::startup_message::*;

use crate::communication::{write_buf};
use crate::messages::{self, Message, MessageName, parse};
use crate::client::Client;

use bb8::ManageConnection;
use bytes::{BufMut, Buf};
use std::io::BufRead;

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
        let mut stream = match tokio::net::TcpStream::connect(host).await {
            Ok(stream) => stream,
            Err(err) => return Err("ERROR: cannot connect to server"),
        };

        Ok(Server{
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
            println!("DEBUG: server state {:?}", self.state);

            match self.state {
                ServerState::Connecting => {
                    let startup: Vec<u8> = StartupMessage::new(&self.username, &self.database).into();
                    match self.stream.write_all(&startup).await {
                        Ok(_) => (),
                        Err(err) => return Err("ERROR: server socket died"),
                    };

                    self.state = ServerState::WaitingForChallenge;
                },

                ServerState::WaitingForChallenge => {
                    // let b = tokio::io::ReadBuf::new(&mut self.buffer);
                    self.stream.read_buf(&mut self.buffer).await;

                    let (len, message_name) = parse(&self.buffer)?;

                    match message_name {
                        MessageName::AuthenticationOk => {
                            self.state = ServerState::LoginSuccessful;
                            self.buffer.clear();
                        },

                        MessageName::AuthenticationMD5Password => {
                            let challenge = AuthenticationMD5Password::parse(&self.buffer, len as i32).unwrap();
                            let password_message: Vec<u8> = PasswordMessage::new(
                                &self.username,
                                &self.password,
                                &challenge.salt,
                            )
                            .into();

                            self.stream.write_all(&password_message).await;
                            self.buffer.clear();
                        },

                        MessageName::ErrorResponse => {
                            return Err("ERROR: bad password for server");
                        },

                        _ => return Err("ERROR: bad message from server"),
                    }
                },

                ServerState::LoginSuccessful => {
                    // Parse server status messages
                    self.state = ServerState::Idle;
                    break;
                },

                ServerState::Active => {
                    // Data in buffer?
                    if self.buffer.len() > 0 {
                        self.stream.write_buf(&mut self.buffer).await;
                        self.buffer.clear();

                        // TODO: handle multiple statements in a transaction
                        self.state = ServerState::WaitingForBackend;
                    }

                    else {
                        self.state = ServerState::Idle;
                    }
                },

                ServerState::WaitingForBackend => {
                    let n = self.stream.read_buf(&mut self.buffer).await.unwrap();
                    self.state = ServerState::DataReady;
                    break;
                },

                ServerState::DataReady => {
                    break;
                },

                ServerState::Idle => {
                    break;
                },

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
            _ => return Err("ERROR: client and server out of sync"),
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
                Ok(())
            },

            _ => {
                Err("ERROR: server and client out of sync with bad state")
            },
        }
    }
}
