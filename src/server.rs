use md5::{Digest, Md5};
/// The PostgreSQL backend.
use tokio::io::{AsyncReadExt, AsyncWriteExt};

use crate::messages::authentication_md5_password::*;
use crate::messages::error_response::*;
use crate::messages::parameter_status::*;
use crate::messages::password_message::*;
use crate::messages::startup_message::*;

use crate::communication::{buffer, buffer_until_complete, check, read_all, write_all};
use crate::messages::{self, Message, MessageName, parse};
use crate::client::Client;

use bb8::ManageConnection;

pub enum ServerState {
    Connecting,
    WaitingForChallenge,
    LoginSuccessful,
    LoginRequest,
    Idle,
}

pub struct Server {
    host: String,
    username: String,
    password: String,
    database: String,
    stream: tokio::net::TcpStream,
    state: ServerState,
    buffer: Vec<u8>,
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
            buffer: vec![0u8; 8196],
        })

        // Prepare and send startup message
        // let startup: Vec<u8> = StartupMessage::new(username, "lev").into();
        // match sock.write_all(&startup).await {
        //     Ok(_) => (),
        //     Err(err) => return Err(Error::BadServer),
        // };

        // // Our work space buffer for connection setup only
        // let mut buf = vec![0u8; 1024];
        // let mut n = 0; // parsed bytes

        // // Expecting only one message here,
        // // either AuthenticationOk if no password is required or
        // // AuthenticationMd5Password challenge.
        // let buffer_result = match buffer_until_complete(&mut sock, &mut buf).await {
        //     Ok(r) => r,
        //     Err(_err) => return Err(Error::BadServer),
        // };

        // match messages::parse(&buf) {
        //     Ok((len, name)) => {
        //         n += len;

        //         match name {
        //             // No password required, what a treat!
        //             MessageName::AuthenticationOk => (),

        //             MessageName::AuthenticationMD5Password => {
        //                 println!("DEBUG: Received MD5 challenge request");
        //                 // Check that you gave me a password here
        //                 match password {
        //                     Some(_p) => (),
        //                     None => return Err(Error::UsernamePasswordIncorrect),
        //                 }

        //                 match AuthenticationMD5Password::parse(&buf, len as i32) {
        //                     Some(challenge) => {
        //                         println!("DEBUG: Sending encrypted password");

        //                         let password_message: Vec<u8> = PasswordMessage::new(
        //                             username,
        //                             password.unwrap(),
        //                             &challenge.salt,
        //                         )
        //                         .into();
        //                         match sock.write_all(&password_message).await {
        //                             Ok(_) => (),
        //                             Err(err) => return Err(Error::BadServer),
        //                         };

        //                         // Reset buffer
        //                         n = 0;

        //                         // Grab the remaining packets
        //                         buffer_until_complete(&mut sock, &mut buf).await;

        //                         match messages::parse(&buf) {
        //                             Ok((len, name)) => {
        //                                 n += len;

        //                                 match name {
        //                                     MessageName::AuthenticationOk => (),
        //                                     MessageName::ErrorResponse => {
        //                                         // Something bad happened
        //                                         let error = ErrorResponse::parse(&buf, len as i32);

        //                                         println!("DEBUG: {:?}", error);

        //                                         return Err(Error::BadServer);
        //                                     }
        //                                     _ => return Err(Error::BadServer),
        //                                 }
        //                             }

        //                             Err(_) => return Err(Error::BadServer),
        //                         }
        //                     },

        //                     None => return Err(Error::BadServer),
        //                 }
        //             }

        //             _ => return Err(Error::BadServer),
        //         }
        //     }

        //     Err(_) => {
        //         println!("DEBUG: No packets received");
        //         return Err(Error::BadServer);
        //     }
        // };

        // // Parse the params
        // // check(&buf[n..], Some(MessageName::ReadyForQuery));

        // Ok(Server {
        //     host: String::new(),
        //     username: username.to_string(),
        //     password: Some(password.unwrap().to_string()),
        //     database: "lev".to_string(),
        //     stream: sock,
        //     buffer: vec![0u8; 8192],
        //     state: ServerState::Idle,
        // })
    }

    pub async fn handle(&mut self) -> Result<(), &'static str> {
        loop {
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
                    read_all(&mut self.stream, &mut self.buffer).await?;

                    let (len, message_name) = parse(&self.buffer)?;

                    match message_name {
                        MessageName::AuthenticationOk => {
                            self.state = ServerState::LoginSuccessful;
                        },

                        MessageName::AuthenticationMD5Password => {
                            let challenge = AuthenticationMD5Password::parse(&self.buffer, len as i32).unwrap();
                            let password_message: Vec<u8> = PasswordMessage::new(
                                &self.username,
                                &self.password,
                                &challenge.salt,
                            )
                            .into();

                            write_all(&mut self.stream, &password_message).await?;
                        },

                        MessageName::ErrorResponse => {
                            return Err("ERROR: bad password for server");
                        },

                        _ => return Err("ERROR: bad message from server"),
                    }
                },

                ServerState::LoginSuccessful => {
                    break;
                }
                _ => (),
            };
        }

        Ok(())
    }

    pub async fn handle_query(
        &mut self,
        query: &messages::query::Query,
    ) -> Result<(), Box<dyn std::error::Error>> {
        Ok(())
    }

    async fn handle_message(&self, buf: &[u8]) {}
}
