use md5::{Digest, Md5};
/// The PostgreSQL backend.
use tokio::io::{AsyncReadExt, AsyncWriteExt};

use crate::messages::authentication_md5_password::*;
use crate::messages::error_response::*;
use crate::messages::parameter_status::*;
use crate::messages::password_message::*;
use crate::messages::startup_message::*;

use crate::communication::{buffer, buffer_until_complete, check};
use crate::messages::{self, Message, MessageName};

use bb8::ManageConnection;

pub struct Server {
    pub stream: tokio::net::TcpStream,
    buf: Vec<u8>,
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
        password: Option<&str>,
        database: Option<&str>,
    ) -> Result<Server, Error> {
        // TCP connection
        let mut sock = match tokio::net::TcpStream::connect(host).await {
            Ok(sock) => sock,
            Err(err) => {
                println!("ERROR: cannot connect to {}, err: {}", host, err);
                return Err(Error::BadServer);
            }
        };

        // Prepare and send startup message
        let startup: Vec<u8> = StartupMessage::new(username, database).into();
        match sock.write_all(&startup).await {
            Ok(_) => (),
            Err(err) => return Err(Error::BadServer),
        };

        // Our work space buffer for connection setup only
        let mut buf = vec![0u8; 1024];
        let mut n = 0; // parsed bytes

        // Expecting only one message here,
        // either AuthenticationOk if no password is required or
        // AuthenticationMd5Password challenge.
        let buffer_result = match buffer_until_complete(&mut sock, &mut buf).await {
            Ok(r) => r,
            Err(_err) => return Err(Error::BadServer),
        };

        match messages::parse(&buf) {
            Ok((len, name)) => {
                n += len;

                match name {
                    // No password required, what a treat!
                    MessageName::AuthenticationOk => (),

                    MessageName::AuthenticationMD5Password => {
                        println!("DEBUG: Received MD5 challenge request");
                        // Check that you gave me a password here
                        match password {
                            Some(_p) => (),
                            None => return Err(Error::UsernamePasswordIncorrect),
                        }

                        match AuthenticationMD5Password::parse(&buf, len as i32) {
                            Some(challenge) => {
                                println!("DEBUG: Sending encrypted password");

                                let password_message: Vec<u8> = PasswordMessage::new(
                                    username,
                                    password.unwrap(),
                                    &challenge.salt,
                                )
                                .into();
                                match sock.write_all(&password_message).await {
                                    Ok(_) => (),
                                    Err(err) => return Err(Error::BadServer),
                                };

                                // Reset buffer
                                n = 0;

                                // Grab the remaining packets
                                buffer_until_complete(&mut sock, &mut buf).await;

                                match messages::parse(&buf) {
                                    Ok((len, name)) => {
                                        n += len;

                                        match name {
                                            MessageName::AuthenticationOk => (),
                                            MessageName::ErrorResponse => {
                                                // Something bad happened
                                                let error = ErrorResponse::parse(&buf, len as i32);

                                                println!("DEBUG: {:?}", error);

                                                return Err(Error::BadServer);
                                            }
                                            _ => return Err(Error::BadServer),
                                        }
                                    }

                                    Err(_) => return Err(Error::BadServer),
                                }
                            },

                            None => return Err(Error::BadServer),
                        }
                    }

                    _ => return Err(Error::BadServer),
                }
            }

            Err(_) => {
                println!("DEBUG: No packets received");
                return Err(Error::BadServer);
            }
        };

        // Parse the params
        // check(&buf[n..], Some(MessageName::ReadyForQuery));

        Ok(Server {
            stream: sock,
            buf: vec![0u8; 8192],
        })
    }

    pub async fn send(&mut self, buf: &[u8]) -> Result<(), Box<dyn std::error::Error>> {
        let _ = self.stream.write_all(&buf).await?;
        Ok(())
    }

    pub async fn recv(&mut self, buf: &mut [u8]) -> Result<(), Box<dyn std::error::Error>> {
        buffer_until_complete(&mut self.stream, buf).await;
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
