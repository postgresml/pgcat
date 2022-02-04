#![allow(dead_code)]
#![allow(unused_variables)]

use bytes::{Buf, BufMut, BytesMut};
use tokio::io::{AsyncReadExt, BufReader};
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::net::TcpStream;

use crate::errors::Error;
use crate::messages::*;

pub struct Server {
    read: BufReader<OwnedReadHalf>,
    write: OwnedWriteHalf,
    buffer: BytesMut,
    server_info: BytesMut,
    backend_id: i32,
    secret_key: i32,
    in_transaction: bool,
    bad: bool,
}

impl Server {
    pub async fn startup(
        host: &str,
        port: &str,
        user: &str,
        password: &str,
        database: &str,
    ) -> Result<Server, Error> {
        let mut stream = match TcpStream::connect(&format!("{}:{}", host, port)).await {
            Ok(stream) => stream,
            Err(err) => {
                println!(">> Could not connect to server: {}", err);
                return Err(Error::SocketError);
            }
        };

        startup(&mut stream, user, database).await?;

        let mut server_info = BytesMut::with_capacity(25);
        let mut backend_id: i32 = 0;
        let mut secret_key: i32 = 0;

        loop {
            let code = match stream.read_u8().await {
                Ok(code) => code as char,
                Err(_) => return Err(Error::SocketError),
            };

            let len = match stream.read_i32().await {
                Ok(len) => len,
                Err(_) => return Err(Error::SocketError),
            };

            match code {
                'R' => {
                    // Auth can proceed
                    let code = match stream.read_i32().await {
                        Ok(code) => code,
                        Err(_) => return Err(Error::SocketError),
                    };

                    match code {
                        // MD5
                        5 => {
                            let mut salt = vec![0u8; 4];

                            match stream.read_exact(&mut salt).await {
                                Ok(_) => (),
                                Err(_) => return Err(Error::SocketError),
                            };

                            md5_password(&mut stream, user, password, &salt[..]).await?;
                        }

                        // We're in!
                        0 => {
                            println!(">> Server authentication successful!");
                        }

                        _ => {
                            println!(">> Unsupported authentication mechanism: {}", code);
                            return Err(Error::ServerError);
                        }
                    }
                }

                'E' => {
                    println!(">> Database error");
                    return Err(Error::ServerError);
                }

                'S' => {
                    // Parameter
                    let mut param = vec![0u8; len as usize - 4];
                    match stream.read_exact(&mut param).await {
                        Ok(_) => (),
                        Err(_) => return Err(Error::SocketError),
                    };

                    server_info.put_u8(b'S');
                    server_info.put_i32(len);
                    server_info.put_slice(&param[..]);
                }

                'K' => {
                    // Query cancellation data.
                    backend_id = match stream.read_i32().await {
                        Ok(id) => id,
                        Err(err) => return Err(Error::SocketError),
                    };

                    secret_key = match stream.read_i32().await {
                        Ok(id) => id,
                        Err(err) => return Err(Error::SocketError),
                    };
                }

                'Z' => {
                    let mut idle = vec![0u8; len as usize - 4];

                    match stream.read_exact(&mut idle).await {
                        Ok(_) => (),
                        Err(_) => return Err(Error::SocketError),
                    };

                    // Startup finished
                    let (read, write) = stream.into_split();

                    return Ok(Server {
                        read: BufReader::new(read),
                        write: write,
                        buffer: BytesMut::with_capacity(8196),
                        server_info: server_info,
                        backend_id: backend_id,
                        secret_key: secret_key,
                        in_transaction: false,
                        bad: false,
                    });
                }

                _ => {
                    println!(">> Unknown code: {}", code);
                    return Err(Error::ProtocolSyncError);
                }
            };
        }
    }

    pub async fn send(&mut self, messages: BytesMut) -> Result<(), Error> {
        match write_all_half(&mut self.write, messages).await {
            Ok(_) => Ok(()),
            Err(err) => {
                println!(">> Terminating server because of: {:?}", err);
                self.bad = true;
                Err(err)
            }
        }
    }

    pub async fn recv(&mut self) -> Result<BytesMut, Error> {
        loop {
            let mut message = match read_message(&mut self.read).await {
                Ok(message) => message,
                Err(err) => {
                    println!(">> Terminating server because of: {:?}", err);
                    self.bad = true;
                    return Err(err);
                }
            };

            // Buffer the message we'll forward to the client in a bit.
            self.buffer.put(&message[..]);

            let code = message.get_u8() as char;
            let _len = message.get_i32();

            match code {
                'Z' => {
                    // Ready for query, time to forward buffer to client.
                    let transaction_state = message.get_u8() as char;

                    match transaction_state {
                        'T' => {
                            self.in_transaction = true;
                        }

                        'I' => {
                            self.in_transaction = false;
                        }

                        // Error client didn't clean up!
                        // We shuold drop this server
                        'E' => {
                            self.in_transaction = true;
                        }

                        _ => {
                            self.bad = true;
                            return Err(Error::ProtocolSyncError);
                        }
                    };

                    break;
                }

                _ => {
                    // Keep buffering,
                }
            };
        }

        let bytes = self.buffer.clone();
        self.buffer.clear();

        Ok(bytes)
    }

    pub fn in_transaction(&self) -> bool {
        self.in_transaction
    }

    pub fn is_bad(&self) -> bool {
        self.bad
    }

    pub fn server_info(&self) -> BytesMut {
        self.server_info.clone()
    }

    pub fn mark_bad(&mut self) {
        println!(">> Server marked bad");
        self.bad = true;
    }

    pub async fn query(&mut self, query: &str) -> Result<(), Error> {
        let mut query = BytesMut::from(&query.as_bytes()[..]);
        query.put_u8(0);

        let len = query.len() as i32 + 4;

        let mut msg = BytesMut::with_capacity(len as usize + 1);

        msg.put_u8(b'Q');
        msg.put_i32(len);
        msg.put_slice(&query[..]);

        self.send(msg).await?;
        let _ = self.recv().await?;

        Ok(())
    }

    pub async fn set_name(&mut self, name: &str) -> Result<(), Error> {
        Ok(self
            .query(&format!("SET application_name = '{}'", name))
            .await?)
    }
}
