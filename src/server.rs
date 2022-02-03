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
                }

                'K' => {
                    // TODO: save cancellation secret
                    let mut cancel_secret = vec![0u8; len as usize - 4];
                    match stream.read_exact(&mut cancel_secret).await {
                        Ok(_) => (),
                        Err(_) => return Err(Error::SocketError),
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
        Ok(write_all_half(&mut self.write, messages).await?)
    }

    pub async fn recv(&mut self) -> Result<BytesMut, Error> {
        loop {
            let mut message = read_message(&mut self.read).await?;

            // Buffer the message we'll forward to the client in a bit.
            self.buffer.put(&message[..]);

            let code = message.get_u8() as char;

            match code {
                'Z' => {
                    // Ready for query, time to forward buffer to client.
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
}
