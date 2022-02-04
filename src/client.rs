use tokio::io::{AsyncReadExt, BufReader, Interest};
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
/// PostgreSQL client (frontend).
/// We are pretending to be the backend.
use tokio::net::TcpStream;

use bytes::{Buf, BufMut, BytesMut};

use crate::errors::Error;
use crate::messages::*;

use crate::pool::ServerPool;
use bb8::Pool;
use rand::{distributions::Alphanumeric, Rng};

pub struct Client {
    read: BufReader<OwnedReadHalf>,
    write: OwnedWriteHalf,
    buffer: BytesMut,
    name: String,
}

impl Client {
    pub async fn startup(mut stream: TcpStream) -> Result<Client, Error> {
        loop {
            // Could be StartupMessage or SSLRequest
            // which makes this variable length.
            let len = match stream.read_i32().await {
                Ok(len) => len,
                Err(_) => return Err(Error::ClientBadStartup),
            };

            // Read whatever is left.
            let mut startup = vec![0u8; len as usize - 4];

            match stream.read_exact(&mut startup).await {
                Ok(_) => (),
                Err(_) => return Err(Error::ClientBadStartup),
            };

            let mut bytes = BytesMut::from(&startup[..]);
            let code = bytes.get_i32();

            match code {
                // Client wants SSL. We don't support it at the moment.
                80877103 => {
                    let mut no = BytesMut::with_capacity(1);
                    no.put_u8(b'N');

                    write_all(&mut stream, no).await?;
                }

                // Regular startup message.
                196608 => {
                    // TODO: perform actual auth.
                    // TODO: record startup parameters client sends over.
                    auth_ok(&mut stream).await?;
                    ready_for_query(&mut stream).await?;

                    let (read, write) = stream.into_split();

                    let name: String = rand::thread_rng()
                        .sample_iter(&Alphanumeric)
                        .take(7)
                        .map(char::from)
                        .collect();

                    return Ok(Client {
                        read: BufReader::new(read),
                        write: write,
                        buffer: BytesMut::with_capacity(8196),
                        name: name,
                    });
                }

                _ => {
                    return Err(Error::ProtocolSyncError);
                }
            };
        }
    }

    pub async fn handle(&mut self, pool: Pool<ServerPool>) -> Result<(), Error> {
        loop {
            // Only grab a connection once we have some traffic on the socket
            // TODO: this is not the most optimal way to share servers.
            match self.read.get_ref().ready(Interest::READABLE).await {
                Ok(_) => (),
                Err(_) => return Err(Error::ClientDisconnected),
            };

            let mut proxy = pool.get().await.unwrap();
            let server = &mut *proxy;

            server.set_name(&self.name).await?;

            loop {
                let mut message = match read_message(&mut self.read).await {
                    Ok(message) => message,
                    Err(err) => {
                        if server.in_transaction() {
                            // TODO: this is what PgBouncer does
                            // which leads to connection thrashing.
                            //
                            // I think we could issue a ROLLBACK here instead.
                            server.mark_bad();
                        }

                        return Err(err);
                    }
                };

                let original = message.clone(); // To be forwarded to the server
                let code = message.get_u8() as char;
                let _len = message.get_i32() as usize;

                match code {
                    'Q' => {
                        server.send(original).await?;
                        let response = server.recv().await?;
                        match write_all_half(&mut self.write, response).await {
                            Ok(_) => (),
                            Err(err) => {
                                server.mark_bad();
                                return Err(err);
                            }
                        };

                        // Release server
                        if !server.in_transaction() {
                            drop(server);
                            break;
                        }
                    }

                    'X' => {
                        // Client closing
                        return Ok(());
                    }

                    'P' => {
                        // Extended protocol, let's buffer most of it
                        self.buffer.put(&original[..]);
                    }

                    'B' => {
                        self.buffer.put(&original[..]);
                    }

                    'D' => {
                        self.buffer.put(&original[..]);
                    }

                    'E' => {
                        self.buffer.put(&original[..]);
                    }

                    'S' => {
                        // Extended protocol, client requests sync
                        self.buffer.put(&original[..]);
                        server.send(self.buffer.clone()).await?;
                        self.buffer.clear();

                        let response = server.recv().await?;
                        match write_all_half(&mut self.write, response).await {
                            Ok(_) => (),
                            Err(err) => {
                                server.mark_bad();
                                return Err(err);
                            }
                        };

                        // Release server
                        if !server.in_transaction() {
                            drop(server);
                            break;
                        }
                    }

                    _ => {
                        println!(">>> Unexpected code: {}", code);
                    }
                }
            }
        }
    }
}
