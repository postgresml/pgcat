/// Implementation of the PostgreSQL client.
/// We are pretending to the server in this scenario,
/// and this module implements that.
use bb8::Pool;
use bytes::{Buf, BufMut, BytesMut};
use rand::{distributions::Alphanumeric, Rng};
use tokio::io::{AsyncReadExt, BufReader, Interest};
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::net::TcpStream;

use crate::errors::Error;
use crate::messages::*;
use crate::pool::ServerPool;
use crate::server::Server;
use crate::ClientServerMap;

/// The client state.
pub struct Client {
    read: BufReader<OwnedReadHalf>,
    write: OwnedWriteHalf,
    buffer: BytesMut,
    name: String,
    cancel_mode: bool,
    process_id: i32,
    secret_key: i32,
    client_server_map: ClientServerMap,
}

impl Client {
    /// Given a TCP socket, trick the client into thinking we are
    /// the Postgres server. Perform the authentication and place
    /// the client in query-ready mode.
    pub async fn startup(
        mut stream: TcpStream,
        client_server_map: ClientServerMap,
    ) -> Result<Client, Error> {
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

                    // Generate random backend ID and secret key
                    let process_id: i32 = rand::random();
                    let secret_key: i32 = rand::random();

                    auth_ok(&mut stream).await?;
                    server_parameters(&mut stream).await?;
                    backend_key_data(&mut stream, process_id, secret_key).await?;
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
                        cancel_mode: false,
                        process_id: process_id,
                        secret_key: secret_key,
                        client_server_map: client_server_map,
                    });
                }

                // Cancel request
                80877102 => {
                    let (read, write) = stream.into_split();

                    let process_id = bytes.get_i32();
                    let secret_key = bytes.get_i32();

                    return Ok(Client {
                        read: BufReader::new(read),
                        write: write,
                        buffer: BytesMut::with_capacity(8196),
                        name: String::from("cancel_mode"),
                        cancel_mode: true,
                        process_id: process_id,
                        secret_key: secret_key,
                        client_server_map: client_server_map,
                    });
                }

                _ => {
                    return Err(Error::ProtocolSyncError);
                }
            };
        }
    }

    /// Client loop. We handle all messages between the client and the database here.
    pub async fn handle(&mut self, pool: Pool<ServerPool>) -> Result<(), Error> {
        // Special: cancelling existing running query
        if self.cancel_mode {
            let (process_id, secret_key) = {
                let guard = self.client_server_map.lock().unwrap();
                match guard.get(&(self.process_id, self.secret_key)) {
                    Some((process_id, secret_key)) => (process_id.clone(), secret_key.clone()),
                    None => return Ok(()),
                }
            };

            // TODO: pass actual server host and port somewhere.
            return Ok(Server::cancel("127.0.0.1", "5432", process_id, secret_key).await?);
        }

        loop {
            // Only grab a connection once we have some traffic on the socket
            // TODO: this is not the most optimal way to share servers.
            match self.read.get_ref().ready(Interest::READABLE).await {
                Ok(_) => (),
                Err(_) => return Err(Error::ClientDisconnected),
            };

            let mut proxy = pool.get().await.unwrap();
            let server = &mut *proxy;

            // TODO: maybe don't do this, I don't think it's useful.
            server.set_name(&self.name).await?;

            // Claim this server as mine for query cancellation.
            server.claim(self.process_id, self.secret_key);

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

                        loop {
                            let response = server.recv().await?;
                            match write_all_half(&mut self.write, response).await {
                                Ok(_) => (),
                                Err(err) => {
                                    server.mark_bad();
                                    return Err(err);
                                }
                            };

                            if !server.is_data_available() {
                                break;
                            }
                        }

                        // Release server
                        if !server.in_transaction() {
                            break;
                        }
                    }

                    'X' => {
                        // Client closing
                        if server.in_transaction() {
                            server.query("ROLLBACK").await?;
                        }

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

                        loop {
                            let response = server.recv().await?;
                            match write_all_half(&mut self.write, response).await {
                                Ok(_) => (),
                                Err(err) => {
                                    server.mark_bad();
                                    return Err(err);
                                }
                            };

                            if !server.is_data_available() {
                                break;
                            }
                        }

                        // Release server
                        if !server.in_transaction() {
                            break;
                        }
                    }

                    // CopyData
                    'd' => {
                        // Forward the data to the server,
                        // don't buffer it since it can be rather large.
                        server.send(original).await?;
                    }

                    'c' | 'f' => {
                        // Copy is done.
                        server.send(original).await?;
                        let response = server.recv().await?;
                        match write_all_half(&mut self.write, response).await {
                            Ok(_) => (),
                            Err(err) => {
                                server.mark_bad();
                                return Err(err);
                            }
                        };
                    }

                    _ => {
                        println!(">>> Unexpected code: {}", code);
                    }
                }
            }

            self.release();
        }
    }

    /// Release the server from being mine. I can't cancel its queries anymore.
    pub fn release(&mut self) {
        let mut guard = self.client_server_map.lock().unwrap();
        guard.remove(&(self.process_id, self.secret_key));
    }
}
