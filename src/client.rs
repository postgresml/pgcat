/// Implementation of the PostgreSQL client.
/// We are pretending to the server in this scenario,
/// and this module implements that.
use bytes::{Buf, BufMut, BytesMut};
use rand::{distributions::Alphanumeric, Rng};
use tokio::io::{AsyncReadExt, BufReader};
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::net::TcpStream;

use crate::errors::Error;
use crate::messages::*;
use crate::pool::{ClientServerMap, ConnectionPool};
use crate::server::Server;

/// The client state. One of these is created per client.
pub struct Client {
    // The reads are buffered (8K by default).
    read: BufReader<OwnedReadHalf>,

    // We buffer the writes ourselves because we know the protocol
    // better than a stock buffer.
    write: OwnedWriteHalf,

    // Internal buffer, where we place messages until we have to flush
    // them to the backend.
    buffer: BytesMut,

    // The client was started with the sole reason to cancel another running query.
    cancel_mode: bool,

    // In transaction mode, the connection is released after each transaction.
    // Session mode has slightly higher throughput per client, but lower capacity.
    transaction_mode: bool,

    // For query cancellation, the client is given a random process ID and secret on startup.
    process_id: i32,
    secret_key: i32,

    // Clients are mapped to servers while they use them. This allows a client
    // to connect and cancel a query.
    client_server_map: ClientServerMap,
}

impl Client {
    /// Given a TCP socket, trick the client into thinking we are
    /// the Postgres server. Perform the authentication and place
    /// the client in query-ready mode.
    pub async fn startup(
        mut stream: TcpStream,
        client_server_map: ClientServerMap,
        transaction_mode: bool,
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

                    // Split the read and write streams
                    // so we can control buffering.
                    let (read, write) = stream.into_split();

                    return Ok(Client {
                        read: BufReader::new(read),
                        write: write,
                        buffer: BytesMut::with_capacity(8196),
                        cancel_mode: false,
                        transaction_mode: transaction_mode,
                        process_id: process_id,
                        secret_key: secret_key,
                        client_server_map: client_server_map,
                    });
                }

                // Query cancel request.
                80877102 => {
                    let (read, write) = stream.into_split();

                    let process_id = bytes.get_i32();
                    let secret_key = bytes.get_i32();

                    return Ok(Client {
                        read: BufReader::new(read),
                        write: write,
                        buffer: BytesMut::with_capacity(8196),
                        cancel_mode: true,
                        transaction_mode: transaction_mode,
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
    pub async fn handle(&mut self, pool: ConnectionPool) -> Result<(), Error> {
        // Special: cancelling existing running query
        if self.cancel_mode {
            let (process_id, secret_key, address, port) = {
                let guard = self.client_server_map.lock().unwrap();
                match guard.get(&(self.process_id, self.secret_key)) {
                    // Drop the mutex as soon as possible.
                    Some((process_id, secret_key, address, port)) => (
                        process_id.clone(),
                        secret_key.clone(),
                        address.clone(),
                        port.clone(),
                    ),
                    None => return Ok(()),
                }
            };

            // TODO: pass actual server host and port somewhere.
            return Ok(Server::cancel(&address, &port, process_id, secret_key).await?);
        }

        loop {
            // Read a complete message from the client, which normally would be
            // either a `Q` (query) or `P` (prepare, extended protocol).
            // We can parse it here before grabbing a server from the pool,
            // in case the client is sending some control messages, e.g.
            // SET sharding_context.key = '1234';
            let mut message = read_message(&mut self.read).await?;

            // TODO: parse the message here. If it's part of our protocol,
            // don't grab a server yet and continue loop.

            // The message is part of the regular protocol.
            // self.buffer.put(message);

            // Grab a server from the pool.
            // None = any shard
            let connection = pool.get(None).await.unwrap();
            let mut proxy = connection.0;
            let _address = connection.1;
            let server = &mut *proxy;

            // Claim this server as mine for query cancellation.
            server.claim(self.process_id, self.secret_key);

            loop {
                // No messages in the buffer, read one.
                let mut message = if message.len() == 0 {
                    match read_message(&mut self.read).await {
                        Ok(message) => message,
                        Err(err) => {
                            // Client disconnected without warning.
                            if server.in_transaction() {
                                // TODO: this is what PgBouncer does
                                // which leads to connection thrashing.
                                //
                                // I think we could issue a ROLLBACK here instead.
                                // server.mark_bad();
                                server.query("ROLLBACK; DISCARD ALL;").await?;
                            }

                            return Err(err);
                        }
                    }
                } else {
                    let msg = message.clone();
                    message.clear();
                    msg
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
                        if !server.in_transaction() && self.transaction_mode {
                            break;
                        }
                    }

                    'X' => {
                        // Client closing. Rollback and clean up
                        // connection before releasing into the pool.
                        // Pgbouncer closes the connection which leads to
                        // connection thrashing when clients misbehave.
                        // This pool will protect the database. :salute:
                        if server.in_transaction() {
                            server.query("ROLLBACK; DISCARD ALL;").await?;
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

                    // Describe
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
                        if !server.in_transaction() && self.transaction_mode {
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

                        // Release the server
                        if !server.in_transaction() && self.transaction_mode {
                            println!("Releasing after copy done");
                            break;
                        }
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
