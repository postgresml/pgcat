#![allow(dead_code)]
#![allow(unused_variables)]

///! Implementation of the PostgreSQL server (database) protocol.
///! Here we are pretending to the a Postgres client.
use bytes::{Buf, BufMut, BytesMut};
use tokio::io::{AsyncReadExt, BufReader};
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::net::TcpStream;

use crate::config::{Address, Role};
use crate::errors::Error;
use crate::messages::*;
use crate::ClientServerMap;

/// Server state.
pub struct Server {
    // Server host, e.g. localhost
    host: String,

    // Server port: e.g. 5432
    port: String,

    // Buffered read socket
    read: BufReader<OwnedReadHalf>,

    // Unbuffered write socket (our client code buffers)
    write: OwnedWriteHalf,

    // Our server response buffer
    buffer: BytesMut,

    // Server information the server sent us over on startup
    server_info: BytesMut,

    // Backend id and secret key used for query cancellation.
    backend_id: i32,
    secret_key: i32,

    // Is the server inside a transaction at the moment.
    in_transaction: bool,

    // Is there more data for the client to read.
    data_available: bool,

    // Is the server broken? We'll remote it from the pool if so.
    bad: bool,

    // Mapping of clients and servers used for query cancellation.
    client_server_map: ClientServerMap,

    // Server role, e.g. primary or replica.
    role: Role,

    // Server connected at
    connected_at: chrono::naive::NaiveDateTime,
}

impl Server {
    /// Pretend to be the Postgres client and connect to the server given host, port and credentials.
    /// Perform the authentication and return the server in a ready-for-query mode.
    pub async fn startup(
        host: &str,
        port: &str,
        user: &str,
        password: &str,
        database: &str,
        client_server_map: ClientServerMap,
        role: Role,
    ) -> Result<Server, Error> {
        let mut stream = match TcpStream::connect(&format!("{}:{}", host, port)).await {
            Ok(stream) => stream,
            Err(err) => {
                println!(">> Could not connect to server: {}", err);
                return Err(Error::SocketError);
            }
        };

        // Send the startup packet.
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

                        // Authentication handshake complete.
                        0 => (),

                        _ => {
                            println!(">> Unsupported authentication mechanism: {}", code);
                            return Err(Error::ServerError);
                        }
                    }
                }

                'E' => {
                    let error_code = match stream.read_u8().await {
                        Ok(error_code) => error_code,
                        Err(_) => return Err(Error::SocketError),
                    };

                    match error_code {
                        0 => (), // Terminator
                        _ => {
                            let mut error = vec![0u8; len as usize - 4 - 1];
                            match stream.read_exact(&mut error).await {
                                Ok(_) => (),
                                Err(_) => return Err(Error::SocketError),
                            };

                            println!(">> Server error: {}", String::from_utf8_lossy(&error));
                        }
                    };
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
                        host: host.to_string(),
                        port: port.to_string(),
                        read: BufReader::new(read),
                        write: write,
                        buffer: BytesMut::with_capacity(8196),
                        server_info: server_info,
                        backend_id: backend_id,
                        secret_key: secret_key,
                        in_transaction: false,
                        data_available: false,
                        bad: false,
                        client_server_map: client_server_map,
                        role: role,
                        connected_at: chrono::offset::Utc::now().naive_utc(),
                    });
                }

                _ => {
                    println!(">> Unknown code: {}", code);
                    return Err(Error::ProtocolSyncError);
                }
            };
        }
    }

    /// Issue a cancellation request to the server.
    /// Uses a separate connection that's not part of the connection pool.
    pub async fn cancel(
        host: &str,
        port: &str,
        process_id: i32,
        secret_key: i32,
    ) -> Result<(), Error> {
        let mut stream = match TcpStream::connect(&format!("{}:{}", host, port)).await {
            Ok(stream) => stream,
            Err(err) => {
                println!(">> Could not connect to server: {}", err);
                return Err(Error::SocketError);
            }
        };

        let mut bytes = BytesMut::with_capacity(16);
        bytes.put_i32(16);
        bytes.put_i32(80877102);
        bytes.put_i32(process_id);
        bytes.put_i32(secret_key);

        Ok(write_all(&mut stream, bytes).await?)
    }

    /// Send data to the server from the client.
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

    /// Receive data from the server in response to a client request sent previously.
    /// This method must be called multiple times while `self.is_data_available()` is true
    /// in order to receive all data the server has to offer.
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

                    self.data_available = false;

                    break;
                }

                'D' => {
                    self.data_available = true;

                    // Don't flush yet, the more we buffer, the faster this goes.
                    // Up to a limit of course.
                    if self.buffer.len() >= 8196 {
                        break;
                    }
                }

                // CopyInResponse: copy is starting from client to server
                'G' => break,

                // CopyOutResponse: copy is starting from the server to the client
                'H' => {
                    self.data_available = true;
                    break;
                }

                // CopyData
                'd' => break,

                // CopyDone
                'c' => {
                    self.data_available = false;
                    // Buffer until ReadyForQuery shows up
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

    /// If the server is still inside a transaction.
    /// If the client disconnects while the server is in a transaction, we will clean it up.
    pub fn in_transaction(&self) -> bool {
        self.in_transaction
    }

    /// We don't buffer all of server responses, e.g. COPY OUT produces too much data.
    /// The client is responsible to call `self.recv()` while this method returns true.
    pub fn is_data_available(&self) -> bool {
        self.data_available
    }

    /// Server & client are out of sync, we must discard this connection.
    /// This happens with clients that misbehave.
    pub fn is_bad(&self) -> bool {
        self.bad
    }

    /// Get server startup information to forward it to the client.
    /// Not used at the moment.
    pub fn server_info(&self) -> BytesMut {
        self.server_info.clone()
    }

    /// Indicate that this server connection cannot be re-used and must be discarded.
    pub fn mark_bad(&mut self) {
        println!(">> Server marked bad");
        self.bad = true;
    }

    /// Claim this server as mine for the purposes of query cancellation.
    pub fn claim(&mut self, process_id: i32, secret_key: i32) {
        let mut guard = self.client_server_map.lock().unwrap();
        guard.insert(
            (process_id, secret_key),
            (
                self.backend_id,
                self.secret_key,
                self.host.clone(),
                self.port.clone(),
            ),
        );
    }

    /// Execute an arbitrary query against the server.
    /// It will use the Simple query protocol.
    /// Result will not be returned, so this is useful for things like `SET` or `ROLLBACK`.
    pub async fn query(&mut self, query: &str) -> Result<(), Error> {
        let mut query = BytesMut::from(&query.as_bytes()[..]);
        query.put_u8(0);

        let len = query.len() as i32 + 4;

        let mut msg = BytesMut::with_capacity(len as usize + 1);

        msg.put_u8(b'Q');
        msg.put_i32(len);
        msg.put_slice(&query[..]);

        self.send(msg).await?;
        loop {
            let _ = self.recv().await?;
            if !self.data_available {
                break;
            }
        }

        Ok(())
    }

    /// A shorthand for `SET application_name = $1`.
    pub async fn set_name(&mut self, name: &str) -> Result<(), Error> {
        Ok(self
            .query(&format!("SET application_name = '{}'", name))
            .await?)
    }

    pub fn address(&self) -> Address {
        Address {
            host: self.host.to_string(),
            port: self.port.to_string(),
            role: self.role,
        }
    }
}

impl Drop for Server {
    // Try to do a clean shut down.
    fn drop(&mut self) {
        let mut bytes = BytesMut::with_capacity(4);
        bytes.put_u8(b'X');
        bytes.put_i32(4);

        match self.write.try_write(&bytes) {
            Ok(n) => (),
            Err(_) => (),
        };

        self.bad = true;

        let now = chrono::offset::Utc::now().naive_utc();
        let duration = now - self.connected_at;

        println!(
            ">> Server connection closed, session duration: {}",
            crate::format_duration(&duration)
        );
    }
}
