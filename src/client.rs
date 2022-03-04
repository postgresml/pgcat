/// Implementation of the PostgreSQL client.
/// We are pretending to the server in this scenario,
/// and this module implements that.
use bytes::{Buf, BufMut, BytesMut};
use log::{debug, error, trace};
use tokio::io::{AsyncReadExt, BufReader};
use tokio::net::{
    tcp::{OwnedReadHalf, OwnedWriteHalf},
    TcpStream,
};

use std::collections::HashMap;

use crate::admin::handle_admin;
use crate::config::get_config;
use crate::constants::*;
use crate::errors::Error;
use crate::messages::*;
use crate::pool::{ClientServerMap, ConnectionPool};
use crate::query_router::{Command, QueryRouter};
use crate::server::Server;
use crate::stats::Reporter;

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

    // Client parameters, e.g. user, client_encoding, etc.
    #[allow(dead_code)]
    parameters: HashMap<String, String>,

    // Statistics
    stats: Reporter,

    // Clients want to talk to admin
    admin: bool,
}

impl Client {
    /// Given a TCP socket, trick the client into thinking we are
    /// the Postgres server. Perform the authentication and place
    /// the client in query-ready mode.
    pub async fn startup(
        mut stream: TcpStream,
        client_server_map: ClientServerMap,
        server_info: BytesMut,
        stats: Reporter,
    ) -> Result<Client, Error> {
        let config = get_config();
        let transaction_mode = config.general.pool_mode.starts_with("t");
        drop(config);
        loop {
            trace!("Waiting for StartupMessage");

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
                SSL_REQUEST_CODE => {
                    trace!("Rejecting SSLRequest");

                    let mut no = BytesMut::with_capacity(1);
                    no.put_u8(b'N');

                    write_all(&mut stream, no).await?;
                }

                // Regular startup message.
                PROTOCOL_VERSION_NUMBER => {
                    trace!("Got StartupMessage");
                    let parameters = parse_startup(bytes.clone())?;
                    let user = match parameters.get(&String::from("user")) {
                        Some(user) => user,
                        None => return Err(Error::ClientBadStartup),
                    };
                    start_auth(&mut stream, user).await?;

                    // Generate random backend ID and secret key
                    let process_id: i32 = rand::random();
                    let secret_key: i32 = rand::random();

                    write_all(&mut stream, server_info).await?;
                    backend_key_data(&mut stream, process_id, secret_key).await?;
                    ready_for_query(&mut stream).await?;
                    trace!("Startup OK");

                    let database = parameters
                        .get("database")
                        .unwrap_or(parameters.get("user").unwrap());
                    let admin = ["pgcat", "pgbouncer"]
                        .iter()
                        .filter(|db| *db == &database)
                        .count()
                        == 1;

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
                        parameters: parameters,
                        stats: stats,
                        admin: admin,
                    });
                }

                // Query cancel request.
                CANCEL_REQUEST_CODE => {
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
                        parameters: HashMap::new(),
                        stats: stats,
                        admin: false,
                    });
                }

                _ => {
                    return Err(Error::ProtocolSyncError);
                }
            };
        }
    }

    /// Client loop. We handle all messages between the client and the database here.
    pub async fn handle(&mut self, mut pool: ConnectionPool) -> Result<(), Error> {
        // The client wants to cancel a query it has issued previously.
        if self.cancel_mode {
            trace!("Sending CancelRequest");

            let (process_id, secret_key, address, port) = {
                let guard = self.client_server_map.lock();

                match guard.get(&(self.process_id, self.secret_key)) {
                    // Drop the mutex as soon as possible.
                    // We found the server the client is using for its query
                    // that it wants to cancel.
                    Some((process_id, secret_key, address, port)) => (
                        process_id.clone(),
                        secret_key.clone(),
                        address.clone(),
                        port.clone(),
                    ),

                    // The client doesn't know / got the wrong server,
                    // we're closing the connection for security reasons.
                    None => return Ok(()),
                }
            };

            // Opens a new separate connection to the server, sends the backend_id
            // and secret_key and then closes it for security reasons. No other interactions
            // take place.
            return Ok(Server::cancel(&address, &port, process_id, secret_key).await?);
        }

        let mut query_router = QueryRouter::new();

        // Our custom protocol loop.
        // We expect the client to either start a transaction with regular queries
        // or issue commands for our sharding and server selection protocols.
        loop {
            trace!("Client idle, waiting for message");

            // Client idle, waiting for messages.
            self.stats.client_idle(self.process_id);

            // Read a complete message from the client, which normally would be
            // either a `Q` (query) or `P` (prepare, extended protocol).
            // We can parse it here before grabbing a server from the pool,
            // in case the client is sending some control messages, e.g.
            // SET SHARDING KEY TO 'bigint';
            let mut message = read_message(&mut self.read).await?;

            // Avoid taking a server if the client just wants to disconnect.
            if message[0] as char == 'X' {
                trace!("Client disconnecting");
                return Ok(());
            }

            // Handle admin database real quick
            if self.admin {
                trace!("Handling admin command");
                handle_admin(&mut self.write, message, pool.clone()).await?;
                continue;
            }

            // Handle all custom protocol commands here.
            match query_router.try_execute_command(message.clone()) {
                // Normal query
                None => {
                    if query_router.query_parser_enabled() && query_router.role() == None {
                        query_router.infer_role(message.clone());
                    }
                }

                Some((Command::SetShard, _)) => {
                    custom_protocol_response_ok(&mut self.write, &format!("SET SHARD")).await?;
                    continue;
                }

                Some((Command::SetShardingKey, _)) => {
                    custom_protocol_response_ok(&mut self.write, &format!("SET SHARDING KEY"))
                        .await?;
                    continue;
                }

                Some((Command::SetServerRole, _)) => {
                    custom_protocol_response_ok(&mut self.write, "SET SERVER ROLE").await?;
                    continue;
                }

                Some((Command::ShowServerRole, value)) => {
                    show_response(&mut self.write, "server role", &value).await?;
                    continue;
                }

                Some((Command::ShowShard, value)) => {
                    show_response(&mut self.write, "shard", &value).await?;
                    continue;
                }
            };

            // Make sure we selected a valid shard.
            if query_router.shard() >= pool.shards() {
                error_response(
                    &mut self.write,
                    &format!(
                        "shard '{}' is more than configured '{}'",
                        query_router.shard(),
                        pool.shards()
                    ),
                )
                .await?;
                continue;
            }

            // Waiting for server connection.
            self.stats.client_waiting(self.process_id);

            debug!("Waiting for connection from pool");

            // Grab a server from the pool: the client issued a regular query.
            let connection = match pool.get(query_router.shard(), query_router.role()).await {
                Ok(conn) => {
                    debug!("Got connection from pool");
                    conn
                }
                Err(err) => {
                    error!("Could not get connection from pool: {:?}", err);
                    error_response(&mut self.write, "could not get connection from the pool")
                        .await?;
                    continue;
                }
            };

            let mut reference = connection.0;
            let _address = connection.1;
            let server = &mut *reference;

            // Claim this server as mine for query cancellation.
            server.claim(self.process_id, self.secret_key);

            // Client active & server active
            self.stats.client_active(self.process_id);
            self.stats.server_active(server.process_id());

            debug!(
                "Client {:?} talking to server {:?}",
                self.write.peer_addr().unwrap(),
                server.address()
            );

            // Transaction loop. Multiple queries can be issued by the client here.
            // The connection belongs to the client until the transaction is over,
            // or until the client disconnects if we are in session mode.
            loop {
                let mut message = if message.len() == 0 {
                    trace!("Waiting for message inside transaction or in session mode");

                    match read_message(&mut self.read).await {
                        Ok(message) => message,
                        Err(err) => {
                            // Client disconnected without warning.
                            if server.in_transaction() {
                                // Client left dirty server. Clean up and proceed
                                // without thrashing this connection.
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

                // The message will be forwarded to the server intact. We still would like to
                // parse it below to figure out what to do with it.
                let original = message.clone();

                let code = message.get_u8() as char;
                let _len = message.get_i32() as usize;

                trace!("Message: {}", code);

                match code {
                    // ReadyForQuery
                    'Q' => {
                        debug!("Sending query to server");

                        // TODO: implement retries here for read-only transactions.
                        server.send(original).await?;

                        // Read all data the server has to offer, which can be multiple messages
                        // buffered in 8196 bytes chunks.
                        loop {
                            // TODO: implement retries here for read-only transactions.
                            let response = server.recv().await?;

                            // Send server reply to the client.
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

                        // Report query executed statistics.
                        self.stats.query();

                        // The transaction is over, we can release the connection back to the pool.
                        if !server.in_transaction() {
                            // Report transaction executed statistics.
                            self.stats.transaction();

                            // Release server back to the pool if we are in transaction mode.
                            // If we are in session mode, we keep the server until the client disconnects.
                            if self.transaction_mode {
                                self.stats.server_idle(server.process_id());
                                break;
                            }
                        }
                    }

                    // Terminate
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

                    // Parse
                    // The query with placeholders is here, e.g. `SELECT * FROM users WHERE email = $1 AND active = $2`.
                    'P' => {
                        self.buffer.put(&original[..]);
                    }

                    // Bind
                    // The placeholder's replacements are here, e.g. 'user@email.com' and 'true'
                    'B' => {
                        self.buffer.put(&original[..]);
                    }

                    // Describe
                    // Command a client can issue to describe a previously prepared named statement.
                    'D' => {
                        self.buffer.put(&original[..]);
                    }

                    // Execute
                    // Execute a prepared statement prepared in `P` and bound in `B`.
                    'E' => {
                        self.buffer.put(&original[..]);
                    }

                    // Sync
                    // Frontend (client) is asking for the query result now.
                    'S' => {
                        debug!("Sending query to server");

                        self.buffer.put(&original[..]);

                        // TODO: retries for read-only transactions.
                        server.send(self.buffer.clone()).await?;

                        self.buffer.clear();

                        // Read all data the server has to offer, which can be multiple messages
                        // buffered in 8196 bytes chunks.
                        loop {
                            // TODO: retries for read-only transactions
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

                        // Report query executed statistics.
                        self.stats.query();

                        // Release server back to the pool if we are in transaction mode.
                        // If we are in session mode, we keep the server until the client disconnects.
                        if !server.in_transaction() {
                            self.stats.transaction();

                            if self.transaction_mode {
                                self.stats.server_idle(server.process_id());
                                break;
                            }
                        }
                    }

                    // CopyData
                    'd' => {
                        // Forward the data to the server,
                        // don't buffer it since it can be rather large.
                        server.send(original).await?;
                    }

                    // CopyDone or CopyFail
                    // Copy is done, successfully or not.
                    'c' | 'f' => {
                        server.send(original).await?;

                        let response = server.recv().await?;

                        match write_all_half(&mut self.write, response).await {
                            Ok(_) => (),
                            Err(err) => {
                                server.mark_bad();
                                return Err(err);
                            }
                        };

                        // Release server back to the pool if we are in transaction mode.
                        // If we are in session mode, we keep the server until the client disconnects.
                        if !server.in_transaction() {
                            self.stats.transaction();

                            if self.transaction_mode {
                                self.stats.server_idle(server.process_id());
                                break;
                            }
                        }
                    }

                    // Some unexpected message. We either did not implement the protocol correctly
                    // or this is not a Postgres client we're talking to.
                    _ => {
                        error!("Unexpected code: {}", code);
                    }
                }
            }

            // The server is no longer bound to us, we can't cancel it's queries anymore.
            debug!("Releasing server back into the pool");
            self.release();
        }
    }

    /// Release the server from being mine. I can't cancel its queries anymore.
    pub fn release(&self) {
        let mut guard = self.client_server_map.lock();
        guard.remove(&(self.process_id, self.secret_key));
    }
}

impl Drop for Client {
    fn drop(&mut self) {
        self.stats.client_disconnecting(self.process_id);
    }
}
