/// Implementation of the PostgreSQL client.
/// We are pretending to the server in this scenario,
/// and this module implements that.
use bytes::{Buf, BufMut, BytesMut};
use once_cell::sync::OnceCell;
use regex::Regex;
use tokio::io::{AsyncReadExt, BufReader};
use tokio::net::{
    tcp::{OwnedReadHalf, OwnedWriteHalf},
    TcpStream,
};

use std::collections::HashMap;

use crate::config::Role;
use crate::constants::*;
use crate::errors::Error;
use crate::messages::*;
use crate::pool::{ClientServerMap, ConnectionPool};
use crate::server::Server;
use crate::sharding::Sharder;
use crate::stats::Reporter;

pub const SHARDING_REGEX: &str = r"SET SHARDING KEY TO '[0-9]+';";
pub const ROLE_REGEX: &str = r"SET SERVER ROLE TO '(PRIMARY|REPLICA)';";

pub static SHARDING_REGEX_RE: OnceCell<Regex> = OnceCell::new();
pub static ROLE_REGEX_RE: OnceCell<Regex> = OnceCell::new();

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

    // Unless client specifies, route queries to the servers that have this role,
    // e.g. primary or replicas or any.
    default_server_role: Option<Role>,

    // Client parameters, e.g. user, client_encoding, etc.
    #[allow(dead_code)]
    parameters: HashMap<String, String>,

    // Statistics
    stats: Reporter,
}

impl Client {
    /// Given a TCP socket, trick the client into thinking we are
    /// the Postgres server. Perform the authentication and place
    /// the client in query-ready mode.
    pub async fn startup(
        mut stream: TcpStream,
        client_server_map: ClientServerMap,
        transaction_mode: bool,
        default_server_role: Option<Role>,
        server_info: BytesMut,
        stats: Reporter,
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
                SSL_REQUEST_CODE => {
                    let mut no = BytesMut::with_capacity(1);
                    no.put_u8(b'N');

                    write_all(&mut stream, no).await?;
                }

                // Regular startup message.
                PROTOCOL_VERSION_NUMBER => {
                    // TODO: perform actual auth.
                    let parameters = parse_startup(bytes.clone())?;

                    // Generate random backend ID and secret key
                    let process_id: i32 = rand::random();
                    let secret_key: i32 = rand::random();

                    auth_ok(&mut stream).await?;
                    write_all(&mut stream, server_info).await?;
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
                        default_server_role: default_server_role,
                        parameters: parameters,
                        stats: stats,
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
                        default_server_role: default_server_role,
                        parameters: HashMap::new(),
                        stats: stats,
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

        // Active shard we're talking to.
        // The lifetime of this depends on the pool mode:
        // - if in session mode, this lives until the client disconnects,
        // - if in transaction mode, this lives for the duration of one transaction.
        let mut shard: Option<usize> = None;

        // Active database role we want to talk to, e.g. primary or replica.
        let mut role: Option<Role> = self.default_server_role;

        loop {
            // Read a complete message from the client, which normally would be
            // either a `Q` (query) or `P` (prepare, extended protocol).
            // We can parse it here before grabbing a server from the pool,
            // in case the client is sending some control messages, e.g.
            // SET SHARDING KEY TO 'bigint';
            let mut message = read_message(&mut self.read).await?;

            // Parse for special select shard command.
            // SET SHARDING KEY TO 'bigint';
            match self.select_shard(message.clone(), pool.shards()) {
                Some(s) => {
                    custom_protocol_response_ok(&mut self.write, "SET SHARDING KEY").await?;
                    shard = Some(s);
                    continue;
                }
                None => (),
            };

            // Parse for special server role selection command.
            // SET SERVER ROLE TO '(primary|replica)';
            match self.select_role(message.clone()) {
                Some(r) => {
                    custom_protocol_response_ok(&mut self.write, "SET SERVER ROLE").await?;
                    role = Some(r);
                    continue;
                }
                None => (),
            };

            // Grab a server from the pool.
            let connection = match pool.get(shard, role).await {
                Ok(conn) => conn,
                Err(err) => {
                    println!(">> Could not get connection from pool: {:?}", err);
                    return Err(err);
                }
            };

            let mut reference = connection.0;
            let _address = connection.1;
            let server = &mut *reference;

            // Claim this server as mine for query cancellation.
            server.claim(self.process_id, self.secret_key);

            // Transaction loop. Multiple queries can be issued by the client here.
            // The connection belongs to the client until the transaction is over,
            // or until the client disconnects if we are in session mode.
            loop {
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

                // The message will be forwarded to the server intact. We still would like to
                // parse it below to figure out what to do with it.
                let original = message.clone();

                let code = message.get_u8() as char;
                let _len = message.get_i32() as usize;

                match code {
                    // ReadyForQuery
                    'Q' => {
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
                                // Report this client as idle.
                                self.stats.client_idle();

                                shard = None;
                                role = self.default_server_role;

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
                                self.stats.client_idle();

                                shard = None;
                                role = self.default_server_role;

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
                                shard = None;
                                role = self.default_server_role;

                                break;
                            }
                        }
                    }

                    // Some unexpected message. We either did not implement the protocol correctly
                    // or this is not a Postgres client we're talking to.
                    _ => {
                        println!(">>> Unexpected code: {}", code);
                    }
                }
            }

            // The server is no longer bound to us, we can't cancel it's queries anymore.
            self.release();
        }
    }

    /// Release the server from being mine. I can't cancel its queries anymore.
    pub fn release(&self) {
        let mut guard = self.client_server_map.lock().unwrap();
        guard.remove(&(self.process_id, self.secret_key));
    }

    /// Determine if the query is part of our special syntax, extract
    /// the shard key, and return the shard to query based on Postgres'
    /// PARTITION BY HASH function.
    fn select_shard(&self, mut buf: BytesMut, shards: usize) -> Option<usize> {
        let code = buf.get_u8() as char;

        // Only supporting simpe protocol here, so
        // one would have to execute something like this:
        // psql -c "SET SHARDING KEY TO '1234'"
        // after sanitizing the value manually, which can be just done with an
        // int parser, e.g. `let key = "1234".parse::<i64>().unwrap()`.
        match code {
            'Q' => (),
            _ => return None,
        };

        let len = buf.get_i32();
        let query = String::from_utf8_lossy(&buf[..len as usize - 4 - 1]).to_ascii_uppercase(); // Don't read the ternminating null

        let rgx = match SHARDING_REGEX_RE.get() {
            Some(r) => r,
            None => return None,
        };

        if rgx.is_match(&query) {
            let shard = query.split("'").collect::<Vec<&str>>()[1];

            match shard.parse::<i64>() {
                Ok(shard) => {
                    let sharder = Sharder::new(shards);
                    Some(sharder.pg_bigint_hash(shard))
                }

                Err(_) => None,
            }
        } else {
            None
        }
    }

    // Pick a primary or a replica from the pool.
    fn select_role(&self, mut buf: BytesMut) -> Option<Role> {
        let code = buf.get_u8() as char;

        // Same story as select_shard() above.
        match code {
            'Q' => (),
            _ => return None,
        };

        let len = buf.get_i32();
        let query = String::from_utf8_lossy(&buf[..len as usize - 4 - 1]).to_ascii_uppercase();

        let rgx = match ROLE_REGEX_RE.get() {
            Some(r) => r,
            None => return None,
        };

        // Copy / paste from above. If we get one more of these use cases,
        // it'll be time to abstract :).
        if rgx.is_match(&query) {
            let role = query.split("'").collect::<Vec<&str>>()[1];

            match role {
                "PRIMARY" => Some(Role::Primary),
                "REPLICA" => Some(Role::Replica),
                _ => return None,
            }
        } else {
            None
        }
    }
}
