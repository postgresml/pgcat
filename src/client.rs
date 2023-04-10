use crate::errors::{ClientIdentifier, Error};
use crate::pool::BanReason;
/// Handle clients by pretending to be a PostgreSQL server.
use bytes::{Buf, BufMut, BytesMut};
use log::{debug, error, info, trace, warn};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;
use tokio::io::{split, AsyncReadExt, BufReader, ReadHalf, WriteHalf};
use tokio::net::TcpStream;
use tokio::sync::broadcast::Receiver;
use tokio::sync::mpsc::Sender;

use crate::admin::{generate_server_info_for_admin, handle_admin};
use crate::auth_passthrough::refetch_auth_hash;
use crate::config::{get_config, get_idle_client_in_transaction_timeout, Address, PoolMode};
use crate::constants::*;
use crate::messages::*;
use crate::pool::{get_pool, ClientServerMap, ConnectionPool};
use crate::query_router::{Command, QueryRouter};
use crate::server::Server;
use crate::stats::{ClientStats, PoolStats, ServerStats};
use crate::tls::Tls;

use tokio_rustls::server::TlsStream;

/// Type of connection received from client.
enum ClientConnectionType {
    Startup,
    Tls,
    CancelQuery,
}

/// The client state. One of these is created per client.
pub struct Client<S, T> {
    /// The reads are buffered (8K by default).
    read: BufReader<S>,

    /// We buffer the writes ourselves because we know the protocol
    /// better than a stock buffer.
    write: T,

    /// Internal buffer, where we place messages until we have to flush
    /// them to the backend.
    buffer: BytesMut,

    /// Address
    addr: std::net::SocketAddr,

    /// The client was started with the sole reason to cancel another running query.
    cancel_mode: bool,

    /// In transaction mode, the connection is released after each transaction.
    /// Session mode has slightly higher throughput per client, but lower capacity.
    transaction_mode: bool,

    /// For query cancellation, the client is given a random process ID and secret on startup.
    process_id: i32,
    secret_key: i32,

    /// Clients are mapped to servers while they use them. This allows a client
    /// to connect and cancel a query.
    client_server_map: ClientServerMap,

    /// Client parameters, e.g. user, client_encoding, etc.
    #[allow(dead_code)]
    parameters: HashMap<String, String>,

    /// Statistics related to this client
    stats: Arc<ClientStats>,

    /// Clients want to talk to admin database.
    admin: bool,

    /// Last address the client talked to.
    last_address_id: Option<usize>,

    /// Last server process stats we talked to.
    last_server_stats: Option<Arc<ServerStats>>,

    /// Connected to server
    connected_to_server: bool,

    /// Name of the server pool for this client (This comes from the database name in the connection string)
    pool_name: String,

    /// Postgres user for this client (This comes from the user in the connection string)
    username: String,

    /// Application name for this client (defaults to pgcat)
    application_name: String,

    /// Used to notify clients about an impending shutdown
    shutdown: Receiver<()>,
}

/// Client entrypoint.
pub async fn client_entrypoint(
    mut stream: TcpStream,
    client_server_map: ClientServerMap,
    shutdown: Receiver<()>,
    drain: Sender<i32>,
    admin_only: bool,
    tls_certificate: Option<String>,
    log_client_connections: bool,
) -> Result<(), Error> {
    // Figure out if the client wants TLS or not.
    let addr = stream.peer_addr().unwrap();

    match get_startup::<TcpStream>(&mut stream).await {
        // Client requested a TLS connection.
        Ok((ClientConnectionType::Tls, _)) => {
            // TLS settings are configured, will setup TLS now.
            if tls_certificate != None {
                debug!("Accepting TLS request");

                let mut yes = BytesMut::new();
                yes.put_u8(b'S');
                write_all(&mut stream, yes).await?;

                // Negotiate TLS.
                match startup_tls(stream, client_server_map, shutdown, admin_only).await {
                    Ok(mut client) => {
                        if log_client_connections {
                            info!("Client {:?} connected (TLS)", addr);
                        } else {
                            debug!("Client {:?} connected (TLS)", addr);
                        }

                        if !client.is_admin() {
                            let _ = drain.send(1).await;
                        }

                        let result = client.handle().await;

                        if !client.is_admin() {
                            let _ = drain.send(-1).await;

                            if result.is_err() {
                                client.stats.disconnect();
                            }
                        }

                        result
                    }
                    Err(err) => Err(err),
                }
            }
            // TLS is not configured, we cannot offer it.
            else {
                // Rejecting client request for TLS.
                let mut no = BytesMut::new();
                no.put_u8(b'N');
                write_all(&mut stream, no).await?;

                // Attempting regular startup. Client can disconnect now
                // if they choose.
                match get_startup::<TcpStream>(&mut stream).await {
                    // Client accepted unencrypted connection.
                    Ok((ClientConnectionType::Startup, bytes)) => {
                        let (read, write) = split(stream);

                        // Continue with regular startup.
                        match Client::startup(
                            read,
                            write,
                            addr,
                            bytes,
                            client_server_map,
                            shutdown,
                            admin_only,
                        )
                        .await
                        {
                            Ok(mut client) => {
                                if log_client_connections {
                                    info!("Client {:?} connected (plain)", addr);
                                } else {
                                    debug!("Client {:?} connected (plain)", addr);
                                }

                                if !client.is_admin() {
                                    let _ = drain.send(1).await;
                                }

                                let result = client.handle().await;

                                if !client.is_admin() {
                                    let _ = drain.send(-1).await;

                                    if result.is_err() {
                                        client.stats.disconnect();
                                    }
                                }

                                result
                            }
                            Err(err) => Err(err),
                        }
                    }

                    // Client probably disconnected rejecting our plain text connection.
                    Ok((ClientConnectionType::Tls, _))
                    | Ok((ClientConnectionType::CancelQuery, _)) => Err(Error::ProtocolSyncError(
                        "Bad postgres client (plain)".into(),
                    )),

                    Err(err) => Err(err),
                }
            }
        }

        // Client wants to use plain connection without encryption.
        Ok((ClientConnectionType::Startup, bytes)) => {
            let (read, write) = split(stream);

            // Continue with regular startup.
            match Client::startup(
                read,
                write,
                addr,
                bytes,
                client_server_map,
                shutdown,
                admin_only,
            )
            .await
            {
                Ok(mut client) => {
                    if log_client_connections {
                        info!("Client {:?} connected (plain)", addr);
                    } else {
                        debug!("Client {:?} connected (plain)", addr);
                    }

                    if !client.is_admin() {
                        let _ = drain.send(1).await;
                    }

                    let result = client.handle().await;

                    if !client.is_admin() {
                        let _ = drain.send(-1).await;

                        if result.is_err() {
                            client.stats.disconnect();
                        }
                    }

                    result
                }
                Err(err) => Err(err),
            }
        }

        // Client wants to cancel a query.
        Ok((ClientConnectionType::CancelQuery, bytes)) => {
            let (read, write) = split(stream);

            // Continue with cancel query request.
            match Client::cancel(read, write, addr, bytes, client_server_map, shutdown).await {
                Ok(mut client) => {
                    info!("Client {:?} issued a cancel query request", addr);

                    if !client.is_admin() {
                        let _ = drain.send(1).await;
                    }

                    let result = client.handle().await;

                    if !client.is_admin() {
                        let _ = drain.send(-1).await;

                        if result.is_err() {
                            client.stats.disconnect();
                        }
                    }
                    result
                }

                Err(err) => Err(err),
            }
        }

        // Something failed, probably the socket.
        Err(err) => Err(err),
    }
}

/// Handle the first message the client sends.
async fn get_startup<S>(stream: &mut S) -> Result<(ClientConnectionType, BytesMut), Error>
where
    S: tokio::io::AsyncRead + std::marker::Unpin + tokio::io::AsyncWrite,
{
    // Get startup message length.
    let len = match stream.read_i32().await {
        Ok(len) => len,
        Err(_) => return Err(Error::ClientBadStartup),
    };

    // Get the rest of the message.
    let mut startup = vec![0u8; len as usize - 4];
    match stream.read_exact(&mut startup).await {
        Ok(_) => (),
        Err(_) => return Err(Error::ClientBadStartup),
    };

    let mut bytes = BytesMut::from(&startup[..]);
    let code = bytes.get_i32();

    match code {
        // Client is requesting SSL (TLS).
        SSL_REQUEST_CODE => Ok((ClientConnectionType::Tls, bytes)),

        // Client wants to use plain text, requesting regular startup.
        PROTOCOL_VERSION_NUMBER => Ok((ClientConnectionType::Startup, bytes)),

        // Client is requesting to cancel a running query (plain text connection).
        CANCEL_REQUEST_CODE => Ok((ClientConnectionType::CancelQuery, bytes)),

        // Something else, probably something is wrong and it's not our fault,
        // e.g. badly implemented Postgres client.
        _ => Err(Error::ProtocolSyncError(format!(
            "Unexpected startup code: {}",
            code
        ))),
    }
}

/// Handle TLS connection negotiation.
pub async fn startup_tls(
    stream: TcpStream,
    client_server_map: ClientServerMap,
    shutdown: Receiver<()>,
    admin_only: bool,
) -> Result<Client<ReadHalf<TlsStream<TcpStream>>, WriteHalf<TlsStream<TcpStream>>>, Error> {
    // Negotiate TLS.
    let tls = Tls::new()?;
    let addr = stream.peer_addr().unwrap();

    let mut stream = match tls.acceptor.accept(stream).await {
        Ok(stream) => stream,

        // TLS negotiation failed.
        Err(err) => {
            error!("TLS negotiation failed: {:?}", err);
            return Err(Error::TlsError);
        }
    };

    // TLS negotiation successful.
    // Continue with regular startup using encrypted connection.
    match get_startup::<TlsStream<TcpStream>>(&mut stream).await {
        // Got good startup message, proceeding like normal except we
        // are encrypted now.
        Ok((ClientConnectionType::Startup, bytes)) => {
            let (read, write) = split(stream);

            Client::startup(
                read,
                write,
                addr,
                bytes,
                client_server_map,
                shutdown,
                admin_only,
            )
            .await
        }

        // Bad Postgres client.
        Ok((ClientConnectionType::Tls, _)) | Ok((ClientConnectionType::CancelQuery, _)) => {
            Err(Error::ProtocolSyncError("Bad postgres client (tls)".into()))
        }

        Err(err) => Err(err),
    }
}

impl<S, T> Client<S, T>
where
    S: tokio::io::AsyncRead + std::marker::Unpin,
    T: tokio::io::AsyncWrite + std::marker::Unpin,
{
    pub fn is_admin(&self) -> bool {
        self.admin
    }

    /// Handle Postgres client startup after TLS negotiation is complete
    /// or over plain text.
    pub async fn startup(
        mut read: S,
        mut write: T,
        addr: std::net::SocketAddr,
        bytes: BytesMut, // The rest of the startup message.
        client_server_map: ClientServerMap,
        shutdown: Receiver<()>,
        admin_only: bool,
    ) -> Result<Client<S, T>, Error> {
        let parameters = parse_startup(bytes.clone())?;

        // This parameter is mandatory by the protocol.
        let username = match parameters.get("user") {
            Some(user) => user,
            None => {
                return Err(Error::ClientError(
                    "Missing user parameter on client startup".into(),
                ))
            }
        };

        let pool_name = match parameters.get("database") {
            Some(db) => db,
            None => username,
        };

        let application_name = match parameters.get("application_name") {
            Some(application_name) => application_name,
            None => "pgcat",
        };

        let client_identifier = ClientIdentifier::new(&application_name, &username, &pool_name);

        let admin = ["pgcat", "pgbouncer"]
            .iter()
            .filter(|db| *db == pool_name)
            .count()
            == 1;

        // Kick any client that's not admin while we're in admin-only mode.
        if !admin && admin_only {
            debug!(
                "Rejecting non-admin connection to {} when in admin only mode",
                pool_name
            );
            error_response_terminal(
                &mut write,
                "terminating connection due to administrator command",
            )
            .await?;
            return Err(Error::ShuttingDown);
        }

        // Generate random backend ID and secret key
        let process_id: i32 = rand::random();
        let secret_key: i32 = rand::random();

        // Perform MD5 authentication.
        // TODO: Add SASL support.
        let salt = md5_challenge(&mut write).await?;

        let code = match read.read_u8().await {
            Ok(p) => p,
            Err(_) => {
                return Err(Error::ClientSocketError(
                    "password code".into(),
                    client_identifier,
                ))
            }
        };

        // PasswordMessage
        if code as char != 'p' {
            return Err(Error::ProtocolSyncError(format!(
                "Expected p, got {}",
                code as char
            )));
        }

        let len = match read.read_i32().await {
            Ok(len) => len,
            Err(_) => {
                return Err(Error::ClientSocketError(
                    "password message length".into(),
                    client_identifier,
                ))
            }
        };

        let mut password_response = vec![0u8; (len - 4) as usize];

        match read.read_exact(&mut password_response).await {
            Ok(_) => (),
            Err(_) => {
                return Err(Error::ClientSocketError(
                    "password message".into(),
                    client_identifier,
                ))
            }
        };

        // Authenticate admin user.
        let (transaction_mode, server_info) = if admin {
            let config = get_config();

            // Compare server and client hashes.
            let password_hash = md5_hash_password(
                &config.general.admin_username,
                &config.general.admin_password,
                &salt,
            );

            if password_hash != password_response {
                let error = Error::ClientGeneralError("Invalid password".into(), client_identifier);

                warn!("{}", error);
                wrong_password(&mut write, username).await?;

                return Err(error);
            }

            (false, generate_server_info_for_admin())
        }
        // Authenticate normal user.
        else {
            let mut pool = match get_pool(pool_name, username) {
                Some(pool) => pool,
                None => {
                    error_response(
                        &mut write,
                        &format!(
                            "No pool configured for database: {:?}, user: {:?}",
                            pool_name, username
                        ),
                    )
                    .await?;

                    return Err(Error::ClientGeneralError(
                        "Invalid pool name".into(),
                        client_identifier,
                    ));
                }
            };

            // Obtain the hash to compare, we give preference to that written in cleartext in config
            // if there is nothing set in cleartext and auth passthrough (auth_query) is configured, we use the hash obtained
            // when the pool was created. If there is no hash there, we try to fetch it one more time.
            let password_hash = if let Some(password) = &pool.settings.user.password {
                Some(md5_hash_password(username, password, &salt))
            } else {
                if !get_config().is_auth_query_configured() {
                    return Err(Error::ClientAuthImpossible(username.into()));
                }

                let mut hash = (*pool.auth_hash.read()).clone();

                if hash.is_none() {
                    warn!(
                        "Query auth configured \
                          but no hash password found \
                          for pool {}. Will try to refetch it.",
                        pool_name
                    );

                    match refetch_auth_hash(&pool).await {
                        Ok(fetched_hash) => {
                            warn!("Password for {}, obtained. Updating.", client_identifier);

                            {
                                let mut pool_auth_hash = pool.auth_hash.write();
                                *pool_auth_hash = Some(fetched_hash.clone());
                            }

                            hash = Some(fetched_hash);
                        }

                        Err(err) => {
                            return Err(Error::ClientAuthPassthroughError(
                                err.to_string(),
                                client_identifier,
                            ));
                        }
                    }
                };

                Some(md5_hash_second_pass(&hash.unwrap(), &salt))
            };

            // Once we have the resulting hash, we compare with what the client gave us.
            // If they do not match and auth query is set up, we try to refetch the hash one more time
            // to see if the password has changed since the pool was created.
            //
            // @TODO: we could end up fetching again the same password twice (see above).
            if password_hash.unwrap() != password_response {
                warn!(
                    "Invalid password {}, will try to refetch it.",
                    client_identifier
                );

                let fetched_hash = refetch_auth_hash(&pool).await?;
                let new_password_hash = md5_hash_second_pass(&fetched_hash, &salt);

                // Ok password changed in server an auth is possible.
                if new_password_hash == password_response {
                    warn!(
                        "Password for {}, changed in server. Updating.",
                        client_identifier
                    );

                    {
                        let mut pool_auth_hash = pool.auth_hash.write();
                        *pool_auth_hash = Some(fetched_hash);
                    }
                } else {
                    wrong_password(&mut write, username).await?;
                    return Err(Error::ClientGeneralError(
                        "Invalid password".into(),
                        client_identifier,
                    ));
                }
            }

            let transaction_mode = pool.settings.pool_mode == PoolMode::Transaction;

            // If the pool hasn't been validated yet,
            // connect to the servers and figure out what's what.
            if !pool.validated() {
                match pool.validate().await {
                    Ok(_) => (),
                    Err(err) => {
                        error_response(
                            &mut write,
                            &format!(
                                "Pool down for database: {:?}, user: {:?}",
                                pool_name, username
                            ),
                        )
                        .await?;
                        return Err(Error::ClientError(format!("Pool down: {:?}", err)));
                    }
                }
            }

            (transaction_mode, pool.server_info())
        };

        debug!("Password authentication successful");

        auth_ok(&mut write).await?;
        write_all(&mut write, server_info).await?;
        backend_key_data(&mut write, process_id, secret_key).await?;
        ready_for_query(&mut write).await?;

        trace!("Startup OK");
        let pool_stats = match get_pool(pool_name, username) {
            Some(pool) => {
                if !admin {
                    pool.stats
                } else {
                    Arc::new(PoolStats::default())
                }
            }
            None => Arc::new(PoolStats::default()),
        };

        let stats = Arc::new(ClientStats::new(
            process_id,
            application_name,
            username,
            pool_name,
            tokio::time::Instant::now(),
            pool_stats,
        ));

        Ok(Client {
            read: BufReader::new(read),
            write,
            addr,
            buffer: BytesMut::with_capacity(8196),
            cancel_mode: false,
            transaction_mode,
            process_id,
            secret_key,
            client_server_map,
            parameters: parameters.clone(),
            stats,
            admin,
            last_address_id: None,
            last_server_stats: None,
            pool_name: pool_name.clone(),
            username: username.clone(),
            application_name: application_name.to_string(),
            shutdown,
            connected_to_server: false,
        })
    }

    /// Handle cancel request.
    pub async fn cancel(
        read: S,
        write: T,
        addr: std::net::SocketAddr,
        mut bytes: BytesMut, // The rest of the startup message.
        client_server_map: ClientServerMap,
        shutdown: Receiver<()>,
    ) -> Result<Client<S, T>, Error> {
        let process_id = bytes.get_i32();
        let secret_key = bytes.get_i32();
        Ok(Client {
            read: BufReader::new(read),
            write,
            addr,
            buffer: BytesMut::with_capacity(8196),
            cancel_mode: true,
            transaction_mode: false,
            process_id,
            secret_key,
            client_server_map,
            parameters: HashMap::new(),
            stats: Arc::new(ClientStats::default()),
            admin: false,
            last_address_id: None,
            last_server_stats: None,
            pool_name: String::from("undefined"),
            username: String::from("undefined"),
            application_name: String::from("undefined"),
            shutdown,
            connected_to_server: false,
        })
    }

    /// Handle a connected and authenticated client.
    pub async fn handle(&mut self) -> Result<(), Error> {
        // The client wants to cancel a query it has issued previously.
        if self.cancel_mode {
            trace!("Sending CancelRequest");

            let (process_id, secret_key, address, port) = {
                let guard = self.client_server_map.lock();

                match guard.get(&(self.process_id, self.secret_key)) {
                    // Drop the mutex as soon as possible.
                    // We found the server the client is using for its query
                    // that it wants to cancel.
                    Some((process_id, secret_key, address, port)) => {
                        (*process_id, *secret_key, address.clone(), *port)
                    }

                    // The client doesn't know / got the wrong server,
                    // we're closing the connection for security reasons.
                    None => return Ok(()),
                }
            };

            // Opens a new separate connection to the server, sends the backend_id
            // and secret_key and then closes it for security reasons. No other interactions
            // take place.
            return Server::cancel(&address, port, process_id, secret_key).await;
        }

        // The query router determines where the query is going to go,
        // e.g. primary, replica, which shard.
        let mut query_router = QueryRouter::new();

        self.stats.register(self.stats.clone());

        // Our custom protocol loop.
        // We expect the client to either start a transaction with regular queries
        // or issue commands for our sharding and server selection protocol.
        loop {
            trace!(
                "Client idle, waiting for message, transaction mode: {}",
                self.transaction_mode
            );

            // Read a complete message from the client, which normally would be
            // either a `Q` (query) or `P` (prepare, extended protocol).
            // We can parse it here before grabbing a server from the pool,
            // in case the client is sending some custom protocol messages, e.g.
            // SET SHARDING KEY TO 'bigint';

            let message = tokio::select! {
                _ = self.shutdown.recv() => {
                    if !self.admin {
                        error_response_terminal(
                            &mut self.write,
                            "terminating connection due to administrator command"
                        ).await?;

                        self.stats.disconnect();
                        return Ok(());
                    }

                    // Admin clients ignore shutdown.
                    else {
                        read_message(&mut self.read).await?
                    }
                },
                message_result = read_message(&mut self.read) => message_result?
            };

            match message[0] as char {
                // Buffer extended protocol messages even if we do not have
                // a server connection yet. Hopefully, when we get the S message
                // we'll be able to allocate a connection. Also, clients do not expect
                // the server to respond to these messages so even if we were not able to
                // allocate a connection, we wouldn't be able to send back an error message
                // to the client so we buffer them and defer the decision to error out or not
                // to when we get the S message
                'D' | 'E' => {
                    self.buffer.put(&message[..]);
                    continue;
                }

                'Q' => {
                    if query_router.query_parser_enabled() {
                        query_router.infer(&message);
                    }
                }

                'P' => {
                    self.buffer.put(&message[..]);

                    if query_router.query_parser_enabled() {
                        query_router.infer(&message);
                    }

                    continue;
                }

                'B' => {
                    self.buffer.put(&message[..]);

                    if query_router.query_parser_enabled() {
                        query_router.infer_shard_from_bind(&message);
                    }

                    continue;
                }

                'X' => {
                    debug!("Client disconnecting");

                    self.stats.disconnect();

                    return Ok(());
                }

                _ => (),
            }

            // Handle admin database queries.
            if self.admin {
                debug!("Handling admin command");
                handle_admin(&mut self.write, message, self.client_server_map.clone()).await?;
                continue;
            }

            // Get a pool instance referenced by the most up-to-date
            // pointer. This ensures we always read the latest config
            // when starting a query.
            let mut pool = self.get_pool().await?;

            // Check if the pool is paused and wait until it's resumed.
            if pool.wait_paused().await {
                // Refresh pool information, something might have changed.
                pool = self.get_pool().await?;
            }

            query_router.update_pool_settings(pool.settings.clone());

            let current_shard = query_router.shard();

            // Handle all custom protocol commands, if any.
            match query_router.try_execute_command(&message) {
                // Normal query, not a custom command.
                None => (),

                // SET SHARD TO
                Some((Command::SetShard, _)) => {
                    // Selected shard is not configured.
                    if query_router.shard() >= pool.shards() {
                        // Set the shard back to what it was.
                        query_router.set_shard(current_shard);

                        error_response(
                            &mut self.write,
                            &format!(
                                "shard {} is more than configured {}, staying on shard {} (shard numbers start at 0)",
                                query_router.shard(),
                                pool.shards(),
                                current_shard,
                            ),
                        )
                            .await?;
                    } else {
                        custom_protocol_response_ok(&mut self.write, "SET SHARD").await?;
                    }
                    continue;
                }

                // SET PRIMARY READS TO
                Some((Command::SetPrimaryReads, _)) => {
                    custom_protocol_response_ok(&mut self.write, "SET PRIMARY READS").await?;
                    continue;
                }

                // SET SHARDING KEY TO
                Some((Command::SetShardingKey, _)) => {
                    custom_protocol_response_ok(&mut self.write, "SET SHARDING KEY").await?;
                    continue;
                }

                // SET SERVER ROLE TO
                Some((Command::SetServerRole, _)) => {
                    custom_protocol_response_ok(&mut self.write, "SET SERVER ROLE").await?;
                    continue;
                }

                // SHOW SERVER ROLE
                Some((Command::ShowServerRole, value)) => {
                    show_response(&mut self.write, "server role", &value).await?;
                    continue;
                }

                // SHOW SHARD
                Some((Command::ShowShard, value)) => {
                    show_response(&mut self.write, "shard", &value).await?;
                    continue;
                }

                // SHOW PRIMARY READS
                Some((Command::ShowPrimaryReads, value)) => {
                    show_response(&mut self.write, "primary reads", &value).await?;
                    continue;
                }
            };

            debug!("Waiting for connection from pool");
            if !self.admin {
                self.stats.waiting();
            }

            // Grab a server from the pool.
            let connection = match pool
                .get(query_router.shard(), query_router.role(), &self.stats)
                .await
            {
                Ok(conn) => {
                    debug!("Got connection from pool");
                    conn
                }
                Err(err) => {
                    // Client is attempting to get results from the server,
                    // but we were unable to grab a connection from the pool
                    // We'll send back an error message and clean the extended
                    // protocol buffer
                    self.stats.idle();

                    if message[0] as char == 'S' {
                        error!("Got Sync message but failed to get a connection from the pool");
                        self.buffer.clear();
                    }

                    error_response(&mut self.write, "could not get connection from the pool")
                        .await?;

                    error!(
                        "Could not get connection from pool: \
                        {{ \
                            pool_name: {:?}, \
                            username: {:?}, \
                            shard: {:?}, \
                            role: \"{:?}\", \
                            error: \"{:?}\" \
                        }}",
                        self.pool_name,
                        self.username,
                        query_router.shard(),
                        query_router.role(),
                        err
                    );

                    continue;
                }
            };

            let mut reference = connection.0;
            let address = connection.1;
            let server = &mut *reference;

            // Server is assigned to the client in case the client wants to
            // cancel a query later.
            server.claim(self.process_id, self.secret_key);
            self.connected_to_server = true;

            // Update statistics
            self.stats.active();

            self.last_address_id = Some(address.id);
            self.last_server_stats = Some(server.stats());

            debug!(
                "Client {:?} talking to server {:?}",
                self.addr,
                server.address()
            );

            // TODO: investigate other parameters and set them too.

            // Set application_name.
            server.set_name(&self.application_name).await?;

            let mut initial_message = Some(message);

            let idle_client_timeout_duration = match get_idle_client_in_transaction_timeout() {
                0 => tokio::time::Duration::MAX,
                timeout => tokio::time::Duration::from_millis(timeout),
            };

            // Transaction loop. Multiple queries can be issued by the client here.
            // The connection belongs to the client until the transaction is over,
            // or until the client disconnects if we are in session mode.
            //
            // If the client is in session mode, no more custom protocol
            // commands will be accepted.
            loop {
                let message = match initial_message {
                    None => {
                        trace!("Waiting for message inside transaction or in session mode");

                        match tokio::time::timeout(
                            idle_client_timeout_duration,
                            read_message(&mut self.read),
                        )
                        .await
                        {
                            Ok(Ok(message)) => message,
                            Ok(Err(err)) => {
                                // Client disconnected inside a transaction.
                                // Clean up the server and re-use it.
                                self.stats.disconnect();
                                server.checkin_cleanup().await?;

                                return Err(err);
                            }
                            Err(_) => {
                                // Client idle in transaction timeout
                                error_response(&mut self.write, "idle transaction timeout").await?;
                                error!(
                                    "Client idle in transaction timeout: \
                                    {{ \
                                        pool_name: {}, \
                                        username: {}, \
                                        shard: {}, \
                                        role: \"{:?}\" \
                                    }}",
                                    self.pool_name,
                                    self.username,
                                    query_router.shard(),
                                    query_router.role()
                                );

                                break;
                            }
                        }
                    }

                    Some(message) => {
                        initial_message = None;
                        message
                    }
                };

                // The message will be forwarded to the server intact. We still would like to
                // parse it below to figure out what to do with it.

                // Safe to unwrap because we know this message has a certain length and has the code
                // This reads the first byte without advancing the internal pointer and mutating the bytes
                let code = *message.get(0).unwrap() as char;

                trace!("Message: {}", code);

                match code {
                    // Query
                    'Q' => {
                        debug!("Sending query to server");

                        self.send_and_receive_loop(
                            code,
                            Some(&message),
                            server,
                            &address,
                            &pool,
                            &self.stats.clone(),
                        )
                        .await?;

                        if !server.in_transaction() {
                            // Report transaction executed statistics.
                            self.stats.transaction();
                            server.stats().transaction(&self.application_name);

                            // Release server back to the pool if we are in transaction mode.
                            // If we are in session mode, we keep the server until the client disconnects.
                            if self.transaction_mode {
                                self.stats.idle();

                                break;
                            }
                        }
                    }

                    // Terminate
                    'X' => {
                        server.checkin_cleanup().await?;
                        self.stats.disconnect();
                        self.release();

                        return Ok(());
                    }

                    // Parse
                    // The query with placeholders is here, e.g. `SELECT * FROM users WHERE email = $1 AND active = $2`.
                    'P' => {
                        self.buffer.put(&message[..]);
                    }

                    // Bind
                    // The placeholder's replacements are here, e.g. 'user@email.com' and 'true'
                    'B' => {
                        self.buffer.put(&message[..]);
                    }

                    // Describe
                    // Command a client can issue to describe a previously prepared named statement.
                    'D' => {
                        self.buffer.put(&message[..]);
                    }

                    // Execute
                    // Execute a prepared statement prepared in `P` and bound in `B`.
                    'E' => {
                        self.buffer.put(&message[..]);
                    }

                    // Sync
                    // Frontend (client) is asking for the query result now.
                    'S' => {
                        debug!("Sending query to server");

                        self.buffer.put(&message[..]);

                        let first_message_code = (*self.buffer.get(0).unwrap_or(&0)) as char;

                        // Almost certainly true
                        if first_message_code == 'P' {
                            // Message layout
                            // P followed by 32 int followed by null-terminated statement name
                            // So message code should be in offset 0 of the buffer, first character
                            // in prepared statement name would be index 5
                            let first_char_in_name = *self.buffer.get(5).unwrap_or(&0);
                            if first_char_in_name != 0 {
                                // This is a named prepared statement
                                // Server connection state will need to be cleared at checkin
                                server.mark_dirty();
                            }
                        }

                        self.send_and_receive_loop(
                            code,
                            None,
                            server,
                            &address,
                            &pool,
                            &self.stats.clone(),
                        )
                        .await?;

                        self.buffer.clear();

                        if !server.in_transaction() {
                            self.stats.transaction();
                            server.stats().transaction(&self.application_name);

                            // Release server back to the pool if we are in transaction mode.
                            // If we are in session mode, we keep the server until the client disconnects.
                            if self.transaction_mode {
                                break;
                            }
                        }
                    }

                    // CopyData
                    'd' => {
                        self.buffer.put(&message[..]);

                        // Want to limit buffer size
                        if self.buffer.len() > 8196 {
                            // Forward the data to the server,
                            self.send_server_message(server, &self.buffer, &address, &pool)
                                .await?;
                            self.buffer.clear();
                        }
                    }

                    // CopyDone or CopyFail
                    // Copy is done, successfully or not.
                    'c' | 'f' => {
                        // We may already have some copy data in the buffer, add this message to buffer
                        self.buffer.put(&message[..]);

                        self.send_server_message(server, &self.buffer, &address, &pool)
                            .await?;

                        // Clear the buffer
                        self.buffer.clear();

                        let response = self
                            .receive_server_message(server, &address, &pool, &self.stats.clone())
                            .await?;

                        match write_all_half(&mut self.write, &response).await {
                            Ok(_) => (),
                            Err(err) => {
                                server.mark_bad();
                                return Err(err);
                            }
                        };

                        if !server.in_transaction() {
                            self.stats.transaction();
                            server.stats().transaction(&self.application_name);

                            // Release server back to the pool if we are in transaction mode.
                            // If we are in session mode, we keep the server until the client disconnects.
                            if self.transaction_mode {
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
            server.checkin_cleanup().await?;
            server.stats().idle();
            self.connected_to_server = false;

            self.release();
            self.stats.idle();
        }
    }

    /// Retrieve connection pool, if it exists.
    /// Return an error to the client otherwise.
    async fn get_pool(&mut self) -> Result<ConnectionPool, Error> {
        match get_pool(&self.pool_name, &self.username) {
            Some(pool) => Ok(pool),
            None => {
                error_response(
                    &mut self.write,
                    &format!(
                        "No pool configured for database: {}, user: {}",
                        self.pool_name, self.username
                    ),
                )
                .await?;

                Err(Error::ClientError(format!(
                    "Invalid pool name {{ username: {}, pool_name: {}, application_name: {} }}",
                    self.pool_name, self.username, self.application_name
                )))
            }
        }
    }

    /// Release the server from the client: it can't cancel its queries anymore.
    pub fn release(&self) {
        let mut guard = self.client_server_map.lock();
        guard.remove(&(self.process_id, self.secret_key));
    }

    async fn send_and_receive_loop(
        &mut self,
        code: char,
        message: Option<&BytesMut>,
        server: &mut Server,
        address: &Address,
        pool: &ConnectionPool,
        client_stats: &ClientStats,
    ) -> Result<(), Error> {
        debug!("Sending {} to server", code);

        let message = match message {
            Some(message) => message,
            None => &self.buffer,
        };

        self.send_server_message(server, message, address, pool)
            .await?;

        let query_start = Instant::now();
        // Read all data the server has to offer, which can be multiple messages
        // buffered in 8196 bytes chunks.
        loop {
            let response = self
                .receive_server_message(server, address, pool, client_stats)
                .await?;

            match write_all_half(&mut self.write, &response).await {
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
        client_stats.query();
        server.stats().query(
            Instant::now().duration_since(query_start).as_millis() as u64,
            &self.application_name,
        );

        Ok(())
    }

    async fn send_server_message(
        &self,
        server: &mut Server,
        message: &BytesMut,
        address: &Address,
        pool: &ConnectionPool,
    ) -> Result<(), Error> {
        match server.send(message).await {
            Ok(_) => Ok(()),
            Err(err) => {
                pool.ban(address, BanReason::MessageSendFailed, Some(&self.stats));
                Err(err)
            }
        }
    }

    async fn receive_server_message(
        &mut self,
        server: &mut Server,
        address: &Address,
        pool: &ConnectionPool,
        client_stats: &ClientStats,
    ) -> Result<BytesMut, Error> {
        if pool.settings.user.statement_timeout > 0 {
            match tokio::time::timeout(
                tokio::time::Duration::from_millis(pool.settings.user.statement_timeout),
                server.recv(),
            )
            .await
            {
                Ok(result) => match result {
                    Ok(message) => Ok(message),
                    Err(err) => {
                        pool.ban(address, BanReason::MessageReceiveFailed, Some(client_stats));
                        error_response_terminal(
                            &mut self.write,
                            &format!("error receiving data from server: {:?}", err),
                        )
                        .await?;
                        Err(err)
                    }
                },
                Err(_) => {
                    error!(
                        "Statement timeout while talking to {:?} with user {}",
                        address, pool.settings.user.username
                    );
                    server.mark_bad();
                    pool.ban(address, BanReason::StatementTimeout, Some(client_stats));
                    error_response_terminal(&mut self.write, "pool statement timeout").await?;
                    Err(Error::StatementTimeout)
                }
            }
        } else {
            match server.recv().await {
                Ok(message) => Ok(message),
                Err(err) => {
                    pool.ban(address, BanReason::MessageReceiveFailed, Some(client_stats));
                    error_response_terminal(
                        &mut self.write,
                        &format!("error receiving data from server: {:?}", err),
                    )
                    .await?;
                    Err(err)
                }
            }
        }
    }
}

impl<S, T> Drop for Client<S, T> {
    fn drop(&mut self) {
        let mut guard = self.client_server_map.lock();
        guard.remove(&(self.process_id, self.secret_key));

        // Dirty shutdown
        // TODO: refactor, this is not the best way to handle state management.

        if self.connected_to_server && self.last_server_stats.is_some() {
            self.last_server_stats.as_ref().unwrap().idle();
        }
    }
}
