use crate::errors::{ClientIdentifier, Error};
use crate::pool::BanReason;
/// Handle clients by pretending to be a PostgreSQL server.
use bytes::{Buf, BufMut, BytesMut};
use log::{debug, error, info, trace, warn};
use once_cell::sync::Lazy;
use std::collections::{HashMap, VecDeque};
use std::sync::{atomic::AtomicUsize, Arc};
use std::time::Instant;
use tokio::io::{split, AsyncReadExt, BufReader, ReadHalf, WriteHalf};
use tokio::net::TcpStream;
use tokio::sync::broadcast::Receiver;
use tokio::sync::mpsc::Sender;

use crate::admin::{generate_server_parameters_for_admin, handle_admin};
use crate::auth_passthrough::refetch_auth_hash;
use crate::config::{
    get_config, get_idle_client_in_transaction_timeout, Address, AuthType, PoolMode,
};
use crate::constants::*;
use crate::messages::*;
use crate::plugins::PluginOutput;
use crate::pool::{get_pool, ClientServerMap, ConnectionPool};
use crate::query_router::{Command, QueryRouter};
use crate::server::{Server, ServerParameters};
use crate::stats::{ClientStats, ServerStats};
use crate::tls::Tls;

use tokio_rustls::server::TlsStream;

/// Incrementally count prepared statements
/// to avoid random conflicts in places where the random number generator is weak.
pub static PREPARED_STATEMENT_COUNTER: Lazy<Arc<AtomicUsize>> =
    Lazy::new(|| Arc::new(AtomicUsize::new(0)));

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

    /// Used to buffer response messages to the client
    response_message_queue_buffer: BytesMut,

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

    /// Server startup and session parameters that we're going to track
    server_parameters: ServerParameters,

    /// Used to notify clients about an impending shutdown
    shutdown: Receiver<()>,

    /// Whether prepared statements are enabled for this client
    prepared_statements_enabled: bool,

    /// Mapping of client named prepared statement to rewritten parse messages
    prepared_statements: HashMap<String, (Arc<Parse>, u64)>,

    /// Buffered extended protocol data
    extended_protocol_data_buffer: VecDeque<ExtendedProtocolData>,
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
    let addr = match stream.peer_addr() {
        Ok(addr) => addr,
        Err(err) => {
            return Err(Error::SocketError(format!(
                "Failed to get peer address: {:?}",
                err
            )));
        }
    };

    match get_startup::<TcpStream>(&mut stream).await {
        // Client requested a TLS connection.
        Ok((ClientConnectionType::Tls, _)) => {
            // TLS settings are configured, will setup TLS now.
            if tls_certificate.is_some() {
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
                        }

                        if result.is_err() {
                            client.stats.disconnect();
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
                                }

                                if result.is_err() {
                                    client.stats.disconnect();
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
                    }

                    if result.is_err() {
                        client.stats.disconnect();
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
                    }

                    if result.is_err() {
                        client.stats.disconnect();
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
    let addr = match stream.peer_addr() {
        Ok(addr) => addr,
        Err(err) => {
            return Err(Error::SocketError(format!(
                "Failed to get peer address: {:?}",
                err
            )));
        }
    };

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

        let client_identifier = ClientIdentifier::new(application_name, username, pool_name);

        let admin = ["pgcat", "pgbouncer"]
            .iter()
            .filter(|db| *db == pool_name)
            .count()
            == 1;

        if !admin && admin_only {
            // Kick any client that's not admin while we're in admin-only mode.
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

        let mut prepared_statements_enabled = false;

        // Authenticate admin user.
        let (transaction_mode, mut server_parameters) = if admin {
            let config = get_config();
            // TODO: Add SASL support.
            // Perform MD5 authentication.
            match config.general.admin_auth_type {
                AuthType::Trust => (),
                AuthType::MD5 => {
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

                    // Compare server and client hashes.
                    let password_hash = md5_hash_password(
                        &config.general.admin_username,
                        &config.general.admin_password,
                        &salt,
                    );

                    if password_hash != password_response {
                        let error =
                            Error::ClientGeneralError("Invalid password".into(), client_identifier);

                        warn!("{}", error);
                        wrong_password(&mut write, username).await?;

                        return Err(error);
                    }
                }
            }
            (false, generate_server_parameters_for_admin())
        }
        // Authenticate normal user.
        else {
            let pool = match get_pool(pool_name, username) {
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
            match pool.settings.user.auth_type {
                AuthType::Trust => (),
                AuthType::MD5 => {
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

                    let password_hash = if let Some(password) = &pool.settings.user.password {
                        Some(md5_hash_password(username, password, &salt))
                    } else {
                        if !get_config().is_auth_query_configured() {
                            wrong_password(&mut write, username).await?;
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
                                    warn!(
                                        "Password for {}, obtained. Updating.",
                                        client_identifier
                                    );

                                    {
                                        let mut pool_auth_hash = pool.auth_hash.write();
                                        *pool_auth_hash = Some(fetched_hash.clone());
                                    }

                                    hash = Some(fetched_hash);
                                }

                                Err(err) => {
                                    wrong_password(&mut write, username).await?;

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

                        let fetched_hash = match refetch_auth_hash(&pool).await {
                            Ok(fetched_hash) => fetched_hash,
                            Err(err) => {
                                wrong_password(&mut write, username).await?;

                                return Err(err);
                            }
                        };

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
                }
            }
            let transaction_mode = pool.settings.pool_mode == PoolMode::Transaction;
            prepared_statements_enabled =
                transaction_mode && pool.prepared_statement_cache.is_some();

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

            (transaction_mode, pool.server_parameters())
        };

        // Update the parameters to merge what the application sent and what's originally on the server
        server_parameters.set_from_hashmap(&parameters, false);

        debug!("Password authentication successful");

        auth_ok(&mut write).await?;
        write_all(&mut write, (&server_parameters).into()).await?;
        backend_key_data(&mut write, process_id, secret_key).await?;
        send_ready_for_query(&mut write).await?;

        trace!("Startup OK");
        let stats = Arc::new(ClientStats::new(
            process_id,
            application_name,
            username,
            pool_name,
            tokio::time::Instant::now(),
        ));

        Ok(Client {
            read: BufReader::new(read),
            write,
            buffer: BytesMut::with_capacity(8196),
            response_message_queue_buffer: BytesMut::with_capacity(8196),
            addr,
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
            connected_to_server: false,
            pool_name: pool_name.clone(),
            username: username.clone(),
            server_parameters,
            shutdown,
            prepared_statements_enabled,
            prepared_statements: HashMap::new(),
            extended_protocol_data_buffer: VecDeque::new(),
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
            buffer: BytesMut::with_capacity(8196),
            response_message_queue_buffer: BytesMut::with_capacity(8196),
            addr,
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
            connected_to_server: false,
            pool_name: String::from("undefined"),
            username: String::from("undefined"),
            server_parameters: ServerParameters::new(),
            shutdown,
            prepared_statements_enabled: false,
            prepared_statements: HashMap::new(),
            extended_protocol_data_buffer: VecDeque::new(),
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

        // Result returned by one of the plugins.
        let mut plugin_output = None;

        let client_identifier = ClientIdentifier::new(
            self.server_parameters.get_application_name(),
            &self.username,
            &self.pool_name,
        );

        // Get a pool instance referenced by the most up-to-date
        // pointer. This ensures we always read the latest config
        // when starting a query.
        let mut pool = if self.admin {
            // Admin clients do not use pools.
            ConnectionPool::default()
        } else {
            self.get_pool().await?
        };

        query_router.update_pool_settings(&pool.settings);
        query_router.set_default_role();

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

            if message[0] as char == 'X' {
                debug!("Client disconnecting");

                self.stats.disconnect();

                return Ok(());
            }

            // Handle admin database queries.
            if self.admin {
                debug!("Handling admin command");
                handle_admin(&mut self.write, message, self.client_server_map.clone()).await?;
                continue;
            }

            // Handle all custom protocol commands, if any.
            if self
                .handle_custom_protocol(&mut query_router, &message, &pool)
                .await?
            {
                continue;
            }

            let mut initial_parsed_ast = None;

            match message[0] as char {
                // Query
                'Q' => {
                    if query_router.query_parser_enabled() {
                        match query_router.parse(&message) {
                            Ok(ast) => {
                                let plugin_result = query_router.execute_plugins(&ast).await;

                                match plugin_result {
                                    Ok(PluginOutput::Deny(error)) => {
                                        error_response(&mut self.write, &error).await?;
                                        continue;
                                    }

                                    Ok(PluginOutput::Intercept(result)) => {
                                        write_all(&mut self.write, result).await?;
                                        continue;
                                    }

                                    _ => (),
                                };

                                let _ = query_router.infer(&ast);

                                initial_parsed_ast = Some(ast);
                            }
                            Err(error) => {
                                warn!(
                                    "Query parsing error: {} (client: {})",
                                    error, client_identifier
                                );
                            }
                        }
                    }
                }

                // Buffer extended protocol messages even if we do not have
                // a server connection yet. Hopefully, when we get the S message
                // we'll be able to allocate a connection. Also, clients do not expect
                // the server to respond to these messages so even if we were not able to
                // allocate a connection, we wouldn't be able to send back an error message
                // to the client so we buffer them and defer the decision to error out or not
                // to when we get the S message
                // Parse
                'P' => {
                    if query_router.query_parser_enabled() {
                        match query_router.parse(&message) {
                            Ok(ast) => {
                                if let Ok(output) = query_router.execute_plugins(&ast).await {
                                    plugin_output = Some(output);
                                }

                                let _ = query_router.infer(&ast);
                            }
                            Err(error) => {
                                warn!(
                                    "Query parsing error: {} (client: {})",
                                    error, client_identifier
                                );
                            }
                        };
                    }

                    self.buffer_parse(message, &pool)?;

                    continue;
                }

                // Bind
                'B' => {
                    if query_router.query_parser_enabled() {
                        query_router.infer_shard_from_bind(&message);
                    }

                    self.buffer_bind(message).await?;

                    continue;
                }

                // Describe
                'D' => {
                    self.buffer_describe(message).await?;
                    continue;
                }

                'E' => {
                    self.extended_protocol_data_buffer
                        .push_back(ExtendedProtocolData::create_new_execute(message));
                    continue;
                }

                // Close (F)
                'C' => {
                    let close: Close = (&message).try_into()?;

                    self.extended_protocol_data_buffer
                        .push_back(ExtendedProtocolData::create_new_close(message, close));
                    continue;
                }

                _ => (),
            }

            // Check on plugin results.
            if let Some(PluginOutput::Deny(error)) = plugin_output {
                self.reset_buffered_state();
                error_response(&mut self.write, &error).await?;
                plugin_output = None;
                continue;
            };

            // Check if the pool is paused and wait until it's resumed.
            pool.wait_paused().await;

            // Refresh pool information, something might have changed.
            pool = self.get_pool().await?;
            query_router.update_pool_settings(&pool.settings);

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
                        self.reset_buffered_state();
                    }

                    error_response(
                        &mut self.write,
                        format!("could not get connection from the pool - {}", err).as_str(),
                    )
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

            server.sync_parameters(&self.server_parameters).await?;

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

                        // This is not an initial message so discard the initial_parsed_ast
                        initial_parsed_ast.take();

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
                                        shard: {:?}, \
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
                let code = *message.first().unwrap() as char;

                trace!("Client message: {}", code);

                match code {
                    // Query
                    'Q' => {
                        if query_router.query_parser_enabled() {
                            // We don't want to parse again if we already parsed it as the initial message
                            let ast = match initial_parsed_ast {
                                Some(_) => Some(initial_parsed_ast.take().unwrap()),
                                None => match query_router.parse(&message) {
                                    Ok(ast) => Some(ast),
                                    Err(error) => {
                                        warn!(
                                            "Query parsing error: {} (client: {})",
                                            error, client_identifier
                                        );
                                        None
                                    }
                                },
                            };

                            if let Some(ast) = ast {
                                let plugin_result = query_router.execute_plugins(&ast).await;

                                match plugin_result {
                                    Ok(PluginOutput::Deny(error)) => {
                                        error_response(&mut self.write, &error).await?;
                                        continue;
                                    }

                                    Ok(PluginOutput::Intercept(result)) => {
                                        write_all(&mut self.write, result).await?;
                                        continue;
                                    }

                                    _ => (),
                                };
                            }
                        }

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
                            server
                                .stats()
                                .transaction(self.server_parameters.get_application_name());

                            // Release server back to the pool if we are in transaction mode.
                            // If we are in session mode, we keep the server until the client disconnects.
                            if self.transaction_mode && !server.in_copy_mode() {
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
                        if query_router.query_parser_enabled() {
                            if let Ok(ast) = query_router.parse(&message) {
                                if let Ok(output) = query_router.execute_plugins(&ast).await {
                                    plugin_output = Some(output);
                                }
                            }
                        }

                        self.buffer_parse(message, &pool)?;
                    }

                    // Bind
                    // The placeholder's replacements are here, e.g. 'user@email.com' and 'true'
                    'B' => {
                        self.buffer_bind(message).await?;
                    }

                    // Describe
                    // Command a client can issue to describe a previously prepared named statement.
                    'D' => {
                        self.buffer_describe(message).await?;
                    }

                    // Execute
                    // Execute a prepared statement prepared in `P` and bound in `B`.
                    'E' => {
                        self.extended_protocol_data_buffer
                            .push_back(ExtendedProtocolData::create_new_execute(message));
                    }

                    // Close
                    // Close the prepared statement.
                    'C' => {
                        let close: Close = (&message).try_into()?;

                        self.extended_protocol_data_buffer
                            .push_back(ExtendedProtocolData::create_new_close(message, close));
                    }

                    // Sync
                    // Frontend (client) is asking for the query result now.
                    'S' => {
                        debug!("Sending query to server");

                        match plugin_output {
                            Some(PluginOutput::Deny(error)) => {
                                error_response(&mut self.write, &error).await?;
                                plugin_output = None;
                                self.reset_buffered_state();
                                continue;
                            }

                            Some(PluginOutput::Intercept(result)) => {
                                write_all(&mut self.write, result).await?;
                                plugin_output = None;
                                self.reset_buffered_state();
                                continue;
                            }

                            _ => (),
                        };

                        // Prepared statements can arrive like this
                        // 1. Without named describe
                        //      Client: Parse, with name, query and params
                        //              Sync
                        //      Server: ParseComplete
                        //              ReadyForQuery
                        // 3. Without named describe
                        //      Client: Parse, with name, query and params
                        //              Describe, with no name
                        //              Sync
                        //      Server: ParseComplete
                        //              ParameterDescription
                        //              RowDescription
                        //              ReadyForQuery
                        // 2. With named describe
                        //      Client: Parse, with name, query and params
                        //              Describe, with name
                        //              Sync
                        //      Server: ParseComplete
                        //              ParameterDescription
                        //              RowDescription
                        //              ReadyForQuery

                        // Iterate over our extended protocol data that we've buffered
                        while let Some(protocol_data) =
                            self.extended_protocol_data_buffer.pop_front()
                        {
                            match protocol_data {
                                ExtendedProtocolData::Parse { data, metadata } => {
                                    debug!("Have parse in extended buffer");
                                    let (parse, hash) = match metadata {
                                        Some(metadata) => metadata,
                                        None => {
                                            let first_char_in_name = *data.get(5).unwrap_or(&0);
                                            if first_char_in_name != 0 {
                                                // This is a named prepared statement while prepared statements are disabled
                                                // Server connection state will need to be cleared at checkin
                                                server.mark_dirty();
                                            }
                                            // Not a prepared statement
                                            self.buffer.put(&data[..]);
                                            continue;
                                        }
                                    };

                                    // This is a prepared statement we already have on the checked out server
                                    if server.has_prepared_statement(&parse.name) {
                                        debug!(
                                            "Prepared statement `{}` found in server cache",
                                            parse.name
                                        );

                                        // We don't want to send the parse message to the server
                                        // Instead queue up a parse complete message to send to the client
                                        self.response_message_queue_buffer.put(parse_complete());
                                    } else {
                                        debug!(
                                            "Prepared statement `{}` not found in server cache",
                                            parse.name
                                        );

                                        // TODO: Consider adding the close logic that this function can send for eviction to the client buffer instead
                                        // In this case we don't want to send the parse message to the server since the client is sending it
                                        self.register_parse_to_server_cache(
                                            false, &hash, &parse, &pool, server, &address,
                                        )
                                        .await?;

                                        // Add parse message to buffer
                                        self.buffer.put(&data[..]);
                                    }
                                }
                                ExtendedProtocolData::Bind { data, metadata } => {
                                    // This is using a prepared statement
                                    if let Some(client_given_name) = metadata {
                                        self.ensure_prepared_statement_is_on_server(
                                            client_given_name,
                                            &pool,
                                            server,
                                            &address,
                                        )
                                        .await?;
                                    }

                                    self.buffer.put(&data[..]);
                                }
                                ExtendedProtocolData::Describe { data, metadata } => {
                                    // This is using a prepared statement
                                    if let Some(client_given_name) = metadata {
                                        self.ensure_prepared_statement_is_on_server(
                                            client_given_name,
                                            &pool,
                                            server,
                                            &address,
                                        )
                                        .await?;
                                    }

                                    self.buffer.put(&data[..]);
                                }
                                ExtendedProtocolData::Execute { data } => {
                                    self.buffer.put(&data[..])
                                }
                                ExtendedProtocolData::Close { data, close } => {
                                    // We don't send the close message to the server if prepared statements are enabled
                                    // and it's a close with a prepared statement name provided
                                    if self.prepared_statements_enabled
                                        && close.is_prepared_statement()
                                        && !close.anonymous()
                                    {
                                        self.prepared_statements.remove(&close.name);

                                        // Queue up a close complete message to send to the client
                                        self.response_message_queue_buffer.put(close_complete());
                                    } else {
                                        self.buffer.put(&data[..]);
                                    }
                                }
                            }
                        }

                        // Add the sync message
                        self.buffer.put(&message[..]);

                        let mut should_send_to_server = true;

                        // If we have just a sync message left (maybe after omitting sending some messages to the server) no need to send it to the server
                        if *self.buffer.first().unwrap() == b'S' {
                            should_send_to_server = false;
                            // queue up a ready for query message to send to the client, respecting the transaction state of the server
                            self.response_message_queue_buffer
                                .put(ready_for_query(server.in_transaction()));
                        }

                        // Send all queued messages to the client
                        // NOTE: it's possible we don't perfectly send things back in the same order as postgres would,
                        //       however clients should be able to handle this
                        if !self.response_message_queue_buffer.is_empty() {
                            if let Err(err) = write_all_flush(
                                &mut self.write,
                                &self.response_message_queue_buffer,
                            )
                            .await
                            {
                                // We might be in some kind of error/in between protocol state
                                server.mark_bad(err.to_string().as_str());
                                return Err(err);
                            }

                            self.response_message_queue_buffer.clear();
                        }

                        if should_send_to_server {
                            self.send_and_receive_loop(
                                code,
                                None,
                                server,
                                &address,
                                &pool,
                                &self.stats.clone(),
                            )
                            .await?;
                        }

                        self.buffer.clear();

                        if !server.in_transaction() {
                            self.stats.transaction();
                            server
                                .stats()
                                .transaction(self.server_parameters.get_application_name());

                            // Release server back to the pool if we are in transaction mode.
                            // If we are in session mode, we keep the server until the client disconnects.
                            if self.transaction_mode && !server.in_copy_mode() {
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

                        match write_all_flush(&mut self.write, &response).await {
                            Ok(_) => (),
                            Err(err) => {
                                server.mark_bad(err.to_string().as_str());
                                return Err(err);
                            }
                        };

                        if !server.in_transaction() {
                            self.stats.transaction();
                            server
                                .stats()
                                .transaction(self.server_parameters.get_application_name());

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
                    self.pool_name,
                    self.username,
                    self.server_parameters.get_application_name()
                )))
            }
        }
    }

    /// Handles custom protocol messages
    /// Returns true if the message is custom protocol message, false otherwise
    /// Does not work with prepared statements, only simple and extended protocol without parameters
    async fn handle_custom_protocol(
        &mut self,
        query_router: &mut QueryRouter,
        message: &BytesMut,
        pool: &ConnectionPool,
    ) -> Result<bool, Error> {
        let current_shard = query_router.shard();

        match query_router.try_execute_command(message) {
            None => Ok(false),

            Some(custom) => {
                match custom {
                    // SET SHARD TO
                    (Command::SetShard, _) => {
                        match query_router.shard() {
                            None => {}
                            Some(selected_shard) => {
                                if selected_shard >= pool.shards() {
                                    // Bad shard number, send error message to client.
                                    query_router.set_shard(current_shard);

                                    error_response(
                                                    &mut self.write,
                                                    &format!(
                                                        "shard {} is not configured {}, staying on shard {:?} (shard numbers start at 0)",
                                                        selected_shard,
                                                        pool.shards(),
                                                        current_shard,
                                                    ),
                                                )
                                                    .await?;
                                } else {
                                    custom_protocol_response_ok(&mut self.write, "SET SHARD")
                                        .await?;
                                }
                            }
                        }
                    }

                    // SET PRIMARY READS TO
                    (Command::SetPrimaryReads, _) => {
                        custom_protocol_response_ok(&mut self.write, "SET PRIMARY READS").await?;
                    }

                    // SET SHARDING KEY TO
                    (Command::SetShardingKey, _) => {
                        custom_protocol_response_ok(&mut self.write, "SET SHARDING KEY").await?;
                    }

                    // SET SHARD ALIAS TO
                    (Command::SetShardAlias, _) => {
                        custom_protocol_response_ok(&mut self.write, "SET SHARD ALIAS TO").await?;
                    }

                    // SET SERVER ROLE TO
                    (Command::SetServerRole, _) => {
                        custom_protocol_response_ok(&mut self.write, "SET SERVER ROLE").await?;
                    }

                    // SHOW SERVER ROLE
                    (Command::ShowServerRole, value) => {
                        show_response(&mut self.write, "server role", &value).await?;
                    }

                    // SHOW SHARD
                    (Command::ShowShard, value) => {
                        show_response(&mut self.write, "shard", &value).await?;
                    }

                    // SHOW PRIMARY READS
                    (Command::ShowPrimaryReads, value) => {
                        show_response(&mut self.write, "primary reads", &value).await?;
                    }
                };

                Ok(true)
            }
        }
    }

    /// Makes sure the the checked out server has the prepared statement and sends it to the server if it doesn't
    async fn ensure_prepared_statement_is_on_server(
        &mut self,
        client_name: String,
        pool: &ConnectionPool,
        server: &mut Server,
        address: &Address,
    ) -> Result<(), Error> {
        match self.prepared_statements.get(&client_name) {
            Some((parse, hash)) => {
                debug!("Prepared statement `{}` found in cache", client_name);
                // In this case we want to send the parse message to the server
                // since pgcat is initiating the prepared statement on this specific server
                match self
                    .register_parse_to_server_cache(true, hash, parse, pool, server, address)
                    .await
                {
                    Ok(_) => (),
                    Err(err) => match err {
                        Error::PreparedStatementError => {
                            debug!("Removed {} from client cache", client_name);
                            self.prepared_statements.remove(&client_name);
                        }

                        _ => {
                            return Err(err);
                        }
                    },
                }
            }

            None => {
                return Err(Error::ClientError(format!(
                    "prepared statement `{}` not found",
                    client_name
                )))
            }
        };

        Ok(())
    }

    /// Register the parse to the server cache and send it to the server if requested (ie. requested by pgcat)
    ///
    /// Also updates the pool LRU that this parse was used recently
    async fn register_parse_to_server_cache(
        &self,
        should_send_parse_to_server: bool,
        hash: &u64,
        parse: &Arc<Parse>,
        pool: &ConnectionPool,
        server: &mut Server,
        address: &Address,
    ) -> Result<(), Error> {
        // We want to promote this in the pool's LRU
        pool.promote_prepared_statement_hash(hash);

        debug!("Checking for prepared statement {}", parse.name);

        if let Err(err) = server
            .register_prepared_statement(parse, should_send_parse_to_server)
            .await
        {
            match err {
                // Don't ban for this.
                Error::PreparedStatementError => (),
                _ => {
                    pool.ban(address, BanReason::MessageSendFailed, Some(&self.stats));
                }
            };

            return Err(err);
        }

        Ok(())
    }

    /// Register and rewrite the parse statement to the clients statement cache
    /// and also the pool's statement cache. Add it to extended protocol data.
    fn buffer_parse(&mut self, message: BytesMut, pool: &ConnectionPool) -> Result<(), Error> {
        // Avoid parsing if prepared statements not enabled
        if !self.prepared_statements_enabled {
            debug!("Anonymous parse message");
            self.extended_protocol_data_buffer
                .push_back(ExtendedProtocolData::create_new_parse(message, None));
            return Ok(());
        }

        let client_given_name = Parse::get_name(&message)?;
        let parse: Parse = (&message).try_into()?;

        // Compute the hash of the parse statement
        let hash = parse.get_hash();

        // Add the statement to the cache or check if we already have it
        let new_parse = match pool.register_parse_to_cache(hash, &parse) {
            Some(parse) => parse,
            None => {
                return Err(Error::ClientError(format!(
                    "Could not store Prepared statement `{}`",
                    client_given_name
                )))
            }
        };

        debug!(
            "Renamed prepared statement `{}` to `{}` and saved to cache",
            client_given_name, new_parse.name
        );

        self.prepared_statements
            .insert(client_given_name, (new_parse.clone(), hash));

        self.extended_protocol_data_buffer
            .push_back(ExtendedProtocolData::create_new_parse(
                new_parse.as_ref().try_into()?,
                Some((new_parse.clone(), hash)),
            ));

        Ok(())
    }

    /// Rewrite the Bind (F) message to use the prepared statement name
    /// saved in the client cache.
    async fn buffer_bind(&mut self, message: BytesMut) -> Result<(), Error> {
        // Avoid parsing if prepared statements not enabled
        if !self.prepared_statements_enabled {
            debug!("Anonymous bind message");
            self.extended_protocol_data_buffer
                .push_back(ExtendedProtocolData::create_new_bind(message, None));
            return Ok(());
        }

        let client_given_name = Bind::get_name(&message)?;

        match self.prepared_statements.get(&client_given_name) {
            Some((rewritten_parse, _)) => {
                let message = Bind::rename(message, &rewritten_parse.name)?;

                debug!(
                    "Rewrote bind `{}` to `{}`",
                    client_given_name, rewritten_parse.name
                );

                self.extended_protocol_data_buffer.push_back(
                    ExtendedProtocolData::create_new_bind(message, Some(client_given_name)),
                );

                Ok(())
            }
            None => {
                debug!(
                    "Got bind for unknown prepared statement {:?}",
                    client_given_name
                );

                error_response(
                    &mut self.write,
                    &format!(
                        "prepared statement \"{}\" does not exist",
                        client_given_name
                    ),
                )
                .await?;

                Err(Error::ClientError(format!(
                    "Prepared statement `{}` doesn't exist",
                    client_given_name
                )))
            }
        }
    }

    /// Rewrite the Describe (F) message to use the prepared statement name
    /// saved in the client cache.
    async fn buffer_describe(&mut self, message: BytesMut) -> Result<(), Error> {
        // Avoid parsing if prepared statements not enabled
        if !self.prepared_statements_enabled {
            debug!("Anonymous describe message");
            self.extended_protocol_data_buffer
                .push_back(ExtendedProtocolData::create_new_describe(message, None));

            return Ok(());
        }

        let describe: Describe = (&message).try_into()?;
        if describe.target == 'P' {
            debug!("Portal describe message");
            self.extended_protocol_data_buffer
                .push_back(ExtendedProtocolData::create_new_describe(message, None));

            return Ok(());
        }

        let client_given_name = describe.statement_name.clone();

        match self.prepared_statements.get(&client_given_name) {
            Some((rewritten_parse, _)) => {
                let describe = describe.rename(&rewritten_parse.name);

                debug!(
                    "Rewrote describe `{}` to `{}`",
                    client_given_name, describe.statement_name
                );

                self.extended_protocol_data_buffer.push_back(
                    ExtendedProtocolData::create_new_describe(
                        describe.try_into()?,
                        Some(client_given_name),
                    ),
                );

                Ok(())
            }

            None => {
                debug!("Got describe for unknown prepared statement {:?}", describe);

                error_response(
                    &mut self.write,
                    &format!(
                        "prepared statement \"{}\" does not exist",
                        client_given_name
                    ),
                )
                .await?;

                Err(Error::ClientError(format!(
                    "Prepared statement `{}` doesn't exist",
                    client_given_name
                )))
            }
        }
    }

    fn reset_buffered_state(&mut self) {
        self.buffer.clear();
        self.extended_protocol_data_buffer.clear();
        self.response_message_queue_buffer.clear();
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

            match write_all_flush(&mut self.write, &response).await {
                Ok(_) => (),
                Err(err) => {
                    // We might be in some kind of error/in between protocol state, better to just kill this server
                    server.mark_bad(err.to_string().as_str());
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
            self.server_parameters.get_application_name(),
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
        let statement_timeout_duration = match pool.settings.user.statement_timeout {
            0 => tokio::time::Duration::MAX,
            timeout => tokio::time::Duration::from_millis(timeout),
        };

        match tokio::time::timeout(
            statement_timeout_duration,
            server.recv(Some(&mut self.server_parameters)),
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
                server.mark_bad(
                    format!(
                        "Statement timeout while talking to {:?} with user {}",
                        address, pool.settings.user.username
                    )
                    .as_str(),
                );
                pool.ban(address, BanReason::StatementTimeout, Some(client_stats));
                error_response_terminal(&mut self.write, "pool statement timeout").await?;
                Err(Error::StatementTimeout)
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
