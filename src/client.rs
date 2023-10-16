use crate::errors::{ClientIdentifier, Error};
use crate::pool::BanReason;
use crate::server_xact::TransactionState;
/// Handle clients by pretending to be a PostgreSQL server.
use bytes::{Buf, BufMut, BytesMut};
use log::{debug, error, info, trace, warn};
use once_cell::sync::Lazy;
use sqlparser::ast::Statement;
use std::collections::HashMap;
use std::ops::ControlFlow;
use std::sync::{atomic::AtomicUsize, Arc};
use std::time::{Duration, Instant};
use tokio::io::{split, AsyncReadExt, BufReader, ReadHalf, WriteHalf};
use tokio::net::TcpStream;
use tokio::sync::broadcast::Receiver;
use tokio::sync::mpsc::Sender;

use crate::admin::{generate_server_parameters_for_admin, handle_admin};
use crate::auth_passthrough::refetch_auth_hash;
use crate::client_xact::*;
use crate::config::{
    get_config, get_idle_client_in_transaction_timeout, get_prepared_statements, Address, PoolMode,
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
    pub(crate) write: T,

    /// Internal buffer, where we place messages until we have to flush
    /// them to the backend.
    buffer: BytesMut,

    /// Address
    pub(crate) addr: std::net::SocketAddr,

    /// The client was started with the sole reason to cancel another running query.
    cancel_mode: bool,

    /// In transaction mode, the connection is released after each transaction.
    /// Session mode has slightly higher throughput per client, but lower capacity.
    client_pool_mode: PoolMode,

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
    pub(crate) stats: Arc<ClientStats>,

    /// Clients want to talk to admin database.
    admin: bool,

    /// Last address the client talked to.
    last_address_id: Option<usize>,

    /// Last server process stats we talked to.
    last_server_stats: Option<Arc<ServerStats>>,

    /// Last server key we talked to.
    pub(crate) last_server_key: Option<ServerId>,

    /// Connected to server
    connected_to_server: bool,

    /// Name of the server pool for this client (This comes from the database name in the connection string)
    pool_name: String,

    /// Postgres user for this client (This comes from the user in the connection string)
    username: String,

    /// Server startup and session parameters that we're going to track
    pub(crate) server_parameters: ServerParameters,

    /// Used to notify clients about an impending shutdown
    shutdown: Receiver<()>,

    /// Prepared statements
    prepared_statements: HashMap<String, Parse>,

    pub(crate) xact_info: ClientTxnMetaData,
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
                write_all(&mut stream, &yes).await?;

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
                write_all(&mut stream, &no).await?;

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
        let (client_pool_mode, mut server_parameters) = if admin {
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

            (PoolMode::Session, generate_server_parameters_for_admin())
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
                            warn!("Password for {}, obtained. Updating.", client_identifier);

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

            let client_pool_mode = pool.settings.pool_mode;

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

            (client_pool_mode, pool.server_parameters())
        };

        // Update the parameters to merge what the application sent and what's originally on the server
        server_parameters.set_from_hashmap(&parameters, false);

        debug!("Password authentication successful");

        auth_ok(&mut write).await?;
        write_all(&mut write, &(&server_parameters).into()).await?;
        backend_key_data(&mut write, process_id, secret_key).await?;
        ready_for_query(&mut write).await?;

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
            addr,
            buffer: BytesMut::with_capacity(8196),
            cancel_mode: false,
            client_pool_mode,
            process_id,
            secret_key,
            client_server_map,
            parameters: parameters.clone(),
            stats,
            admin,
            last_address_id: None,
            last_server_stats: None,
            last_server_key: None,
            pool_name: pool_name.clone(),
            username: username.clone(),
            server_parameters,
            shutdown,
            connected_to_server: false,
            prepared_statements: HashMap::new(),
            xact_info: Default::default(),
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
            client_pool_mode: PoolMode::Session,
            process_id,
            secret_key,
            client_server_map,
            parameters: HashMap::new(),
            stats: Arc::new(ClientStats::default()),
            admin: false,
            last_address_id: None,
            last_server_stats: None,
            last_server_key: None,
            pool_name: String::from("undefined"),
            username: String::from("undefined"),
            server_parameters: ServerParameters::new(),
            shutdown,
            connected_to_server: false,
            prepared_statements: HashMap::new(),
            xact_info: Default::default(),
        })
    }

    async fn handle_cancel_mode(&mut self) -> Result<(), Error> {
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
        Server::cancel(&address, port, process_id, secret_key).await
    }

    #[allow(clippy::too_many_arguments)]
    async fn handle_message_in_custom_protocol_loop(
        &mut self,
        mut message: BytesMut,
        client_identifier: &ClientIdentifier,
        query_router: &mut QueryRouter,
        will_prepare: &mut bool,
        prepared_statements_enabled: &mut bool,
        prepared_statement: &mut Option<String>,
        plugin_output: &mut Option<PluginOutput>,
    ) -> Result<ControlFlow<(BytesMut, Option<Vec<Statement>>), ()>, Error> {
        let mut initial_parsed_ast = None;
        match message[0] as char {
            // Buffer extended protocol messages even if we do not have
            // a server connection yet. Hopefully, when we get the S message
            // we'll be able to allocate a connection. Also, clients do not expect
            // the server to respond to these messages so even if we were not able to
            // allocate a connection, we wouldn't be able to send back an error message
            // to the client so we buffer them and defer the decision to error out or not
            // to when we get the S message
            'D' => {
                if *prepared_statements_enabled {
                    let name;
                    (name, message) = self.rewrite_describe(message).await?;

                    if let Some(name) = name {
                        *prepared_statement = Some(name);
                    }
                }

                self.buffer.put(&message[..]);
                return Ok(ControlFlow::Continue(()));
            }

            'E' => {
                self.buffer.put(&message[..]);
                return Ok(ControlFlow::Continue(()));
            }

            'Q' => {
                if query_router.query_parser_enabled() {
                    match query_router.parse(&message) {
                        Ok(ast) => {
                            let plugin_result = query_router.execute_plugins(&ast).await;

                            match plugin_result {
                                Ok(PluginOutput::Deny(error)) => {
                                    error_response(&mut self.write, &error).await?;
                                    return Ok(ControlFlow::Continue(()));
                                }

                                Ok(PluginOutput::Intercept(result)) => {
                                    write_all(&mut self.write, &result).await?;
                                    return Ok(ControlFlow::Continue(()));
                                }

                                _ => (),
                            };

                            let _ = query_router.infer(&ast);

                            initial_parsed_ast = Some(ast);
                        }
                        Err(error) => {
                            if error != Error::UnsupportedStatement {
                                warn!(
                                    "Query parsing error: {} (client: {})",
                                    error, client_identifier
                                );
                            }
                        }
                    }
                }
            }

            'P' => {
                if *prepared_statements_enabled {
                    (*prepared_statement, message) = self.rewrite_parse(message)?;
                    *will_prepare = true;
                }

                self.buffer.put(&message[..]);

                if query_router.query_parser_enabled() {
                    match query_router.parse(&message) {
                        Ok(ast) => {
                            if let Ok(output) = query_router.execute_plugins(&ast).await {
                                *plugin_output = Some(output);
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

                return Ok(ControlFlow::Continue(()));
            }

            'B' => {
                if *prepared_statements_enabled {
                    (*prepared_statement, message) = self.rewrite_bind(message).await?;
                }

                self.buffer.put(&message[..]);

                if query_router.query_parser_enabled() {
                    query_router.infer_shard_from_bind(&message);
                }

                return Ok(ControlFlow::Continue(()));
            }

            // Close (F)
            'C' => {
                if *prepared_statements_enabled {
                    let close: Close = (&message).try_into()?;

                    if close.is_prepared_statement() && !close.anonymous() {
                        self.prepared_statements.remove(&close.name);
                        write_all_flush(&mut self.write, &close_complete()).await?;
                        return Ok(ControlFlow::Continue(()));
                    }
                }
            }

            _ => (),
        }

        Ok(ControlFlow::Break((message, initial_parsed_ast)))
    }

    /// Handle a connected and authenticated client.
    pub async fn handle(&mut self) -> Result<(), Error> {
        // The client wants to cancel a query it has issued previously.
        if self.cancel_mode {
            return self.handle_cancel_mode().await;
        }

        // The query router determines where the query is going to go,
        // e.g. primary, replica, which shard.
        let mut query_router = QueryRouter::new();

        self.stats.register(self.stats.clone());

        // Result returned by one of the plugins.
        let mut plugin_output = None;

        // Prepared statement being executed
        let mut prepared_statement = None;
        let mut will_prepare = false;

        let client_identifier = ClientIdentifier::new(
            self.server_parameters.get_application_name(),
            &self.username,
            &self.pool_name,
        );

        // Our custom protocol loop.
        // We expect the client to either start a transaction with regular queries
        // or issue commands for our sharding and server selection protocol.
        loop {
            trace!(
                "Client idle, waiting for message, pool mode: {:?}",
                self.client_pool_mode
            );

            // Should we rewrite prepared statements and bind messages?
            let mut prepared_statements_enabled = get_prepared_statements();

            // Read a complete message from the client, which normally would be
            // either a `Q` (query) or `P` (prepare, extended protocol).
            // We can parse it here before grabbing a server from the pool,
            // in case the client is sending some custom protocol messages, e.g.
            // SET SHARDING KEY TO 'bigint';

            let mut message = tokio::select! {
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

            // Get a pool instance referenced by the most up-to-date
            // pointer. This ensures we always read the latest config
            // when starting a query.
            let mut pool = self.get_pool().await?;
            query_router.update_pool_settings(pool.settings.clone());

            let initial_parsed_ast;

            (message, initial_parsed_ast) = match self
                .handle_message_in_custom_protocol_loop(
                    message,
                    &client_identifier,
                    &mut query_router,
                    &mut will_prepare,
                    &mut prepared_statements_enabled,
                    &mut prepared_statement,
                    &mut plugin_output,
                )
                .await?
            {
                ControlFlow::Break(res) => res,

                ControlFlow::Continue(()) => {
                    continue;
                }
            };

            // Check on plugin results.
            if let Some(PluginOutput::Deny(error)) = plugin_output {
                self.buffer.clear();
                error_response(&mut self.write, &error).await?;
                plugin_output = None;
                continue;
            };

            // Check if the pool is paused and wait until it's resumed.
            if pool.wait_paused().await {
                // Refresh pool information, something might have changed.
                pool = self.get_pool().await?;
            }

            query_router.update_pool_settings(pool.settings.clone());

            // Reset transaction state, as we are entering a new transaction loop.
            self.reset_client_xact();

            let mut all_conns: HashMap<
                ServerId,
                (bb8::PooledConnection<'_, crate::pool::ServerPool>, Address),
            > = HashMap::new();

            match self
                .handle_transaction_loop(
                    &client_identifier,
                    &mut query_router,
                    &pool,
                    &mut all_conns,
                    Some(message),
                    initial_parsed_ast,
                    &mut will_prepare,
                    &mut prepared_statements_enabled,
                    &mut prepared_statement,
                    &mut plugin_output,
                )
                .await?
            {
                Some(ControlFlow::Break(())) => {
                    return Ok(());
                }

                Some(ControlFlow::Continue(())) => {
                    continue;
                }

                _ => (),
            }

            self.cleanup_custom_protocol_loop_helper(all_conns, prepared_statements_enabled)
                .await?;
        }
    }

    async fn cleanup_custom_protocol_loop_helper(
        &mut self,
        mut all_conns: HashMap<
            usize,
            (bb8::PooledConnection<'_, crate::pool::ServerPool>, Address),
        >,
        prepared_statements_enabled: bool,
    ) -> Result<(), Error> {
        self.distributed_commit_or_abort(&mut all_conns).await?;

        // Reset transaction state for safety reasons. Even if this state will be reset before
        // the next transaction, this dirty state could be seen in-between here and there.
        self.reset_client_xact();

        debug!("Releasing servers back into the pool");
        for conn in all_conns.values_mut() {
            let server = &mut *conn.0;
            let address = &conn.1;

            // The server is no longer bound to us, we can't cancel it's queries anymore.
            debug!("Releasing server back into the pool: {}", address);

            server.checkin_cleanup().await?;

            if prepared_statements_enabled {
                server.maintain_cache().await?;
            }

            server.stats().idle();
        }
        self.connected_to_server = false;
        self.release();
        self.stats.idle();
        Ok(())
    }
    async fn read_message_helper(
        &mut self,
        idle_client_timeout_duration: Duration,
        query_router: &mut QueryRouter,
        all_conns: &mut HashMap<
            ServerId,
            (bb8::PooledConnection<'_, crate::pool::ServerPool>, Address),
        >,
        initial_message: Option<BytesMut>,
        initial_parsed_ast: &mut Option<Vec<Statement>>,
    ) -> Result<ControlFlow<(), BytesMut>, Error> {
        match initial_message {
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
                    Ok(Ok(message)) => Ok(ControlFlow::Continue(message)),
                    Ok(Err(err)) => {
                        // Client disconnected inside a transaction.
                        // Clean up the server and re-use it.
                        self.stats.disconnect();
                        for conn in all_conns.values_mut() {
                            let server = &mut *conn.0;
                            server.checkin_cleanup().await?;
                        }

                        Err(err)
                    }
                    Err(_) => {
                        // Client idle in transaction timeout
                        error_response_with_state(
                            &mut self.write,
                            "idle transaction timeout",
                            self.xact_info.state(),
                        )
                        .await?;
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

                        Ok(ControlFlow::Break(()))
                    }
                }
            }

            Some(message) => Ok(ControlFlow::Continue(message)),
        }
    }

    async fn handle_begin_statement(
        &mut self,
        code: char,
        message: &BytesMut,
        client_identifier: &ClientIdentifier,
        query_router: &mut QueryRouter,
        initial_parsed_ast: &mut Option<Vec<Statement>>,
    ) -> Result<ControlFlow<Option<Vec<Statement>>, ()>, Error> {
        // Query
        if code == 'Q' {
            // If the first message is a `BEGIN` statement, then we are starting a
            // transaction. However, we might not still be on the right shard (as the
            // shard might be inferred from the first query). So we parse the query and
            // store the `BEGIN` statement. Upon receiving the next query (and possibly
            // determining the shard), we will execute the `BEGIN` statement.
            if let Some(ast_vec) = initial_parsed_ast {
                if Self::is_begin_statement(ast_vec) {
                    assert_eq!(ast_vec.len(), 1);

                    let begin_stmt = &ast_vec[0];

                    if let Statement::StartTransaction { .. } = begin_stmt {
                        // This is the first BEGIN statement. We need to register it for later executions.
                        self.xact_info.set_begin_statement(Some(begin_stmt.clone()));

                        self.xact_info.set_state(TransactionState::InTransaction);
                    } else {
                        panic!("Expected BEGIN statement, got {:?}", begin_stmt);
                    }

                    custom_protocol_response_ok_with_state(
                        &mut self.write,
                        "BEGIN",
                        self.xact_info.state(),
                    )
                    .await?;

                    return Ok(ControlFlow::Continue(()));
                }
            }

            if query_router.query_parser_enabled() {
                return self
                    .parse_ast_helper(query_router, initial_parsed_ast, message, client_identifier)
                    .await;
            }
        };
        Ok(ControlFlow::Break(None))
    }

    #[allow(clippy::too_many_arguments)]
    async fn handle_transaction_loop<'a, 'b>(
        &mut self,
        client_identifier: &ClientIdentifier,
        query_router: &mut QueryRouter,
        pool: &'a ConnectionPool,
        all_conns: &mut HashMap<
            ServerId,
            (bb8::PooledConnection<'b, crate::pool::ServerPool>, Address),
        >,
        mut initial_message: Option<BytesMut>,
        mut initial_parsed_ast: Option<Vec<Statement>>,
        will_prepare: &mut bool,
        prepared_statements_enabled: &mut bool,
        prepared_statement: &mut Option<String>,
        plugin_output: &mut Option<PluginOutput>,
    ) -> Result<Option<ControlFlow<()>>, Error>
    where
        'a: 'b,
    {
        let idle_client_timeout_duration = match get_idle_client_in_transaction_timeout() {
            0 => tokio::time::Duration::MAX,
            timeout => tokio::time::Duration::from_millis(timeout),
        };

        // Transaction loop. Multiple queries can be issued by the client here.
        // The connection belongs to the client until the transaction is over,
        // or until the client disconnects if we are in session mode.
        //
        // If the client is in session or transaction modes, no more custom protocol
        // commands will be accepted. However, in transparent mode, the `SET SHARD` and
        // `SET SHARDING KEY` custom protocol commands are still accepted.
        loop {
            let is_first_message_to_server = all_conns.is_empty();

            let mut message: BytesMut = match self
                .read_message_helper(
                    idle_client_timeout_duration,
                    query_router,
                    all_conns,
                    initial_message.take(),
                    &mut initial_parsed_ast,
                )
                .await?
            {
                ControlFlow::Continue(message) => message,
                ControlFlow::Break(_) => {
                    break;
                }
            };

            // Safe to unwrap because we know this message has a certain length and has the code
            // This reads the first byte without advancing the internal pointer and mutating the bytes
            let code = *message.first().unwrap() as char;
            let mut ast = match self
                .handle_begin_statement(
                    code,
                    &message,
                    client_identifier,
                    query_router,
                    &mut initial_parsed_ast,
                )
                .await?
            {
                ControlFlow::Continue(()) => {
                    continue;
                }

                ControlFlow::Break(ast) => ast,
            };

            if all_conns.is_empty() || self.is_transparent_mode() {
                let current_shard = query_router.shard();

                // Handle all custom protocol commands, if any.
                match query_router.try_execute_command(&message) {
                    // Normal query, not a custom command.
                    None => (),

                    // SET SHARD TO
                    Some((Command::SetShard, _)) => {
                        match query_router.shard() {
                            None => (),
                            Some(selected_shard) => {
                                if selected_shard >= pool.shards() {
                                    // Bad shard number, send error message to client.
                                    query_router.set_shard(current_shard);

                                    error_response_with_state(
                                    &mut self.write,
                                    &format!(
                                        "shard {} is not configured {}, staying on shard {:?} (shard numbers start at 0)",
                                        selected_shard,
                                        pool.shards(),
                                        current_shard,
                                    ),
                                    self.xact_info.state(),
                                )
                                    .await?;
                                } else {
                                    custom_protocol_response_ok_with_state(
                                        &mut self.write,
                                        "SET SHARD",
                                        self.xact_info.state(),
                                    )
                                    .await?;
                                }
                            }
                        }
                        if self.is_in_idle_transaction() {
                            return Ok(Some(ControlFlow::Continue(())));
                        } else {
                            continue;
                        }
                    }

                    // SET PRIMARY READS TO
                    Some((Command::SetPrimaryReads, _)) => {
                        custom_protocol_response_ok_with_state(
                            &mut self.write,
                            "SET PRIMARY READS",
                            self.xact_info.state(),
                        )
                        .await?;
                        if self.is_in_idle_transaction() {
                            return Ok(Some(ControlFlow::Continue(())));
                        } else {
                            continue;
                        }
                    }

                    // SET SHARDING KEY TO
                    Some((Command::SetShardingKey, _)) => {
                        custom_protocol_response_ok_with_state(
                            &mut self.write,
                            "SET SHARDING KEY",
                            self.xact_info.state(),
                        )
                        .await?;
                        if self.is_in_idle_transaction() {
                            return Ok(Some(ControlFlow::Continue(())));
                        } else {
                            continue;
                        }
                    }

                    // SET SERVER ROLE TO
                    Some((Command::SetServerRole, _)) => {
                        custom_protocol_response_ok_with_state(
                            &mut self.write,
                            "SET SERVER ROLE",
                            self.xact_info.state(),
                        )
                        .await?;
                        if self.is_in_idle_transaction() {
                            return Ok(Some(ControlFlow::Continue(())));
                        } else {
                            continue;
                        }
                    }

                    // SHOW SERVER ROLE
                    Some((Command::ShowServerRole, value)) => {
                        show_response(&mut self.write, "server role", &value).await?;
                        if self.is_in_idle_transaction() {
                            return Ok(Some(ControlFlow::Continue(())));
                        } else {
                            continue;
                        }
                    }

                    // SHOW SHARD
                    Some((Command::ShowShard, value)) => {
                        show_response(&mut self.write, "shard", &value).await?;
                        if self.is_in_idle_transaction() {
                            return Ok(Some(ControlFlow::Continue(())));
                        } else {
                            continue;
                        }
                    }

                    // SHOW PRIMARY READS
                    Some((Command::ShowPrimaryReads, value)) => {
                        show_response(&mut self.write, "primary reads", &value).await?;
                        if self.is_in_idle_transaction() {
                            return Ok(Some(ControlFlow::Continue(())));
                        } else {
                            continue;
                        }
                    }
                };

                debug!("Waiting for connection from pool");
                if !self.admin {
                    self.stats.waiting();
                }

                let query_router_shard = query_router.shard();
                let server_key = query_router_shard.unwrap_or(0);
                let mut conn_opt = all_conns.get_mut(&server_key);
                if conn_opt.is_none() {
                    // Grab a server from the pool.
                    let connection = match pool
                        .get(query_router_shard, query_router.role(), &self.stats)
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
                                error!(
                                    "Got Sync message but failed to get a connection from the pool"
                                );
                                self.buffer.clear();
                            }

                            error_response_with_state(
                                &mut self.write,
                                format!("could not get connection from the pool - {}", err)
                                    .as_str(),
                                self.xact_info.state(),
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

                            if self.is_in_idle_transaction() {
                                return Ok(Some(ControlFlow::Continue(())));
                            } else {
                                continue;
                            }
                        }
                    };

                    // Before inserting this new connection, if we had only a single connection
                    // before, then it means that we have started a distributed transaction.
                    // At this point, we need to do some prep on the first server.
                    if all_conns.len() == 1 {
                        let (first_server_key, first_conn) = all_conns.iter_mut().next().unwrap();
                        let first_server = &mut *first_conn.0;

                        if !self.acquire_gid(*first_server_key, first_server).await? {
                            break;
                        }
                    }

                    all_conns.insert(server_key, connection);

                    let is_distributed_xact = all_conns.len() > 1;
                    conn_opt = if is_distributed_xact {
                        let conn_opt = all_conns.get_mut(&server_key);
                        let conn = conn_opt.unwrap();
                        let server = &mut *conn.0;
                        let address = &conn.1;

                        debug!(
                                "Sending implicit BEGIN statement to server {} (in transparent mode with distributed transaction)",
                                address
                            );
                        if !self.begin_distributed_xact(server_key, server).await? {
                            break;
                        }
                        Some(conn)
                    } else {
                        all_conns.get_mut(&server_key)
                    }
                }
                let conn = conn_opt.unwrap();
                let address = &conn.1;
                let server = &mut *conn.0;

                // Server is assigned to the client in case the client wants to
                // cancel a query later.
                server.claim(self.process_id, self.secret_key);
                self.connected_to_server = true;

                // Update statistics
                self.stats.active();

                self.last_address_id = Some(address.id);
                self.last_server_stats = Some(server.stats());
                self.last_server_key = Some(server_key);

                debug!(
                    "Client {:?} talking to server {:?}",
                    self.addr,
                    server.address()
                );

                server.sync_parameters(&self.server_parameters).await?;
            }

            self.assign_client_transaction_state(all_conns);

            let is_distributed_xact = all_conns.len() > 1;
            let server_key = query_router.shard().unwrap_or(0);
            let conn = all_conns.get_mut(&server_key).unwrap();
            let server = &mut *conn.0;
            let address = &conn.1;

            // Only check if we should rewrite prepared statements
            // in session mode. In transaction mode, we check at the beginning of
            // each transaction.
            if !self.is_transaction_mode() {
                *prepared_statements_enabled = get_prepared_statements();
            }

            debug!("Prepared statement active: {:?}", *prepared_statement);

            // We are processing a prepared statement.
            if let Some(ref name) = *prepared_statement {
                debug!("Checking prepared statement is on server");
                // Get the prepared statement the server expects to see.
                let statement = match self.prepared_statements.get(name) {
                    Some(statement) => {
                        debug!("Prepared statement `{}` found in cache", name);
                        statement
                    }
                    None => {
                        return Err(Error::ClientError(format!(
                            "prepared statement `{}` not found",
                            name
                        )))
                    }
                };

                // Since it's already in the buffer, we don't need to prepare it on this server.
                if *will_prepare {
                    server.will_prepare(&statement.name);
                    *will_prepare = false;
                } else {
                    // The statement is not prepared on the server, so we need to prepare it.
                    if server.should_prepare(&statement.name) {
                        match server.prepare(statement).await {
                            Ok(_) => (),
                            Err(err) => {
                                pool.ban(address, BanReason::MessageSendFailed, Some(&self.stats));
                                return Err(err);
                            }
                        }
                    }
                }

                // Done processing the prepared statement.
                *prepared_statement = None;
            }

            // The message will be forwarded to the server intact. We still would like to
            // parse it below to figure out what to do with it.

            trace!("Message: {}", code);

            match code {
                // Query
                'Q' => {
                    if is_distributed_xact {
                        // if we are in a distributed transaction, we need to parse the query
                        // to figure out if it's a COMMIT or ABORT statement.
                        // If query parsing is disabled, we need to parse it here. Otherwise,
                        // it's already parsed.
                        if !query_router.query_parser_enabled() {
                            assert_eq!(ast, None);
                            ast = match self
                                .parse_ast_helper(
                                    query_router,
                                    &mut initial_parsed_ast,
                                    &message,
                                    client_identifier,
                                )
                                .await?
                            {
                                ControlFlow::Continue(()) => {
                                    continue;
                                }

                                ControlFlow::Break(ast) => ast,
                            }
                        }

                        if let Some(ast) = &ast {
                            if is_distributed_xact && self.set_commit_or_abort_statement(ast) {
                                break;
                            }
                        }
                    }
                    debug!("Sending query to server");

                    // If this is the first message that we're actually sending to the server,
                    // we need to send the 'BEGIN' statement first if it was issued before this.
                    // This is the case when the client is in transparent mode and A 'BEGIN'
                    // statement was issued before the first query.
                    if is_first_message_to_server {
                        if let Some(begin_stmt) = self.xact_info.get_begin_statement() {
                            let begin_stmt = begin_stmt.clone();
                            let res = server.query(&begin_stmt.to_string()).await;

                            if self.post_query_processing(server, res).await?.is_none() {
                                break;
                            }

                            self.initialize_xact_params(server, &begin_stmt);

                            assert!(server.in_transaction());
                        }
                    }

                    self.send_and_receive_loop(
                        code,
                        Some(&message),
                        server,
                        address,
                        pool,
                        &self.stats.clone(),
                    )
                    .await?;

                    if !server.in_transaction() {
                        // Report transaction executed statistics.
                        self.stats.transaction();
                        server
                            .stats()
                            .transaction(self.server_parameters.get_application_name());

                        // Release server back to the pool if we are in transaction or transparent modes.
                        // If we are in session mode, we keep the server until the client disconnects.
                        if (self.is_transaction_mode() || self.is_transparent_mode())
                            && !server.in_copy_mode()
                        {
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

                    if *prepared_statements_enabled {
                        server.maintain_cache().await?;
                    }

                    return Ok(Some(ControlFlow::Break(())));
                }

                // Parse
                // The query with placeholders is here, e.g. `SELECT * FROM users WHERE email = $1 AND active = $2`.
                'P' => {
                    if *prepared_statements_enabled {
                        (*prepared_statement, message) = self.rewrite_parse(message)?;
                        *will_prepare = true;
                    }

                    if query_router.query_parser_enabled() {
                        if let Ok(ast) = query_router.parse(&message) {
                            if let Ok(output) = query_router.execute_plugins(&ast).await {
                                *plugin_output = Some(output);
                            }
                        }
                    }

                    self.buffer.put(&message[..]);
                }

                // Bind
                // The placeholder's replacements are here, e.g. 'user@email.com' and 'true'
                'B' => {
                    if *prepared_statements_enabled {
                        (*prepared_statement, message) = self.rewrite_bind(message).await?;
                    }

                    self.buffer.put(&message[..]);
                }

                // Describe
                // Command a client can issue to describe a previously prepared named statement.
                'D' => {
                    if *prepared_statements_enabled {
                        let name;
                        (name, message) = self.rewrite_describe(message).await?;

                        if let Some(name) = name {
                            *prepared_statement = Some(name);
                        }
                    }

                    self.buffer.put(&message[..]);
                }

                // Close the prepared statement.
                'C' => {
                    if *prepared_statements_enabled {
                        let close: Close = (&message).try_into()?;

                        if close.is_prepared_statement() && !close.anonymous() {
                            if let Some(parse) = self.prepared_statements.get(&close.name) {
                                server.will_close(&parse.generated_name);
                            } else {
                                // A prepared statement slipped through? Not impossible, since we don't support PREPARE yet.
                            };
                        }
                    }

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

                    match plugin_output {
                        Some(PluginOutput::Deny(error)) => {
                            error_response_with_state(
                                &mut self.write,
                                error,
                                self.xact_info.state(),
                            )
                            .await?;
                            *plugin_output = None;
                            self.buffer.clear();
                            continue;
                        }

                        Some(PluginOutput::Intercept(result)) => {
                            write_all(&mut self.write, result).await?;
                            *plugin_output = None;
                            self.buffer.clear();
                            continue;
                        }

                        _ => (),
                    };

                    self.buffer.put(&message[..]);

                    let first_message_code = (*self.buffer.first().unwrap_or(&0)) as char;

                    // Almost certainly true
                    if first_message_code == 'P' && !*prepared_statements_enabled {
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
                        address,
                        pool,
                        &self.stats.clone(),
                    )
                    .await?;

                    self.buffer.clear();

                    if !server.in_transaction() {
                        self.stats.transaction();
                        server
                            .stats()
                            .transaction(self.server_parameters.get_application_name());

                        // Release server back to the pool if we are in transaction or transparent modes.
                        // If we are in session mode, we keep the server until the client disconnects.
                        if (self.is_transaction_mode() || self.is_transparent_mode())
                            && !server.in_copy_mode()
                        {
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
                        self.send_server_message(server, &self.buffer, address, pool)
                            .await?;
                        self.buffer.clear();
                    }
                }

                // CopyDone or CopyFail
                // Copy is done, successfully or not.
                'c' | 'f' => {
                    // We may already have some copy data in the buffer, add this message to buffer
                    self.buffer.put(&message[..]);

                    self.send_server_message(server, &self.buffer, address, pool)
                        .await?;

                    // Clear the buffer
                    self.buffer.clear();

                    let response = self
                        .receive_server_message(server, address, pool, &self.stats.clone())
                        .await?;

                    match write_all_flush(&mut self.write, &response).await {
                        Ok(_) => (),
                        Err(err) => {
                            server.mark_bad();
                            return Err(err);
                        }
                    };

                    if !server.in_transaction() {
                        self.stats.transaction();
                        server
                            .stats()
                            .transaction(self.server_parameters.get_application_name());

                        // Release server back to the pool if we are in transaction or transparent modes.
                        // If we are in session mode, we keep the server until the client disconnects.
                        if self.is_transaction_mode() || self.is_transparent_mode() {
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

        Ok(None)
    }

    async fn parse_ast_helper(
        &mut self,
        query_router: &mut QueryRouter,
        initial_parsed_ast: &mut Option<Vec<sqlparser::ast::Statement>>,
        message: &BytesMut,
        client_identifier: &ClientIdentifier,
    ) -> Result<ControlFlow<Option<Vec<sqlparser::ast::Statement>>>, Error> {
        Ok({
            // We don't want to parse again if we already parsed it as the initial message
            let ast = parse_ast(initial_parsed_ast, query_router, message, client_identifier);

            if let Some(ast_ref) = &ast {
                let plugin_result = query_router.execute_plugins(ast_ref).await;

                match plugin_result {
                    Ok(PluginOutput::Deny(error)) => {
                        error_response_with_state(&mut self.write, &error, self.xact_info.state())
                            .await?;
                        ControlFlow::Continue(())
                    }

                    Ok(PluginOutput::Intercept(result)) => {
                        write_all(&mut self.write, &result).await?;
                        ControlFlow::Continue(())
                    }

                    _ => ControlFlow::Break(ast),
                }
            } else {
                ControlFlow::Break(None)
            }
        })
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

    /// Rewrite Parse (F) message to set the prepared statement name to one we control.
    /// Save it into the client cache.
    fn rewrite_parse(&mut self, message: BytesMut) -> Result<(Option<String>, BytesMut), Error> {
        let parse: Parse = (&message).try_into()?;

        let name = parse.name.clone();

        // Don't rewrite anonymous prepared statements
        if parse.anonymous() {
            debug!("Anonymous prepared statement");
            return Ok((None, message));
        }

        let parse = parse.rename();

        debug!(
            "Renamed prepared statement `{}` to `{}` and saved to cache",
            name, parse.name
        );

        self.prepared_statements.insert(name.clone(), parse.clone());

        Ok((Some(name), parse.try_into()?))
    }

    /// Rewrite the Bind (F) message to use the prepared statement name
    /// saved in the client cache.
    async fn rewrite_bind(
        &mut self,
        message: BytesMut,
    ) -> Result<(Option<String>, BytesMut), Error> {
        let bind: Bind = (&message).try_into()?;
        let name = bind.prepared_statement.clone();

        if bind.anonymous() {
            debug!("Anonymous bind message");
            return Ok((None, message));
        }

        match self.prepared_statements.get(&name) {
            Some(prepared_stmt) => {
                let bind = bind.reassign(prepared_stmt);

                debug!("Rewrote bind `{}` to `{}`", name, bind.prepared_statement);

                Ok((Some(name), bind.try_into()?))
            }
            None => {
                debug!("Got bind for unknown prepared statement {:?}", bind);

                error_response(
                    &mut self.write,
                    &format!(
                        "prepared statement \"{}\" does not exist",
                        bind.prepared_statement
                    ),
                )
                .await?;

                Err(Error::ClientError(format!(
                    "Prepared statement `{}` doesn't exist",
                    name
                )))
            }
        }
    }

    /// Rewrite the Describe (F) message to use the prepared statement name
    /// saved in the client cache.
    async fn rewrite_describe(
        &mut self,
        message: BytesMut,
    ) -> Result<(Option<String>, BytesMut), Error> {
        let describe: Describe = (&message).try_into()?;
        let name = describe.statement_name.clone();

        if describe.anonymous() {
            debug!("Anonymous describe");
            return Ok((None, message));
        }

        match self.prepared_statements.get(&name) {
            Some(prepared_stmt) => {
                let describe = describe.rename(&prepared_stmt.name);

                debug!(
                    "Rewrote describe `{}` to `{}`",
                    name, describe.statement_name
                );

                Ok((Some(name), describe.try_into()?))
            }

            None => {
                debug!("Got describe for unknown prepared statement {:?}", describe);

                Ok((None, message))
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

            match write_all_flush(&mut self.write, &response).await {
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
            query_start.elapsed().as_millis() as u64,
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
    }

    pub fn is_transaction_mode(&self) -> bool {
        self.client_pool_mode == PoolMode::Transaction
    }

    pub fn is_transparent_mode(&self) -> bool {
        self.client_pool_mode == PoolMode::Transparent
    }

    pub fn is_in_idle_transaction(&self) -> bool {
        self.xact_info.is_idle()
    }

    fn is_begin_statement(ast_vec: &[Statement]) -> bool {
        ast_vec.len() == 1 && matches!(ast_vec[0], Statement::StartTransaction { .. })
    }
}

fn parse_ast(
    initial_parsed_ast: &mut Option<Vec<sqlparser::ast::Statement>>,
    query_router: &mut QueryRouter,
    message: &BytesMut,
    client_identifier: &ClientIdentifier,
) -> Option<Vec<sqlparser::ast::Statement>> {
    // We don't want to parse again if we already parsed it as the initial message
    match *initial_parsed_ast {
        Some(_) => {
            let parsed_ast = initial_parsed_ast.take().unwrap();
            // if 'parsed_ast' is empty, it means that there was a failed
            // attempt to parse the query as a custom command, earlier above.
            if parsed_ast.is_empty() {
                None
            } else {
                Some(parsed_ast)
            }
        }
        None => match query_router.parse(message) {
            Ok(ast) => {
                let _ = query_router.infer(&ast);
                Some(ast)
            }
            Err(error) => {
                if error != Error::UnsupportedStatement {
                    warn!(
                        "Query parsing error: {} (client: {})",
                        error, client_identifier
                    );
                }
                None
            }
        },
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
