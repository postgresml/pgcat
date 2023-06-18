/// Implementation of the PostgreSQL server (database) protocol.
/// Here we are pretending to the a Postgres client.
use bytes::{Buf, BufMut, BytesMut};
use fallible_iterator::FallibleIterator;
use log::{debug, error, info, trace, warn};
use parking_lot::{Mutex, RwLock};
use postgres_protocol::message;
use std::collections::{BTreeSet, HashMap};
use std::io::Read;
use std::net::IpAddr;
use std::sync::Arc;
use std::time::SystemTime;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, BufStream};
use tokio::net::TcpStream;
use tokio_rustls::rustls::{OwnedTrustAnchor, RootCertStore};
use tokio_rustls::{client::TlsStream, TlsConnector};

use crate::config::{get_config, get_prepared_statements_cache_size, Address, User};
use crate::constants::*;
use crate::dns_cache::{AddrSet, CACHED_RESOLVER};
use crate::errors::{Error, ServerIdentifier};
use crate::messages::*;
use crate::mirrors::MirroringManager;
use crate::pool::ClientServerMap;
use crate::scram::ScramSha256;
use crate::stats::ServerStats;
use std::io::Write;

use pin_project::pin_project;

#[pin_project(project = SteamInnerProj)]
pub enum StreamInner {
    Plain {
        #[pin]
        stream: TcpStream,
    },
    Tls {
        #[pin]
        stream: TlsStream<TcpStream>,
    },
}

impl AsyncWrite for StreamInner {
    fn poll_write(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &[u8],
    ) -> std::task::Poll<Result<usize, std::io::Error>> {
        let this = self.project();
        match this {
            SteamInnerProj::Tls { stream } => stream.poll_write(cx, buf),
            SteamInnerProj::Plain { stream } => stream.poll_write(cx, buf),
        }
    }

    fn poll_flush(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), std::io::Error>> {
        let this = self.project();
        match this {
            SteamInnerProj::Tls { stream } => stream.poll_flush(cx),
            SteamInnerProj::Plain { stream } => stream.poll_flush(cx),
        }
    }

    fn poll_shutdown(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), std::io::Error>> {
        let this = self.project();
        match this {
            SteamInnerProj::Tls { stream } => stream.poll_shutdown(cx),
            SteamInnerProj::Plain { stream } => stream.poll_shutdown(cx),
        }
    }
}

impl AsyncRead for StreamInner {
    fn poll_read(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        let this = self.project();
        match this {
            SteamInnerProj::Tls { stream } => stream.poll_read(cx, buf),
            SteamInnerProj::Plain { stream } => stream.poll_read(cx, buf),
        }
    }
}

impl StreamInner {
    pub fn try_write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        match self {
            StreamInner::Tls { stream } => {
                let r = stream.get_mut();
                let mut w = r.1.writer();
                w.write(buf)
            }
            StreamInner::Plain { stream } => stream.try_write(buf),
        }
    }
}

#[derive(Copy, Clone)]
struct CleanupState {
    /// If server connection requires DISCARD ALL before checkin because of set statement
    needs_cleanup_set: bool,

    /// If server connection requires DISCARD ALL before checkin because of prepare statement
    needs_cleanup_prepare: bool,
}

impl CleanupState {
    fn new() -> Self {
        CleanupState {
            needs_cleanup_set: false,
            needs_cleanup_prepare: false,
        }
    }

    fn needs_cleanup(&self) -> bool {
        self.needs_cleanup_set || self.needs_cleanup_prepare
    }

    fn set_true(&mut self) {
        self.needs_cleanup_set = true;
        self.needs_cleanup_prepare = true;
    }

    fn reset(&mut self) {
        self.needs_cleanup_set = false;
        self.needs_cleanup_prepare = false;
    }
}

impl std::fmt::Display for CleanupState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "SET: {}, PREPARE: {}",
            self.needs_cleanup_set, self.needs_cleanup_prepare
        )
    }
}

/// Server state.
pub struct Server {
    /// Server host, e.g. localhost,
    /// port, e.g. 5432, and role, e.g. primary or replica.
    address: Address,

    /// Server TCP connection.
    stream: BufStream<StreamInner>,

    /// Our server response buffer. We buffer data before we give it to the client.
    buffer: BytesMut,

    /// Server information the server sent us over on startup.
    server_info: BytesMut,

    /// Backend id and secret key used for query cancellation.
    process_id: i32,
    secret_key: i32,

    /// Is the server inside a transaction or idle.
    in_transaction: bool,

    /// Is there more data for the client to read.
    data_available: bool,

    /// Is the server broken? We'll remote it from the pool if so.
    bad: bool,

    /// If server connection requires DISCARD ALL before checkin
    cleanup_state: CleanupState,

    /// Mapping of clients and servers used for query cancellation.
    client_server_map: ClientServerMap,

    /// Server connected at.
    connected_at: chrono::naive::NaiveDateTime,

    /// Reports various metrics, e.g. data sent & received.
    stats: Arc<ServerStats>,

    /// Application name using the server at the moment.
    application_name: String,

    /// Last time that a successful server send or response happened
    last_activity: SystemTime,

    mirror_manager: Option<MirroringManager>,

    /// Associated addresses used
    addr_set: Option<AddrSet>,

    /// Should clean up dirty connections?
    cleanup_connections: bool,

    /// Prepared statements
    prepared_statements: BTreeSet<String>,
}

impl Server {
    /// Pretend to be the Postgres client and connect to the server given host, port and credentials.
    /// Perform the authentication and return the server in a ready for query state.
    pub async fn startup(
        address: &Address,
        user: &User,
        database: &str,
        client_server_map: ClientServerMap,
        stats: Arc<ServerStats>,
        auth_hash: Arc<RwLock<Option<String>>>,
        cleanup_connections: bool,
    ) -> Result<Server, Error> {
        let cached_resolver = CACHED_RESOLVER.load();
        let mut addr_set: Option<AddrSet> = None;

        // If we are caching addresses and hostname is not an IP
        if cached_resolver.enabled() && address.host.parse::<IpAddr>().is_err() {
            debug!("Resolving {}", &address.host);
            addr_set = match cached_resolver.lookup_ip(&address.host).await {
                Ok(ok) => {
                    debug!("Obtained: {:?}", ok);
                    Some(ok)
                }
                Err(err) => {
                    warn!("Error trying to resolve {}, ({:?})", &address.host, err);
                    None
                }
            }
        };

        let mut stream =
            match TcpStream::connect(&format!("{}:{}", &address.host, address.port)).await {
                Ok(stream) => stream,
                Err(err) => {
                    error!("Could not connect to server: {}", err);
                    return Err(Error::SocketError(format!(
                        "Could not connect to server: {}",
                        err
                    )));
                }
            };

        // TCP timeouts.
        configure_socket(&stream);

        let config = get_config();

        let mut stream = if config.general.server_tls {
            // Request a TLS connection
            ssl_request(&mut stream).await?;

            let response = match stream.read_u8().await {
                Ok(response) => response as char,
                Err(err) => {
                    return Err(Error::SocketError(format!(
                        "Server socket error: {:?}",
                        err
                    )))
                }
            };

            match response {
                // Server supports TLS
                'S' => {
                    debug!("Connecting to server using TLS");

                    let mut root_store = RootCertStore::empty();
                    root_store.add_server_trust_anchors(
                        webpki_roots::TLS_SERVER_ROOTS.0.iter().map(|ta| {
                            OwnedTrustAnchor::from_subject_spki_name_constraints(
                                ta.subject,
                                ta.spki,
                                ta.name_constraints,
                            )
                        }),
                    );

                    let mut tls_config = rustls::ClientConfig::builder()
                        .with_safe_defaults()
                        .with_root_certificates(root_store)
                        .with_no_client_auth();

                    // Equivalent to sslmode=prefer which is fine most places.
                    // If you want verify-full, change `verify_server_certificate` to true.
                    if !config.general.verify_server_certificate {
                        let mut dangerous = tls_config.dangerous();
                        dangerous.set_certificate_verifier(Arc::new(
                            crate::tls::NoCertificateVerification {},
                        ));
                    }

                    let connector = TlsConnector::from(Arc::new(tls_config));
                    let stream = match connector
                        .connect(address.host.as_str().try_into().unwrap(), stream)
                        .await
                    {
                        Ok(stream) => stream,
                        Err(err) => {
                            return Err(Error::SocketError(format!("Server TLS error: {:?}", err)))
                        }
                    };

                    StreamInner::Tls { stream }
                }

                // Server does not support TLS
                'N' => StreamInner::Plain { stream },

                // Something else?
                m => {
                    return Err(Error::SocketError(format!(
                        "Unknown message: {}",
                        m as char
                    )));
                }
            }
        } else {
            StreamInner::Plain { stream }
        };

        // let (read, write) = split(stream);
        // let (mut read, mut write) = (ReadInner::Plain { stream: read }, WriteInner::Plain { stream: write });

        trace!("Sending StartupMessage");

        // StartupMessage
        let username = match user.server_username {
            Some(ref server_username) => server_username,
            None => &user.username,
        };

        let password = match user.server_password {
            Some(ref server_password) => Some(server_password),
            None => match user.password {
                Some(ref password) => Some(password),
                None => None,
            },
        };

        startup(&mut stream, username, database).await?;

        let mut server_info = BytesMut::new();
        let mut process_id: i32 = 0;
        let mut secret_key: i32 = 0;
        let server_identifier = ServerIdentifier::new(username, &database);

        // We'll be handling multiple packets, but they will all be structured the same.
        // We'll loop here until this exchange is complete.
        let mut scram: Option<ScramSha256> = match password {
            Some(password) => Some(ScramSha256::new(password)),
            None => None,
        };

        loop {
            let code = match stream.read_u8().await {
                Ok(code) => code as char,
                Err(_) => {
                    return Err(Error::ServerStartupError(
                        "message code".into(),
                        server_identifier,
                    ))
                }
            };

            let len = match stream.read_i32().await {
                Ok(len) => len,
                Err(_) => {
                    return Err(Error::ServerStartupError(
                        "message len".into(),
                        server_identifier,
                    ))
                }
            };

            trace!("Message: {}", code);

            match code {
                // Authentication
                'R' => {
                    // Determine which kind of authentication is required, if any.
                    let auth_code = match stream.read_i32().await {
                        Ok(auth_code) => auth_code,
                        Err(_) => {
                            return Err(Error::ServerStartupError(
                                "auth code".into(),
                                server_identifier,
                            ))
                        }
                    };

                    trace!("Auth: {}", auth_code);

                    match auth_code {
                        MD5_ENCRYPTED_PASSWORD => {
                            // The salt is 4 bytes.
                            // See: https://www.postgresql.org/docs/12/protocol-message-formats.html
                            let mut salt = vec![0u8; 4];

                            match stream.read_exact(&mut salt).await {
                                Ok(_) => (),
                                Err(_) => {
                                    return Err(Error::ServerStartupError(
                                        "salt".into(),
                                        server_identifier,
                                    ))
                                }
                            };

                            match password {
                                // Using plaintext password
                                Some(password) => {
                                    md5_password(&mut stream, username, password, &salt[..]).await?
                                }

                                // Using auth passthrough, in this case we should already have a
                                // hash obtained when the pool was validated. If we reach this point
                                // and don't have a hash, we return an error.
                                None => {
                                    let option_hash = (*auth_hash.read()).clone();
                                    match option_hash {
                                        Some(hash) =>
                                            md5_password_with_hash(
                                                &mut stream,
                                                &hash,
                                                &salt[..],
                                            )
                                            .await?,
                                        None => return Err(
                                            Error::ServerAuthError(
                                                "Auth passthrough (auth_query) failed and no user password is set in cleartext".into(),
                                                server_identifier
                                            )
                                        ),
                                    }
                                }
                            }
                        }

                        AUTHENTICATION_SUCCESSFUL => (),

                        SASL => {
                            if scram.is_none() {
                                return Err(Error::ServerAuthError(
                                    "SASL auth required and no password specified. \
                                    Auth passthrough (auth_query) method is currently \
                                    unsupported for SASL auth"
                                        .into(),
                                    server_identifier,
                                ));
                            }

                            debug!("Starting SASL authentication");

                            let sasl_len = (len - 8) as usize;
                            let mut sasl_auth = vec![0u8; sasl_len];

                            match stream.read_exact(&mut sasl_auth).await {
                                Ok(_) => (),
                                Err(_) => {
                                    return Err(Error::ServerStartupError(
                                        "sasl message".into(),
                                        server_identifier,
                                    ))
                                }
                            };

                            let sasl_type = String::from_utf8_lossy(&sasl_auth[..sasl_len - 2]);

                            if sasl_type.contains(SCRAM_SHA_256) {
                                debug!("Using {}", SCRAM_SHA_256);

                                // Generate client message.
                                let sasl_response = scram.as_mut().unwrap().message();

                                // SASLInitialResponse (F)
                                let mut res = BytesMut::new();
                                res.put_u8(b'p');

                                // length + String length + length + length of sasl response
                                res.put_i32(
                                    4 // i32 size
                                        + SCRAM_SHA_256.len() as i32 // length of SASL version string,
                                        + 1 // Null terminator for the SASL version string,
                                        + 4 // i32 size
                                        + sasl_response.len() as i32, // length of SASL response
                                );

                                res.put_slice(format!("{}\0", SCRAM_SHA_256).as_bytes());
                                res.put_i32(sasl_response.len() as i32);
                                res.put(sasl_response);

                                write_all_flush(&mut stream, &res).await?;
                            } else {
                                error!("Unsupported SCRAM version: {}", sasl_type);
                                return Err(Error::ServerError);
                            }
                        }

                        SASL_CONTINUE => {
                            trace!("Continuing SASL");

                            let mut sasl_data = vec![0u8; (len - 8) as usize];

                            match stream.read_exact(&mut sasl_data).await {
                                Ok(_) => (),
                                Err(_) => {
                                    return Err(Error::ServerStartupError(
                                        "sasl cont message".into(),
                                        server_identifier,
                                    ))
                                }
                            };

                            let msg = BytesMut::from(&sasl_data[..]);
                            let sasl_response = scram.as_mut().unwrap().update(&msg)?;

                            // SASLResponse
                            let mut res = BytesMut::new();
                            res.put_u8(b'p');
                            res.put_i32(4 + sasl_response.len() as i32);
                            res.put(sasl_response);

                            write_all_flush(&mut stream, &res).await?;
                        }

                        SASL_FINAL => {
                            trace!("Final SASL");

                            let mut sasl_final = vec![0u8; len as usize - 8];
                            match stream.read_exact(&mut sasl_final).await {
                                Ok(_) => (),
                                Err(_) => {
                                    return Err(Error::ServerStartupError(
                                        "sasl final message".into(),
                                        server_identifier,
                                    ))
                                }
                            };

                            match scram
                                .as_mut()
                                .unwrap()
                                .finish(&BytesMut::from(&sasl_final[..]))
                            {
                                Ok(_) => {
                                    debug!("SASL authentication successful");
                                }

                                Err(err) => {
                                    debug!("SASL authentication failed");
                                    return Err(err);
                                }
                            };
                        }

                        _ => {
                            error!("Unsupported authentication mechanism: {}", auth_code);
                            return Err(Error::ServerError);
                        }
                    }
                }

                // ErrorResponse
                'E' => {
                    let error_code = match stream.read_u8().await {
                        Ok(error_code) => error_code,
                        Err(_) => {
                            return Err(Error::ServerStartupError(
                                "error code message".into(),
                                server_identifier,
                            ))
                        }
                    };

                    trace!("Error: {}", error_code);

                    match error_code {
                        // No error message is present in the message.
                        MESSAGE_TERMINATOR => (),

                        // An error message will be present.
                        _ => {
                            // Read the error message without the terminating null character.
                            let mut error = vec![0u8; len as usize - 4 - 1];

                            match stream.read_exact(&mut error).await {
                                Ok(_) => (),
                                Err(_) => {
                                    return Err(Error::ServerStartupError(
                                        "error message".into(),
                                        server_identifier,
                                    ))
                                }
                            };

                            // TODO: the error message contains multiple fields; we can decode them and
                            // present a prettier message to the user.
                            // See: https://www.postgresql.org/docs/12/protocol-error-fields.html
                            error!("Server error: {}", String::from_utf8_lossy(&error));
                        }
                    };

                    return Err(Error::ServerError);
                }

                // ParameterStatus
                'S' => {
                    let mut param = vec![0u8; len as usize - 4];

                    match stream.read_exact(&mut param).await {
                        Ok(_) => (),
                        Err(_) => {
                            return Err(Error::ServerStartupError(
                                "parameter status message".into(),
                                server_identifier,
                            ))
                        }
                    };

                    // Save the parameter so we can pass it to the client later.
                    // These can be server_encoding, client_encoding, server timezone, Postgres version,
                    // and many more interesting things we should know about the Postgres server we are talking to.
                    server_info.put_u8(b'S');
                    server_info.put_i32(len);
                    server_info.put_slice(&param[..]);
                }

                // BackendKeyData
                'K' => {
                    // The frontend must save these values if it wishes to be able to issue CancelRequest messages later.
                    // See: <https://www.postgresql.org/docs/12/protocol-message-formats.html>.
                    process_id = match stream.read_i32().await {
                        Ok(id) => id,
                        Err(_) => {
                            return Err(Error::ServerStartupError(
                                "process id message".into(),
                                server_identifier,
                            ))
                        }
                    };

                    secret_key = match stream.read_i32().await {
                        Ok(id) => id,
                        Err(_) => {
                            return Err(Error::ServerStartupError(
                                "secret key message".into(),
                                server_identifier,
                            ))
                        }
                    };
                }

                // ReadyForQuery
                'Z' => {
                    let mut idle = vec![0u8; len as usize - 4];

                    match stream.read_exact(&mut idle).await {
                        Ok(_) => (),
                        Err(_) => {
                            return Err(Error::ServerStartupError(
                                "transaction status message".into(),
                                server_identifier,
                            ))
                        }
                    };

                    let mut server = Server {
                        address: address.clone(),
                        stream: BufStream::new(stream),
                        buffer: BytesMut::with_capacity(8196),
                        server_info,
                        process_id,
                        secret_key,
                        in_transaction: false,
                        data_available: false,
                        bad: false,
                        cleanup_state: CleanupState::new(),
                        client_server_map,
                        addr_set,
                        connected_at: chrono::offset::Utc::now().naive_utc(),
                        stats,
                        application_name: String::new(),
                        last_activity: SystemTime::now(),
                        mirror_manager: match address.mirrors.len() {
                            0 => None,
                            _ => Some(MirroringManager::from_addresses(
                                user.clone(),
                                database.to_owned(),
                                address.mirrors.clone(),
                            )),
                        },
                        cleanup_connections,
                        prepared_statements: BTreeSet::new(),
                    };

                    server.set_name("pgcat").await?;

                    return Ok(server);
                }

                // We have an unexpected message from the server during this exchange.
                // Means we implemented the protocol wrong or we're not talking to a Postgres server.
                _ => {
                    error!("Unknown code: {}", code);
                    return Err(Error::ProtocolSyncError(format!(
                        "Unknown server code: {}",
                        code
                    )));
                }
            };
        }
    }

    /// Issue a query cancellation request to the server.
    /// Uses a separate connection that's not part of the connection pool.
    pub async fn cancel(
        host: &str,
        port: u16,
        process_id: i32,
        secret_key: i32,
    ) -> Result<(), Error> {
        let mut stream = match TcpStream::connect(&format!("{}:{}", host, port)).await {
            Ok(stream) => stream,
            Err(err) => {
                error!("Could not connect to server: {}", err);
                return Err(Error::SocketError("Error reading cancel message".into()));
            }
        };
        configure_socket(&stream);

        debug!("Sending CancelRequest");

        let mut bytes = BytesMut::with_capacity(16);
        bytes.put_i32(16);
        bytes.put_i32(CANCEL_REQUEST_CODE);
        bytes.put_i32(process_id);
        bytes.put_i32(secret_key);

        write_all_flush(&mut stream, &bytes).await
    }

    /// Send messages to the server from the client.
    pub async fn send(&mut self, messages: &BytesMut) -> Result<(), Error> {
        self.mirror_send(messages);
        self.stats().data_sent(messages.len());

        match write_all_flush(&mut self.stream, &messages).await {
            Ok(_) => {
                // Successfully sent to server
                self.last_activity = SystemTime::now();
                Ok(())
            }
            Err(err) => {
                error!(
                    "Terminating server {:?} because of: {:?}",
                    self.address, err
                );
                self.bad = true;
                Err(err)
            }
        }
    }

    /// Receive data from the server in response to a client request.
    /// This method must be called multiple times while `self.is_data_available()` is true
    /// in order to receive all data the server has to offer.
    pub async fn recv(&mut self) -> Result<BytesMut, Error> {
        loop {
            let mut message = match read_message(&mut self.stream).await {
                Ok(message) => message,
                Err(err) => {
                    error!(
                        "Terminating server {:?} because of: {:?}",
                        self.address, err
                    );
                    self.bad = true;
                    return Err(err);
                }
            };

            // Buffer the message we'll forward to the client later.
            self.buffer.put(&message[..]);

            let code = message.get_u8() as char;
            let _len = message.get_i32();

            trace!("Message: {}", code);

            match code {
                // ReadyForQuery
                'Z' => {
                    let transaction_state = message.get_u8() as char;

                    match transaction_state {
                        // In transaction.
                        'T' => {
                            self.in_transaction = true;
                        }

                        // Idle, transaction over.
                        'I' => {
                            self.in_transaction = false;
                        }

                        // Some error occurred, the transaction was rolled back.
                        'E' => {
                            self.in_transaction = true;
                        }

                        // Something totally unexpected, this is not a Postgres server we know.
                        _ => {
                            self.bad = true;
                            return Err(Error::ProtocolSyncError(format!(
                                "Unknown transaction state: {}",
                                transaction_state
                            )));
                        }
                    };

                    // There is no more data available from the server.
                    self.data_available = false;

                    break;
                }

                // CommandComplete
                'C' => {
                    let mut command_tag = String::new();
                    match message.reader().read_to_string(&mut command_tag) {
                        Ok(_) => {
                            // Non-exhaustive list of commands that are likely to change session variables/resources
                            // which can leak between clients. This is a best effort to block bad clients
                            // from poisoning a transaction-mode pool by setting inappropriate session variables
                            match command_tag.as_str() {
                                "SET\0" => {
                                    // We don't detect set statements in transactions
                                    // No great way to differentiate between set and set local
                                    // As a result, we will miss cases when set statements are used in transactions
                                    // This will reduce amount of discard statements sent
                                    if !self.in_transaction {
                                        debug!("Server connection marked for clean up");
                                        self.cleanup_state.needs_cleanup_set = true;
                                    }
                                }
                                "PREPARE\0" => {
                                    debug!("Server connection marked for clean up");
                                    self.cleanup_state.needs_cleanup_prepare = true;
                                }
                                _ => (),
                            }
                        }

                        Err(err) => {
                            warn!("Encountered an error while parsing CommandTag {}", err);
                        }
                    }
                }

                // DataRow
                'D' => {
                    // More data is available after this message, this is not the end of the reply.
                    self.data_available = true;

                    // Don't flush yet, the more we buffer, the faster this goes...up to a limit.
                    if self.buffer.len() >= 8196 {
                        break;
                    }
                }

                // CopyInResponse: copy is starting from client to server.
                'G' => break,

                // CopyOutResponse: copy is starting from the server to the client.
                'H' => {
                    self.data_available = true;
                    break;
                }

                // CopyData
                'd' => {
                    // Don't flush yet, buffer until we reach limit
                    if self.buffer.len() >= 8196 {
                        break;
                    }
                }

                // CopyDone
                // Buffer until ReadyForQuery shows up, so don't exit the loop yet.
                'c' => (),

                // Anything else, e.g. errors, notices, etc.
                // Keep buffering until ReadyForQuery shows up.
                _ => (),
            };
        }

        let bytes = self.buffer.clone();

        // Keep track of how much data we got from the server for stats.
        self.stats().data_received(bytes.len());

        // Clear the buffer for next query.
        self.buffer.clear();

        // Successfully received data from server
        self.last_activity = SystemTime::now();

        // Pass the data back to the client.
        Ok(bytes)
    }

    /// Add the prepared statement to being tracked by this server.
    /// The client is processing data that will create a prepared statement on this server.
    pub fn will_prepare(&mut self, name: &str) {
        debug!("Will prepare `{}`", name);

        self.prepared_statements.insert(name.to_string());
        self.stats.prepared_cache_add();
    }

    /// Check if we should prepare a statement on the server.
    pub fn should_prepare(&self, name: &str) -> bool {
        let should_prepare = !self.prepared_statements.contains(name);

        debug!("Should prepare `{}`: {}", name, should_prepare);

        if should_prepare {
            self.stats.prepared_cache_miss();
        } else {
            self.stats.prepared_cache_hit();
        }

        should_prepare
    }

    /// Create a prepared statement on the server.
    pub async fn prepare(&mut self, parse: &Parse) -> Result<(), Error> {
        debug!("Preparing `{}`", parse.name);

        let bytes: BytesMut = parse.try_into()?;
        self.send(&bytes).await?;
        self.send(&flush()).await?;

        // Read and discard ParseComplete (B)
        match read_message(&mut self.stream).await {
            Ok(_) => (),
            Err(err) => {
                self.bad = true;
                return Err(err);
            }
        }

        self.prepared_statements.insert(parse.name.to_string());
        self.stats.prepared_cache_add();

        debug!("Prepared `{}`", parse.name);

        Ok(())
    }

    /// Maintain adequate cache size on the server.
    pub async fn maintain_cache(&mut self) -> Result<(), Error> {
        debug!("Cache maintenance run");

        let max_cache_size = get_prepared_statements_cache_size();
        let mut names = Vec::new();

        while self.prepared_statements.len() >= max_cache_size {
            // The prepared statmeents are alphanumerically sorted by the BTree.
            // FIFO.
            if let Some(name) = self.prepared_statements.pop_last() {
                names.push(name);
            }
        }

        self.deallocate(names).await?;

        Ok(())
    }

    /// Remove the prepared statement from being tracked by this server.
    /// The client is processing data that will cause the server to close the prepared statement.
    pub fn will_close(&mut self, name: &str) {
        debug!("Will close `{}`", name);

        self.prepared_statements.remove(name);
    }

    /// Close a prepared statement on the server.
    pub async fn deallocate(&mut self, names: Vec<String>) -> Result<(), Error> {
        for name in &names {
            debug!("Deallocating prepared statement `{}`", name);

            let close = Close::new(name);
            let bytes: BytesMut = close.try_into()?;

            self.send(&bytes).await?;
        }

        self.send(&flush()).await?;

        // Read and discard CloseComplete (3)
        for name in &names {
            match read_message(&mut self.stream).await {
                Ok(_) => {
                    self.prepared_statements.remove(name);
                    self.stats.prepared_cache_remove();
                    debug!("Closed `{}`", name);
                }

                Err(err) => {
                    self.bad = true;
                    return Err(err);
                }
            };
        }

        Ok(())
    }

    /// If the server is still inside a transaction.
    /// If the client disconnects while the server is in a transaction, we will clean it up.
    pub fn in_transaction(&self) -> bool {
        debug!("Server in transaction: {}", self.in_transaction);
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
        if self.bad {
            return self.bad;
        };
        let cached_resolver = CACHED_RESOLVER.load();
        if cached_resolver.enabled() {
            if let Some(addr_set) = &self.addr_set {
                if cached_resolver.has_changed(self.address.host.as_str(), addr_set) {
                    warn!(
                        "DNS changed for {}, it was {:?}. Dropping server connection.",
                        self.address.host.as_str(),
                        addr_set
                    );
                    return true;
                }
            }
        }
        false
    }

    /// Get server startup information to forward it to the client.
    /// Not used at the moment.
    pub fn server_info(&self) -> BytesMut {
        self.server_info.clone()
    }

    /// Indicate that this server connection cannot be re-used and must be discarded.
    pub fn mark_bad(&mut self) {
        error!("Server {:?} marked bad", self.address);
        self.bad = true;
    }

    /// Claim this server as mine for the purposes of query cancellation.
    pub fn claim(&mut self, process_id: i32, secret_key: i32) {
        let mut guard = self.client_server_map.lock();
        guard.insert(
            (process_id, secret_key),
            (
                self.process_id,
                self.secret_key,
                self.address.host.clone(),
                self.address.port,
            ),
        );
    }

    /// Execute an arbitrary query against the server.
    /// It will use the simple query protocol.
    /// Result will not be returned, so this is useful for things like `SET` or `ROLLBACK`.
    pub async fn query(&mut self, query: &str) -> Result<(), Error> {
        debug!("Running `{}` on server {:?}", query, self.address);

        let query = simple_query(query);

        self.send(&query).await?;

        loop {
            let _ = self.recv().await?;

            if !self.data_available {
                break;
            }
        }

        Ok(())
    }

    /// Perform any necessary cleanup before putting the server
    /// connection back in the pool
    pub async fn checkin_cleanup(&mut self) -> Result<(), Error> {
        // Client disconnected with an open transaction on the server connection.
        // Pgbouncer behavior is to close the server connection but that can cause
        // server connection thrashing if clients repeatedly do this.
        // Instead, we ROLLBACK that transaction before putting the connection back in the pool
        if self.in_transaction() {
            warn!("Server returned while still in transaction, rolling back transaction");
            self.query("ROLLBACK").await?;
        }

        // Client disconnected but it performed session-altering operations such as
        // SET statement_timeout to 1 or create a prepared statement. We clear that
        // to avoid leaking state between clients. For performance reasons we only
        // send `DISCARD ALL` if we think the session is altered instead of just sending
        // it before each checkin.
        if self.cleanup_state.needs_cleanup() && self.cleanup_connections {
            warn!("Server returned with session state altered, discarding state ({}) for application {}", self.cleanup_state, self.application_name);
            self.query("DISCARD ALL").await?;
            self.query("RESET ROLE").await?;
            self.cleanup_state.reset();
        }

        Ok(())
    }

    /// A shorthand for `SET application_name = $1`.
    pub async fn set_name(&mut self, name: &str) -> Result<(), Error> {
        if self.application_name != name {
            self.application_name = name.to_string();
            // We don't want `SET application_name` to mark the server connection
            // as needing cleanup
            let needs_cleanup_before = self.cleanup_state;

            let result = Ok(self
                .query(&format!("SET application_name = '{}'", name))
                .await?);
            self.cleanup_state = needs_cleanup_before;
            result
        } else {
            Ok(())
        }
    }

    /// get Server stats
    pub fn stats(&self) -> Arc<ServerStats> {
        self.stats.clone()
    }

    /// Get the servers address.
    #[allow(dead_code)]
    pub fn address(&self) -> Address {
        self.address.clone()
    }

    // Get server's latest response timestamp
    pub fn last_activity(&self) -> SystemTime {
        self.last_activity
    }

    // Marks a connection as needing DISCARD ALL at checkin
    pub fn mark_dirty(&mut self) {
        self.cleanup_state.set_true();
    }

    pub fn mirror_send(&mut self, bytes: &BytesMut) {
        match self.mirror_manager.as_mut() {
            Some(manager) => manager.send(bytes),
            None => (),
        }
    }

    pub fn mirror_disconnect(&mut self) {
        match self.mirror_manager.as_mut() {
            Some(manager) => manager.disconnect(),
            None => (),
        }
    }

    // This is so we can execute out of band queries to the server.
    // The connection will be opened, the query executed and closed.
    pub async fn exec_simple_query(
        address: &Address,
        user: &User,
        query: &str,
    ) -> Result<Vec<String>, Error> {
        let client_server_map: ClientServerMap = Arc::new(Mutex::new(HashMap::new()));

        debug!("Connecting to server to obtain auth hashes.");
        let mut server = Server::startup(
            address,
            user,
            &address.database,
            client_server_map,
            Arc::new(ServerStats::default()),
            Arc::new(RwLock::new(None)),
            true,
        )
        .await?;
        debug!("Connected!, sending query.");
        server.send(&simple_query(query)).await?;
        let mut message = server.recv().await?;

        Ok(parse_query_message(&mut message).await?)
    }
}

async fn parse_query_message(message: &mut BytesMut) -> Result<Vec<String>, Error> {
    let mut pair = Vec::<String>::new();
    match message::backend::Message::parse(message) {
        Ok(Some(message::backend::Message::RowDescription(_description))) => {}
        Ok(Some(message::backend::Message::ErrorResponse(err))) => {
            return Err(Error::ProtocolSyncError(format!(
                "Protocol error parsing response. Err: {:?}",
                err.fields()
                    .iterator()
                    .fold(String::default(), |acc, element| acc
                        + element.unwrap().value())
            )))
        }
        Ok(_) => {
            return Err(Error::ProtocolSyncError(
                "Protocol error, expected Row Description.".to_string(),
            ))
        }
        Err(err) => {
            return Err(Error::ProtocolSyncError(format!(
                "Protocol error parsing response. Err: {:?}",
                err
            )))
        }
    }

    while !message.is_empty() {
        match message::backend::Message::parse(message) {
            Ok(postgres_message) => {
                match postgres_message {
                    Some(message::backend::Message::DataRow(data)) => {
                        let buf = data.buffer();
                        trace!("Data: {:?}", buf);

                        for item in data.ranges().iterator() {
                            match item.as_ref() {
                                Ok(range) => match range {
                                    Some(range) => {
                                        pair.push(String::from_utf8_lossy(&buf[range.clone()]).to_string());
                                    }
                                    None => return Err(Error::ProtocolSyncError(String::from(
                                        "Data expected while receiving query auth data, found nothing.",
                                    ))),
                                },
                                Err(err) => {
                                    return Err(Error::ProtocolSyncError(format!(
                                        "Data error, err: {:?}",
                                        err
                                    )))
                                }
                            }
                        }
                    }
                    Some(message::backend::Message::CommandComplete(_)) => {}
                    Some(message::backend::Message::ReadyForQuery(_)) => {}
                    _ => {
                        return Err(Error::ProtocolSyncError(
                            "Unexpected message while receiving auth query data.".to_string(),
                        ))
                    }
                }
            }
            Err(err) => {
                return Err(Error::ProtocolSyncError(format!(
                    "Parse error, err: {:?}",
                    err
                )))
            }
        };
    }
    Ok(pair)
}

impl Drop for Server {
    /// Try to do a clean shut down. Best effort because
    /// the socket is in non-blocking mode, so it may not be ready
    /// for a write.
    fn drop(&mut self) {
        self.mirror_disconnect();

        // Update statistics
        self.stats.disconnect();

        let mut bytes = BytesMut::with_capacity(5);
        bytes.put_u8(b'X');
        bytes.put_i32(4);

        match self.stream.get_mut().try_write(&bytes) {
            Ok(5) => (),
            _ => debug!("Dirty shutdown"),
        };

        let now = chrono::offset::Utc::now().naive_utc();
        let duration = now - self.connected_at;

        let message = if self.bad {
            "Server connection terminated"
        } else {
            "Server connection closed"
        };

        info!(
            "{} {:?}, session duration: {}",
            message,
            self.address,
            crate::format_duration(&duration)
        );
    }
}
