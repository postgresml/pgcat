//! Errors.

/// Various errors.
#[derive(Debug, PartialEq, Clone)]
pub enum Error {
    SocketError(String),
    ClientSocketError(String, ClientIdentifier),
    ClientGeneralError(String, ClientIdentifier),
    ClientAuthImpossible(String),
    ClientAuthPassthroughError(String, ClientIdentifier),
    ClientBadStartup,
    ProtocolSyncError(String),
    BadQuery(String),
    ServerError,
    ServerStartupError(String, ServerIdentifier),
    ServerAuthError(String, ServerIdentifier),
    BadConfig,
    AllServersDown,
    ClientError(String),
    TlsError,
    StatementTimeout,
    DNSCachedError(String),
    ShuttingDown,
    ParseBytesError(String),
    AuthError(String),
    AuthPassthroughError(String),
    UnsupportedStatement,
    QueryRouterParserError(String),
    QueryRouterError(String),
}

#[derive(Clone, PartialEq, Debug)]
pub struct ClientIdentifier {
    pub application_name: String,
    pub username: String,
    pub pool_name: String,
}

impl ClientIdentifier {
    pub fn new<S: ToString>(application_name: S, username: S, pool_name: S) -> ClientIdentifier {
        ClientIdentifier {
            application_name: application_name.to_string(),
            username: username.to_string(),
            pool_name: pool_name.to_string(),
        }
    }
}

impl std::fmt::Display for ClientIdentifier {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(
            f,
            "{{ application_name: {}, username: {}, pool_name: {} }}",
            self.application_name, self.username, self.pool_name
        )
    }
}

#[derive(Clone, PartialEq, Debug)]
pub struct ServerIdentifier {
    pub username: String,
    pub database: String,
}

impl ServerIdentifier {
    pub fn new<S: ToString>(username: S, database: S) -> ServerIdentifier {
        ServerIdentifier {
            username: username.to_string(),
            database: database.to_string(),
        }
    }
}

impl std::fmt::Display for ServerIdentifier {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(
            f,
            "{{ username: {}, database: {} }}",
            self.username, self.database
        )
    }
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match &self {
            Error::ClientSocketError(error, client_identifier) => {
                write!(f, "Error reading {error} from client {client_identifier}",)
            }
            Error::ClientGeneralError(error, client_identifier) => {
                write!(f, "{error} {client_identifier}")
            }
            Error::ClientAuthImpossible(username) => write!(
                f,
                "Client auth not possible, \
                no cleartext password set for username: {username} \
                in config and auth passthrough (query_auth) \
                is not set up."
            ),
            Error::ClientAuthPassthroughError(error, client_identifier) => write!(
                f,
                "No cleartext password set, \
                    and no auth passthrough could not \
                    obtain the hash from server for {client_identifier}, \
                    the error was: {error}",
            ),
            Error::ServerStartupError(error, server_identifier) => write!(
                f,
                "Error reading {error} on server startup {server_identifier}",
            ),
            Error::ServerAuthError(error, server_identifier) => {
                write!(f, "{error} for {server_identifier}")
            }

            // The rest can use Debug.
            err => write!(f, "{err:?}"),
        }
    }
}

impl From<std::ffi::NulError> for Error {
    fn from(err: std::ffi::NulError) -> Self {
        Error::QueryRouterError(err.to_string())
    }
}
