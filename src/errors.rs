//! Errors.

/// Various errors.
#[derive(Debug, PartialEq)]
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
    BadConfig,
    AllServersDown,
    ClientError(String),
    TlsError,
    StatementTimeout,
    ShuttingDown,
    ParseBytesError(String),
    AuthError(String),
    AuthPassthroughError(String),
}

#[derive(Clone, PartialEq, Debug)]
pub struct ClientIdentifier {
    pub application_name: String,
    pub username: String,
    pub pool_name: String,
}

impl ClientIdentifier {
    pub fn new(application_name: &str, username: &str, pool_name: &str) -> ClientIdentifier {
        ClientIdentifier {
            application_name: application_name.into(),
            username: username.into(),
            pool_name: pool_name.into(),
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

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match &self {
            &Error::ClientSocketError(error, client_identifier) => write!(
                f,
                "Error reading {} from client {}",
                error, client_identifier
            ),
            &Error::ClientGeneralError(error, client_identifier) => {
                write!(f, "{} {}", error, client_identifier)
            }
            &Error::ClientAuthImpossible(username) => write!(
                f,
                "Client auth not possible, \
                no cleartext password set for username: {} \
                in config and auth passthrough (query_auth) \
                is not set up.",
                username
            ),
            &Error::ClientAuthPassthroughError(error, client_identifier) => write!(
                f,
                "No cleartext password set, \
                    and no auth passthrough could not \
                    obtain the hash from server for {}, \
                    the error was: {}",
                client_identifier, error
            ),
            _ => todo!(),
        }
    }
}
