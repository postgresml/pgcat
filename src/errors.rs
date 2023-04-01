/// Errors.

/// Various errors.
#[derive(Debug, PartialEq)]
pub enum Error {
    SocketError(String),
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
}
