/// Errors.

/// Various errors.
#[derive(Debug, PartialEq)]
pub enum Error {
    SocketError(String),
    ClientBadStartup,
    ClientIdleTransactionTimeout,
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
