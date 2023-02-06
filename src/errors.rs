/// Errors.

/// Various errors.
#[derive(Debug, PartialEq)]
pub enum Error {
    SocketError(String),
    ClientBadStartup,
    ProtocolSyncError(String),
    ServerError,
    BadConfig,
    AllServersDown,
    ClientError(String),
    TlsError,
    StatementTimeout,
    IdleTransactionTimeout,
    ShuttingDown,
    ParseBytesError(String),
}
