/// Errors.

/// Various errors.
#[derive(Debug, PartialEq)]
pub enum Error {
    SocketError,
    ClientBadStartup,
    ProtocolSyncError,
    ServerError,
    BadConfig,
    AllServersDown,
}
