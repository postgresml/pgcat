#[derive(Debug, PartialEq)]
pub enum Error {
    SocketError,
    ClientDisconneted,
    ClientBadStartup,
    ProtocolSyncError,
}