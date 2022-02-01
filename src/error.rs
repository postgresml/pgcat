#[derive(Debug, PartialEq, Copy, Clone)]
pub enum ErrorCode {
    SocketClosed,
    SocketError,
    ClientDisconnected,
}

#[derive(Debug, Clone, Copy)]
pub struct Error {
    pub code: ErrorCode,
}

impl Error {
    pub fn new(code: ErrorCode) -> Error {
        Error { code: code }
    }
}
