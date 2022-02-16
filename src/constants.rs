/// Various protocol constants, as defined in
/// https://www.postgresql.org/docs/12/protocol-message-formats.html
/// and elsewhere in the source code.

// Used in the StartupMessage to indicate regular handshake.
pub const PROTOCOL_VERSION_NUMBER: i32 = 196608;

// SSLRequest: used to indicate we want an SSL connection.
pub const SSL_REQUEST_CODE: i32 = 80877103;

// CancelRequest: the cancel request code.
pub const CANCEL_REQUEST_CODE: i32 = 80877102;
