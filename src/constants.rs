/// Various protocol constants, as defined in
/// https://www.postgresql.org/docs/12/protocol-message-formats.html
/// and elsewhere in the source code.
/// Also other constants we use elsewhere.

// Used in the StartupMessage to indicate regular handshake.
pub const PROTOCOL_VERSION_NUMBER: i32 = 196608;

// SSLRequest: used to indicate we want an SSL connection.
pub const SSL_REQUEST_CODE: i32 = 80877103;

// CancelRequest: the cancel request code.
pub const CANCEL_REQUEST_CODE: i32 = 80877102;

// AuthenticationMD5Password
pub const MD5_ENCRYPTED_PASSWORD: i32 = 5;

// AuthenticationOk
pub const AUTHENTICATION_SUCCESSFUL: i32 = 0;

// ErrorResponse: A code identifying the field type; if zero, this is the message terminator and no string follows.
pub const MESSAGE_TERMINATOR: u8 = 0;
