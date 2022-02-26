/// Helper functions to send one-off protocol messages
/// and handle TcpStream (TCP socket).
use bytes::{Buf, BufMut, BytesMut};
use md5::{Digest, Md5};
use tokio::io::{AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::{
    tcp::{OwnedReadHalf, OwnedWriteHalf},
    TcpStream,
};

use std::collections::HashMap;

use crate::errors::Error;

/// Tell the client that authentication handshake completed successfully.
pub async fn auth_ok(stream: &mut TcpStream) -> Result<(), Error> {
    let mut auth_ok = BytesMut::with_capacity(9);

    auth_ok.put_u8(b'R');
    auth_ok.put_i32(8);
    auth_ok.put_i32(0);

    Ok(write_all(stream, auth_ok).await?)
}

/// Give the client the process_id and secret we generated
/// used in query cancellation.
pub async fn backend_key_data(
    stream: &mut TcpStream,
    backend_id: i32,
    secret_key: i32,
) -> Result<(), Error> {
    let mut key_data = BytesMut::from(&b"K"[..]);
    key_data.put_i32(12);
    key_data.put_i32(backend_id);
    key_data.put_i32(secret_key);

    Ok(write_all(stream, key_data).await?)
}

/// Construct a `Q`: Query message.
pub fn simple_query(query: &str) -> BytesMut {
    let mut res = BytesMut::from(&b"Q"[..]);
    let query = format!("{}\0", query);

    res.put_i32(query.len() as i32 + 4);
    res.put_slice(&query.as_bytes());

    res
}

/// Tell the client we're ready for another query.
pub async fn ready_for_query(stream: &mut TcpStream) -> Result<(), Error> {
    let mut bytes = BytesMut::with_capacity(5);

    bytes.put_u8(b'Z');
    bytes.put_i32(5);
    bytes.put_u8(b'I'); // Idle

    Ok(write_all(stream, bytes).await?)
}

/// Send the startup packet the server. We're pretending we're a Pg client.
/// This tells the server which user we are and what database we want.
pub async fn startup(stream: &mut TcpStream, user: &str, database: &str) -> Result<(), Error> {
    let mut bytes = BytesMut::with_capacity(25);

    bytes.put_i32(196608); // Protocol number

    // User
    bytes.put(&b"user\0"[..]);
    bytes.put_slice(&user.as_bytes());
    bytes.put_u8(0);

    // Database
    bytes.put(&b"database\0"[..]);
    bytes.put_slice(&database.as_bytes());
    bytes.put_u8(0);
    bytes.put_u8(0); // Null terminator

    let len = bytes.len() as i32 + 4i32;

    let mut startup = BytesMut::with_capacity(len as usize);

    startup.put_i32(len);
    startup.put(bytes);

    match stream.write_all(&startup).await {
        Ok(_) => Ok(()),
        Err(_) => return Err(Error::SocketError),
    }
}

/// Parse StartupMessage parameters.
/// e.g. user, database, application_name, etc.
pub fn parse_startup(mut bytes: BytesMut) -> Result<HashMap<String, String>, Error> {
    let mut result = HashMap::new();
    let mut buf = Vec::new();
    let mut tmp = String::new();

    while bytes.has_remaining() {
        let mut c = bytes.get_u8();

        // Null-terminated C-strings.
        while c != 0 {
            tmp.push(c as char);
            c = bytes.get_u8();
        }

        if tmp.len() > 0 {
            buf.push(tmp.clone());
            tmp.clear();
        }
    }

    // Expect pairs of name and value
    // and at least one pair to be present.
    if buf.len() % 2 != 0 && buf.len() >= 2 {
        return Err(Error::ClientBadStartup);
    }

    let mut i = 0;
    while i < buf.len() {
        let name = buf[i].clone();
        let value = buf[i + 1].clone();
        let _ = result.insert(name, value);
        i += 2;
    }

    // Minimum required parameters
    // I want to have the user at the very minimum, according to the protocol spec.
    if !result.contains_key("user") {
        return Err(Error::ClientBadStartup);
    }

    Ok(result)
}

/// Send password challenge response to the server.
/// This is the MD5 challenge.
pub async fn md5_password(
    stream: &mut TcpStream,
    user: &str,
    password: &str,
    salt: &[u8],
) -> Result<(), Error> {
    let mut md5 = Md5::new();

    // First pass
    md5.update(&password.as_bytes());
    md5.update(&user.as_bytes());

    let output = md5.finalize_reset();

    // Second pass
    md5.update(format!("{:x}", output));
    md5.update(salt);

    let mut password = format!("md5{:x}", md5.finalize())
        .chars()
        .map(|x| x as u8)
        .collect::<Vec<u8>>();
    password.push(0);

    let mut message = BytesMut::with_capacity(password.len() as usize + 5);

    message.put_u8(b'p');
    message.put_i32(password.len() as i32 + 4);
    message.put_slice(&password[..]);

    Ok(write_all(stream, message).await?)
}

/// Implements a response to our custom `SET SHARDING KEY`
/// and `SET SERVER ROLE` commands.
/// This tells the client we're ready for the next query.
pub async fn custom_protocol_response_ok(
    stream: &mut OwnedWriteHalf,
    message: &str,
) -> Result<(), Error> {
    let mut res = BytesMut::with_capacity(25);

    let set_complete = BytesMut::from(&format!("{}\0", message)[..]);
    let len = (set_complete.len() + 4) as i32;

    // CommandComplete
    res.put_u8(b'C');
    res.put_i32(len);
    res.put_slice(&set_complete[..]);

    // ReadyForQuery (idle)
    res.put_u8(b'Z');
    res.put_i32(5);
    res.put_u8(b'I');

    write_all_half(stream, res).await
}

/// Send a custom error message to the client.
/// Tell the client we are ready for the next query and no rollback is necessary.
/// Docs on error codes: https://www.postgresql.org/docs/12/errcodes-appendix.html
pub async fn error_response(stream: &mut OwnedWriteHalf, message: &str) -> Result<(), Error> {
    let mut error = BytesMut::new();

    // Error level
    error.put_u8(b'S');
    error.put_slice(&b"FATAL\0"[..]);

    // Error level (non-translatable)
    error.put_u8(b'V');
    error.put_slice(&b"FATAL\0"[..]);

    // Error code: not sure how much this matters.
    error.put_u8(b'C');
    error.put_slice(&b"58000\0"[..]); // system_error, see Appendix A.

    // The short error message.
    error.put_u8(b'M');
    error.put_slice(&format!("{}\0", message).as_bytes());

    // No more fields follow.
    error.put_u8(0);

    // Ready for query, no rollback needed (I = idle).
    let mut ready_for_query = BytesMut::new();

    ready_for_query.put_u8(b'Z');
    ready_for_query.put_i32(5);
    ready_for_query.put_u8(b'I');

    // Compose the two message reply.
    let mut res = BytesMut::with_capacity(error.len() + ready_for_query.len() + 5);

    res.put_u8(b'E');
    res.put_i32(error.len() as i32 + 4);

    res.put(error);
    res.put(ready_for_query);

    Ok(write_all_half(stream, res).await?)
}

/// Respond to a SHOW SHARD command.
pub async fn show_response(
    stream: &mut OwnedWriteHalf,
    name: &str,
    value: &str,
) -> Result<(), Error> {
    // A SELECT response consists of:
    // 1. RowDescription
    // 2. One or more DataRow
    // 3. CommandComplete
    // 4. ReadyForQuery

    // RowDescription
    let mut row_desc = BytesMut::new();

    // Number of columns: 1
    row_desc.put_i16(1);

    // Column name
    row_desc.put_slice(&format!("{}\0", name).as_bytes());

    // Doesn't belong to any table
    row_desc.put_i32(0);

    // Doesn't belong to any table
    row_desc.put_i16(0);

    // Text
    row_desc.put_i32(25);

    // Text size = variable (-1)
    row_desc.put_i16(-1);

    // Type modifier: none that I know
    row_desc.put_i32(0); //TODO maybe -1?

    // Format being used: text (0), binary (1)
    row_desc.put_i16(0);

    // DataRow
    let mut data_row = BytesMut::new();

    // Number of columns
    data_row.put_i16(1);

    // Size of the column content (length of the string really)
    data_row.put_i32(value.len() as i32);

    // The content
    data_row.put_slice(value.as_bytes());

    // CommandComplete
    let mut command_complete = BytesMut::new();

    // Number of rows returned (just one)
    command_complete.put_slice(&b"SELECT 1\0"[..]);

    // The final messages sent to the client
    let mut res = BytesMut::new();

    // RowDescription
    res.put_u8(b'T');
    res.put_i32(row_desc.len() as i32 + 4);
    res.put(row_desc);

    // DataRow
    res.put_u8(b'D');
    res.put_i32(data_row.len() as i32 + 4);
    res.put(data_row);

    // CommandComplete
    res.put_u8(b'C');
    res.put_i32(command_complete.len() as i32 + 4);
    res.put(command_complete);

    // ReadyForQuery
    res.put_u8(b'Z');
    res.put_i32(5);
    res.put_u8(b'I');

    write_all_half(stream, res).await
}

/// Write all data in the buffer to the TcpStream.
pub async fn write_all(stream: &mut TcpStream, buf: BytesMut) -> Result<(), Error> {
    match stream.write_all(&buf).await {
        Ok(_) => Ok(()),
        Err(_) => return Err(Error::SocketError),
    }
}

/// Write all the data in the buffer to the TcpStream, write owned half (see mpsc).
pub async fn write_all_half(stream: &mut OwnedWriteHalf, buf: BytesMut) -> Result<(), Error> {
    match stream.write_all(&buf).await {
        Ok(_) => Ok(()),
        Err(_) => return Err(Error::SocketError),
    }
}

/// Read a complete message from the socket.
pub async fn read_message(stream: &mut BufReader<OwnedReadHalf>) -> Result<BytesMut, Error> {
    let code = match stream.read_u8().await {
        Ok(code) => code,
        Err(_) => return Err(Error::SocketError),
    };

    let len = match stream.read_i32().await {
        Ok(len) => len,
        Err(_) => return Err(Error::SocketError),
    };

    let mut buf = vec![0u8; len as usize - 4];

    match stream.read_exact(&mut buf).await {
        Ok(_) => (),
        Err(_) => return Err(Error::SocketError),
    };

    let mut bytes = BytesMut::with_capacity(len as usize + 1);

    bytes.put_u8(code);
    bytes.put_i32(len);
    bytes.put_slice(&buf);

    Ok(bytes)
}
