use bytes::{BufMut, BytesMut};
use md5::{Digest, Md5};
use tokio::io::{AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::net::TcpStream;

use crate::errors::Error;

// This is a funny one. `psql` parses this to figure out which
// queries to send when using shortcuts, e.g. \d+.
//
// TODO: Actually get the version from the server itself.
//
const SERVER_VESION: &str = "12.9 (Ubuntu 12.9-0ubuntu0.20.04.1)";

pub async fn auth_ok(stream: &mut TcpStream) -> Result<(), Error> {
    let mut auth_ok = BytesMut::with_capacity(9);

    auth_ok.put_u8(b'R');
    auth_ok.put_i32(8);
    auth_ok.put_i32(0);

    Ok(write_all(stream, auth_ok).await?)
}

pub async fn server_parameters(stream: &mut TcpStream) -> Result<(), Error> {
    let client_encoding = BytesMut::from(&b"client_encoding\0UTF8\0"[..]);
    let server_version =
        BytesMut::from(&format!("server_version\0{}\0", SERVER_VESION).as_bytes()[..]);

    // Client encoding
    let len = client_encoding.len() as i32 + 4; // TODO: add more parameters here
    let mut res = BytesMut::with_capacity(len as usize + 1);

    res.put_u8(b'S');
    res.put_i32(len);
    res.put_slice(&client_encoding[..]);

    let len = server_version.len() as i32 + 4;
    res.put_u8(b'S');
    res.put_i32(len);
    res.put_slice(&server_version[..]);

    Ok(write_all(stream, res).await?)
}

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

pub async fn ready_for_query(stream: &mut TcpStream) -> Result<(), Error> {
    let mut bytes = BytesMut::with_capacity(5);

    bytes.put_u8(b'Z');
    bytes.put_i32(5);
    bytes.put_u8(b'I'); // Idle

    Ok(write_all(stream, bytes).await?)
}

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

pub async fn write_all(stream: &mut TcpStream, buf: BytesMut) -> Result<(), Error> {
    match stream.write_all(&buf).await {
        Ok(_) => Ok(()),
        Err(_) => return Err(Error::SocketError),
    }
}

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
