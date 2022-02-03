
use tokio::net::TcpStream;
use tokio::net::tcp::OwnedReadHalf;
use tokio::io::{AsyncWriteExt, BufReader, AsyncReadExt};
use bytes::{BufMut, BytesMut};

use crate::errors::Error;

pub async fn auth_ok(stream: &mut TcpStream) -> Result<(), Error> {
    let mut auth_ok = BytesMut::with_capacity(9);

    auth_ok.put_u8(b'R');
    auth_ok.put_i32(8);
    auth_ok.put_i32(0);

    Ok(write_all(stream, auth_ok).await?)
}

pub async fn ready_for_query(stream: &mut TcpStream) -> Result<(), Error> {
    let mut bytes = BytesMut::with_capacity(5);

    bytes.put_u8(b'Z');
    bytes.put_i32(5);
    bytes.put_u8(b'I'); // Idle

    Ok(write_all(stream, bytes).await?)
}

pub async fn write_all(stream: &mut TcpStream, buf: BytesMut) -> Result<(), Error> {
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