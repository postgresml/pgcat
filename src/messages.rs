
use tokio::net::TcpStream;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use bytes::{Buf, BufMut, BytesMut};

use crate::errors::Error;

/// Handle the startup phase for the client.
/// This one is special because Startup and SSLRequest
/// packages don't start with a u8 letter code.
pub async fn handle_client_startup(stream: &mut TcpStream) -> Result<(), Error> {
    loop {
        // Could be StartupMessage or SSLRequest
        // which makes this variable length.
        let len = match stream.read_i32().await {
            Ok(len) => len,
            Err(_) => return Err(Error::ClientBadStartup),
        };

        // Read whatever is left.
        let mut startup = vec![0u8; len as usize - 4];

        match stream.read_exact(&mut startup).await {
            Ok(_) => (),
            Err(_) => return Err(Error::ClientBadStartup),
        };

        let mut bytes = BytesMut::from(&startup[..]);
        let code = bytes.get_i32();

        match code {
            // Client wants SSL. We don't support it at the moment.
            80877103 => {
                let mut no = BytesMut::with_capacity(1);
                no.put_u8(b'N');

                write_all(stream, no).await?;
            },

            // Regular startup message.
            196608 => {
                // TODO: perform actual auth.
                // TODO: record startup parameters client sends over.
                auth_ok(stream).await?;
                ready_for_query(stream).await?;
                return Ok(());
            },

            _ => {
                return Err(Error::ProtocolSyncError);
            }
        };
    }
}


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