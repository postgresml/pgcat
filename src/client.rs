/// PostgreSQL client (frontend).
/// We are pretending to be the backend.

use tokio::net::TcpStream;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

use bytes::{BytesMut, Buf, BufMut};

use crate::errors::Error;
use crate::messages::*;

pub struct Client {
    stream: TcpStream,
}

impl Client {
    pub async fn startup(mut stream: TcpStream) -> Result<Client, Error> {
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

                    write_all(&mut stream, no).await?;
                },

                // Regular startup message.
                196608 => {
                    // TODO: perform actual auth.
                    // TODO: record startup parameters client sends over.
                    auth_ok(&mut stream).await?;
                    ready_for_query(&mut stream).await?;

                    return Ok(Client {
                        stream: stream,
                    });
                },

                _ => {
                    return Err(Error::ProtocolSyncError);
                }
            };
        }
    }
}