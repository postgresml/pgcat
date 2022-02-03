use tokio::io::{AsyncReadExt, BufReader};
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
/// PostgreSQL client (frontend).
/// We are pretending to be the backend.
use tokio::net::TcpStream;

use bytes::{Buf, BufMut, BytesMut};

use crate::errors::Error;
use crate::messages::*;
use crate::server::Server;

pub struct Client {
    read: BufReader<OwnedReadHalf>,
    write: OwnedWriteHalf,
    buffer: BytesMut,
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
                }

                // Regular startup message.
                196608 => {
                    // TODO: perform actual auth.
                    // TODO: record startup parameters client sends over.
                    auth_ok(&mut stream).await?;
                    ready_for_query(&mut stream).await?;

                    let (read, write) = stream.into_split();

                    return Ok(Client {
                        read: BufReader::new(read),
                        write: write,
                        buffer: BytesMut::with_capacity(8196),
                    });
                }

                _ => {
                    return Err(Error::ProtocolSyncError);
                }
            };
        }
    }

    pub async fn handle(&mut self, mut server: Server) -> Result<(), Error> {
        loop {
            let mut message = read_message(&mut self.read).await?;
            let original = message.clone(); // To be forwarded to the server
            let code = message.get_u8() as char;
            let _len = message.get_i32() as usize;

            match code {
                'Q' => {
                    server.send(original).await?;
                    let response = server.recv().await?;
                    write_all_half(&mut self.write, response).await?;
                }

                'X' => {
                    // Client closing
                    return Ok(());
                }

                'P' => {
                    // Extended protocol, let's buffer most of it
                    self.buffer.put(&original[..]);
                }

                'B' => {
                    self.buffer.put(&original[..]);
                }

                'D' => {
                    self.buffer.put(&original[..]);
                }

                'E' => {
                    self.buffer.put(&original[..]);
                }

                'S' => {
                    // Extended protocol, client requests sync
                    self.buffer.put(&original[..]);
                    server.send(self.buffer.clone()).await?;
                    self.buffer.clear();

                    let response = server.recv().await?;
                    write_all_half(&mut self.write, response).await?;
                }

                _ => {
                    println!(">>> Unexpected code: {}", code);
                }
            }
        }
    }
}
