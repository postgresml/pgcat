// Stream wrapper.

use bytes::{Buf, BufMut, BytesMut};
use rustls_pemfile::{certs, rsa_private_keys};
use std::path::Path;
use std::sync::Arc;
use tokio::io::{split, AsyncReadExt, AsyncWriteExt, BufReader, ReadHalf, WriteHalf};
use tokio::net::{
    tcp::{OwnedReadHalf, OwnedWriteHalf},
    TcpStream,
};
use tokio_rustls::rustls::{self, Certificate, PrivateKey};
use tokio_rustls::server::TlsStream;
use tokio_rustls::TlsAcceptor;

use crate::config::get_config;
use crate::errors::Error;

// TLS
fn load_certs(path: &std::path::Path) -> std::io::Result<Vec<Certificate>> {
    certs(&mut std::io::BufReader::new(std::fs::File::open(path)?))
        .map_err(|_| std::io::Error::new(std::io::ErrorKind::InvalidInput, "invalid cert"))
        .map(|mut certs| certs.drain(..).map(Certificate).collect())
}

fn load_keys(path: &std::path::Path) -> std::io::Result<Vec<PrivateKey>> {
    rsa_private_keys(&mut std::io::BufReader::new(std::fs::File::open(path)?))
        .map_err(|_| std::io::Error::new(std::io::ErrorKind::InvalidInput, "invalid key"))
        .map(|mut keys| keys.drain(..).map(PrivateKey).collect())
}

pub struct Tls {
    pub acceptor: TlsAcceptor,
}

impl Tls {
    pub fn new() -> Result<Self, Error> {
        let config = get_config();

        let certs = match load_certs(&Path::new(&config.general.tls_certificate.unwrap())) {
            Ok(certs) => certs,
            Err(_) => return Err(Error::TlsError),
        };

        let mut keys = match load_keys(&Path::new(&config.general.tls_private_key.unwrap())) {
            Ok(keys) => keys,
            Err(_) => return Err(Error::TlsError),
        };

        let config = match rustls::ServerConfig::builder()
            .with_safe_defaults()
            .with_no_client_auth()
            .with_single_cert(certs, keys.remove(0))
            .map_err(|err| std::io::Error::new(std::io::ErrorKind::InvalidInput, err))
        {
            Ok(c) => c,
            Err(_) => return Err(Error::TlsError),
        };

        Ok(Tls {
            acceptor: TlsAcceptor::from(Arc::new(config)),
        })
    }
}

struct Stream {
    read: Option<BufReader<OwnedReadHalf>>,
    write: Option<OwnedWriteHalf>,
    tls_read: Option<BufReader<ReadHalf<TlsStream<TcpStream>>>>,
    tls_write: Option<WriteHalf<TlsStream<TcpStream>>>,
}

impl Stream {
    pub async fn new(stream: TcpStream, tls: Option<Tls>) -> Result<Stream, Error> {
        let config = get_config();

        match tls {
            None => {
                let (read, write) = stream.into_split();
                let read = BufReader::new(read);
                Ok(Self {
                    read: Some(read),
                    write: Some(write),
                    tls_read: None,
                    tls_write: None,
                })
            }

            Some(tls) => {
                let mut tls_stream = match tls.acceptor.accept(stream).await {
                    Ok(stream) => stream,
                    Err(_) => return Err(Error::TlsError),
                };

                let (read, write) = split(tls_stream);

                Ok(Self {
                    read: None,
                    write: None,
                    tls_read: Some(BufReader::new(read)),
                    tls_write: Some(write),
                })
            }
        }
    }
}

// impl tokio::io::AsyncRead for Stream {
//      fn poll_read(
//         mut self: core::pin::Pin<&mut Self>,
//         cx: &mut core::task::Context<'_>,
//         buf: &mut tokio::io::ReadBuf<'_>
//     ) -> core::task::Poll<std::io::Result<()>> {
//         match &mut self.get_mut().tls_read {
//             None => core::pin::Pin::new(self.read.as_mut().unwrap()).poll_read(cx, buf),
//             Some(mut tls) => core::pin::Pin::new(&mut tls).poll_read(cx, buf),
//         }
//      }
// }
