// Stream wrapper.

use bytes::{Buf, BufMut, BytesMut};
use tokio::io::{AsyncReadExt, AsyncWriteExt, BufReader, split, ReadHalf, WriteHalf};
use tokio::net::{
    tcp::{OwnedReadHalf, OwnedWriteHalf},
    TcpStream,
};
use tokio_rustls::server::TlsStream;
use rustls_pemfile::{certs, rsa_private_keys};
use tokio_rustls::rustls::{self, Certificate, PrivateKey};
use tokio_rustls::TlsAcceptor;
use std::sync::Arc;
use std::path::Path;

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

struct Tls {
	acceptor: TlsAcceptor,
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
        .map_err(|err| std::io::Error::new(std::io::ErrorKind::InvalidInput, err)) {
        	Ok(c) => c,
        	Err(_) => return Err(Error::TlsError)
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
				Ok(
					Self {
						read: Some(read),
						write: Some(write),
						tls_read: None,
						tls_write: None,
					}
				)
			}

			Some(tls) => {
				let mut tls_stream = match tls.acceptor.accept(stream).await {
					Ok(stream) => stream,
					Err(_) => return Err(Error::TlsError),
				};

				let (read, write) = split(tls_stream);

				Ok(Self{
					read: None,
					write: None,
					tls_read: Some(BufReader::new(read)),
					tls_write: Some(write),
				})
			}
		}
	}

	async fn read<S>(stream: &mut S) -> Result<BytesMut, Error>
	 where S: tokio::io::AsyncRead + std::marker::Unpin  {

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

	async fn write<S>(stream: &mut S, buf: &BytesMut) -> Result<(), Error>
	where S: tokio::io::AsyncWrite + std::marker::Unpin {
		match stream.write_all(buf).await {
			Ok(_) => Ok(()),
			Err(_) => return Err(Error::SocketError),
		}
	}

	pub async fn read_message(&mut self) -> Result<BytesMut, Error> {
		match &self.read {
			Some(read) => Self::read(self.read.as_mut().unwrap()).await,
			None => Self::read(self.tls_read.as_mut().unwrap()).await,
		}
	}

	pub async fn write_all(&mut self, buf: &BytesMut) -> Result<(), Error> {
		match &self.write {
			Some(write) => Self::write(self.write.as_mut().unwrap(), buf).await,
			None => Self::write(self.tls_write.as_mut().unwrap(), buf).await,
		}
	}
}