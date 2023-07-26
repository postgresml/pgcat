// Stream wrapper.

use rustls_pemfile::{certs, read_one, Item};
use std::iter;
use std::path::Path;
use std::sync::Arc;
use std::time::SystemTime;
use tokio_rustls::rustls::{
    self,
    client::{ServerCertVerified, ServerCertVerifier},
    Certificate, PrivateKey, ServerName,
};
use tokio_rustls::TlsAcceptor;

use crate::config::get_config;
use crate::errors::Error;

// TLS
pub fn load_certs(path: &Path) -> std::io::Result<Vec<Certificate>> {
    certs(&mut std::io::BufReader::new(std::fs::File::open(path)?))
        .map_err(|_| std::io::Error::new(std::io::ErrorKind::InvalidInput, "invalid cert"))
        .map(|mut certs| certs.drain(..).map(Certificate).collect())
}

pub fn load_keys(path: &Path) -> std::io::Result<Vec<PrivateKey>> {
    let mut rd = std::io::BufReader::new(std::fs::File::open(path)?);

    iter::from_fn(|| read_one(&mut rd).transpose())
        .filter_map(|item| match item {
            Err(err) => Some(Err(err)),
            Ok(Item::RSAKey(key)) => Some(Ok(PrivateKey(key))),
            Ok(Item::ECKey(key)) => Some(Ok(PrivateKey(key))),
            Ok(Item::PKCS8Key(key)) => Some(Ok(PrivateKey(key))),
            _ => None,
        })
        .collect()
}

pub struct Tls {
    pub acceptor: TlsAcceptor,
}

impl Tls {
    pub fn new() -> Result<Self, Error> {
        let config = get_config();

        let certs = load_certs(Path::new(&config.general.tls_certificate.unwrap()))
            .map_err(|_| Error::TlsError)?;
        let key_der = load_keys(Path::new(&config.general.tls_private_key.unwrap()))
            .map_err(|_| Error::TlsError)?
            .remove(0);

        let config = rustls::ServerConfig::builder()
            .with_safe_defaults()
            .with_no_client_auth()
            .with_single_cert(certs, key_der)
            .map_err(|_| Error::TlsError)?;

        Ok(Tls {
            acceptor: TlsAcceptor::from(Arc::new(config)),
        })
    }
}

pub struct NoCertificateVerification;

impl ServerCertVerifier for NoCertificateVerification {
    fn verify_server_cert(
        &self,
        _end_entity: &Certificate,
        _intermediates: &[Certificate],
        _server_name: &ServerName,
        _scts: &mut dyn Iterator<Item = &[u8]>,
        _ocsp_response: &[u8],
        _now: SystemTime,
    ) -> Result<ServerCertVerified, rustls::Error> {
        Ok(ServerCertVerified::assertion())
    }
}
