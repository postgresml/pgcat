// Stream wrapper.

use crate::config::get_config;
use crate::errors::Error;
use rustls::client::danger::HandshakeSignatureValid;
use rustls::pki_types::{CertificateDer, PrivateKeyDer, ServerName, UnixTime};
use rustls::DigitallySignedStruct;
use rustls_pemfile::{certs, read_one, Item};
use std::iter;
use std::path::Path;
use std::sync::Arc;
use tokio_rustls::rustls::crypto::{verify_tls12_signature, verify_tls13_signature};
use tokio_rustls::TlsAcceptor;

// TLS
pub fn load_certs(path: &Path) -> std::io::Result<Vec<CertificateDer>> {
    certs(&mut std::io::BufReader::new(std::fs::File::open(path)?))
        .collect::<Result<Vec<_>, _>>()
        .map_err(|_| std::io::Error::new(std::io::ErrorKind::InvalidInput, "invalid cert"))
        .map(|mut certs| std::mem::take(&mut certs))
}

pub fn load_keys(path: &Path) -> std::io::Result<Vec<PrivateKeyDer>> {
    let mut rd = std::io::BufReader::new(std::fs::File::open(path)?);

    iter::from_fn(|| read_one(&mut rd).transpose())
        .filter_map(|item| match item {
            Err(err) => Some(Err(err)),
            Ok(Item::Pkcs1Key(key)) => Some(Ok(PrivateKeyDer::Pkcs1(key))),
            Ok(Item::Sec1Key(key)) => Some(Ok(PrivateKeyDer::Sec1(key))),
            Ok(Item::Pkcs8Key(key)) => Some(Ok(PrivateKeyDer::Pkcs8(key))),
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
        let cert_ref: String = config.general.tls_certificate.unwrap();
        let certs = match load_certs(Path::new(cert_ref.leak())) {
            Ok(certs) => certs,
            Err(_) => return Err(Error::TlsError),
        };
        let pk_ref = config.general.tls_private_key.unwrap();
        let mut keys = match load_keys(Path::new(pk_ref.leak())) {
            Ok(keys) => keys,
            Err(_) => return Err(Error::TlsError),
        };

        let config = match rustls::ServerConfig::builder()
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

#[derive(Debug)]
pub struct NoCertificateVerification;

impl rustls::client::danger::ServerCertVerifier for NoCertificateVerification {
    fn verify_server_cert(
        &self,
        _end_entity: &CertificateDer<'_>,
        _intermediates: &[CertificateDer<'_>],
        _server_name: &ServerName<'_>,
        _ocsp: &[u8],
        _now: UnixTime,
    ) -> Result<rustls::client::danger::ServerCertVerified, rustls::Error> {
        Ok(rustls::client::danger::ServerCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        verify_tls12_signature(
            message,
            cert,
            dss,
            &rustls::crypto::ring::default_provider().signature_verification_algorithms,
        )
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        verify_tls13_signature(
            message,
            cert,
            dss,
            &rustls::crypto::ring::default_provider().signature_verification_algorithms,
        )
    }

    fn supported_verify_schemes(&self) -> Vec<rustls::SignatureScheme> {
        rustls::crypto::ring::default_provider()
            .signature_verification_algorithms
            .supported_schemes()
    }
}
