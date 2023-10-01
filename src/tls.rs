// Stream wrapper.

use rustls_pemfile::{certs, read_one, Item};
use std::iter;
use std::path::Path;
use std::sync::Arc;
use std::time::SystemTime;
use tokio_rustls::rustls::{
    self,
    client::{verify_server_cert_signed_by_trust_anchor, ServerCertVerified, ServerCertVerifier},
    server::ParsedCertificate,
    Certificate, PrivateKey, RootCertStore, ServerName,
};
use tokio_rustls::TlsAcceptor;

use crate::config::get_config;
use crate::errors::Error;

use arc_swap::ArcSwap;
use log::{error, info, warn};
use once_cell::sync::Lazy;

pub static ROOT_CERT_STORE: Lazy<ArcSwap<RootCertStore>> =
    Lazy::new(|| ArcSwap::from_pointee(RootCertStore::empty()));

pub async fn reload_root_cert_store() -> Result<(), Error> {
    let mut store = RootCertStore::empty();

    match rustls_native_certs::load_native_certs() {
        Ok(certs) => {
            if certs.is_empty() {
                warn!("The OS does not have root certificates");
            } else {
                let result = store.add_parsable_certificates(&certs);

                info!(
                    "Root certificates have been reloaded: {} valid and {} invalid certs",
                    result.0, result.1
                );

                ROOT_CERT_STORE.store(Arc::new(store));
            }

            Ok(())
        }

        Err(err) => {
            error!("Root certificates reload error: {:?}", err);

            Err(Error::TlsError)
        }
    }
}

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

        let certs = match load_certs(Path::new(&config.general.tls_certificate.unwrap())) {
            Ok(certs) => certs,
            Err(_) => return Err(Error::TlsError),
        };

        let mut keys = match load_keys(Path::new(&config.general.tls_private_key.unwrap())) {
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

/// This structure is a stub for certificate validation in `rustls` and is needed for
/// the "prefer" certificate validation mode. (verify_server_certificate = false)
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

/// This structure is a stub for certificate validation in `rustls` and is needed for
/// the "verify-ca" certificate validation mode. (verify_server_certificate = "verify-ca")
pub struct OnlyRootCertificateVerification {}

impl ServerCertVerifier for OnlyRootCertificateVerification {
    /// This piece of code is taken from `tokio_rustls::rustls::client::WebPkiVerifier`.
    /// And it does everything the same, except for two things: which are either
    /// not needed by `PGCat` at this point in time, or not needed for the current
    /// implementation. (see commented out fragments below)
    fn verify_server_cert(
        &self,
        end_entity: &Certificate,
        intermediates: &[Certificate],
        _server_name: &ServerName,
        _scts: &mut dyn Iterator<Item = &[u8]>,
        _ocsp_response: &[u8],
        now: SystemTime,
    ) -> Result<ServerCertVerified, rustls::Error> {
        let cert = ParsedCertificate::try_from(end_entity)?;

        verify_server_cert_signed_by_trust_anchor(
            &cert,
            &(*(*ROOT_CERT_STORE.load())),
            intermediates,
            now,
        )?;

        // skip the policy check, for the reason that when `verify_server_certificate` is used,
        // this verification is not used in PGCat now.
        /*
        if let Some(policy) = &self.ct_policy {
            policy.verify(end_entity, now, scts)?;
        }
        */

        // omit trace output, since the rustls::log crate is private.
        /*
        if !ocsp_response.is_empty() {
            trace!("Unvalidated OCSP response: {:?}", ocsp_response.to_vec());
        }
        */

        // skip server name validation, for the reason that this code section is not needed for
        // the "verify-ca" validation mode.
        /*
        verify_server_name(&cert, server_name)?;
        */

        Ok(ServerCertVerified::assertion())
    }
}
