use crate::scram;
use base64::Engine;
use bytes::Bytes;
use hmac::{Hmac, Mac};
use rand::Rng;
use serde_derive::{Deserialize, Serialize};
use sha2::digest::FixedOutput;
use sha2::{Digest, Sha256};
use tokio::io::AsyncReadExt;

use crate::{
    errors::{ClientIdentifier, Error},
    messages::{authentication_sasl, authentication_sasl_continue},
};

/// Supported Authentication Methods
#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq, Hash)]
pub enum AuthMethod {
    #[serde(rename = "md5")]
    /// Perform MD5 authentication to verify the user's password
    Md5,
    #[serde(rename = "scram-sha-256")]
    /// Perform SCRAM-SHA-256 authentication to verify the user's password
    ScramSha256,
}

impl std::fmt::Display for AuthMethod {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            AuthMethod::Md5 => write!(f, "md5"),
            AuthMethod::ScramSha256 => write!(f, "scram-sha-256"),
        }
    }
}

/// Generate a random alphanumeric string of length `len`
pub fn rand_alphanumeric(len: usize) -> String {
    let mut rng = rand::thread_rng();
    let chars: String = std::iter::repeat(())
        .map(|()| rng.sample(rand::distributions::Alphanumeric))
        .map(char::from)
        .take(7)
        .collect();

    chars
}

/// Generate a random nonce
pub trait GenerateNonce {
    fn generate_nonce(len: usize) -> String;
}

pub struct DefaultNonceGenerator;

impl GenerateNonce for DefaultNonceGenerator {
    fn generate_nonce(len: usize) -> String {
        rand_alphanumeric(len)
    }
}

pub struct SaslAuthentication<'a, S, T> {
    read: &'a mut S,
    write: &'a mut T,
    client_identifier: &'a ClientIdentifier,
}

impl<'a, S, T> SaslAuthentication<'a, S, T> {
    pub fn new(read: &'a mut S, write: &'a mut T, client_identifier: &'a ClientIdentifier) -> Self {
        Self {
            read,
            write,
            client_identifier,
        }
    }
}

pub struct ClientFirstMessageBare {
    nonce: Bytes,
    raw: Bytes,
}

impl<'a, S, T> SaslAuthentication<'a, S, T>
where
    S: tokio::io::AsyncRead + std::marker::Unpin + Send + 'a,
    T: tokio::io::AsyncWrite + std::marker::Unpin + Send + 'a,
{
    pub async fn authenticate<N: GenerateNonce>(
        &mut self,
        password: &str,
        salt: &[u8],
        iteration_count: u32,
    ) -> Result<(), Error> {
        // Channel binding is not currently supported, so we only advertise the non-PLUS
        // variant
        let supported_auth_mechanisms = vec![SaslMechanism::ScramSha256];
        authentication_sasl(self.write, &supported_auth_mechanisms).await?;
        let client_first_message_bare = self
            .recv_client_sasl_initial_response(&supported_auth_mechanisms)
            .await?;

        let s_nonce: String = N::generate_nonce(12);
        let c_nonce = match std::str::from_utf8(client_first_message_bare.nonce.as_ref()) {
            Ok(s) => s,
            Err(_) => {
                return Err(Error::ProtocolSyncError(
                    "invalid client-first-message".to_string(),
                ))
            }
        };

        let server_first_message = self
            .send_auth_sasl_continue(
                s_nonce.as_bytes(),
                client_first_message_bare.nonce.as_ref(),
                &salt,
                iteration_count,
            )
            .await?;

        let client_final_message = self.recv_client_final_message().await?;
        let (client_final_message_without_proof, client_proof_b64) =
            match std::str::from_utf8(&client_final_message[..]) {
                Ok(s) => match s.rsplit_once(',') {
                    Some((without_proof, proof)) => {
                        if proof.starts_with("p=") {
                            (without_proof, &proof[2..])
                        } else {
                            return Err(Error::ProtocolSyncError(
                                "invalid client final message".to_string(),
                            ));
                        }
                    }
                    None => {
                        return Err(Error::ProtocolSyncError(
                            "invalid client final message".to_string(),
                        ));
                    }
                },
                Err(_) => {
                    return Err(Error::ProtocolSyncError(
                        "invalid final message".to_string(),
                    ))
                }
            };

        // Verify nonce
        let saved_nonce = format!("{}{}", c_nonce, s_nonce);
        match client_final_message_without_proof.split_once(",") {
            Some((_, nonce)) if nonce.starts_with("r=") && saved_nonce.as_str() == &nonce[2..] => {
                ()
            }
            _ => return Err(Error::AuthError("failed to validate nonce".to_string())),
        }

        let client_proof = match base64::engine::general_purpose::STANDARD.decode(client_proof_b64)
        {
            Ok(proof) => proof,
            Err(_) => return Err(Error::ProtocolSyncError("invalid client proof".to_string())),
        };

        let salted_password = scram::ScramSha256::hi(
            &scram::normalize(password.as_bytes()),
            &salt,
            iteration_count,
        );

        let mut hmac = match Hmac::<Sha256>::new_from_slice(&salted_password) {
            Ok(hmac) => hmac,
            Err(_) => return Err(Error::ServerError),
        };

        hmac.update(b"Client Key");

        let client_key = hmac.finalize().into_bytes();

        let mut hash = Sha256::default();
        hash.update(client_key.as_slice());

        let stored_key = hash.finalize_fixed();
        let auth_message = format!(
            "{},{},{}",
            String::from_utf8_lossy(client_first_message_bare.raw.as_ref()),
            server_first_message,
            client_final_message_without_proof,
        );

        let mut hmac = match Hmac::<Sha256>::new_from_slice(&stored_key) {
            Ok(hmac) => hmac,
            Err(_) => return Err(Error::ServerError),
        };

        hmac.update(auth_message.as_bytes());

        let client_signature = hmac.finalize().into_bytes();

        if client_signature.len() != client_proof.len() {
            return Err(Error::AuthError("invalid client proof".into()));
        }

        let unverified_client_key: Vec<u8> = client_signature
            .iter()
            .zip(client_proof)
            .map(|(x, y)| x ^ y)
            .collect();

        let mut hash = Sha256::default();
        hash.update(unverified_client_key.as_slice());

        if hash.finalize_fixed() != stored_key {
            return Err(Error::AuthError("invalid client proof".into()));
        }

        Ok(())
    }

    async fn recv_client_sasl_initial_response(
        &mut self,
        supported_auth_mechanisms: &[SaslMechanism],
    ) -> Result<ClientFirstMessageBare, Error> {
        let client_identifier = self.client_identifier.clone();
        match self.read.read_u8().await {
            Ok(p) => {
                if p != b'p' {
                    return Err(Error::ProtocolSyncError(format!(
                        "Expected p, got {}",
                        p as char
                    )));
                }
            }
            Err(e) => {
                log::error!("Error reading from client: {}", e);
                return Err(Error::ClientSocketError(
                    "sasl initial response code".into(),
                    client_identifier,
                ));
            }
        };

        let len = match self.read.read_i32().await {
            Ok(len) => len,
            Err(_) => {
                return Err(Error::ClientSocketError(
                    "sasl initial response length".into(),
                    client_identifier,
                ))
            }
        };

        // Rest of message. We buffer the entire message here to avoid
        // making multiple syscalls
        let mut rest = vec![0u8; (len - 4) as usize];

        match self.read.read_exact(&mut rest).await {
            Ok(_) => (),
            Err(_) => {
                return Err(Error::ClientSocketError(
                    "sasl initial response".into(),
                    client_identifier,
                ))
            }
        };

        let mut mechanism = String::new();
        let mut cursor = std::io::Cursor::new(&rest[..]);

        loop {
            // This incurs no overhead from the async call, as data inside
            // the cursor will be ready immediately
            match cursor.read_u8().await {
                Ok(0) => break,
                Ok(b) => mechanism.push(b as char),
                Err(_) => {
                    return Err(Error::ClientSocketError(
                        "sasl initial response mechanism".into(),
                        client_identifier,
                    ))
                }
            }
        }

        // Ensure that the requested auth mechanism the client requested is
        // supported
        if !supported_auth_mechanisms
            .iter()
            .map(|m| m.to_string())
            .collect::<Vec<String>>()
            .contains(&mechanism)
        {
            return Err(Error::AuthError(format!(
                "unsupported SASL mechanism: {}",
                mechanism
            )));
        }

        let client_first_message_len = match cursor.read_i32().await {
            Ok(i) => i,
            Err(_) => {
                return Err(Error::ClientSocketError(
                    "client first message length".into(),
                    client_identifier,
                ))
            }
        };

        if client_first_message_len == -1 {
            return Err(Error::AuthError("sasl initial response expected".into()));
        }

        let mut client_first_message = vec![0u8; client_first_message_len as usize];

        match cursor.read_exact(&mut client_first_message).await {
            Ok(_) => (),
            Err(_) => {
                return Err(Error::ClientSocketError(
                    "sasl initial response".into(),
                    client_identifier,
                ))
            }
        };

        let client_first_message = match std::str::from_utf8(&client_first_message[..]) {
            Ok(s) => s,
            Err(_) => {
                return Err(Error::ProtocolSyncError(
                    "invalid client-first-message".to_string(),
                ))
            }
        };

        let parts = client_first_message
            .split(|c| c == ',')
            .collect::<Vec<&str>>();

        if parts.len() != 4 {
            return Err(Error::ProtocolSyncError(
                "extensions are not supported".to_string(),
            ));
        }

        match parts[0] {
            "n" | "y" => (),
            "p" => {
                return Err(Error::ProtocolSyncError(
                    "channel binding is not supported".to_string(),
                ))
            }
            _ => {
                return Err(Error::AuthError("Invalid client-first-message".into()));
            }
        }

        let client_first_message_bare =
            Self::parse_client_first_message_bare(&parts[2..].join(",").as_bytes())?;

        Ok(client_first_message_bare)
    }

    fn parse_client_first_message_bare(
        client_first_message_bare: &[u8],
    ) -> Result<ClientFirstMessageBare, Error> {
        let mut i = 0;
        let mut start = 0;
        while client_first_message_bare[i] != b',' {
            i += 1;
        }

        // We check the format of the message, but don't actually care
        // about storing the username
        match std::str::from_utf8(&client_first_message_bare[start..i]) {
            Ok(s) => {
                if s.starts_with("m=") {
                    return Err(Error::AuthError(
                        "mandatory extensions are not supported".into(),
                    ));
                } else if !s.starts_with("n=") {
                    return Err(Error::ProtocolSyncError(
                        "invalid username format".to_string(),
                    ));
                }
            }
            Err(_) => {
                return Err(Error::ProtocolSyncError(
                    "invalid client-first-message-bare".to_string(),
                ))
            }
        }

        start = i + 1;

        match std::str::from_utf8(&client_first_message_bare[start..]) {
            Ok(s) => {
                if !s.starts_with("r=") {
                    return Err(Error::AuthError("invalid nonce format".into()));
                }
            }
            Err(_) => {
                return Err(Error::ProtocolSyncError(
                    "invalid client-first-message-bare".to_string(),
                ))
            }
        }

        let raw = Bytes::copy_from_slice(client_first_message_bare);
        let nonce = raw.slice((start + 2)..);

        // We don't care about extensions at this time, so we stop after we
        // gather the nonce

        Ok(ClientFirstMessageBare { nonce, raw })
    }

    /// Server generated `AuthenticationSASLContinue` message
    async fn send_auth_sasl_continue(
        &mut self,
        s_nonce: &[u8],
        c_nonce: &[u8],
        salt: &[u8],
        i: u32,
    ) -> Result<String, Error> {
        let nonce = format!(
            "{}{}",
            String::from_utf8_lossy(c_nonce),
            String::from_utf8_lossy(s_nonce)
        );

        let server_first_message = format!(
            "r={},s={},i={}",
            nonce,
            base64::engine::general_purpose::STANDARD.encode(salt),
            i
        );

        authentication_sasl_continue(self.write, server_first_message.as_bytes()).await?;

        Ok(server_first_message)
    }

    /// Returns the SASL mechanism specific message data associated with the
    /// client's final message
    async fn recv_client_final_message(&mut self) -> Result<Bytes, Error> {
        let client_identifier = self.client_identifier.clone();
        match self.read.read_u8().await {
            Ok(p) => {
                if p != b'p' {
                    return Err(Error::ProtocolSyncError(format!(
                        "Expected p, got {}",
                        p as char
                    )));
                }
            }
            Err(_) => {
                return Err(Error::ClientSocketError(
                    "sasl response code".into(),
                    client_identifier,
                ))
            }
        };

        let len = match self.read.read_i32().await {
            Ok(len) => len,
            Err(_) => {
                return Err(Error::ClientSocketError(
                    "sasl response length".into(),
                    client_identifier,
                ))
            }
        };

        let mut rest = vec![0u8; (len - 4) as usize];

        match self.read.read_exact(&mut rest).await {
            Ok(_) => (),
            Err(_) => {
                return Err(Error::ClientSocketError(
                    "sasl response".into(),
                    client_identifier,
                ))
            }
        }

        Ok(Bytes::copy_from_slice(&rest[..]))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SaslMechanism {
    ScramSha256,
}

impl std::fmt::Display for SaslMechanism {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SaslMechanism::ScramSha256 => {
                write!(f, "SCRAM-SHA-256")
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::{BufMut, BytesMut};
    use std::ffi::CString;
    use std::io::Cursor;
    use tokio::io::{split, ReadHalf, WriteHalf};

    fn build_sasl_initial_response(
        sasl_mechanism: &String,
        client_first_message: &str,
    ) -> BytesMut {
        let mut bytes = BytesMut::new();
        let sasl_mechanism = CString::new(sasl_mechanism.to_string()).unwrap();

        bytes.put_u8(b'p');
        bytes.put_i32(
            (8 + sasl_mechanism.as_bytes_with_nul().len() + client_first_message.len()) as i32,
        );
        bytes.put_slice(sasl_mechanism.as_bytes_with_nul());
        bytes.put_i32(client_first_message.len() as i32);
        bytes.put_slice(client_first_message.as_bytes());

        bytes
    }

    fn build_sasl_final_response(client_final_message: &str) -> BytesMut {
        let mut bytes = BytesMut::new();
        // let client_final_message = CString::new(client_final_message).unwrap();

        bytes.put_u8(b'p');
        bytes.put_i32((4 + client_final_message.len()) as i32);
        bytes.put_slice(client_final_message.as_bytes());

        bytes
    }

    fn build_sasl_initial_response_handles(
        sasl_mechanism: &String,
        client_first_message: &str,
    ) -> (ReadHalf<Cursor<Vec<u8>>>, WriteHalf<Cursor<Vec<u8>>>) {
        let bytes = build_sasl_initial_response(sasl_mechanism, client_first_message);

        split(Cursor::new(bytes.to_vec()))
    }

    /// The exchange found in this test is taken from the example SCRAM authentication
    /// exchange found in [RFC 7677](https://datatracker.ietf.org/doc/html/rfc7677#section-3)
    #[tokio::test]
    async fn test_scram_sha_256_auth_success() {
        let mut c_bytes = BytesMut::new();

        struct TestNonceGenerator;

        impl GenerateNonce for TestNonceGenerator {
            fn generate_nonce(_: usize) -> String {
                "%hvYDpWUa2RaTCAfuxFIlj)hNlF$k0".to_string()
            }
        }

        let sasl_initial_response = build_sasl_initial_response(
            &SaslMechanism::ScramSha256.to_string(),
            "n,,n=user,r=rOprNGfwEbeRWgbNEkqO",
        );
        c_bytes.put_slice(sasl_initial_response.as_ref());
        let sasl_final_response = build_sasl_final_response(
            "c=biws,r=rOprNGfwEbeRWgbNEkqO%hvYDpWUa2RaTCAfuxFIlj)hNlF$k0,p=dHzbZapWIk4jUhN+Ute9ytag9zjfMHgsqmmiz7AndVQ=",
        );
        c_bytes.put_slice(sasl_final_response.as_ref());

        let mut read = Cursor::new(c_bytes.to_vec());
        // We don't care about validating the server's response, so we provide this dummy container
        let mut write = Cursor::new(Vec::<u8>::new());

        let client_identifier = ClientIdentifier::new("test_app", "user", "pool");

        let mut sasl_auth = SaslAuthentication::new(&mut read, &mut write, &client_identifier);

        let salt = base64::engine::general_purpose::STANDARD
            .decode("W22ZaJ0SNY7soEsUEjb6gQ==")
            .unwrap();
        let iteration_count = 4096;

        match sasl_auth
            .authenticate::<TestNonceGenerator>("pencil", &salt[..], iteration_count)
            .await
        {
            Ok(_) => assert!(true),
            Err(e) => assert!(
                false,
                "SASL authentication should succeed. Received error {:?}",
                e
            ),
        }
    }

    #[tokio::test]
    async fn test_sasl_recv_initial_response() {
        let (mut read, mut write) = build_sasl_initial_response_handles(
            &SaslMechanism::ScramSha256.to_string(),
            "n,,n=user,r=fyko+d2lbbFgONRv9qkxdawL",
        );
        let client_identifier = ClientIdentifier::new("test_app", "user", "pool");

        let mut sasl_auth = SaslAuthentication::new(&mut read, &mut write, &client_identifier);

        match sasl_auth
            .recv_client_sasl_initial_response(&[SaslMechanism::ScramSha256])
            .await
        {
            Ok(_) => assert!(true),
            Err(e) => assert!(
                false,
                "Receiving initial response should succeed. Received error {:?}",
                e
            ),
        }
    }

    #[tokio::test]
    async fn test_sasl_initial_response_unsupported_mechanism() {
        let (mut read, mut write) = build_sasl_initial_response_handles(
            &"SCRAM-SHA-256-PLUS".to_string(),
            "n,,n=user,r=fyko+d2lbbFgONRv9qkxdawL",
        );

        let client_identifier = ClientIdentifier::new("test_app", "user", "pool");

        let mut sasl_auth = SaslAuthentication::new(&mut read, &mut write, &client_identifier);

        match sasl_auth
            .recv_client_sasl_initial_response(&[SaslMechanism::ScramSha256])
            .await
        {
            Ok(_) => assert!(
                false,
                "SASL authentication with unsupported mechanisms should fail"
            ),
            Err(e) => assert!(
                matches!(e, Error::AuthError(_)),
                "SASL authentication should fail with an AuthError. Received error {:?}",
                e
            ),
        }
    }

    #[tokio::test]
    async fn test_sasl_initial_response_channel_binding() {
        let supported_binds = ["n", "y"];
        let unsupported_binds = ["p"];
        let invalid_binds = ["w", "x", "z"];

        for bind in [
            &supported_binds[..],
            &unsupported_binds[..],
            &invalid_binds[..],
        ]
        .concat()
        .iter()
        {
            let (mut read, mut write) = build_sasl_initial_response_handles(
                &SaslMechanism::ScramSha256.to_string(),
                &format!("{},,n=user,r=fyko+d2lbbFgONRv9qkxdawL", bind),
            );

            let client_identifier = ClientIdentifier::new("test_app", "user", "pool");

            let mut sasl_auth = SaslAuthentication::new(&mut read, &mut write, &client_identifier);

            match sasl_auth
                .recv_client_sasl_initial_response(&[SaslMechanism::ScramSha256])
                .await
            {
                Ok(_) if supported_binds.contains(bind) => assert!(true),
                Ok(_) => assert!(
                    false,
                    "SASL authentication with unsupported channel binding should fail"
                ),
                Err(e) => {
                    if unsupported_binds.contains(bind) {
                        assert!(
                            matches!(e, Error::ProtocolSyncError(_)),
                            "SASL authentication with unsupported bind should fail with a ProtocolSyncError. Received error {:?}",
                            e
                        );
                    } else {
                        assert!(
                            matches!(e, Error::AuthError(_)),
                            "SASL authentication with invalid channel binding should fail with a AuthError. Received error {:?}",
                            e
                        );
                    }
                }
            }
        }
    }
}
