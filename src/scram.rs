// SCRAM-SHA-256 authentication. Heavily inspired by
// https://github.com/sfackler/rust-postgres/
// SASL implementation.

use bytes::BytesMut;
use hmac::{Hmac, Mac};
use rand::{self, Rng};
use sha2::digest::FixedOutput;
use sha2::{Digest, Sha256};

use std::fmt::Write;

use crate::constants::*;
use crate::errors::Error;

/// Normalize a password string. Postgres
/// passwords don't have to be UTF-8.
fn normalize(pass: &[u8]) -> Vec<u8> {
    let pass = match std::str::from_utf8(pass) {
        Ok(pass) => pass,
        Err(_) => return pass.to_vec(),
    };

    match stringprep::saslprep(pass) {
        Ok(pass) => pass.into_owned().into_bytes(),
        Err(_) => pass.as_bytes().to_vec(),
    }
}

/// Keep the SASL state through the exchange.
/// It takes 3 messages to complete the authentication.
pub struct ScramSha256 {
    password: String,
    salted_password: [u8; 32],
    auth_message: String,
    message: BytesMut,
    nonce: String,
}

impl ScramSha256 {
    /// Create the Scram state from a password. It'll automatically
    /// generate a nonce.
    pub fn new(password: &str) -> ScramSha256 {
        let mut rng = rand::thread_rng();
        let nonce = (0..NONCE_LENGTH)
            .map(|_| {
                let mut v = rng.gen_range(0x21u8..0x7e);
                if v == 0x2c {
                    v = 0x7e
                }
                v as char
            })
            .collect::<String>();

        Self::from_nonce(password, &nonce)
    }

    /// Used for testing.
    pub fn from_nonce(password: &str, nonce: &str) -> ScramSha256 {
        let message = BytesMut::from(format!("{}n=,r={}", "n,,", nonce).as_bytes());

        ScramSha256 {
            password: password.to_string(),
            nonce: String::from(nonce),
            message,
            salted_password: [0u8; 32],
            auth_message: String::new(),
        }
    }

    /// Get the current state of the SASL authentication.
    pub fn message(&mut self) -> BytesMut {
        self.message.clone()
    }

    /// Update the state with message received from server.
    pub fn update(&mut self, message: &BytesMut) -> Result<BytesMut, Error> {
        let server_message = Message::parse(message)?;

        if !server_message.nonce.starts_with(&self.nonce) {
            return Err(Error::ProtocolSyncError);
        }

        let salt = match base64::decode(&server_message.salt) {
            Ok(salt) => salt,
            Err(_) => return Err(Error::ProtocolSyncError),
        };

        let salted_password = Self::hi(
            &normalize(self.password.as_bytes()),
            &salt,
            server_message.iterations,
        );

        // Save for verification of final server message.
        self.salted_password = salted_password;

        let mut hmac = match Hmac::<Sha256>::new_from_slice(&salted_password) {
            Ok(hmac) => hmac,
            Err(_) => return Err(Error::ServerError),
        };

        hmac.update(b"Client Key");

        let client_key = hmac.finalize().into_bytes();

        let mut hash = Sha256::default();
        hash.update(client_key.as_slice());

        let stored_key = hash.finalize_fixed();
        let mut cbind_input = vec![];
        cbind_input.extend("n,,".as_bytes());

        let cbind_input = base64::encode(&cbind_input);

        self.message.clear();

        // Start writing the client reply.
        match write!(
            &mut self.message,
            "c={},r={}",
            cbind_input, server_message.nonce
        ) {
            Ok(_) => (),
            Err(_) => return Err(Error::ServerError),
        };

        let auth_message = format!(
            "n=,r={},{},{}",
            self.nonce,
            String::from_utf8_lossy(&message[..]),
            String::from_utf8_lossy(&self.message[..])
        );

        let mut hmac = match Hmac::<Sha256>::new_from_slice(&stored_key) {
            Ok(hmac) => hmac,
            Err(_) => return Err(Error::ServerError),
        };
        hmac.update(auth_message.as_bytes());

        // Save the auth message for server final message verification.
        self.auth_message = auth_message;

        let client_signature = hmac.finalize().into_bytes();

        // Sign the client proof.
        let mut client_proof = client_key;
        for (proof, signature) in client_proof.iter_mut().zip(client_signature) {
            *proof ^= signature;
        }

        match write!(&mut self.message, ",p={}", base64::encode(&*client_proof)) {
            Ok(_) => (),
            Err(_) => return Err(Error::ServerError),
        };

        Ok(self.message.clone())
    }

    /// Verify final server message.
    pub fn finish(&mut self, message: &BytesMut) -> Result<(), Error> {
        let final_message = FinalMessage::parse(message)?;

        let verifier = match base64::decode(&final_message.value) {
            Ok(verifier) => verifier,
            Err(_) => return Err(Error::ProtocolSyncError),
        };

        let mut hmac = match Hmac::<Sha256>::new_from_slice(&self.salted_password) {
            Ok(hmac) => hmac,
            Err(_) => return Err(Error::ServerError),
        };
        hmac.update(b"Server Key");
        let server_key = hmac.finalize().into_bytes();

        let mut hmac = match Hmac::<Sha256>::new_from_slice(&server_key) {
            Ok(hmac) => hmac,
            Err(_) => return Err(Error::ServerError),
        };
        hmac.update(self.auth_message.as_bytes());

        match hmac.verify_slice(&verifier) {
            Ok(_) => Ok(()),
            Err(_) => Err(Error::ServerError),
        }
    }

    /// Hash the password with the salt i-times.
    fn hi(str: &[u8], salt: &[u8], i: u32) -> [u8; 32] {
        let mut hmac =
            Hmac::<Sha256>::new_from_slice(str).expect("HMAC is able to accept all key sizes");
        hmac.update(salt);
        hmac.update(&[0, 0, 0, 1]);
        let mut prev = hmac.finalize().into_bytes();

        let mut hi = prev;

        for _ in 1..i {
            let mut hmac = Hmac::<Sha256>::new_from_slice(str).expect("already checked above");
            hmac.update(&prev);
            prev = hmac.finalize().into_bytes();

            for (hi, prev) in hi.iter_mut().zip(prev) {
                *hi ^= prev;
            }
        }

        hi.into()
    }
}

/// Parse the server challenge.
struct Message {
    nonce: String,
    salt: String,
    iterations: u32,
}

impl Message {
    /// Parse the server SASL challenge.
    fn parse(message: &BytesMut) -> Result<Message, Error> {
        let parts = String::from_utf8_lossy(&message[..])
            .split(',')
            .map(|s| s.to_string())
            .collect::<Vec<String>>();

        if parts.len() != 3 {
            return Err(Error::ProtocolSyncError);
        }

        let nonce = str::replace(&parts[0], "r=", "");
        let salt = str::replace(&parts[1], "s=", "");
        let iterations = match str::replace(&parts[2], "i=", "").parse::<u32>() {
            Ok(iterations) => iterations,
            Err(_) => return Err(Error::ProtocolSyncError),
        };

        Ok(Message {
            nonce,
            salt,
            iterations,
        })
    }
}

/// Parse server final validation message.
struct FinalMessage {
    value: String,
}

impl FinalMessage {
    /// Parse the server final validation message.
    pub fn parse(message: &BytesMut) -> Result<FinalMessage, Error> {
        if !message.starts_with(b"v=") || message.len() < 4 {
            return Err(Error::ProtocolSyncError);
        }

        Ok(FinalMessage {
            value: String::from_utf8_lossy(&message[2..]).to_string(),
        })
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn parse_server_first_message() {
        let message = BytesMut::from(
            "r=fyko+d2lbbFgONRv9qkxdawL3rfcNHYJY1ZVvWVs7j,s=QSXCR+Q6sek8bf92,i=4096".as_bytes(),
        );
        let message = Message::parse(&message).unwrap();
        assert_eq!(message.nonce, "fyko+d2lbbFgONRv9qkxdawL3rfcNHYJY1ZVvWVs7j");
        assert_eq!(message.salt, "QSXCR+Q6sek8bf92");
        assert_eq!(message.iterations, 4096);
    }

    #[test]
    fn parse_server_last_message() {
        let f = FinalMessage::parse(&BytesMut::from(
            "v=U+ppxD5XUKtradnv8e2MkeupiA8FU87Sg8CXzXHDAzw".as_bytes(),
        ))
        .unwrap();
        assert_eq!(
            f.value,
            "U+ppxD5XUKtradnv8e2MkeupiA8FU87Sg8CXzXHDAzw".to_string()
        );
    }

    // recorded auth exchange from psql
    #[test]
    fn exchange() {
        let password = "foobar";
        let nonce = "9IZ2O01zb9IgiIZ1WJ/zgpJB";

        let client_first = "n,,n=,r=9IZ2O01zb9IgiIZ1WJ/zgpJB";
        let server_first =
            "r=9IZ2O01zb9IgiIZ1WJ/zgpJBjx/oIRLs02gGSHcw1KEty3eY,s=fs3IXBy7U7+IvVjZ,i\
             =4096";
        let client_final =
            "c=biws,r=9IZ2O01zb9IgiIZ1WJ/zgpJBjx/oIRLs02gGSHcw1KEty3eY,p=AmNKosjJzS3\
             1NTlQYNs5BTeQjdHdk7lOflDo5re2an8=";
        let server_final = "v=U+ppxD5XUKtradnv8e2MkeupiA8FU87Sg8CXzXHDAzw=";

        let mut scram = ScramSha256::from_nonce(password, nonce);

        let message = scram.message();
        assert_eq!(std::str::from_utf8(&message).unwrap(), client_first);

        let result = scram
            .update(&BytesMut::from(server_first.as_bytes()))
            .unwrap();
        assert_eq!(std::str::from_utf8(&result).unwrap(), client_final);

        scram
            .finish(&BytesMut::from(server_final.as_bytes()))
            .unwrap();
    }
}
