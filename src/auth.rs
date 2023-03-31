//! Module implementing various client authentication mechanisms.
//!
//! Currently supported: plain (via TLS), md5 (via TLS and plain text connection).

use crate::errors::Error;
use crate::tokio::io::AsyncReadExt;
use crate::{
    auth_passthrough::AuthPassthrough,
    config::get_config,
    messages::{
        error_response, md5_hash_password, md5_hash_second_pass, write_all, wrong_password,
    },
    pool::{get_pool, ConnectionPool},
};
use bytes::{BufMut, BytesMut};
use log::debug;

async fn refetch_auth_hash<S>(
    pool: &ConnectionPool,
    stream: &mut S,
    username: &str,
    pool_name: &str,
) -> Result<String, Error>
where
    S: tokio::io::AsyncWrite + std::marker::Unpin + std::marker::Send,
{
    let config = get_config();

    debug!("Fetching auth hash");

    if config.is_auth_query_configured() {
        let address = pool.address(0, 0);
        if let Some(apt) = AuthPassthrough::from_pool_settings(&pool.settings) {
            let hash = apt.fetch_hash(address).await?;

            debug!("Auth query succeeded");

            return Ok(hash);
        }
    } else {
        debug!("Auth query not configured on pool");
    }

    error_response(
        stream,
        &format!(
            "No password set and auth passthrough failed for database: {}, user: {}",
            pool_name, username
        ),
    )
    .await?;

    Err(Error::ClientError(format!(
        "Could not obtain hash for {{ username: {:?}, database: {:?} }}. Auth passthrough not enabled.",
        pool_name, username
    )))
}

/// Read 'p' message from client.
async fn response<R>(stream: &mut R) -> Result<Vec<u8>, Error>
where
    R: tokio::io::AsyncRead + std::marker::Unpin + std::marker::Send,
{
    let code = match stream.read_u8().await {
        Ok(code) => code,
        Err(_) => {
            return Err(Error::SocketError(
                "Error reading password code from client".to_string(),
            ))
        }
    };

    if code as char != 'p' {
        return Err(Error::SocketError(format!("Expected p, got {}", code)));
    }

    let len = match stream.read_i32().await {
        Ok(len) => len,
        Err(_) => {
            return Err(Error::SocketError(
                "Error reading password length from client".to_string(),
            ))
        }
    };

    let mut response = vec![0; (len - 4) as usize];

    // Too short to be a password (null-terminated)
    if response.len() < 2 {
        return Err(Error::ClientError(format!("Password response too short")));
    }

    match stream.read_exact(&mut response).await {
        Ok(_) => (),
        Err(_) => {
            return Err(Error::SocketError(
                "Error reading password from client".to_string(),
            ))
        }
    };

    Ok(response.to_vec())
}

/// Make sure the pool we authenticated to has at least one server connection
/// that can serve our request.
async fn validate_pool<W>(
    stream: &mut W,
    mut pool: ConnectionPool,
    username: &str,
    pool_name: &str,
) -> Result<(), Error>
where
    W: tokio::io::AsyncWrite + std::marker::Unpin + std::marker::Send,
{
    if !pool.validated() {
        match pool.validate().await {
            Ok(_) => Ok(()),
            Err(err) => {
                error_response(
                    stream,
                    &format!("Pool down for database: {}, user: {}", pool_name, username,),
                )
                .await?;

                Err(Error::ClientError(format!("Pool down: {:?}", err)))
            }
        }
    } else {
        Ok(())
    }
}

/// Clear text authentication.
///
/// The client will send the password in plain text over the wire.
/// To protect against obvious security issues, this is only used over TLS.
///
/// Clear text authentication is used to support zero-downtime password rotation.
/// It allows the client to use multiple passwords when talking to the PgCat
/// while the password is being rotated across multiple app instances.
pub struct ClearText {
    username: String,
    pool_name: String,
    application_name: String,
}

impl ClearText {
    /// Create a new ClearText authentication mechanism.
    pub fn new(username: &str, pool_name: &str, application_name: &str) -> ClearText {
        ClearText {
            username: username.to_string(),
            pool_name: pool_name.to_string(),
            application_name: application_name.to_string(),
        }
    }

    /// Issue 'R' clear text challenge to client.
    pub async fn challenge<W>(&self, stream: &mut W) -> Result<(), Error>
    where
        W: tokio::io::AsyncWrite + std::marker::Unpin + std::marker::Send,
    {
        debug!("Sending plain challenge");

        let mut msg = BytesMut::new();
        msg.put_u8(b'R');
        msg.put_i32(8);
        msg.put_i32(3); // Clear text

        write_all(stream, msg).await
    }

    /// Authenticate client with server password or secret.
    pub async fn authenticate<R, W>(
        &self,
        read: &mut R,
        write: &mut W,
    ) -> Result<Option<String>, Error>
    where
        R: tokio::io::AsyncRead + std::marker::Unpin + std::marker::Send,
        W: tokio::io::AsyncWrite + std::marker::Unpin + std::marker::Send,
    {
        let response = response(read).await?;

        let secret = String::from_utf8_lossy(&response[0..response.len() - 1]).to_string();

        match get_pool(&self.pool_name, &self.username, Some(secret.clone())) {
            None => match get_pool(&self.pool_name, &self.username, None) {
                Some(pool) => {
                    match pool.settings.user.password {
                        Some(ref password) => {
                            if password != &secret {
                                wrong_password(write, &self.username).await?;
                                Err(Error::ClientError(format!(
                                        "Invalid password {{ username: {}, pool_name: {}, application_name: {} }}",
                                        self.username, self.pool_name, self.application_name
                                )))
                            } else {
                                validate_pool(write, pool, &self.username, &self.pool_name).await?;

                                Ok(None)
                            }
                        }

                        None => {
                            // Server is storing hashes, we can't query it for the plain text password.
                            error_response(
                                write,
                                &format!(
                                    "No server password configured for database: {}, user: {}",
                                    self.pool_name, self.username
                                ),
                            )
                            .await?;

                            Err(Error::ClientError(format!(
                                "No server password configured for {{ username: {}, pool_name: {}, application_name: {} }}",
                                self.username, self.pool_name, self.application_name
                            )))
                        }
                    }
                }

                None => {
                    error_response(
                        write,
                        &format!(
                            "No pool configured for database: {}, user: {}",
                            self.pool_name, self.username
                        ),
                    )
                    .await?;

                    Err(Error::ClientError(format!(
                        "Invalid pool name {{ username: {}, pool_name: {}, application_name: {} }}",
                        self.username, self.pool_name, self.application_name
                    )))
                }
            },
            Some(pool) => {
                validate_pool(write, pool, &self.username, &self.pool_name).await?;
                Ok(Some(secret))
            }
        }
    }
}

/// MD5 hash authentication.
///
/// Deprecated, but widely used everywhere, and currently required for poolers
/// to authencticate clients without involving Postgres.
///
/// Admin clients are required to use MD5.
pub struct Md5 {
    username: String,
    pool_name: String,
    application_name: String,
    salt: [u8; 4],
    admin: bool,
}

impl Md5 {
    pub fn new(username: &str, pool_name: &str, application_name: &str, admin: bool) -> Md5 {
        let salt: [u8; 4] = [
            rand::random(),
            rand::random(),
            rand::random(),
            rand::random(),
        ];

        Md5 {
            username: username.to_string(),
            pool_name: pool_name.to_string(),
            application_name: application_name.to_string(),
            salt,
            admin,
        }
    }

    /// Issue a 'R' MD5 challenge to the client.
    pub async fn challenge<W>(&self, stream: &mut W) -> Result<(), Error>
    where
        W: tokio::io::AsyncWrite + std::marker::Unpin + std::marker::Send,
    {
        let mut res = BytesMut::new();
        res.put_u8(b'R');
        res.put_i32(12);
        res.put_i32(5); // MD5
        res.put_slice(&self.salt[..]);

        write_all(stream, res).await
    }

    /// Authenticate client with MD5. This is used for both admin and normal users.
    pub async fn authenticate<R, W>(&self, read: &mut R, write: &mut W) -> Result<(), Error>
    where
        R: tokio::io::AsyncRead + std::marker::Unpin + std::marker::Send,
        W: tokio::io::AsyncWrite + std::marker::Unpin + std::marker::Send,
    {
        let password_hash = response(read).await?;

        if self.admin {
            let config = get_config();

            // Compare server and client hashes.
            let our_hash = md5_hash_password(
                &config.general.admin_username,
                &config.general.admin_password,
                &self.salt,
            );

            if our_hash != password_hash {
                wrong_password(write, &self.username).await?;

                Err(Error::ClientError(format!(
                    "Invalid password {{ username: {}, pool_name: {}, application_name: {} }}",
                    self.username, self.pool_name, self.application_name
                )))
            } else {
                Ok(())
            }
        } else {
            match get_pool(&self.pool_name, &self.username, None) {
                Some(pool) => {
                    match &pool.settings.user.password {
                        Some(ref password) => {
                            let our_hash = md5_hash_password(&self.username, password, &self.salt);

                            if our_hash != password_hash {
                                wrong_password(write, &self.username).await?;

                                Err(Error::ClientError(format!(
                                    "Invalid password {{ username: {}, pool_name: {}, application_name: {} }}",
                                    self.username, self.pool_name, self.application_name
                                )))
                            } else {
                                validate_pool(write, pool, &self.username, &self.pool_name).await?;
                                Ok(())
                            }
                        }

                        None => {
                            if !get_config().is_auth_query_configured() {
                                error_response(
                                    write,
                                    &format!(
                                        "No password configured and auth_query is not set: {}, user: {}",
                                        self.pool_name, self.username
                                    ),
                                )
                                .await?;

                                return Err(Error::ClientError(format!(
                                    "No password configured and auth_query is not set"
                                )));
                            }

                            debug!("Using auth_query");

                            // Fetch hash from server
                            let hash = (*pool.auth_hash.read()).clone();

                            let hash = match hash {
                                Some(hash) => {
                                    debug!("Using existing hash: {}", hash);
                                    hash.clone()
                                }
                                None => {
                                    debug!("Pool has no hash set, fetching new one");

                                    let hash = refetch_auth_hash(
                                        &pool,
                                        write,
                                        &self.username,
                                        &self.pool_name,
                                    )
                                    .await?;

                                    (*pool.auth_hash.write()) = Some(hash.clone());

                                    hash
                                }
                            };

                            let our_hash = md5_hash_second_pass(&hash, &self.salt);

                            // Compare hashes
                            if our_hash != password_hash {
                                debug!("Pool auth query hash did not match, refetching");

                                // Server hash maybe changed
                                let hash = refetch_auth_hash(
                                    &pool,
                                    write,
                                    &self.username,
                                    &self.pool_name,
                                )
                                .await?;

                                let our_hash = md5_hash_second_pass(&hash, &self.salt);

                                if our_hash != password_hash {
                                    debug!("Auth query failed, passwords don't match");

                                    wrong_password(write, &self.username).await?;

                                    Err(Error::ClientError(format!(
                                        "Invalid password {{ username: {}, pool_name: {}, application_name: {} }}",
                                        self.username, self.pool_name, self.application_name
                                    )))
                                } else {
                                    (*pool.auth_hash.write()) = Some(hash);

                                    validate_pool(
                                        write,
                                        pool.clone(),
                                        &self.username,
                                        &self.pool_name,
                                    )
                                    .await?;

                                    Ok(())
                                }
                            } else {
                                validate_pool(write, pool.clone(), &self.username, &self.pool_name)
                                    .await?;

                                Ok(())
                            }
                        }
                    }
                }

                None => {
                    error_response(
                        write,
                        &format!(
                            "No pool configured for database: {}, user: {}",
                            self.pool_name, self.username
                        ),
                    )
                    .await?;

                    return Err(Error::ClientError(format!(
                        "Invalid pool name {{ username: {}, pool_name: {}, application_name: {} }}",
                        self.username, self.pool_name, self.application_name
                    )));
                }
            }
        }
    }
}
