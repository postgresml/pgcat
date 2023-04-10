use crate::errors::Error;
use crate::pool::ConnectionPool;
use crate::server::Server;
use log::debug;

#[derive(Clone, Debug)]
pub struct AuthPassthrough {
    password: String,
    query: String,
    user: String,
}

impl AuthPassthrough {
    /// Initializes an AuthPassthrough.
    pub fn new(query: &str, user: &str, password: &str) -> Self {
        AuthPassthrough {
            password: password.to_string(),
            query: query.to_string(),
            user: user.to_string(),
        }
    }

    /// Returns an AuthPassthrough given the pool configuration.
    /// If any of required values is not set, None is returned.
    pub fn from_pool_config(pool_config: &crate::config::Pool) -> Option<Self> {
        if pool_config.is_auth_query_configured() {
            return Some(AuthPassthrough::new(
                pool_config.auth_query.as_ref().unwrap(),
                pool_config.auth_query_user.as_ref().unwrap(),
                pool_config.auth_query_password.as_ref().unwrap(),
            ));
        }

        None
    }

    /// Returns an AuthPassthrough given the pool settings.
    /// If any of required values is not set, None is returned.
    pub fn from_pool_settings(pool_settings: &crate::pool::PoolSettings) -> Option<Self> {
        let pool_config = crate::config::Pool {
            auth_query: pool_settings.auth_query.clone(),
            auth_query_password: pool_settings.auth_query_password.clone(),
            auth_query_user: pool_settings.auth_query_user.clone(),
            ..Default::default()
        };

        AuthPassthrough::from_pool_config(&pool_config)
    }

    /// Connects to server and executes auth_query for the specified address.
    /// If the response is a row with two columns containing the username set in the address.
    /// and its MD5 hash, the MD5 hash returned.
    ///
    /// Note that the query is executed, changing $1 with the name of the user
    /// this is so we only hold in memory (and transfer) the least amount of 'sensitive' data.
    /// Also, it is compatible with pgbouncer.
    ///
    /// # Arguments
    ///
    /// * `address` - An Address of the server we want to connect to. The username for the hash will be obtained from this value.
    ///
    /// # Examples
    ///
    /// ```
    /// use pgcat::auth_passthrough::AuthPassthrough;
    /// use pgcat::config::Address;
    /// let auth_passthrough = AuthPassthrough::new("SELECT * FROM public.user_lookup('$1');", "postgres", "postgres");
    /// auth_passthrough.fetch_hash(&Address::default());
    /// ```
    ///
    pub async fn fetch_hash(&self, address: &crate::config::Address) -> Result<String, Error> {
        let auth_user = crate::config::User {
            username: self.user.clone(),
            password: Some(self.password.clone()),
            pool_size: 1,
            statement_timeout: 0,
            pool_mode: None,
        };

        let user = &address.username;

        debug!("Connecting to server to obtain auth hashes");

        let auth_query = self.query.replace("$1", user);

        match Server::exec_simple_query(address, &auth_user, &auth_query).await {
            Ok(password_data) => {
                if password_data.len() == 2 && password_data.first().unwrap() == user {
                    if let Some(stripped_hash) = password_data
                        .last()
                        .unwrap()
                        .to_string()
                        .strip_prefix("md5") {
                            Ok(stripped_hash.to_string())
                        }
                    else {
                        Err(Error::AuthPassthroughError(
                            "Obtained hash from auth_query does not seem to be in md5 format.".to_string(),
                        ))
                    }
                } else {
                    Err(Error::AuthPassthroughError(
                        "Data obtained from query does not follow the scheme 'user','hash'."
                            .to_string(),
                    ))
                 }
            }
            Err(err) => {
                Err(Error::AuthPassthroughError(
                    format!("Error trying to obtain password from auth_query, ignoring hash for user '{}'. Error: {:?}",
                        user, err))
                )
            }
        }
    }
}

pub async fn refetch_auth_hash(pool: &ConnectionPool) -> Result<String, Error> {
    let address = pool.address(0, 0);
    if let Some(apt) = AuthPassthrough::from_pool_settings(&pool.settings) {
        let hash = apt.fetch_hash(address).await?;

        return Ok(hash);
    }

    Err(Error::ClientError(format!(
        "Could not obtain hash for {{ username: {:?}, database: {:?} }}. Auth passthrough not enabled.",
        address.username, address.database
    )))
}
