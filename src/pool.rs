
use async_trait::async_trait;
use bb8::{ManageConnection, PooledConnection};

use crate::server::Server;
use crate::errors::Error;

pub struct ServerPool {
    host: String,
    port: String,
    user: String,
    password: String,
    database: String,
}

impl ServerPool {
    pub fn new(host: &str, port: &str, user: &str, password: &str, database: &str) -> ServerPool {
        ServerPool {
            host: host.to_string(),
            port: port.to_string(),
            user: user.to_string(),
            password: password.to_string(),
            database: database.to_string(),
        }
    }
}

#[async_trait]
impl ManageConnection for ServerPool {
    type Connection = Server;
    type Error = Error;

   /// Attempts to create a new connection.
    async fn connect(&self) -> Result<Self::Connection, Self::Error> {
        println!(">> Getting connetion from pool");
        Ok(Server::startup(&self.host, &self.port, &self.user, &self.password, &self.database).await?)
    }

    /// Determines if the connection is still connected to the database.
    async fn is_valid(&self, _conn: &mut PooledConnection<'_, Self>) -> Result<(), Self::Error> {
        Ok(())
    }

    /// Synchronously determine if the connection is no longer usable, if possible.
    fn has_broken(&self, _conn: &mut Self::Connection) -> bool {
        false
    }
}