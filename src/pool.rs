use async_trait::async_trait;
use bb8::{ManageConnection, Pool, PooledConnection};
use chrono::naive::NaiveDateTime;

use crate::config::{Address, User};
use crate::errors::Error;
use crate::server::Server;

use std::collections::HashMap;
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc, Mutex,
};

// Banlist: bad servers go in here.
pub type BanList = Arc<Mutex<HashMap<Address, NaiveDateTime>>>;
pub type Counter = Arc<AtomicUsize>;
pub type ClientServerMap = Arc<Mutex<HashMap<(i32, i32), (i32, i32, String, String)>>>;

pub struct ServerPool {
    host: String,
    port: String,
    user: String,
    password: String,
    database: String,
    client_server_map: ClientServerMap,
}

impl ServerPool {
    pub fn new(
        host: &str,
        port: &str,
        user: &str,
        password: &str,
        database: &str,
        client_server_map: ClientServerMap,
    ) -> ServerPool {
        ServerPool {
            host: host.to_string(),
            port: port.to_string(),
            user: user.to_string(),
            password: password.to_string(),
            database: database.to_string(),
            client_server_map: client_server_map,
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

        //
        // TODO: Pick a random connection from a replica pool here.
        //
        Ok(Server::startup(
            &self.host,
            &self.port,
            &self.user,
            &self.password,
            &self.database,
            self.client_server_map.clone(),
        )
        .await?)
    }

    /// Determines if the connection is still connected to the database.
    async fn is_valid(&self, conn: &mut PooledConnection<'_, Self>) -> Result<(), Self::Error> {
        let server = &mut *conn;

        // Client disconnected before cleaning up
        if server.in_transaction() {
            return Err(Error::DirtyServer);
        }

        // If this fails, the connection will be closed and another will be grabbed from the pool quietly :-).
        // Failover, step 1, complete.
        match tokio::time::timeout(
            tokio::time::Duration::from_millis(1000),
            server.query("SELECT 1"),
        )
        .await
        {
            Ok(_) => Ok(()),
            Err(_err) => Err(Error::ServerTimeout),
        }
    }

    /// Synchronously determine if the connection is no longer usable, if possible.
    fn has_broken(&self, conn: &mut Self::Connection) -> bool {
        conn.is_bad()
    }
}

/// A collection of servers, which could either be a single primary,
/// many sharded primaries or replicas.
#[derive(Clone)]
pub struct ReplicaPool {
    replicas: Vec<Pool<ServerPool>>,
    addresses: Vec<Address>,
    // user: User,
    round_robin: Counter,
    banlist: BanList,
}

impl ReplicaPool {
    pub async fn new(
        addresses: Vec<Address>,
        user: User,
        database: &str,
        client_server_map: ClientServerMap,
    ) -> ReplicaPool {
        let mut replicas = Vec::new();

        for address in &addresses {
            let client_server_map = client_server_map.clone();

            let manager = ServerPool::new(
                &address.host,
                &address.port,
                &user.name,
                &user.password,
                database,
                client_server_map,
            );

            let pool = Pool::builder().max_size(15).build(manager).await.unwrap();

            replicas.push(pool);
        }

        ReplicaPool {
            addresses: addresses,
            replicas: replicas,
            // user: user,
            round_robin: Arc::new(AtomicUsize::new(0)),
            banlist: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    pub fn ban(&mut self, address: &Address) {
        let now = chrono::offset::Utc::now().naive_utc();
        let mut guard = self.banlist.lock().unwrap();
        guard.insert(address.clone(), now);
    }

    pub fn unban(&mut self, address: &Address) {
        let mut guard = self.banlist.lock().unwrap();
        guard.remove(address);
    }

    pub fn is_banned(&self, address: &Address) -> bool {
        let mut guard = self.banlist.lock().unwrap();

        // Everything is banned, nothig is banned
        if guard.len() == self.addresses.len() {
            guard.clear();
            return false;
        }

        // I expect this to miss 99.9999% of the time.
        match guard.get(address) {
            Some(timestamp) => {
                let now = chrono::offset::Utc::now().naive_utc();
                if now.timestamp() - timestamp.timestamp() > 60 {
                    // 1 minute
                    guard.remove(address);
                    false
                } else {
                    true
                }
            }

            None => false,
        }
    }

    pub fn get(&mut self) -> (Address, Pool<ServerPool>) {
        loop {
            // We'll never hit a 64-bit overflow right....right? :-)
            let index = self.round_robin.fetch_add(1, Ordering::SeqCst) % self.addresses.len();

            let address = &self.addresses[index];
            if !self.is_banned(address) {
                return (address.clone(), self.replicas[index].clone());
            } else {
                continue;
            }
        }
    }
}
