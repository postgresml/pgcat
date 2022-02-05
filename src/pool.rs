use async_trait::async_trait;
use bb8::{ManageConnection, PooledConnection};
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
    replica_pool: ReplicaPool,
    user: User,
    database: String,
    client_server_map: ClientServerMap,
}

impl ServerPool {
    pub fn new(
        replica_pool: ReplicaPool,
        user: User,
        database: &str,
        client_server_map: ClientServerMap,
    ) -> ServerPool {
        ServerPool {
            replica_pool: replica_pool,
            user: user,
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
        println!(">> Getting new connection from the pool");
        let address = self.replica_pool.get();

        match Server::startup(
            &address.host,
            &address.port,
            &self.user.name,
            &self.user.password,
            &self.database,
            self.client_server_map.clone(),
        )
        .await
        {
            Ok(server) => {
                self.replica_pool.unban(&address);
                Ok(server)
            }
            Err(err) => {
                self.replica_pool.ban(&address);
                Err(err)
            }
        }
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
            Err(_err) => {
                println!(">> Unhealthy!");
                self.replica_pool.ban(&server.address());
                Err(Error::ServerTimeout)
            }
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
    addresses: Vec<Address>,
    round_robin: Counter,
    banlist: BanList,
}

impl ReplicaPool {
    pub async fn new(addresses: Vec<Address>) -> ReplicaPool {
        ReplicaPool {
            addresses: addresses,
            round_robin: Arc::new(AtomicUsize::new(0)),
            banlist: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    pub fn ban(&self, address: &Address) {
        println!(">> Banning {:?}", address);
        let now = chrono::offset::Utc::now().naive_utc();
        let mut guard = self.banlist.lock().unwrap();
        guard.insert(address.clone(), now);
    }

    pub fn unban(&self, address: &Address) {
        let mut guard = self.banlist.lock().unwrap();
        guard.remove(address);
    }

    pub fn is_banned(&self, address: &Address) -> bool {
        let mut guard = self.banlist.lock().unwrap();

        // Everything is banned, nothig is banned
        if guard.len() == self.addresses.len() {
            guard.clear();
            drop(guard);
            println!(">> Unbanning all");
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

    pub fn get(&self) -> Address {
        loop {
            // We'll never hit a 64-bit overflow right....right? :-)
            let index = self.round_robin.fetch_add(1, Ordering::SeqCst) % self.addresses.len();

            let address = &self.addresses[index];
            if !self.is_banned(address) {
                return address.clone();
            } else {
                continue;
            }
        }
    }
}
