/// Pooling and failover and banlist.
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

// 60 seconds of ban time.
// After that, the replica will be allowed to serve traffic again.
const BAN_TIME: i64 = 60;
//
// Poor man's config
//
const POOL_SIZE: u32 = 15;

#[derive(Clone)]
pub struct ConnectionPool {
    databases: Vec<Vec<Pool<ServerPool>>>,
    addresses: Vec<Vec<Address>>,
    round_robin: Counter,
    banlist: BanList,
}

impl ConnectionPool {
    pub async fn new(
        addresses: Vec<Address>,
        user: User,
        database: &str,
        client_server_map: ClientServerMap,
    ) -> ConnectionPool {
        let mut databases = Vec::new();

        for address in &addresses {
            let manager = ServerPool::new(
                address.clone(),
                user.clone(),
                database,
                client_server_map.clone(),
            );
            let pool = Pool::builder()
                .max_size(POOL_SIZE)
                .connection_timeout(std::time::Duration::from_millis(5000))
                .test_on_check_out(false)
                .build(manager)
                .await
                .unwrap();

            databases.push(pool);
        }

        ConnectionPool {
            databases: vec![databases],
            addresses: vec![addresses],
            round_robin: Arc::new(AtomicUsize::new(0)),
            banlist: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    /// Get a connection from the pool. Either round-robin or pick a specific one in case they are sharded.
    pub async fn get(
        &self,
        shard: Option<usize>,
    ) -> Result<(PooledConnection<'_, ServerPool>, Address), Error> {
        // Set this to false to gain ~3-4% speed.
        let with_health_check = true;

        let shard = match shard {
            Some(shard) => shard,
            None => 0, // TODO: pick a shard at random
        };

        loop {
            let index =
                self.round_robin.fetch_add(1, Ordering::SeqCst) % self.databases[shard].len();
            let address = self.addresses[shard][index].clone();

            if self.is_banned(&address) {
                continue;
            }

            // Check if we can connect
            let mut conn = match self.databases[shard][index].get().await {
                Ok(conn) => conn,
                Err(err) => {
                    println!(">> Banning replica {}, error: {:?}", index, err);
                    self.ban(&address);
                    continue;
                }
            };

            if !with_health_check {
                return Ok((conn, address));
            }

            // // Check if this server is alive with a health check
            let server = &mut *conn;

            match tokio::time::timeout(
                tokio::time::Duration::from_millis(1000),
                server.query("SELECT 1"),
            )
            .await
            {
                Ok(_) => return Ok((conn, address)),
                Err(_) => {
                    println!(
                        ">> Banning replica {} because of failed health check",
                        index
                    );
                    self.ban(&address);
                    continue;
                }
            }
        }
    }

    /// Ban an address (i.e. replica). It no longer will serve
    /// traffic for any new transactions. Existing transactions on that replica
    /// will finish successfully or error out to the clients.
    pub fn ban(&self, address: &Address) {
        println!(">> Banning {:?}", address);
        let now = chrono::offset::Utc::now().naive_utc();
        let mut guard = self.banlist.lock().unwrap();
        guard.insert(address.clone(), now);
    }

    /// Clear the replica to receive traffic again. Takes effect immediately
    /// for all new transactions.
    pub fn unban(&self, address: &Address) {
        let mut guard = self.banlist.lock().unwrap();
        guard.remove(address);
    }

    /// Check if a replica can serve traffic. If all replicas are banned,
    /// we unban all of them. Better to try then not to.
    pub fn is_banned(&self, address: &Address) -> bool {
        let mut guard = self.banlist.lock().unwrap();

        // Everything is banned, nothig is banned
        if guard.len() == self.databases.len() {
            guard.clear();
            drop(guard);
            println!(">> Unbanning all replicas.");
            return false;
        }

        // I expect this to miss 99.9999% of the time.
        match guard.get(address) {
            Some(timestamp) => {
                let now = chrono::offset::Utc::now().naive_utc();
                if now.timestamp() - timestamp.timestamp() > BAN_TIME {
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

    pub fn shards(&self) -> usize {
        self.databases.len()
    }
}

pub struct ServerPool {
    address: Address,
    user: User,
    database: String,
    client_server_map: ClientServerMap,
}

impl ServerPool {
    pub fn new(
        address: Address,
        user: User,
        database: &str,
        client_server_map: ClientServerMap,
    ) -> ServerPool {
        ServerPool {
            address: address,
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

        Server::startup(
            &self.address.host,
            &self.address.port,
            &self.user.name,
            &self.user.password,
            &self.database,
            self.client_server_map.clone(),
        )
        .await
    }

    /// Determines if the connection is still connected to the database.
    async fn is_valid(&self, _conn: &mut PooledConnection<'_, Self>) -> Result<(), Self::Error> {
        Ok(())
    }

    /// Synchronously determine if the connection is no longer usable, if possible.
    fn has_broken(&self, conn: &mut Self::Connection) -> bool {
        conn.is_bad()
    }
}
