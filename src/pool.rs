/// Pooling and failover and banlist.
use async_trait::async_trait;
use bb8::{ManageConnection, Pool, PooledConnection};
use chrono::naive::NaiveDateTime;

use crate::config::{Address, Config, Role, User};
use crate::errors::Error;
use crate::server::Server;

use std::collections::HashMap;
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc, Mutex,
};

// Banlist: bad servers go in here.
pub type BanList = Arc<Mutex<Vec<HashMap<Address, NaiveDateTime>>>>;
pub type Counter = Arc<AtomicUsize>;
pub type ClientServerMap = Arc<Mutex<HashMap<(i32, i32), (i32, i32, String, String)>>>;

#[derive(Clone, Debug)]
pub struct ConnectionPool {
    databases: Vec<Vec<Pool<ServerPool>>>,
    addresses: Vec<Vec<Address>>,
    round_robin: Counter,
    banlist: BanList,
    healthcheck_timeout: u64,
    ban_time: i64,
    pool_size: u32,
}

impl ConnectionPool {
    /// Construct the connection pool from a config file.
    pub async fn from_config(config: Config, client_server_map: ClientServerMap) -> ConnectionPool {
        let mut shards = Vec::new();
        let mut addresses = Vec::new();
        let mut banlist = Vec::new();
        let mut shard_ids = config
            .shards
            .clone()
            .into_keys()
            .map(|x| x.to_string())
            .collect::<Vec<String>>();
        shard_ids.sort_by_key(|k| k.parse::<i64>().unwrap());

        for shard in shard_ids {
            let shard = &config.shards[&shard];
            let mut pools = Vec::new();
            let mut replica_addresses = Vec::new();

            for server in &shard.servers {
                let role = match server.2.as_ref() {
                    "primary" => Role::Primary,
                    "replica" => Role::Replica,
                    _ => {
                        println!("> Config error: server role can be 'primary' or 'replica', have: '{}'. Defaulting to 'replica'.", server.2);
                        Role::Replica
                    }
                };

                let address = Address {
                    host: server.0.clone(),
                    port: server.1.to_string(),
                    role: role,
                };

                let manager = ServerPool::new(
                    address.clone(),
                    config.user.clone(),
                    &shard.database,
                    client_server_map.clone(),
                );

                let pool = Pool::builder()
                    .max_size(config.general.pool_size)
                    .connection_timeout(std::time::Duration::from_millis(
                        config.general.connect_timeout,
                    ))
                    .test_on_check_out(false)
                    .build(manager)
                    .await
                    .unwrap();

                pools.push(pool);
                replica_addresses.push(address);
            }

            shards.push(pools);
            addresses.push(replica_addresses);
            banlist.push(HashMap::new());
        }

        ConnectionPool {
            databases: shards,
            addresses: addresses,
            round_robin: Arc::new(AtomicUsize::new(0)),
            banlist: Arc::new(Mutex::new(banlist)),
            healthcheck_timeout: config.general.healthcheck_timeout,
            ban_time: config.general.ban_time,
            pool_size: config.general.pool_size,
        }
    }

    /// Get a connection from the pool.
    pub async fn get(
        &self,
        shard: Option<usize>,
        role: Option<Role>,
    ) -> Result<(PooledConnection<'_, ServerPool>, Address), Error> {
        // Set this to false to gain ~3-4% speed.
        let with_health_check = true;

        let shard = match shard {
            Some(shard) => shard,
            None => 0, // TODO: pick a shard at random
        };

        let mut allowed_attempts = match role {
            // Primary-specific queries get one attempt, if the primary is down,
            // nothing we should do about it I think. It's dangerous to retry
            // write queries.
            Some(Role::Primary) => {
                // Make sure we have a primary in the pool configured.
                let primary_present = self.addresses[shard]
                    .iter()
                    .filter(|&db| db.role == Role::Primary)
                    .count();

                // TODO: return this error to the client, so people don't have to look in
                // the logs to figure out what happened.
                if primary_present == 0 {
                    println!(">> Error: Primary requested but none are configured.");
                    return Err(Error::AllServersDown);
                }

                // Primary gets one attempt.
                1
            }

            // Replicas get to try as many times as there are replicas
            // and connections in the pool.
            _ => self.databases[shard].len() * self.pool_size as usize,
        };

        while allowed_attempts > 0 {
            // TODO: think about making this local, so multiple clients
            // don't compete for the same round-robin integer.
            // Especially since we're going to be skipping (see role selection below).
            let index =
                self.round_robin.fetch_add(1, Ordering::SeqCst) % self.databases[shard].len();
            let address = self.addresses[shard][index].clone();

            // Make sure you're getting a primary or a replica
            // as per request.
            match role {
                Some(role) => {
                    // If the client wants a specific role,
                    // we'll do our best to pick it, but if we only
                    // have one server in the cluster, it's probably only a primary
                    // (or only a replica), so the client will just get what we have.
                    if address.role != role && self.addresses[shard].len() > 1 {
                        continue;
                    }
                }
                None => (),
            };

            if self.is_banned(&address, shard, role) {
                continue;
            }

            allowed_attempts -= 1;

            // Check if we can connect
            // TODO: implement query wait timeout, i.e. time to get a conn from the pool
            let mut conn = match self.databases[shard][index].get().await {
                Ok(conn) => conn,
                Err(err) => {
                    println!(">> Banning replica {}, error: {:?}", index, err);
                    self.ban(&address, shard);
                    continue;
                }
            };

            if !with_health_check {
                return Ok((conn, address));
            }

            // // Check if this server is alive with a health check
            let server = &mut *conn;

            match tokio::time::timeout(
                tokio::time::Duration::from_millis(self.healthcheck_timeout),
                server.query("SELECT 1"),
            )
            .await
            {
                // Check if health check succeeded
                Ok(res) => match res {
                    Ok(_) => return Ok((conn, address)),
                    Err(_) => {
                        println!(
                            ">> Banning replica {} because of failed health check",
                            index
                        );
                        // Don't leave a bad connection in the pool.
                        server.mark_bad();

                        self.ban(&address, shard);
                        continue;
                    }
                },
                // Health check never came back, database is really really down
                Err(_) => {
                    println!(
                        ">> Banning replica {} because of health check timeout",
                        index
                    );
                    // Don't leave a bad connection in the pool.
                    server.mark_bad();

                    self.ban(&address, shard);
                    continue;
                }
            }
        }

        return Err(Error::AllServersDown);
    }

    /// Ban an address (i.e. replica). It no longer will serve
    /// traffic for any new transactions. Existing transactions on that replica
    /// will finish successfully or error out to the clients.
    pub fn ban(&self, address: &Address, shard: usize) {
        println!(">> Banning {:?}", address);
        let now = chrono::offset::Utc::now().naive_utc();
        let mut guard = self.banlist.lock().unwrap();
        guard[shard].insert(address.clone(), now);
    }

    /// Clear the replica to receive traffic again. Takes effect immediately
    /// for all new transactions.
    pub fn _unban(&self, address: &Address, shard: usize) {
        let mut guard = self.banlist.lock().unwrap();
        guard[shard].remove(address);
    }

    /// Check if a replica can serve traffic. If all replicas are banned,
    /// we unban all of them. Better to try then not to.
    pub fn is_banned(&self, address: &Address, shard: usize, role: Option<Role>) -> bool {
        // If primary is requested explicitely, it can never be banned.
        if Some(Role::Primary) == role {
            return false;
        }

        // If you're not asking for the primary,
        // all databases are treated as replicas.
        let mut guard = self.banlist.lock().unwrap();

        // Everything is banned = nothing is banned.
        if guard[shard].len() == self.databases[shard].len() {
            guard[shard].clear();
            drop(guard);
            println!(">> Unbanning all replicas.");
            return false;
        }

        // I expect this to miss 99.9999% of the time.
        match guard[shard].get(address) {
            Some(timestamp) => {
                let now = chrono::offset::Utc::now().naive_utc();
                // Ban expired.
                if now.timestamp() - timestamp.timestamp() > self.ban_time {
                    guard[shard].remove(address);
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
        println!(">> Creating a new connection for the pool");

        Server::startup(
            &self.address.host,
            &self.address.port,
            &self.user.name,
            &self.user.password,
            &self.database,
            self.client_server_map.clone(),
            self.address.role,
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
