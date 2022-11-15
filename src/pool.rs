use arc_swap::ArcSwap;
use async_trait::async_trait;
use bb8::{ManageConnection, Pool, PooledConnection};
use bytes::BytesMut;
use chrono::naive::NaiveDateTime;
use log::{debug, error, info, warn};
use once_cell::sync::Lazy;
use parking_lot::{Mutex, RwLock};
use rand::seq::SliceRandom;
use rand::thread_rng;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Instant;

use crate::config::{get_config, Address, PoolMode, Role, User};
use crate::errors::Error;

use crate::server::Server;
use crate::sharding::ShardingFunction;
use crate::stats::{get_reporter, Reporter};

pub type ProcessId = i32;
pub type SecretKey = i32;
pub type ServerHost = String;
pub type ServerPort = u16;

pub type BanList = Arc<RwLock<Vec<HashMap<Address, NaiveDateTime>>>>;
pub type ClientServerMap =
    Arc<Mutex<HashMap<(ProcessId, SecretKey), (ProcessId, SecretKey, ServerHost, ServerPort)>>>;
pub type PoolMap = HashMap<PoolIdentifier, ConnectionPool>;
/// The connection pool, globally available.
/// This is atomic and safe and read-optimized.
/// The pool is recreated dynamically when the config is reloaded.
pub static POOLS: Lazy<ArcSwap<PoolMap>> = Lazy::new(|| ArcSwap::from_pointee(HashMap::default()));
static POOLS_HASH: Lazy<ArcSwap<HashSet<crate::config::Pool>>> =
    Lazy::new(|| ArcSwap::from_pointee(HashSet::default()));

/// An identifier for a PgCat pool,
/// a database visible to clients.
#[derive(Hash, Debug, Clone, PartialEq, Eq)]
pub struct PoolIdentifier {
    // The name of the database clients want to connect to.
    pub db: String,

    /// The username the client connects with. Each user gets its own pool.
    pub user: String,
}

impl PoolIdentifier {
    /// Create a new user/pool identifier.
    pub fn new(db: &str, user: &str) -> PoolIdentifier {
        PoolIdentifier {
            db: db.to_string(),
            user: user.to_string(),
        }
    }
}

/// Pool settings.
#[derive(Clone, Debug)]
pub struct PoolSettings {
    /// Transaction or Session.
    pub pool_mode: PoolMode,

    // Number of shards.
    pub shards: usize,

    // Connecting user.
    pub user: User,

    // Default server role to connect to.
    pub default_role: Option<Role>,

    // Enable/disable query parser.
    pub query_parser_enabled: bool,

    // Read from the primary as well or not.
    pub primary_reads_enabled: bool,

    // Sharding function.
    pub sharding_function: ShardingFunction,

    // Sharding key
    pub automatic_sharding_key: Option<String>,
}

impl Default for PoolSettings {
    fn default() -> PoolSettings {
        PoolSettings {
            pool_mode: PoolMode::Transaction,
            shards: 1,
            user: User::default(),
            default_role: None,
            query_parser_enabled: false,
            primary_reads_enabled: true,
            sharding_function: ShardingFunction::PgBigintHash,
            automatic_sharding_key: None,
        }
    }
}

/// The globally accessible connection pool.
#[derive(Clone, Debug, Default)]
pub struct ConnectionPool {
    /// The pools handled internally by bb8.
    databases: Vec<Vec<Pool<ServerPool>>>,

    /// The addresses (host, port, role) to handle
    /// failover and load balancing deterministically.
    addresses: Vec<Vec<Address>>,

    /// List of banned addresses (see above)
    /// that should not be queried.
    banlist: BanList,

    /// The statistics aggregator runs in a separate task
    /// and receives stats from clients, servers, and the pool.
    stats: Reporter,

    /// The server information (K messages) have to be passed to the
    /// clients on startup. We pre-connect to all shards and replicas
    /// on pool creation and save the K messages here.
    server_info: BytesMut,

    /// Pool configuration.
    pub settings: PoolSettings,
}

impl ConnectionPool {
    /// Construct the connection pool from the configuration.
    pub async fn from_config(client_server_map: ClientServerMap) -> Result<(), Error> {
        let config = get_config();

        let mut new_pools = HashMap::new();
        let mut address_id = 0;

        let mut pools_hash = (*(*POOLS_HASH.load())).clone();

        for (pool_name, pool_config) in &config.pools {
            let changed = pools_hash.insert(pool_config.clone());

            // There is one pool per database/user pair.
            for user in pool_config.users.values() {
                // If the pool hasn't changed, get existing reference and insert it into the new_pools.
                // We replace all pools at the end, but if the reference is kept, the pool won't get re-created (bb8).
                if !changed {
                    match get_pool(pool_name, &user.username) {
                        Some(pool) => {
                            info!(
                                "[pool: {}][user: {}] has not changed",
                                pool_name, user.username
                            );
                            new_pools.insert(
                                PoolIdentifier::new(pool_name, &user.username),
                                pool.clone(),
                            );
                            continue;
                        }
                        None => (),
                    }
                }

                info!(
                    "[pool: {}][user: {}] creating new pool",
                    pool_name, user.username
                );

                let mut shards = Vec::new();
                let mut addresses = Vec::new();
                let mut banlist = Vec::new();
                let mut shard_ids = pool_config
                    .shards
                    .clone()
                    .into_keys()
                    .collect::<Vec<String>>();

                // Sort by shard number to ensure consistency.
                shard_ids.sort_by_key(|k| k.parse::<i64>().unwrap());

                for shard_idx in &shard_ids {
                    let shard = &pool_config.shards[shard_idx];
                    let mut pools = Vec::new();
                    let mut servers = Vec::new();
                    let mut replica_number = 0;

                    for (address_index, server) in shard.servers.iter().enumerate() {
                        let address = Address {
                            id: address_id,
                            database: shard.database.clone(),
                            host: server.host.clone(),
                            port: server.port,
                            role: server.role,
                            address_index,
                            replica_number,
                            shard: shard_idx.parse::<usize>().unwrap(),
                            username: user.username.clone(),
                            pool_name: pool_name.clone(),
                        };

                        address_id += 1;

                        if server.role == Role::Replica {
                            replica_number += 1;
                        }

                        let manager = ServerPool::new(
                            address.clone(),
                            user.clone(),
                            &shard.database,
                            client_server_map.clone(),
                            get_reporter(),
                        );

                        let connect_timeout = match pool_config.connect_timeout {
                            Some(connect_timeout) => connect_timeout,
                            None => config.general.connect_timeout,
                        };

                        let pool = Pool::builder()
                            .max_size(user.pool_size)
                            .connection_timeout(std::time::Duration::from_millis(connect_timeout))
                            .test_on_check_out(false)
                            .build(manager)
                            .await
                            .unwrap();

                        pools.push(pool);
                        servers.push(address);
                    }

                    shards.push(pools);
                    addresses.push(servers);
                    banlist.push(HashMap::new());
                }

                assert_eq!(shards.len(), addresses.len());

                let mut pool = ConnectionPool {
                    databases: shards,
                    addresses,
                    banlist: Arc::new(RwLock::new(banlist)),
                    stats: get_reporter(),
                    server_info: BytesMut::new(),
                    settings: PoolSettings {
                        pool_mode: pool_config.pool_mode,
                        // shards: pool_config.shards.clone(),
                        shards: shard_ids.len(),
                        user: user.clone(),
                        default_role: match pool_config.default_role.as_str() {
                            "any" => None,
                            "replica" => Some(Role::Replica),
                            "primary" => Some(Role::Primary),
                            _ => unreachable!(),
                        },
                        query_parser_enabled: pool_config.query_parser_enabled,
                        primary_reads_enabled: pool_config.primary_reads_enabled,
                        sharding_function: pool_config.sharding_function,
                        automatic_sharding_key: pool_config.automatic_sharding_key.clone(),
                    },
                };

                // Connect to the servers to make sure pool configuration is valid
                // before setting it globally.
                match pool.validate().await {
                    Ok(_) => (),
                    Err(err) => {
                        error!("Could not validate connection pool: {:?}", err);
                        return Err(err);
                    }
                };

                // There is one pool per database/user pair.
                new_pools.insert(PoolIdentifier::new(pool_name, &user.username), pool);
            }
        }

        POOLS.store(Arc::new(new_pools.clone()));
        POOLS_HASH.store(Arc::new(pools_hash.clone()));

        Ok(())
    }

    /// Connect to all shards and grab server information.
    /// Return server information we will pass to the clients
    /// when they connect.
    /// This also warms up the pool for clients that connect when
    /// the pooler starts up.
    async fn validate(&mut self) -> Result<(), Error> {
        let mut server_infos = Vec::new();
        for shard in 0..self.shards() {
            for server in 0..self.servers(shard) {
                let connection = match self.databases[shard][server].get().await {
                    Ok(conn) => conn,
                    Err(err) => {
                        error!("Shard {} down or misconfigured: {:?}", shard, err);
                        continue;
                    }
                };

                let proxy = connection;
                let server = &*proxy;
                let server_info = server.server_info();

                if !server_infos.is_empty() {
                    // Compare against the last server checked.
                    if server_info != server_infos[server_infos.len() - 1] {
                        warn!(
                            "{:?} has different server configuration than the last server",
                            proxy.address()
                        );
                    }
                }

                server_infos.push(server_info);
            }
        }

        // TODO: compare server information to make sure
        // all shards are running identical configurations.
        if server_infos.is_empty() {
            return Err(Error::AllServersDown);
        }

        // We're assuming all servers are identical.
        // TODO: not true.
        self.server_info = server_infos[0].clone();

        Ok(())
    }

    /// Get a connection from the pool.
    pub async fn get(
        &self,
        shard: usize,       // shard number
        role: Option<Role>, // primary or replica
        process_id: i32,    // client id
    ) -> Result<(PooledConnection<'_, ServerPool>, Address), Error> {
        let mut candidates: Vec<&Address> = self.addresses[shard]
            .iter()
            .filter(|address| address.role == role)
            .collect();

        // Random load balancing
        candidates.shuffle(&mut thread_rng());

        let healthcheck_timeout = get_config().general.healthcheck_timeout;
        let healthcheck_delay = get_config().general.healthcheck_delay as u128;

        while !candidates.is_empty() {
            // Get the next candidate
            let address = match candidates.pop() {
                Some(address) => address,
                None => break,
            };

            if self.is_banned(address, role) {
                debug!("Address {:?} is banned", address);
                continue;
            }

            // Indicate we're waiting on a server connection from a pool.
            let now = Instant::now();
            self.stats.client_waiting(process_id);

            // Check if we can connect
            let mut conn = match self.databases[address.shard][address.address_index]
                .get()
                .await
            {
                Ok(conn) => conn,
                Err(err) => {
                    error!("Banning instance {:?}, error: {:?}", address, err);
                    self.ban(address, process_id);
                    self.stats.client_checkout_error(process_id, address.id);
                    continue;
                }
            };

            // // Check if this server is alive with a health check.
            let server = &mut *conn;

            // Will return error if timestamp is greater than current system time, which it should never be set to
            let require_healthcheck =
                server.last_activity().elapsed().unwrap().as_millis() > healthcheck_delay;

            // Do not issue a health check unless it's been a little while
            // since we last checked the server is ok.
            // Health checks are pretty expensive.
            if !require_healthcheck {
                self.stats
                    .checkout_time(now.elapsed().as_micros(), process_id, server.server_id());
                self.stats.server_active(process_id, server.server_id());
                return Ok((conn, address.clone()));
            }

            debug!("Running health check on server {:?}", address);

            self.stats.server_tested(server.server_id());

            match tokio::time::timeout(
                tokio::time::Duration::from_millis(healthcheck_timeout),
                server.query(";"), // Cheap query as it skips the query planner
            )
            .await
            {
                // Check if health check succeeded.
                Ok(res) => match res {
                    Ok(_) => {
                        self.stats.checkout_time(
                            now.elapsed().as_micros(),
                            process_id,
                            conn.server_id(),
                        );
                        self.stats.server_active(process_id, conn.server_id());
                        return Ok((conn, address.clone()));
                    }

                    // Health check failed.
                    Err(err) => {
                        error!(
                            "Banning instance {:?} because of failed health check, {:?}",
                            address, err
                        );

                        // Don't leave a bad connection in the pool.
                        server.mark_bad();

                        self.ban(address, process_id);
                        continue;
                    }
                },

                // Health check timed out.
                Err(err) => {
                    error!(
                        "Banning instance {:?} because of health check timeout, {:?}",
                        address, err
                    );
                    // Don't leave a bad connection in the pool.
                    server.mark_bad();

                    self.ban(address, process_id);
                    continue;
                }
            }
        }

        Err(Error::AllServersDown)
    }

    /// Ban an address (i.e. replica). It no longer will serve
    /// traffic for any new transactions. Existing transactions on that replica
    /// will finish successfully or error out to the clients.
    pub fn ban(&self, address: &Address, client_id: i32) {
        error!("Banning {:?}", address);
        self.stats.client_ban_error(client_id, address.id);

        let now = chrono::offset::Utc::now().naive_utc();
        let mut guard = self.banlist.write();
        guard[address.shard].insert(address.clone(), now);
    }

    /// Clear the replica to receive traffic again. Takes effect immediately
    /// for all new transactions.
    pub fn _unban(&self, address: &Address) {
        let mut guard = self.banlist.write();
        guard[address.shard].remove(address);
    }

    /// Check if a replica can serve traffic. If all replicas are banned,
    /// we unban all of them. Better to try then not to.
    pub fn is_banned(&self, address: &Address, role: Option<Role>) -> bool {
        let replicas_available = match role {
            Some(Role::Replica) => self.addresses[address.shard]
                .iter()
                .filter(|addr| addr.role == Role::Replica)
                .count(),
            None => self.addresses[address.shard].len(),
            Some(Role::Primary) => return false, // Primary cannot be banned.
        };

        debug!("Available targets for {:?}: {}", role, replicas_available);

        let guard = self.banlist.read();

        // Everything is banned = nothing is banned.
        if guard[address.shard].len() == replicas_available {
            drop(guard);
            let mut guard = self.banlist.write();
            guard[address.shard].clear();
            drop(guard);
            warn!("Unbanning all replicas.");
            return false;
        }

        // I expect this to miss 99.9999% of the time.
        match guard[address.shard].get(address) {
            Some(timestamp) => {
                let now = chrono::offset::Utc::now().naive_utc();
                let config = get_config();

                // Ban expired.
                if now.timestamp() - timestamp.timestamp() > config.general.ban_time {
                    drop(guard);
                    warn!("Unbanning {:?}", address);
                    let mut guard = self.banlist.write();
                    guard[address.shard].remove(address);
                    false
                } else {
                    debug!("{:?} is banned", address);
                    true
                }
            }

            None => {
                debug!("{:?} is ok", address);
                false
            }
        }
    }

    /// Get the number of configured shards.
    pub fn shards(&self) -> usize {
        self.databases.len()
    }

    /// Get the number of servers (primary and replicas)
    /// configured for a shard.
    pub fn servers(&self, shard: usize) -> usize {
        self.addresses[shard].len()
    }

    /// Get the total number of servers (databases) we are connected to.
    pub fn databases(&self) -> usize {
        let mut databases = 0;
        for shard in 0..self.shards() {
            databases += self.servers(shard);
        }
        databases
    }

    /// Get pool state for a particular shard server as reported by bb8.
    pub fn pool_state(&self, shard: usize, server: usize) -> bb8::State {
        self.databases[shard][server].state()
    }

    /// Get the address information for a shard server.
    pub fn address(&self, shard: usize, server: usize) -> &Address {
        &self.addresses[shard][server]
    }

    pub fn server_info(&self) -> BytesMut {
        self.server_info.clone()
    }
}

/// Wrapper for the bb8 connection pool.
pub struct ServerPool {
    address: Address,
    user: User,
    database: String,
    client_server_map: ClientServerMap,
    stats: Reporter,
}

impl ServerPool {
    pub fn new(
        address: Address,
        user: User,
        database: &str,
        client_server_map: ClientServerMap,
        stats: Reporter,
    ) -> ServerPool {
        ServerPool {
            address,
            user,
            database: database.to_string(),
            client_server_map,
            stats,
        }
    }
}

#[async_trait]
impl ManageConnection for ServerPool {
    type Connection = Server;
    type Error = Error;

    /// Attempts to create a new connection.
    async fn connect(&self) -> Result<Self::Connection, Self::Error> {
        info!("Creating a new server connection {:?}", self.address);
        let server_id = rand::random::<i32>();

        self.stats.server_register(
            server_id,
            self.address.id,
            self.address.name(),
            self.address.pool_name.clone(),
            self.address.username.clone(),
        );
        self.stats.server_login(server_id);

        // Connect to the PostgreSQL server.
        match Server::startup(
            server_id,
            &self.address,
            &self.user,
            &self.database,
            self.client_server_map.clone(),
            self.stats.clone(),
        )
        .await
        {
            Ok(conn) => {
                self.stats.server_idle(server_id);
                Ok(conn)
            }
            Err(err) => {
                self.stats.server_disconnecting(server_id);
                Err(err)
            }
        }
    }

    /// Determines if the connection is still connected to the database.
    async fn is_valid(&self, _conn: &mut Self::Connection) -> Result<(), Self::Error> {
        Ok(())
    }

    /// Synchronously determine if the connection is no longer usable, if possible.
    fn has_broken(&self, conn: &mut Self::Connection) -> bool {
        conn.is_bad()
    }
}

/// Get the connection pool
pub fn get_pool(db: &str, user: &str) -> Option<ConnectionPool> {
    (*(*POOLS.load()))
        .get(&PoolIdentifier::new(db, user))
        .cloned()
}

/// Get a pointer to all configured pools.
pub fn get_all_pools() -> HashMap<PoolIdentifier, ConnectionPool> {
    (*(*POOLS.load())).clone()
}

/// How many total servers we have in the config.
pub fn get_number_of_addresses() -> usize {
    get_all_pools()
        .iter()
        .map(|(_, pool)| pool.databases())
        .sum()
}
