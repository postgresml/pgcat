use arc_swap::ArcSwap;
use async_trait::async_trait;
use bb8::{ManageConnection, Pool, PooledConnection};
use bytes::{BufMut, BytesMut};
use chrono::naive::NaiveDateTime;
use log::{debug, error, info, warn};
use once_cell::sync::Lazy;
use parking_lot::{Mutex, RwLock};
use rand::seq::SliceRandom;
use rand::thread_rng;
use regex::Regex;
use std::collections::HashMap;
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};
use std::time::Instant;
use tokio::sync::Notify;

use crate::config::{get_config, Address, General, LoadBalancingMode, PoolMode, Role, User};
use crate::errors::Error;

use crate::auth_passthrough::AuthPassthrough;
use crate::server::Server;
use crate::sharding::ShardingFunction;
use crate::stats::{AddressStats, ClientStats, PoolStats, ServerStats};

pub type ProcessId = i32;
pub type SecretKey = i32;
pub type ServerHost = String;
pub type ServerPort = u16;

pub type BanList = Arc<RwLock<Vec<HashMap<Address, (BanReason, NaiveDateTime)>>>>;
pub type ClientServerMap =
    Arc<Mutex<HashMap<(ProcessId, SecretKey), (ProcessId, SecretKey, ServerHost, ServerPort)>>>;
pub type PoolMap = HashMap<PoolIdentifier, ConnectionPool>;
/// The connection pool, globally available.
/// This is atomic and safe and read-optimized.
/// The pool is recreated dynamically when the config is reloaded.
pub static POOLS: Lazy<ArcSwap<PoolMap>> = Lazy::new(|| ArcSwap::from_pointee(HashMap::default()));

// Reasons for banning a server.
#[derive(Debug, PartialEq, Clone)]
pub enum BanReason {
    FailedHealthCheck,
    MessageSendFailed,
    MessageReceiveFailed,
    FailedCheckout,
    StatementTimeout,
    AdminBan(i64),
}

/// An identifier for a PgCat pool,
/// a database visible to clients.
#[derive(Hash, Debug, Clone, PartialEq, Eq, Default)]
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

impl From<&Address> for PoolIdentifier {
    fn from(address: &Address) -> PoolIdentifier {
        PoolIdentifier::new(&address.database, &address.username)
    }
}

/// Pool settings.
#[derive(Clone, Debug)]
pub struct PoolSettings {
    /// Transaction or Session.
    pub pool_mode: PoolMode,

    /// Random or LeastOutstandingConnections.
    pub load_balancing_mode: LoadBalancingMode,

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

    // Health check timeout
    pub healthcheck_timeout: u64,

    // Health check delay
    pub healthcheck_delay: u64,

    // Ban time
    pub ban_time: i64,

    // Regex for searching for the sharding key in SQL statements
    pub sharding_key_regex: Option<Regex>,

    // Regex for searching for the shard id in SQL statements
    pub shard_id_regex: Option<Regex>,

    // Limit how much of each query is searched for a potential shard regex match
    pub regex_search_limit: usize,

    // Auth query parameters
    pub auth_query: Option<String>,
    pub auth_query_user: Option<String>,
    pub auth_query_password: Option<String>,
}

impl Default for PoolSettings {
    fn default() -> PoolSettings {
        PoolSettings {
            pool_mode: PoolMode::Transaction,
            load_balancing_mode: LoadBalancingMode::Random,
            shards: 1,
            user: User::default(),
            default_role: None,
            query_parser_enabled: false,
            primary_reads_enabled: true,
            sharding_function: ShardingFunction::PgBigintHash,
            automatic_sharding_key: None,
            healthcheck_delay: General::default_healthcheck_delay(),
            healthcheck_timeout: General::default_healthcheck_timeout(),
            ban_time: General::default_ban_time(),
            sharding_key_regex: None,
            shard_id_regex: None,
            regex_search_limit: 1000,
            auth_query: None,
            auth_query_user: None,
            auth_query_password: None,
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

    /// The server information (K messages) have to be passed to the
    /// clients on startup. We pre-connect to all shards and replicas
    /// on pool creation and save the K messages here.
    server_info: Arc<RwLock<BytesMut>>,

    /// Pool configuration.
    pub settings: PoolSettings,

    /// If not validated, we need to double check the pool is available before allowing a client
    /// to use it.
    validated: Arc<AtomicBool>,

    /// Hash value for the pool configs. It is used to compare new configs
    /// against current config to decide whether or not we need to recreate
    /// the pool after a RELOAD command
    pub config_hash: u64,

    /// If the pool has been paused or not.
    paused: Arc<AtomicBool>,
    paused_waiter: Arc<Notify>,

    pub stats: Arc<PoolStats>,

    /// AuthInfo
    pub auth_hash: Arc<RwLock<Option<String>>>,
}

impl ConnectionPool {
    /// Construct the connection pool from the configuration.
    pub async fn from_config(client_server_map: ClientServerMap) -> Result<(), Error> {
        let config = get_config();

        let mut new_pools = HashMap::new();
        let mut address_id: usize = 0;

        for (pool_name, pool_config) in &config.pools {
            let new_pool_hash_value = pool_config.hash_value();

            // There is one pool per database/user pair.
            for user in pool_config.users.values() {
                let old_pool_ref = get_pool(pool_name, &user.username);
                let identifier = PoolIdentifier::new(pool_name, &user.username);

                match old_pool_ref {
                    Some(pool) => {
                        // If the pool hasn't changed, get existing reference and insert it into the new_pools.
                        // We replace all pools at the end, but if the reference is kept, the pool won't get re-created (bb8).
                        if pool.config_hash == new_pool_hash_value {
                            info!(
                                "[pool: {}][user: {}] has not changed",
                                pool_name, user.username
                            );
                            new_pools.insert(identifier.clone(), pool.clone());
                            continue;
                        }
                    }
                    None => (),
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
                let pool_stats = Arc::new(PoolStats::new(identifier, pool_config.clone()));

                // Allow the pool to be seen in statistics
                pool_stats.register(pool_stats.clone());

                // Sort by shard number to ensure consistency.
                shard_ids.sort_by_key(|k| k.parse::<i64>().unwrap());
                let pool_auth_hash: Arc<RwLock<Option<String>>> = Arc::new(RwLock::new(None));

                for shard_idx in &shard_ids {
                    let shard = &pool_config.shards[shard_idx];
                    let mut pools = Vec::new();
                    let mut servers = Vec::new();
                    let mut replica_number = 0;

                    // Load Mirror settings
                    for (address_index, server) in shard.servers.iter().enumerate() {
                        let mut mirror_addresses = vec![];
                        if let Some(mirror_settings_vec) = &shard.mirrors {
                            for (mirror_idx, mirror_settings) in
                                mirror_settings_vec.iter().enumerate()
                            {
                                if mirror_settings.mirroring_target_index != address_index {
                                    continue;
                                }
                                mirror_addresses.push(Address {
                                    id: address_id,
                                    database: shard.database.clone(),
                                    host: mirror_settings.host.clone(),
                                    port: mirror_settings.port,
                                    role: server.role,
                                    address_index: mirror_idx,
                                    replica_number,
                                    shard: shard_idx.parse::<usize>().unwrap(),
                                    username: user.username.clone(),
                                    pool_name: pool_name.clone(),
                                    mirrors: vec![],
                                    stats: Arc::new(AddressStats::default()),
                                });
                                address_id += 1;
                            }
                        }

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
                            mirrors: mirror_addresses,
                            stats: Arc::new(AddressStats::default()),
                        };

                        address_id += 1;

                        if server.role == Role::Replica {
                            replica_number += 1;
                        }

                        // We assume every server in the pool share user/passwords
                        let auth_passthrough = AuthPassthrough::from_pool_config(pool_config);

                        if let Some(apt) = &auth_passthrough {
                            match apt.fetch_hash(&address).await {
				Ok(ok) => {
				    if let Some(ref pool_auth_hash_value) = *(pool_auth_hash.read()) {
					if ok != *pool_auth_hash_value {
					    warn!("Hash is not the same across shards of the same pool, client auth will \
						   be done using last obtained hash. Server: {}:{}, Database: {}", server.host, server.port, shard.database);
					}
				    }
				    debug!("Hash obtained for {:?}", address);
				    {
					let mut pool_auth_hash = pool_auth_hash.write();
					*pool_auth_hash = Some(ok.clone());
				    }
				},
				Err(err) => warn!("Could not obtain password hashes using auth_query config, ignoring. Error: {:?}", err),
			    }
                        }

                        let manager = ServerPool::new(
                            address.clone(),
                            user.clone(),
                            &shard.database,
                            client_server_map.clone(),
                            pool_stats.clone(),
                            pool_auth_hash.clone(),
                        );

                        let connect_timeout = match pool_config.connect_timeout {
                            Some(connect_timeout) => connect_timeout,
                            None => config.general.connect_timeout,
                        };

                        let idle_timeout = match pool_config.idle_timeout {
                            Some(idle_timeout) => idle_timeout,
                            None => config.general.idle_timeout,
                        };

                        let pool = Pool::builder()
                            .max_size(user.pool_size)
                            .connection_timeout(std::time::Duration::from_millis(connect_timeout))
                            .idle_timeout(Some(std::time::Duration::from_millis(idle_timeout)))
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
                if let Some(ref _auth_hash) = *(pool_auth_hash.clone().read()) {
                    info!(
                        "Auth hash obtained from query_auth for pool {{ name: {}, user: {} }}",
                        pool_name, user.username
                    );
                }

                let pool = ConnectionPool {
                    databases: shards,
                    stats: pool_stats,
                    addresses,
                    banlist: Arc::new(RwLock::new(banlist)),
                    config_hash: new_pool_hash_value,
                    server_info: Arc::new(RwLock::new(BytesMut::new())),
                    auth_hash: pool_auth_hash,
                    settings: PoolSettings {
                        pool_mode: match user.pool_mode {
                            Some(pool_mode) => pool_mode,
                            None => pool_config.pool_mode,
                        },
                        load_balancing_mode: pool_config.load_balancing_mode,
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
                        healthcheck_delay: config.general.healthcheck_delay,
                        healthcheck_timeout: config.general.healthcheck_timeout,
                        ban_time: config.general.ban_time,
                        sharding_key_regex: pool_config
                            .sharding_key_regex
                            .clone()
                            .map(|regex| Regex::new(regex.as_str()).unwrap()),
                        shard_id_regex: pool_config
                            .shard_id_regex
                            .clone()
                            .map(|regex| Regex::new(regex.as_str()).unwrap()),
                        regex_search_limit: pool_config.regex_search_limit.unwrap_or(1000),
                        auth_query: pool_config.auth_query.clone(),
                        auth_query_user: pool_config.auth_query_user.clone(),
                        auth_query_password: pool_config.auth_query_password.clone(),
                    },
                    validated: Arc::new(AtomicBool::new(false)),
                    paused: Arc::new(AtomicBool::new(false)),
                    paused_waiter: Arc::new(Notify::new()),
                };

                // Connect to the servers to make sure pool configuration is valid
                // before setting it globally.
                // Do this async and somewhere else, we don't have to wait here.
                let mut validate_pool = pool.clone();
                tokio::task::spawn(async move {
                    let _ = validate_pool.validate().await;
                });

                // There is one pool per database/user pair.
                new_pools.insert(PoolIdentifier::new(pool_name, &user.username), pool);
            }
        }

        POOLS.store(Arc::new(new_pools.clone()));
        Ok(())
    }

    /// Connect to all shards, grab server information, and possibly
    /// passwords to use in client auth.
    /// Return server information we will pass to the clients
    /// when they connect.
    /// This also warms up the pool for clients that connect when
    /// the pooler starts up.
    pub async fn validate(&mut self) -> Result<(), Error> {
        let mut futures = Vec::new();
        let validated = Arc::clone(&self.validated);

        for shard in 0..self.shards() {
            for server in 0..self.servers(shard) {
                let databases = self.databases.clone();
                let validated = Arc::clone(&validated);
                let pool_server_info = Arc::clone(&self.server_info);

                let task = tokio::task::spawn(async move {
                    let connection = match databases[shard][server].get().await {
                        Ok(conn) => conn,
                        Err(err) => {
                            error!("Shard {} down or misconfigured: {:?}", shard, err);
                            return;
                        }
                    };

                    let proxy = connection;
                    let server = &*proxy;
                    let server_info = server.server_info();

                    let mut guard = pool_server_info.write();
                    guard.clear();
                    guard.put(server_info.clone());
                    validated.store(true, Ordering::Relaxed);
                });

                futures.push(task);
            }
        }

        futures::future::join_all(futures).await;

        // TODO: compare server information to make sure
        // all shards are running identical configurations.
        if self.server_info.read().is_empty() {
            error!("Could not validate connection pool");
            return Err(Error::AllServersDown);
        }

        Ok(())
    }

    /// The pool can be used by clients.
    ///
    /// If not, we need to validate it first by connecting to servers.
    /// Call `validate()` to do so.
    pub fn validated(&self) -> bool {
        self.validated.load(Ordering::Relaxed)
    }

    /// Pause the pool, allowing no more queries and make clients wait.
    pub fn pause(&self) {
        self.paused.store(true, Ordering::Relaxed);
    }

    /// Resume the pool, allowing queries and resuming any pending queries.
    pub fn resume(&self) {
        self.paused.store(false, Ordering::Relaxed);
        self.paused_waiter.notify_waiters();
    }

    /// Check if the pool is paused.
    pub fn paused(&self) -> bool {
        self.paused.load(Ordering::Relaxed)
    }

    /// Check if the pool is paused and wait until it's resumed.
    pub async fn wait_paused(&self) -> bool {
        let waiter = self.paused_waiter.notified();
        let paused = self.paused.load(Ordering::Relaxed);

        if paused {
            waiter.await;
        }

        paused
    }

    /// Get a connection from the pool.
    pub async fn get(
        &self,
        shard: usize,               // shard number
        role: Option<Role>,         // primary or replica
        client_stats: &ClientStats, // client id
    ) -> Result<(PooledConnection<'_, ServerPool>, Address), Error> {
        let mut candidates: Vec<&Address> = self.addresses[shard]
            .iter()
            .filter(|address| address.role == role)
            .collect();

        // We shuffle even if least_outstanding_queries is used to avoid imbalance
        // in cases where all candidates have more or less the same number of outstanding
        // queries
        candidates.shuffle(&mut thread_rng());
        if self.settings.load_balancing_mode == LoadBalancingMode::LeastOutstandingConnections {
            candidates.sort_by(|a, b| {
                self.busy_connection_count(b)
                    .partial_cmp(&self.busy_connection_count(a))
                    .unwrap()
            });
        }

        while !candidates.is_empty() {
            // Get the next candidate
            let address = match candidates.pop() {
                Some(address) => address,
                None => break,
            };

            let mut force_healthcheck = false;

            if self.is_banned(address) {
                if self.try_unban(&address).await {
                    force_healthcheck = true;
                } else {
                    debug!("Address {:?} is banned", address);
                    continue;
                }
            }

            // Indicate we're waiting on a server connection from a pool.
            let now = Instant::now();
            client_stats.waiting();

            // Check if we can connect
            let mut conn = match self.databases[address.shard][address.address_index]
                .get()
                .await
            {
                Ok(conn) => conn,
                Err(err) => {
                    error!("Banning instance {:?}, error: {:?}", address, err);
                    self.ban(address, BanReason::FailedCheckout, Some(client_stats));
                    address.stats.error();
                    client_stats.idle();
                    client_stats.checkout_error();
                    continue;
                }
            };

            // // Check if this server is alive with a health check.
            let server = &mut *conn;

            // Will return error if timestamp is greater than current system time, which it should never be set to
            let require_healthcheck = force_healthcheck
                || server.last_activity().elapsed().unwrap().as_millis()
                    > self.settings.healthcheck_delay as u128;

            // Do not issue a health check unless it's been a little while
            // since we last checked the server is ok.
            // Health checks are pretty expensive.
            if !require_healthcheck {
                let checkout_time: u64 = now.elapsed().as_micros() as u64;
                client_stats.checkout_time(checkout_time);
                server
                    .stats()
                    .checkout_time(checkout_time, client_stats.application_name());
                server.stats().active(client_stats.application_name());

                return Ok((conn, address.clone()));
            }

            if self
                .run_health_check(address, server, now, client_stats)
                .await
            {
                return Ok((conn, address.clone()));
            } else {
                continue;
            }
        }
        Err(Error::AllServersDown)
    }

    async fn run_health_check(
        &self,
        address: &Address,
        server: &mut Server,
        start: Instant,
        client_info: &ClientStats,
    ) -> bool {
        debug!("Running health check on server {:?}", address);

        server.stats().tested();

        match tokio::time::timeout(
            tokio::time::Duration::from_millis(self.settings.healthcheck_timeout),
            server.query(";"), // Cheap query as it skips the query planner
        )
        .await
        {
            // Check if health check succeeded.
            Ok(res) => match res {
                Ok(_) => {
                    let checkout_time: u64 = start.elapsed().as_micros() as u64;
                    client_info.checkout_time(checkout_time);
                    server
                        .stats()
                        .checkout_time(checkout_time, client_info.application_name());
                    server.stats().active(client_info.application_name());

                    return true;
                }

                // Health check failed.
                Err(err) => {
                    error!(
                        "Banning instance {:?} because of failed health check, {:?}",
                        address, err
                    );
                }
            },

            // Health check timed out.
            Err(err) => {
                error!(
                    "Banning instance {:?} because of health check timeout, {:?}",
                    address, err
                );
            }
        }

        // Don't leave a bad connection in the pool.
        server.mark_bad();

        self.ban(&address, BanReason::FailedHealthCheck, Some(client_info));
        return false;
    }

    /// Ban an address (i.e. replica). It no longer will serve
    /// traffic for any new transactions. Existing transactions on that replica
    /// will finish successfully or error out to the clients.
    pub fn ban(&self, address: &Address, reason: BanReason, client_info: Option<&ClientStats>) {
        // Primary can never be banned
        if address.role == Role::Primary {
            return;
        }

        let now = chrono::offset::Utc::now().naive_utc();
        let mut guard = self.banlist.write();
        error!("Banning {:?}", address);
        if let Some(client_info) = client_info {
            client_info.ban_error();
            address.stats.error();
        }
        guard[address.shard].insert(address.clone(), (reason, now));
    }

    /// Clear the replica to receive traffic again. Takes effect immediately
    /// for all new transactions.
    pub fn unban(&self, address: &Address) {
        let mut guard = self.banlist.write();
        guard[address.shard].remove(address);
    }

    /// Check if address is banned
    /// true if banned, false otherwise
    pub fn is_banned(&self, address: &Address) -> bool {
        let guard = self.banlist.read();

        match guard[address.shard].get(address) {
            Some(_) => true,
            None => {
                debug!("{:?} is ok", address);
                false
            }
        }
    }

    /// Determines trying to unban this server was successful
    pub async fn try_unban(&self, address: &Address) -> bool {
        // If somehow primary ends up being banned we should return true here
        if address.role == Role::Primary {
            return true;
        }

        // Check if all replicas are banned, in that case unban all of them
        let replicas_available = self.addresses[address.shard]
            .iter()
            .filter(|addr| addr.role == Role::Replica)
            .count();

        debug!("Available targets: {}", replicas_available);

        let read_guard = self.banlist.read();
        let all_replicas_banned = read_guard[address.shard].len() == replicas_available;
        drop(read_guard);

        if all_replicas_banned {
            let mut write_guard = self.banlist.write();
            warn!("Unbanning all replicas.");
            write_guard[address.shard].clear();

            return true;
        }

        // Check if ban time is expired
        let read_guard = self.banlist.read();
        let exceeded_ban_time = match read_guard[address.shard].get(address) {
            Some((ban_reason, timestamp)) => {
                let now = chrono::offset::Utc::now().naive_utc();
                match ban_reason {
                    BanReason::AdminBan(duration) => {
                        now.timestamp() - timestamp.timestamp() > *duration
                    }
                    _ => now.timestamp() - timestamp.timestamp() > self.settings.ban_time,
                }
            }
            None => return true,
        };
        drop(read_guard);

        if exceeded_ban_time {
            warn!("Unbanning {:?}", address);
            let mut write_guard = self.banlist.write();
            write_guard[address.shard].remove(address);
            drop(write_guard);

            true
        } else {
            debug!("{:?} is banned", address);
            false
        }
    }

    /// Get the number of configured shards.
    pub fn shards(&self) -> usize {
        self.databases.len()
    }

    pub fn get_bans(&self) -> Vec<(Address, (BanReason, NaiveDateTime))> {
        let mut bans: Vec<(Address, (BanReason, NaiveDateTime))> = Vec::new();
        let guard = self.banlist.read();
        for banlist in guard.iter() {
            for (address, (reason, timestamp)) in banlist.iter() {
                bans.push((address.clone(), (reason.clone(), timestamp.clone())));
            }
        }
        return bans;
    }

    /// Get the address from the host url
    pub fn get_addresses_from_host(&self, host: &str) -> Vec<Address> {
        let mut addresses = Vec::new();
        for shard in 0..self.shards() {
            for server in 0..self.servers(shard) {
                let address = self.address(shard, server);
                if address.host == host {
                    addresses.push(address.clone());
                }
            }
        }
        addresses
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
        self.server_info.read().clone()
    }

    fn busy_connection_count(&self, address: &Address) -> u32 {
        let state = self.pool_state(address.shard, address.address_index);
        let idle = state.idle_connections;
        let provisioned = state.connections;

        if idle > provisioned {
            // Unlikely but avoids an overflow panic if this ever happens
            return 0;
        }
        let busy = provisioned - idle;
        debug!("{:?} has {:?} busy connections", address, busy);
        return busy;
    }
}

/// Wrapper for the bb8 connection pool.
pub struct ServerPool {
    address: Address,
    user: User,
    database: String,
    client_server_map: ClientServerMap,
    stats: Arc<PoolStats>,
    auth_hash: Arc<RwLock<Option<String>>>,
}

impl ServerPool {
    pub fn new(
        address: Address,
        user: User,
        database: &str,
        client_server_map: ClientServerMap,
        stats: Arc<PoolStats>,
        auth_hash: Arc<RwLock<Option<String>>>,
    ) -> ServerPool {
        ServerPool {
            address,
            user: user.clone(),
            database: database.to_string(),
            client_server_map,
            stats,
            auth_hash,
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

        let stats = Arc::new(ServerStats::new(
            self.address.clone(),
            self.stats.clone(),
            tokio::time::Instant::now(),
        ));

        stats.register(stats.clone());

        // Connect to the PostgreSQL server.
        match Server::startup(
            &self.address,
            &self.user,
            &self.database,
            self.client_server_map.clone(),
            stats.clone(),
            self.auth_hash.clone(),
        )
        .await
        {
            Ok(conn) => {
                stats.idle();
                Ok(conn)
            }
            Err(err) => {
                stats.disconnect();
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
