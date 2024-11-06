use arc_swap::ArcSwap;
use async_trait::async_trait;
use bb8::{ManageConnection, Pool, PooledConnection, QueueStrategy};
use chrono::naive::NaiveDateTime;
use log::{debug, error, info, warn};
use lru::LruCache;
use once_cell::sync::Lazy;
use parking_lot::{Mutex, RwLock};
use rand::seq::SliceRandom;
use rand::thread_rng;
use regex::Regex;
use std::collections::HashMap;
use std::fmt::{Display, Formatter};
use std::num::NonZeroUsize;
use std::sync::atomic::AtomicU64;
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};
use std::time::Instant;
use tokio::sync::Notify;

use crate::config::{
    get_config, Address, DefaultShard, General, LoadBalancingMode, Plugins, PoolMode, Role, User,
};
use crate::errors::Error;

use crate::auth_passthrough::AuthPassthrough;
use crate::messages::Parse;
use crate::plugins::prewarmer;
use crate::server::{Server, ServerParameters};
use crate::sharding::ShardingFunction;
use crate::stats::{AddressStats, ClientStats, ServerStats};

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

pub type PreparedStatementCacheType = Arc<Mutex<PreparedStatementCache>>;

// TODO: Add stats the this cache
// TODO: Add application name to the cache value to help identify which application is using the cache
// TODO: Create admin command to show which statements are in the cache
#[derive(Debug)]
pub struct PreparedStatementCache {
    cache: LruCache<u64, Arc<Parse>>,
}

impl PreparedStatementCache {
    pub fn new(mut size: usize) -> Self {
        // Cannot be zeros
        if size == 0 {
            size = 1;
        }

        PreparedStatementCache {
            cache: LruCache::new(NonZeroUsize::new(size).unwrap()),
        }
    }

    /// Adds the prepared statement to the cache if it doesn't exist with a new name
    /// if it already exists will give you the existing parse
    ///
    /// Pass the hash to this so that we can do the compute before acquiring the lock
    pub fn get_or_insert(&mut self, parse: &Parse, hash: u64) -> Arc<Parse> {
        match self.cache.get(&hash) {
            Some(rewritten_parse) => rewritten_parse.clone(),
            None => {
                let new_parse = Arc::new(parse.clone().rewrite());
                let evicted = self.cache.push(hash, new_parse.clone());

                if let Some((_, evicted_parse)) = evicted {
                    debug!(
                        "Evicted prepared statement {} from cache",
                        evicted_parse.name
                    );
                }

                new_parse
            }
        }
    }

    /// Marks the hash as most recently used if it exists
    pub fn promote(&mut self, hash: &u64) {
        self.cache.promote(hash);
    }
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

static POOL_REAPER_RATE: u64 = 30_000; // 30 seconds by default

impl PoolIdentifier {
    /// Create a new user/pool identifier.
    pub fn new(db: &str, user: &str) -> PoolIdentifier {
        PoolIdentifier {
            db: db.to_string(),
            user: user.to_string(),
        }
    }
}

impl Display for PoolIdentifier {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}@{}", self.user, self.db)
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
    pub db: String,

    // Default server role to connect to.
    pub default_role: Option<Role>,

    // Enable/disable query parser.
    pub query_parser_enabled: bool,

    // Max length of query the parser will parse.
    pub query_parser_max_length: Option<usize>,

    // Infer role
    pub query_parser_read_write_splitting: bool,

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

    // What to do when no shard is selected in a sharded system
    pub default_shard: DefaultShard,

    // Limit how much of each query is searched for a potential shard regex match
    pub regex_search_limit: usize,

    // Auth query parameters
    pub auth_query: Option<String>,
    pub auth_query_user: Option<String>,
    pub auth_query_password: Option<String>,

    /// Plugins
    pub plugins: Option<Plugins>,
}

impl Default for PoolSettings {
    fn default() -> PoolSettings {
        PoolSettings {
            pool_mode: PoolMode::Transaction,
            load_balancing_mode: LoadBalancingMode::Random,
            shards: 1,
            user: User::default(),
            db: String::default(),
            default_role: None,
            query_parser_enabled: false,
            query_parser_max_length: None,
            query_parser_read_write_splitting: false,
            primary_reads_enabled: true,
            sharding_function: ShardingFunction::PgBigintHash,
            automatic_sharding_key: None,
            healthcheck_delay: General::default_healthcheck_delay(),
            healthcheck_timeout: General::default_healthcheck_timeout(),
            ban_time: General::default_ban_time(),
            sharding_key_regex: None,
            shard_id_regex: None,
            regex_search_limit: 1000,
            default_shard: DefaultShard::Shard(0),
            auth_query: None,
            auth_query_user: None,
            auth_query_password: None,
            plugins: None,
        }
    }
}

/// The globally accessible connection pool.
#[derive(Clone, Debug, Default)]
pub struct ConnectionPool {
    /// The pools handled internally by bb8.
    databases: Arc<Vec<Vec<Pool<ServerPool>>>>,

    /// The addresses (host, port, role) to handle
    /// failover and load balancing deterministically.
    addresses: Arc<Vec<Vec<Address>>>,

    /// List of banned addresses (see above)
    /// that should not be queried.
    banlist: BanList,

    /// The server information has to be passed to the
    /// clients on startup. We pre-connect to all shards and replicas
    /// on pool creation and save the startup parameters here.
    original_server_parameters: Arc<RwLock<ServerParameters>>,

    /// Pool configuration.
    pub settings: Arc<PoolSettings>,

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

    /// AuthInfo
    pub auth_hash: Arc<RwLock<Option<String>>>,

    /// Cache
    pub prepared_statement_cache: Option<PreparedStatementCacheType>,
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

                if let Some(pool) = old_pool_ref {
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
                                    error_count: Arc::new(AtomicU64::new(0)),
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
                            error_count: Arc::new(AtomicU64::new(0)),
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
                                    if let Some(ref pool_auth_hash_value) = *(pool_auth_hash.read())
                                    {
                                        if ok != *pool_auth_hash_value {
                                            warn!(
                                                "Hash is not the same across shards \
                                                of the same pool, client auth will \
                                                be done using last obtained hash. \
                                                Server: {}:{}, Database: {}",
                                                server.host, server.port, shard.database,
                                            );
                                        }
                                    }

                                    debug!("Hash obtained for {:?}", address);

                                    {
                                        let mut pool_auth_hash = pool_auth_hash.write();
                                        *pool_auth_hash = Some(ok.clone());
                                    }
                                }
                                Err(err) => warn!(
                                    "Could not obtain password hashes \
                                        using auth_query config, ignoring. \
                                        Error: {:?}",
                                    err,
                                ),
                            }
                        }

                        let manager = ServerPool::new(
                            address.clone(),
                            user.clone(),
                            &shard.database,
                            client_server_map.clone(),
                            pool_auth_hash.clone(),
                            match pool_config.plugins {
                                Some(ref plugins) => Some(plugins.clone()),
                                None => config.plugins.clone(),
                            },
                            pool_config.cleanup_server_connections,
                            pool_config.log_client_parameter_status_changes,
                            pool_config.prepared_statements_cache_size,
                        );

                        let connect_timeout = match user.connect_timeout {
                            Some(connect_timeout) => connect_timeout,
                            None => match pool_config.connect_timeout {
                                Some(connect_timeout) => connect_timeout,
                                None => config.general.connect_timeout,
                            },
                        };

                        let idle_timeout = match user.idle_timeout {
                            Some(idle_timeout) => idle_timeout,
                            None => match pool_config.idle_timeout {
                                Some(idle_timeout) => idle_timeout,
                                None => config.general.idle_timeout,
                            },
                        };

                        let server_lifetime = match user.server_lifetime {
                            Some(server_lifetime) => server_lifetime,
                            None => match pool_config.server_lifetime {
                                Some(server_lifetime) => server_lifetime,
                                None => config.general.server_lifetime,
                            },
                        };

                        let reaper_rate = *[idle_timeout, server_lifetime, POOL_REAPER_RATE]
                            .iter()
                            .min()
                            .unwrap();

                        let queue_strategy = match config.general.server_round_robin {
                            true => QueueStrategy::Fifo,
                            false => QueueStrategy::Lifo,
                        };

                        debug!(
                            "[pool: {}][user: {}] Pool reaper rate: {}ms",
                            pool_name, user.username, reaper_rate
                        );

                        let pool = Pool::builder()
                            .max_size(user.pool_size)
                            .min_idle(user.min_pool_size)
                            .connection_timeout(std::time::Duration::from_millis(connect_timeout))
                            .idle_timeout(Some(std::time::Duration::from_millis(idle_timeout)))
                            .max_lifetime(Some(std::time::Duration::from_millis(server_lifetime)))
                            .reaper_rate(std::time::Duration::from_millis(reaper_rate))
                            .queue_strategy(queue_strategy)
                            .test_on_check_out(false);

                        let pool = if config.general.validate_config {
                            pool.build(manager).await?
                        } else {
                            pool.build_unchecked(manager)
                        };

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
                    databases: Arc::new(shards),
                    addresses: Arc::new(addresses),
                    banlist: Arc::new(RwLock::new(banlist)),
                    config_hash: new_pool_hash_value,
                    original_server_parameters: Arc::new(RwLock::new(ServerParameters::new())),
                    auth_hash: pool_auth_hash,
                    settings: Arc::new(PoolSettings {
                        pool_mode: match user.pool_mode {
                            Some(pool_mode) => pool_mode,
                            None => pool_config.pool_mode,
                        },
                        load_balancing_mode: pool_config.load_balancing_mode,
                        // shards: pool_config.shards.clone(),
                        shards: shard_ids.len(),
                        user: user.clone(),
                        db: pool_name.clone(),
                        default_role: match pool_config.default_role.as_str() {
                            "any" => None,
                            "replica" => Some(Role::Replica),
                            "primary" => Some(Role::Primary),
                            _ => unreachable!(),
                        },
                        query_parser_enabled: pool_config.query_parser_enabled,
                        query_parser_max_length: pool_config.query_parser_max_length,
                        query_parser_read_write_splitting: pool_config
                            .query_parser_read_write_splitting,
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
                        default_shard: pool_config.default_shard,
                        auth_query: pool_config.auth_query.clone(),
                        auth_query_user: pool_config.auth_query_user.clone(),
                        auth_query_password: pool_config.auth_query_password.clone(),
                        plugins: match pool_config.plugins {
                            Some(ref plugins) => Some(plugins.clone()),
                            None => config.plugins.clone(),
                        },
                    }),
                    validated: Arc::new(AtomicBool::new(false)),
                    paused: Arc::new(AtomicBool::new(false)),
                    paused_waiter: Arc::new(Notify::new()),
                    prepared_statement_cache: match pool_config.prepared_statements_cache_size {
                        0 => None,
                        _ => Some(Arc::new(Mutex::new(PreparedStatementCache::new(
                            pool_config.prepared_statements_cache_size,
                        )))),
                    },
                };

                // Connect to the servers to make sure pool configuration is valid
                // before setting it globally.
                // Do this async and somewhere else, we don't have to wait here.
                if config.general.validate_config {
                    let validate_pool = pool.clone();
                    tokio::task::spawn(async move {
                        let _ = validate_pool.validate().await;
                    });
                }

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
    pub async fn validate(&self) -> Result<(), Error> {
        let mut futures = Vec::new();
        let validated = Arc::clone(&self.validated);

        for shard in 0..self.shards() {
            for server in 0..self.servers(shard) {
                let databases = self.databases.clone();
                let validated = Arc::clone(&validated);
                let pool_server_parameters = Arc::clone(&self.original_server_parameters);

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
                    let server_parameters: ServerParameters = server.server_parameters();

                    let mut guard = pool_server_parameters.write();
                    *guard = server_parameters;
                    validated.store(true, Ordering::Relaxed);
                });

                futures.push(task);
            }
        }

        futures::future::join_all(futures).await;

        // TODO: compare server information to make sure
        // all shards are running identical configurations.
        if !self.validated() {
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
        shard: Option<usize>,       // shard number
        role: Option<Role>,         // primary or replica
        client_stats: &ClientStats, // client id
    ) -> Result<(PooledConnection<'_, ServerPool>, Address), Error> {
        let effective_shard_id = if self.shards() == 1 {
            // The base, unsharded case
            Some(0)
        } else {
            if !self.valid_shard_id(shard) {
                // None is valid shard ID so it is safe to unwrap here
                return Err(Error::InvalidShardId(shard.unwrap()));
            }
            shard
        };

        let mut candidates = self
            .addresses
            .iter()
            .flatten()
            .filter(|address| address.role == role)
            .collect::<Vec<&Address>>();

        // We start with a shuffled list of addresses even if we end up resorting
        // this is meant to avoid hitting instance 0 everytime if the sorting metric
        // ends up being the same for all instances
        candidates.shuffle(&mut thread_rng());

        match effective_shard_id {
            Some(shard_id) => candidates.retain(|address| address.shard == shard_id),
            None => match self.settings.default_shard {
                DefaultShard::Shard(shard_id) => {
                    candidates.retain(|address| address.shard == shard_id)
                }
                DefaultShard::Random => (),
                DefaultShard::RandomHealthy => {
                    candidates.sort_by(|a, b| {
                        b.error_count
                            .load(Ordering::Relaxed)
                            .partial_cmp(&a.error_count.load(Ordering::Relaxed))
                            .unwrap()
                    });
                },
                DefaultShard::Fail => return Err(Error::NoShardSelected)
            },
        };

        if self.settings.load_balancing_mode == LoadBalancingMode::LeastOutstandingConnections {
            candidates.sort_by(|a, b| {
                self.busy_connection_count(b)
                    .partial_cmp(&self.busy_connection_count(a))
                    .unwrap()
            });
        }

        // Indicate we're waiting on a server connection from a pool.
        let now = Instant::now();
        client_stats.waiting();

        while !candidates.is_empty() {
            // Get the next candidate
            let address = match candidates.pop() {
                Some(address) => address,
                None => break,
            };

            let mut force_healthcheck = false;

            if self.is_banned(address) {
                if self.try_unban(address).await {
                    force_healthcheck = true;
                } else {
                    debug!("Address {:?} is banned", address);
                    continue;
                }
            }

            // Check if we can connect
            let mut conn = match self.databases[address.shard][address.address_index]
                .get()
                .await
            {
                Ok(conn) => {
                    address.reset_error_count();
                    conn
                }
                Err(err) => {
                    error!(
                        "Connection checkout error for instance {:?}, error: {:?}",
                        address, err
                    );
                    self.ban(address, BanReason::FailedCheckout, Some(client_stats));
                    address.stats.error();
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
                let checkout_time = now.elapsed().as_micros() as u64;
                client_stats.checkout_success();
                server
                    .stats()
                    .checkout_time(checkout_time, client_stats.application_name());
                server.stats().active(client_stats.application_name());
                client_stats.active();
                return Ok((conn, address.clone()));
            }

            if self
                .run_health_check(address, server, now, client_stats)
                .await
            {
                let checkout_time = now.elapsed().as_micros() as u64;
                client_stats.checkout_success();
                server
                    .stats()
                    .checkout_time(checkout_time, client_stats.application_name());
                server.stats().active(client_stats.application_name());
                client_stats.active();
                return Ok((conn, address.clone()));
            } else {
                continue;
            }
        }

        client_stats.checkout_error();

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
                    client_info.checkout_success();
                    server
                        .stats()
                        .checkout_time(checkout_time, client_info.application_name());
                    server.stats().active(client_info.application_name());

                    return true;
                }

                // Health check failed.
                Err(err) => {
                    error!(
                        "Failed health check on instance {:?}, error: {:?}",
                        address, err
                    );
                }
            },

            // Health check timed out.
            Err(err) => {
                error!(
                    "Health check timeout on instance {:?}, error: {:?}",
                    address, err
                );
            }
        }

        // Don't leave a bad connection in the pool.
        server.mark_bad("failed health check");

        self.ban(address, BanReason::FailedHealthCheck, Some(client_info));
        false
    }

    /// Ban an address (i.e. replica). It no longer will serve
    /// traffic for any new transactions. Existing transactions on that replica
    /// will finish successfully or error out to the clients.
    pub fn ban(&self, address: &Address, reason: BanReason, client_info: Option<&ClientStats>) {
        // Count the number of errors since the last successful checkout
        // This is used to determine if the shard is down
        match reason {
            BanReason::FailedHealthCheck
            | BanReason::FailedCheckout
            | BanReason::MessageSendFailed
            | BanReason::MessageReceiveFailed => {
                address.increment_error_count();
            }
            _ => (),
        };

        // Primary can never be banned
        if address.role == Role::Primary {
            return;
        }

        error!("Banning instance {:?}, reason: {:?}", address, reason);

        let now = chrono::offset::Utc::now().naive_utc();
        let mut guard = self.banlist.write();

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
                bans.push((address.clone(), (reason.clone(), *timestamp)));
            }
        }
        bans
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

    pub fn server_parameters(&self) -> ServerParameters {
        self.original_server_parameters.read().clone()
    }

    /// Get the number of checked out connection for an address
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
        busy
    }

    fn valid_shard_id(&self, shard: Option<usize>) -> bool {
        match shard {
            None => true,
            Some(shard) => shard < self.shards(),
        }
    }

    /// Register a parse statement to the pool's cache and return the rewritten parse
    ///
    /// Do not pass an anonymous parse statement to this function
    pub fn register_parse_to_cache(&self, hash: u64, parse: &Parse) -> Option<Arc<Parse>> {
        // We should only be calling this function if the cache is enabled
        match self.prepared_statement_cache {
            Some(ref prepared_statement_cache) => {
                let mut cache = prepared_statement_cache.lock();
                Some(cache.get_or_insert(parse, hash))
            }
            None => None,
        }
    }

    /// Promote a prepared statement hash in the LRU
    pub fn promote_prepared_statement_hash(&self, hash: &u64) {
        // We should only be calling this function if the cache is enabled
        if let Some(ref prepared_statement_cache) = self.prepared_statement_cache {
            let mut cache = prepared_statement_cache.lock();
            cache.promote(hash);
        }
    }
}

/// Wrapper for the bb8 connection pool.
pub struct ServerPool {
    /// Server address.
    address: Address,

    /// Server Postgres user.
    user: User,

    /// Server database.
    database: String,

    /// Client/server mapping.
    client_server_map: ClientServerMap,

    /// Server auth hash (for auth passthrough).
    auth_hash: Arc<RwLock<Option<String>>>,

    /// Server plugins.
    plugins: Option<Plugins>,

    /// Should we clean up dirty connections before putting them into the pool?
    cleanup_connections: bool,

    /// Log client parameter status changes
    log_client_parameter_status_changes: bool,

    /// Prepared statement cache size
    prepared_statement_cache_size: usize,
}

impl ServerPool {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        address: Address,
        user: User,
        database: &str,
        client_server_map: ClientServerMap,
        auth_hash: Arc<RwLock<Option<String>>>,
        plugins: Option<Plugins>,
        cleanup_connections: bool,
        log_client_parameter_status_changes: bool,
        prepared_statement_cache_size: usize,
    ) -> ServerPool {
        ServerPool {
            address,
            user,
            database: database.to_string(),
            client_server_map,
            auth_hash,
            plugins,
            cleanup_connections,
            log_client_parameter_status_changes,
            prepared_statement_cache_size,
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
            self.cleanup_connections,
            self.log_client_parameter_status_changes,
            self.prepared_statement_cache_size,
        )
        .await
        {
            Ok(mut conn) => {
                if let Some(ref plugins) = self.plugins {
                    if let Some(ref prewarmer) = plugins.prewarmer {
                        let mut prewarmer = prewarmer::Prewarmer {
                            enabled: prewarmer.enabled,
                            server: &mut conn,
                            queries: &prewarmer.queries,
                        };

                        prewarmer.run().await?;
                    }
                }

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
