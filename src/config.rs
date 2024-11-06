/// Parse the configuration file.
use arc_swap::ArcSwap;
use log::{error, info};
use once_cell::sync::Lazy;
use regex::Regex;
use serde::{Deserializer, Serializer};
use serde_derive::{Deserialize, Serialize};

use std::collections::hash_map::DefaultHasher;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::hash::{Hash, Hasher};
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tokio::fs::File;
use tokio::io::AsyncReadExt;

use crate::dns_cache::CachedResolver;
use crate::errors::Error;
use crate::pool::{ClientServerMap, ConnectionPool};
use crate::sharding::ShardingFunction;
use crate::stats::AddressStats;
use crate::tls::{load_certs, load_keys};

pub const VERSION: &str = env!("CARGO_PKG_VERSION");

/// Globally available configuration.
static CONFIG: Lazy<ArcSwap<Config>> = Lazy::new(|| ArcSwap::from_pointee(Config::default()));

/// Server role: primary or replica.
#[derive(Clone, PartialEq, Serialize, Deserialize, Hash, std::cmp::Eq, Debug, Copy)]
pub enum Role {
    #[serde(alias = "primary", alias = "Primary")]
    Primary,
    #[serde(alias = "replica", alias = "Replica")]
    Replica,
    #[serde(alias = "mirror", alias = "Mirror")]
    Mirror,
}

impl std::fmt::Display for Role {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Role::Primary => write!(f, "primary"),
            Role::Replica => write!(f, "replica"),
            Role::Mirror => write!(f, "mirror"),
        }
    }
}

impl PartialEq<Option<Role>> for Role {
    fn eq(&self, other: &Option<Role>) -> bool {
        match other {
            None => true,
            Some(role) => *self == *role,
        }
    }
}

impl PartialEq<Role> for Option<Role> {
    fn eq(&self, other: &Role) -> bool {
        match *self {
            None => true,
            Some(role) => role == *other,
        }
    }
}

/// Address identifying a PostgreSQL server uniquely.
#[derive(Clone, Debug)]
pub struct Address {
    /// Unique ID per addressable Postgres server.
    pub id: usize,

    /// Server host.
    pub host: String,

    /// Server port.
    pub port: u16,

    /// Shard number of this Postgres server.
    pub shard: usize,

    /// The name of the Postgres database.
    pub database: String,

    /// Server role: replica, primary.
    pub role: Role,

    /// If it's a replica, number it for reference and failover.
    pub replica_number: usize,

    /// Position of the server in the pool for failover.
    pub address_index: usize,

    /// The name of the user configured to use this pool.
    pub username: String,

    /// The name of this pool (i.e. database name visible to the client).
    pub pool_name: String,

    /// List of addresses to receive mirrored traffic.
    pub mirrors: Vec<Address>,

    /// Address stats
    pub stats: Arc<AddressStats>,

    /// Number of errors encountered since last successful checkout
    pub error_count: Arc<AtomicU64>,
}

impl Default for Address {
    fn default() -> Address {
        Address {
            id: 0,
            host: String::from("127.0.0.1"),
            port: 5432,
            shard: 0,
            database: String::from("database"),
            role: Role::Replica,
            replica_number: 0,
            address_index: 0,
            username: String::from("username"),
            pool_name: String::from("pool_name"),
            mirrors: Vec::new(),
            stats: Arc::new(AddressStats::default()),
            error_count: Arc::new(AtomicU64::new(0)),
        }
    }
}

impl std::fmt::Display for Address {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(
            f,
            "[address: {}:{}][database: {}][user: {}]",
            self.host, self.port, self.database, self.username
        )
    }
}

// We need to implement PartialEq by ourselves so we skip stats in the comparison
impl PartialEq for Address {
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id
            && self.host == other.host
            && self.port == other.port
            && self.shard == other.shard
            && self.address_index == other.address_index
            && self.replica_number == other.replica_number
            && self.database == other.database
            && self.role == other.role
            && self.username == other.username
            && self.pool_name == other.pool_name
            && self.mirrors == other.mirrors
    }
}
impl Eq for Address {}

// We need to implement Hash by ourselves so we skip stats in the comparison
impl Hash for Address {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.id.hash(state);
        self.host.hash(state);
        self.port.hash(state);
        self.shard.hash(state);
        self.address_index.hash(state);
        self.replica_number.hash(state);
        self.database.hash(state);
        self.role.hash(state);
        self.username.hash(state);
        self.pool_name.hash(state);
        self.mirrors.hash(state);
    }
}

impl Address {
    /// Address name (aka database) used in `SHOW STATS`, `SHOW DATABASES`, and `SHOW POOLS`.
    pub fn name(&self) -> String {
        match self.role {
            Role::Primary => format!("{}_shard_{}_primary", self.pool_name, self.shard),
            Role::Replica => format!(
                "{}_shard_{}_replica_{}",
                self.pool_name, self.shard, self.replica_number
            ),
            Role::Mirror => format!(
                "{}_shard_{}_mirror_{}",
                self.pool_name, self.shard, self.replica_number
            ),
        }
    }

    pub fn error_count(&self) -> u64 {
        self.error_count.load(Ordering::Relaxed)
    }

    pub fn increment_error_count(&self) {
        self.error_count.fetch_add(1, Ordering::Relaxed);
    }

    pub fn reset_error_count(&self) {
        self.error_count.store(0, Ordering::Relaxed);
    }
}

/// PostgreSQL user.
#[derive(Clone, PartialEq, Hash, Eq, Serialize, Deserialize, Debug)]
pub struct User {
    pub username: String,
    pub password: Option<String>,

    #[serde(default = "User::default_auth_type")]
    pub auth_type: AuthType,
    pub server_username: Option<String>,
    pub server_password: Option<String>,
    pub pool_size: u32,
    pub min_pool_size: Option<u32>,
    pub pool_mode: Option<PoolMode>,
    pub server_lifetime: Option<u64>,
    #[serde(default)] // 0
    pub statement_timeout: u64,
    pub connect_timeout: Option<u64>,
    pub idle_timeout: Option<u64>,
}

impl Default for User {
    fn default() -> User {
        User {
            username: String::from("postgres"),
            password: None,
            auth_type: AuthType::MD5,
            server_username: None,
            server_password: None,
            pool_size: 15,
            min_pool_size: None,
            statement_timeout: 0,
            pool_mode: None,
            server_lifetime: None,
            connect_timeout: None,
            idle_timeout: None,
        }
    }
}

impl User {
    pub fn default_auth_type() -> AuthType {
        AuthType::MD5
    }

    fn validate(&self) -> Result<(), Error> {
        if let Some(min_pool_size) = self.min_pool_size {
            if min_pool_size > self.pool_size {
                error!(
                    "min_pool_size of {} cannot be larger than pool_size of {}",
                    min_pool_size, self.pool_size
                );
                return Err(Error::BadConfig);
            }
        };

        Ok(())
    }
}

/// General configuration.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub struct General {
    #[serde(default = "General::default_host")]
    pub host: String,

    #[serde(default = "General::default_port")]
    pub port: u16,

    pub enable_prometheus_exporter: Option<bool>,

    #[serde(default = "General::default_prometheus_exporter_port")]
    pub prometheus_exporter_port: i16,

    #[serde(default = "General::default_connect_timeout")]
    pub connect_timeout: u64,

    #[serde(default = "General::default_idle_timeout")]
    pub idle_timeout: u64,

    #[serde(default = "General::default_tcp_keepalives_idle")]
    pub tcp_keepalives_idle: u64,
    #[serde(default = "General::default_tcp_keepalives_count")]
    pub tcp_keepalives_count: u32,
    #[serde(default = "General::default_tcp_keepalives_interval")]
    pub tcp_keepalives_interval: u64,
    #[serde(default = "General::default_tcp_user_timeout")]
    pub tcp_user_timeout: u64,

    #[serde(default)] // False
    pub log_client_connections: bool,

    #[serde(default)] // False
    pub log_client_disconnections: bool,

    #[serde(default)] // False
    pub dns_cache_enabled: bool,

    #[serde(default = "General::default_dns_max_ttl")]
    pub dns_max_ttl: u64,

    #[serde(default = "General::default_shutdown_timeout")]
    pub shutdown_timeout: u64,

    #[serde(default = "General::default_healthcheck_timeout")]
    pub healthcheck_timeout: u64,

    #[serde(default = "General::default_healthcheck_delay")]
    pub healthcheck_delay: u64,

    #[serde(default = "General::default_ban_time")]
    pub ban_time: i64,

    #[serde(default = "General::default_idle_client_in_transaction_timeout")]
    pub idle_client_in_transaction_timeout: u64,

    #[serde(default = "General::default_server_lifetime")]
    pub server_lifetime: u64,

    #[serde(default = "General::default_server_round_robin")] // False
    pub server_round_robin: bool,

    #[serde(default = "General::default_worker_threads")]
    pub worker_threads: usize,

    #[serde(default)] // None
    pub autoreload: Option<u64>,

    pub tls_certificate: Option<String>,
    pub tls_private_key: Option<String>,

    #[serde(default)] // false
    pub server_tls: bool,

    #[serde(default)] // false
    pub verify_server_certificate: bool,

    pub admin_username: String,
    pub admin_password: String,

    #[serde(default = "General::default_admin_auth_type")]
    pub admin_auth_type: AuthType,

    #[serde(default = "General::default_validate_config")]
    pub validate_config: bool,

    // Support for auth query
    pub auth_query: Option<String>,
    pub auth_query_user: Option<String>,
    pub auth_query_password: Option<String>,
}

impl General {
    pub fn default_host() -> String {
        "0.0.0.0".into()
    }

    pub fn default_admin_auth_type() -> AuthType {
        AuthType::MD5
    }

    pub fn default_port() -> u16 {
        5432
    }

    pub fn default_server_lifetime() -> u64 {
        1000 * 60 * 60 // 1 hour
    }

    pub fn default_connect_timeout() -> u64 {
        1000
    }

    // These keepalive defaults should detect a dead connection within 30 seconds.
    // Tokio defaults to disabling keepalives which keeps dead connections around indefinitely.
    // This can lead to permanent server pool exhaustion
    pub fn default_tcp_keepalives_idle() -> u64 {
        5 // 5 seconds
    }

    pub fn default_tcp_keepalives_count() -> u32 {
        5 // 5 time
    }

    pub fn default_tcp_keepalives_interval() -> u64 {
        5 // 5 seconds
    }

    pub fn default_tcp_user_timeout() -> u64 {
        10000 // 10000 milliseconds
    }

    pub fn default_idle_timeout() -> u64 {
        600000 // 10 minutes
    }

    pub fn default_shutdown_timeout() -> u64 {
        60000
    }

    pub fn default_dns_max_ttl() -> u64 {
        30
    }

    pub fn default_healthcheck_timeout() -> u64 {
        1000
    }

    pub fn default_healthcheck_delay() -> u64 {
        30000
    }

    pub fn default_ban_time() -> i64 {
        60
    }

    pub fn default_worker_threads() -> usize {
        4
    }

    pub fn default_idle_client_in_transaction_timeout() -> u64 {
        0
    }

    pub fn default_validate_config() -> bool {
        true
    }

    pub fn default_prometheus_exporter_port() -> i16 {
        9930
    }

    pub fn default_server_round_robin() -> bool {
        true
    }
}

impl Default for General {
    fn default() -> General {
        General {
            host: Self::default_host(),
            port: Self::default_port(),
            enable_prometheus_exporter: Some(false),
            prometheus_exporter_port: 9930,
            connect_timeout: General::default_connect_timeout(),
            idle_timeout: General::default_idle_timeout(),
            tcp_keepalives_idle: Self::default_tcp_keepalives_idle(),
            tcp_keepalives_count: Self::default_tcp_keepalives_count(),
            tcp_keepalives_interval: Self::default_tcp_keepalives_interval(),
            tcp_user_timeout: Self::default_tcp_user_timeout(),
            log_client_connections: false,
            log_client_disconnections: false,
            dns_cache_enabled: false,
            dns_max_ttl: Self::default_dns_max_ttl(),
            shutdown_timeout: Self::default_shutdown_timeout(),
            healthcheck_timeout: Self::default_healthcheck_timeout(),
            healthcheck_delay: Self::default_healthcheck_delay(),
            ban_time: Self::default_ban_time(),
            idle_client_in_transaction_timeout: Self::default_idle_client_in_transaction_timeout(),
            server_lifetime: Self::default_server_lifetime(),
            server_round_robin: Self::default_server_round_robin(),
            worker_threads: Self::default_worker_threads(),
            autoreload: None,
            tls_certificate: None,
            tls_private_key: None,
            server_tls: false,
            verify_server_certificate: false,
            admin_username: String::from("admin"),
            admin_password: String::from("admin"),
            admin_auth_type: AuthType::MD5,
            validate_config: true,
            auth_query: None,
            auth_query_user: None,
            auth_query_password: None,
        }
    }
}

/// Pool mode:
/// - transaction: server serves one transaction,
/// - session: server is attached to the client.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, Copy, Hash)]
pub enum PoolMode {
    #[serde(alias = "transaction", alias = "Transaction")]
    Transaction,

    #[serde(alias = "session", alias = "Session")]
    Session,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, Copy, Hash)]
pub enum AuthType {
    #[serde(alias = "trust", alias = "Trust")]
    Trust,

    #[serde(alias = "md5", alias = "MD5")]
    MD5,
}

impl std::fmt::Display for PoolMode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            PoolMode::Transaction => write!(f, "transaction"),
            PoolMode::Session => write!(f, "session"),
        }
    }
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, Copy, Hash)]
pub enum LoadBalancingMode {
    #[serde(alias = "random", alias = "Random")]
    Random,

    #[serde(alias = "loc", alias = "LOC", alias = "least_outstanding_connections")]
    LeastOutstandingConnections,
}

impl std::fmt::Display for LoadBalancingMode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            LoadBalancingMode::Random => write!(f, "random"),
            LoadBalancingMode::LeastOutstandingConnections => {
                write!(f, "least_outstanding_connections")
            }
        }
    }
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, Hash)]
pub struct Pool {
    #[serde(default = "Pool::default_pool_mode")]
    pub pool_mode: PoolMode,

    #[serde(default = "Pool::default_load_balancing_mode")]
    pub load_balancing_mode: LoadBalancingMode,

    #[serde(default = "Pool::default_default_role")]
    pub default_role: String,

    #[serde(default)] // False
    pub query_parser_enabled: bool,

    pub query_parser_max_length: Option<usize>,

    #[serde(default)] // False
    pub query_parser_read_write_splitting: bool,

    #[serde(default)] // False
    pub primary_reads_enabled: bool,

    /// Maximum time to allow for establishing a new server connection.
    pub connect_timeout: Option<u64>,

    /// Close idle connections that have been opened for longer than this.
    pub idle_timeout: Option<u64>,

    /// Close server connections that have been opened for longer than this.
    /// Only applied to idle connections. If the connection is actively used for
    /// longer than this period, the pool will not interrupt it.
    pub server_lifetime: Option<u64>,

    #[serde(default = "Pool::default_sharding_function")]
    pub sharding_function: ShardingFunction,

    #[serde(default = "Pool::default_automatic_sharding_key")]
    pub automatic_sharding_key: Option<String>,

    pub sharding_key_regex: Option<String>,
    pub shard_id_regex: Option<String>,
    pub regex_search_limit: Option<usize>,

    #[serde(default = "Pool::default_default_shard")]
    pub default_shard: DefaultShard,

    pub auth_query: Option<String>,
    pub auth_query_user: Option<String>,
    pub auth_query_password: Option<String>,

    #[serde(default = "Pool::default_cleanup_server_connections")]
    pub cleanup_server_connections: bool,

    #[serde(default)] // False
    pub log_client_parameter_status_changes: bool,

    #[serde(default = "Pool::default_prepared_statements_cache_size")]
    pub prepared_statements_cache_size: usize,

    pub plugins: Option<Plugins>,
    pub shards: BTreeMap<String, Shard>,
    pub users: BTreeMap<String, User>,
    // Note, don't put simple fields below these configs. There's a compatibility issue with TOML that makes it
    // incompatible to have simple fields in TOML after complex objects. See
    // https://users.rust-lang.org/t/why-toml-to-string-get-error-valueaftertable/85903
}

impl Pool {
    pub fn hash_value(&self) -> u64 {
        let mut s = DefaultHasher::new();
        self.hash(&mut s);
        s.finish()
    }

    pub fn is_auth_query_configured(&self) -> bool {
        self.auth_query_password.is_some()
            && self.auth_query_user.is_some()
            && self.auth_query_password.is_some()
    }

    pub fn default_pool_mode() -> PoolMode {
        PoolMode::Transaction
    }

    pub fn default_default_shard() -> DefaultShard {
        DefaultShard::default()
    }

    pub fn default_load_balancing_mode() -> LoadBalancingMode {
        LoadBalancingMode::Random
    }

    pub fn default_automatic_sharding_key() -> Option<String> {
        None
    }

    pub fn default_default_role() -> String {
        "any".into()
    }

    pub fn default_sharding_function() -> ShardingFunction {
        ShardingFunction::PgBigintHash
    }

    pub fn default_cleanup_server_connections() -> bool {
        true
    }

    pub fn default_prepared_statements_cache_size() -> usize {
        0
    }

    pub fn validate(&mut self) -> Result<(), Error> {
        match self.default_role.as_ref() {
            "any" => (),
            "primary" => (),
            "replica" => (),
            other => {
                error!(
                    "Query router default_role must be 'primary', 'replica', or 'any', got: '{}'",
                    other
                );
                return Err(Error::BadConfig);
            }
        };

        for (shard_idx, shard) in &self.shards {
            match shard_idx.parse::<usize>() {
                Ok(_) => (),
                Err(_) => {
                    error!(
                        "Shard '{}' is not a valid number, shards must be numbered starting at 0",
                        shard_idx
                    );
                    return Err(Error::BadConfig);
                }
            };
            shard.validate()?;
        }

        for (option, name) in [
            (&self.shard_id_regex, "shard_id_regex"),
            (&self.sharding_key_regex, "sharding_key_regex"),
        ] {
            if let Some(regex) = option {
                if let Err(parse_err) = Regex::new(regex.as_str()) {
                    error!("{} is not a valid Regex: {}", name, parse_err);
                    return Err(Error::BadConfig);
                }
            }
        }

        if self.query_parser_read_write_splitting && !self.query_parser_enabled {
            error!(
                "query_parser_read_write_splitting is only valid when query_parser_enabled is true"
            );
            return Err(Error::BadConfig);
        }

        if self.plugins.is_some() && !self.query_parser_enabled {
            error!("plugins are only valid when query_parser_enabled is true");
            return Err(Error::BadConfig);
        }

        self.automatic_sharding_key = match &self.automatic_sharding_key {
            Some(key) => {
                // No quotes in the key so we don't have to compare quoted
                // to unquoted idents.
                let key = key.replace('\"', "");

                if key.split('.').count() != 2 {
                    error!(
                        "automatic_sharding_key '{}' must be fully qualified, e.g. t.{}`",
                        key, key
                    );
                    return Err(Error::BadConfig);
                }

                Some(key)
            }
            None => None,
        };

        if let DefaultShard::Shard(shard_number) = self.default_shard {
            if shard_number >= self.shards.len() {
                error!("Invalid shard {:?}", shard_number);
                return Err(Error::BadConfig);
            }
        }

        for user in self.users.values() {
            user.validate()?;
        }

        Ok(())
    }
}

impl Default for Pool {
    fn default() -> Pool {
        Pool {
            pool_mode: Self::default_pool_mode(),
            load_balancing_mode: Self::default_load_balancing_mode(),
            default_role: String::from("any"),
            query_parser_enabled: false,
            query_parser_max_length: None,
            query_parser_read_write_splitting: false,
            primary_reads_enabled: false,
            connect_timeout: None,
            idle_timeout: None,
            server_lifetime: None,
            sharding_function: ShardingFunction::PgBigintHash,
            automatic_sharding_key: None,
            sharding_key_regex: None,
            shard_id_regex: None,
            regex_search_limit: Some(1000),
            default_shard: Self::default_default_shard(),
            auth_query: None,
            auth_query_user: None,
            auth_query_password: None,
            cleanup_server_connections: true,
            log_client_parameter_status_changes: false,
            prepared_statements_cache_size: Self::default_prepared_statements_cache_size(),
            plugins: None,
            shards: BTreeMap::from([(String::from("1"), Shard::default())]),
            users: BTreeMap::default(),
        }
    }
}

#[derive(Clone, PartialEq, Serialize, Deserialize, Debug, Hash, Eq)]
pub struct ServerConfig {
    pub host: String,
    pub port: u16,
    pub role: Role,
}

// No Shard Specified handling.
#[derive(Debug, PartialEq, Clone, Eq, Hash, Copy)]
pub enum DefaultShard {
    Shard(usize),
    Random,
    RandomHealthy,
    Fail
}
impl Default for DefaultShard {
    fn default() -> Self {
        DefaultShard::Shard(0)
    }
}
impl serde::Serialize for DefaultShard {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        match self {
            DefaultShard::Shard(shard) => {
                serializer.serialize_str(&format!("shard_{}", &shard.to_string()))
            }
            DefaultShard::Random => serializer.serialize_str("random"),
            DefaultShard::RandomHealthy => serializer.serialize_str("random_healthy"),
            DefaultShard::Fail => serializer.serialize_str("fail"),
        }
    }
}
impl<'de> serde::Deserialize<'de> for DefaultShard {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        if let Some(s) = s.strip_prefix("shard_") {
            let shard = s.parse::<usize>().map_err(serde::de::Error::custom)?;
            return Ok(DefaultShard::Shard(shard));
        }

        match s.as_str() {
            "random" => Ok(DefaultShard::Random),
            "random_healthy" => Ok(DefaultShard::RandomHealthy),
            "fail" => Ok(DefaultShard::Fail),
            _ => Err(serde::de::Error::custom(
                "invalid value for no_shard_specified_behavior",
            )),
        }
    }
}

#[derive(Clone, PartialEq, Serialize, Deserialize, Debug, Hash, Eq)]
pub struct MirrorServerConfig {
    pub host: String,
    pub port: u16,
    pub mirroring_target_index: usize,
}

/// Shard configuration.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Hash, Eq)]
pub struct Shard {
    pub database: String,
    pub mirrors: Option<Vec<MirrorServerConfig>>,
    pub servers: Vec<ServerConfig>,
}

impl Shard {
    pub fn validate(&self) -> Result<(), Error> {
        // We use addresses as unique identifiers,
        // let's make sure they are unique in the config as well.
        let mut dup_check = HashSet::new();
        let mut primary_count = 0;

        if self.servers.is_empty() {
            error!("Shard {} has no servers configured", self.database);
            return Err(Error::BadConfig);
        }

        for server in &self.servers {
            dup_check.insert(server);

            // Check that we define only zero or one primary.
            if server.role == Role::Primary {
                primary_count += 1
            }
        }

        if primary_count > 1 {
            error!(
                "Shard {} has more than one primary configured",
                self.database
            );
            return Err(Error::BadConfig);
        }

        if dup_check.len() != self.servers.len() {
            error!("Shard {} contains duplicate server configs", self.database);
            return Err(Error::BadConfig);
        }

        Ok(())
    }
}

impl Default for Shard {
    fn default() -> Shard {
        Shard {
            database: String::from("postgres"),
            mirrors: None,
            servers: vec![ServerConfig {
                host: String::from("localhost"),
                port: 5432,
                role: Role::Primary,
            }],
        }
    }
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Default, Hash, Eq)]
pub struct Plugins {
    pub intercept: Option<Intercept>,
    pub table_access: Option<TableAccess>,
    pub query_logger: Option<QueryLogger>,
    pub prewarmer: Option<Prewarmer>,
}

pub trait Plugin {
    fn is_enabled(&self) -> bool;
}

impl std::fmt::Display for Plugins {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        fn is_enabled<T: Plugin>(arg: Option<&T>) -> bool {
            if let Some(arg) = arg {
                arg.is_enabled()
            } else {
                false
            }
        }
        write!(
            f,
            "interceptor: {}, table_access: {}, query_logger: {}, prewarmer: {}",
            is_enabled(self.intercept.as_ref()),
            is_enabled(self.table_access.as_ref()),
            is_enabled(self.query_logger.as_ref()),
            is_enabled(self.prewarmer.as_ref()),
        )
    }
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Default, Hash, Eq)]
pub struct Intercept {
    pub enabled: bool,
    pub queries: BTreeMap<String, Query>,
}

impl Plugin for Intercept {
    fn is_enabled(&self) -> bool {
        self.enabled
    }
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Default, Hash, Eq)]
pub struct TableAccess {
    pub enabled: bool,
    pub tables: Vec<String>,
}

impl Plugin for TableAccess {
    fn is_enabled(&self) -> bool {
        self.enabled
    }
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Default, Hash, Eq)]
pub struct QueryLogger {
    pub enabled: bool,
}

impl Plugin for QueryLogger {
    fn is_enabled(&self) -> bool {
        self.enabled
    }
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Default, Hash, Eq)]
pub struct Prewarmer {
    pub enabled: bool,
    pub queries: Vec<String>,
}

impl Plugin for Prewarmer {
    fn is_enabled(&self) -> bool {
        self.enabled
    }
}

impl Intercept {
    pub fn substitute(&mut self, db: &str, user: &str) {
        for (_, query) in self.queries.iter_mut() {
            query.substitute(db, user);
            query.query = query.query.to_ascii_lowercase();
        }
    }
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Default, Hash, Eq)]
pub struct Query {
    pub query: String,
    pub schema: Vec<Vec<String>>,
    pub result: Vec<Vec<String>>,
}

impl Query {
    #[allow(clippy::needless_range_loop)]
    pub fn substitute(&mut self, db: &str, user: &str) {
        for col in self.result.iter_mut() {
            for i in 0..col.len() {
                col[i] = col[i].replace("${USER}", user).replace("${DATABASE}", db);
            }
        }
    }
}

/// Configuration wrapper.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub struct Config {
    // Serializer maintains the order of fields in the struct
    // so we should always put simple fields before nested fields
    // in all serializable structs to avoid ValueAfterTable errors
    // These errors occur when the toml serializer is about to produce
    // ambiguous toml structure like the one below
    // [main]
    // field1_under_main = 1
    // field2_under_main = 2
    // [main.subconf]
    // field1_under_subconf = 1
    // field3_under_main = 3 # This field will be interpreted as being under subconf and not under main
    #[serde(default = "Config::default_path")]
    pub path: String,

    // General and global settings.
    pub general: General,

    // Plugins that should run in all pools.
    pub plugins: Option<Plugins>,

    // Connection pools.
    pub pools: HashMap<String, Pool>,
}

impl Config {
    pub fn is_auth_query_configured(&self) -> bool {
        self.pools
            .iter()
            .any(|(_name, pool)| pool.is_auth_query_configured())
    }

    pub fn default_path() -> String {
        String::from("pgcat.toml")
    }

    pub fn fill_up_auth_query_config(&mut self) {
        for (_name, pool) in self.pools.iter_mut() {
            if pool.auth_query.is_none() {
                pool.auth_query.clone_from(&self.general.auth_query);
            }

            if pool.auth_query_user.is_none() {
                pool.auth_query_user
                    .clone_from(&self.general.auth_query_user);
            }

            if pool.auth_query_password.is_none() {
                pool.auth_query_password
                    .clone_from(&self.general.auth_query_password);
            }
        }
    }
}

impl Default for Config {
    fn default() -> Config {
        Config {
            path: Self::default_path(),
            general: General::default(),
            plugins: None,
            pools: HashMap::default(),
        }
    }
}

impl From<&Config> for std::collections::HashMap<String, String> {
    fn from(config: &Config) -> HashMap<String, String> {
        let mut r: Vec<(String, String)> = config
            .pools
            .iter()
            .flat_map(|(pool_name, pool)| {
                [
                    (
                        format!("pools.{}.pool_mode", pool_name),
                        pool.pool_mode.to_string(),
                    ),
                    (
                        format!("pools.{}.load_balancing_mode", pool_name),
                        pool.load_balancing_mode.to_string(),
                    ),
                    (
                        format!("pools.{}.primary_reads_enabled", pool_name),
                        pool.primary_reads_enabled.to_string(),
                    ),
                    (
                        format!("pools.{}.query_parser_enabled", pool_name),
                        pool.query_parser_enabled.to_string(),
                    ),
                    (
                        format!("pools.{}.query_parser_max_length", pool_name),
                        match pool.query_parser_max_length {
                            Some(max_length) => max_length.to_string(),
                            None => String::from("unlimited"),
                        },
                    ),
                    (
                        format!("pools.{}.query_parser_read_write_splitting", pool_name),
                        pool.query_parser_read_write_splitting.to_string(),
                    ),
                    (
                        format!("pools.{}.default_role", pool_name),
                        pool.default_role.clone(),
                    ),
                    (
                        format!("pools.{}.sharding_function", pool_name),
                        pool.sharding_function.to_string(),
                    ),
                    (
                        format!("pools.{:?}.shard_count", pool_name),
                        pool.shards.len().to_string(),
                    ),
                    (
                        format!("pools.{:?}.users", pool_name),
                        pool.users
                            .values()
                            .map(|user| &user.username)
                            .cloned()
                            .collect::<Vec<String>>()
                            .join(", "),
                    ),
                ]
            })
            .collect();

        let mut static_settings = vec![
            ("host".to_string(), config.general.host.to_string()),
            ("port".to_string(), config.general.port.to_string()),
            (
                "prometheus_exporter_port".to_string(),
                config.general.prometheus_exporter_port.to_string(),
            ),
            (
                "connect_timeout".to_string(),
                config.general.connect_timeout.to_string(),
            ),
            (
                "idle_timeout".to_string(),
                config.general.idle_timeout.to_string(),
            ),
            (
                "healthcheck_timeout".to_string(),
                config.general.healthcheck_timeout.to_string(),
            ),
            (
                "shutdown_timeout".to_string(),
                config.general.shutdown_timeout.to_string(),
            ),
            (
                "healthcheck_delay".to_string(),
                config.general.healthcheck_delay.to_string(),
            ),
            ("ban_time".to_string(), config.general.ban_time.to_string()),
            (
                "idle_client_in_transaction_timeout".to_string(),
                config
                    .general
                    .idle_client_in_transaction_timeout
                    .to_string(),
            ),
        ];

        r.append(&mut static_settings);
        return r.iter().cloned().collect();
    }
}

impl Config {
    /// Print current configuration.
    pub fn show(&self) {
        info!("Config path: {}", self.path);
        info!("Ban time: {}s", self.general.ban_time);
        info!(
            "Idle client in transaction timeout: {}ms",
            self.general.idle_client_in_transaction_timeout
        );
        info!("Worker threads: {}", self.general.worker_threads);
        info!(
            "Healthcheck timeout: {}ms",
            self.general.healthcheck_timeout
        );
        info!("Connection timeout: {}ms", self.general.connect_timeout);
        info!("Idle timeout: {}ms", self.general.idle_timeout);
        info!(
            "Log client connections: {}",
            self.general.log_client_connections
        );
        info!(
            "Log client disconnections: {}",
            self.general.log_client_disconnections
        );
        info!("Shutdown timeout: {}ms", self.general.shutdown_timeout);
        info!("Healthcheck delay: {}ms", self.general.healthcheck_delay);
        info!(
            "Default max server lifetime: {}ms",
            self.general.server_lifetime
        );
        info!("Server round robin: {}", self.general.server_round_robin);
        match self.general.tls_certificate.clone() {
            Some(tls_certificate) => {
                info!("TLS certificate: {}", tls_certificate);

                if let Some(tls_private_key) = self.general.tls_private_key.clone() {
                    info!("TLS private key: {}", tls_private_key);
                    info!("TLS support is enabled");
                }
            }

            None => {
                info!("TLS support is disabled");
            }
        };
        info!("Server TLS enabled: {}", self.general.server_tls);
        info!(
            "Server TLS certificate verification: {}",
            self.general.verify_server_certificate
        );
        info!(
            "Plugins: {}",
            match self.plugins {
                Some(ref plugins) => plugins.to_string(),
                None => "not configured".into(),
            }
        );

        for (pool_name, pool_config) in &self.pools {
            // TODO: Make this output prettier (maybe a table?)
            info!(
                "[pool: {}] Maximum user connections: {}",
                pool_name,
                pool_config
                    .users
                    .values()
                    .map(|user_cfg| user_cfg.pool_size)
                    .sum::<u32>()
                    .to_string()
            );
            info!(
                "[pool: {}] Default pool mode: {}",
                pool_name,
                pool_config.pool_mode.to_string()
            );
            info!(
                "[pool: {}] Load Balancing mode: {:?}",
                pool_name, pool_config.load_balancing_mode
            );
            let connect_timeout = match pool_config.connect_timeout {
                Some(connect_timeout) => connect_timeout,
                None => self.general.connect_timeout,
            };
            info!(
                "[pool: {}] Connection timeout: {}ms",
                pool_name, connect_timeout
            );
            let idle_timeout = match pool_config.idle_timeout {
                Some(idle_timeout) => idle_timeout,
                None => self.general.idle_timeout,
            };
            info!("[pool: {}] Idle timeout: {}ms", pool_name, idle_timeout);
            info!(
                "[pool: {}] Sharding function: {}",
                pool_name,
                pool_config.sharding_function.to_string()
            );
            info!(
                "[pool: {}] Primary reads: {}",
                pool_name, pool_config.primary_reads_enabled
            );
            info!(
                "[pool: {}] Query router: {}",
                pool_name, pool_config.query_parser_enabled
            );

            info!(
                "[pool: {}] Query parser max length: {:?}",
                pool_name, pool_config.query_parser_max_length
            );
            info!(
                "[pool: {}] Infer role from query: {}",
                pool_name, pool_config.query_parser_read_write_splitting
            );
            info!(
                "[pool: {}] Number of shards: {}",
                pool_name,
                pool_config.shards.len()
            );
            info!(
                "[pool: {}] Number of users: {}",
                pool_name,
                pool_config.users.len()
            );
            info!(
                "[pool: {}] Max server lifetime: {}",
                pool_name,
                match pool_config.server_lifetime {
                    Some(server_lifetime) => format!("{}ms", server_lifetime),
                    None => "default".to_string(),
                }
            );
            info!(
                "[pool: {}] Cleanup server connections: {}",
                pool_name, pool_config.cleanup_server_connections
            );
            info!(
                "[pool: {}] Log client parameter status changes: {}",
                pool_name, pool_config.log_client_parameter_status_changes
            );
            info!(
                "[pool: {}] Prepared statements server cache size: {}",
                pool_name, pool_config.prepared_statements_cache_size
            );
            info!(
                "[pool: {}] Plugins: {}",
                pool_name,
                match pool_config.plugins {
                    Some(ref plugins) => plugins.to_string(),
                    None => "not configured".into(),
                }
            );

            for user in &pool_config.users {
                info!(
                    "[pool: {}][user: {}] Pool size: {}",
                    pool_name, user.1.username, user.1.pool_size,
                );
                info!(
                    "[pool: {}][user: {}] Minimum pool size: {}",
                    pool_name,
                    user.1.username,
                    user.1.min_pool_size.unwrap_or(0)
                );
                info!(
                    "[pool: {}][user: {}] Statement timeout: {}",
                    pool_name, user.1.username, user.1.statement_timeout
                );
                info!(
                    "[pool: {}][user: {}] Pool mode: {}",
                    pool_name,
                    user.1.username,
                    match user.1.pool_mode {
                        Some(pool_mode) => pool_mode.to_string(),
                        None => pool_config.pool_mode.to_string(),
                    }
                );
                info!(
                    "[pool: {}][user: {}] Max server lifetime: {}",
                    pool_name,
                    user.1.username,
                    match user.1.server_lifetime {
                        Some(server_lifetime) => format!("{}ms", server_lifetime),
                        None => "default".to_string(),
                    }
                );
                info!(
                    "[pool: {}][user: {}] Connection timeout: {}",
                    pool_name,
                    user.1.username,
                    match user.1.connect_timeout {
                        Some(connect_timeout) => format!("{}ms", connect_timeout),
                        None => "not set".to_string(),
                    }
                );
                info!(
                    "[pool: {}][user: {}] Idle timeout: {}",
                    pool_name,
                    user.1.username,
                    match user.1.idle_timeout {
                        Some(idle_timeout) => format!("{}ms", idle_timeout),
                        None => "not set".to_string(),
                    }
                );
            }
        }
    }

    pub fn validate(&mut self) -> Result<(), Error> {
        // Validation for auth_query feature
        if self.general.auth_query.is_some()
            && (self.general.auth_query_user.is_none()
                || self.general.auth_query_password.is_none())
        {
            error!(
                "If auth_query is specified, \
                you need to provide a value \
                for `auth_query_user`, \
                `auth_query_password`"
            );

            return Err(Error::BadConfig);
        }

        for (name, pool) in self.pools.iter() {
            if pool.auth_query.is_some()
                && (pool.auth_query_user.is_none() || pool.auth_query_password.is_none())
            {
                error!(
                    "Error in pool {{ {} }}. \
                    If auth_query is specified, you need \
                    to provide a value for `auth_query_user`, \
                    `auth_query_password`",
                    name
                );

                return Err(Error::BadConfig);
            }

            for (_name, user_data) in pool.users.iter() {
                if (pool.auth_query.is_none()
                    || pool.auth_query_password.is_none()
                    || pool.auth_query_user.is_none())
                    && user_data.password.is_none()
                {
                    error!(
                        "Error in pool {{ {} }}. \
                        You have to specify a user password \
                        for every pool if auth_query is not specified",
                        name
                    );

                    return Err(Error::BadConfig);
                }
            }
        }

        // Validate TLS!
        if let Some(tls_certificate) = self.general.tls_certificate.clone() {
            match load_certs(Path::new(&tls_certificate)) {
                Ok(_) => {
                    // Cert is okay, but what about the private key?
                    match self.general.tls_private_key.clone() {
                        Some(tls_private_key) => match load_keys(Path::new(&tls_private_key)) {
                            Ok(_) => (),
                            Err(err) => {
                                error!("tls_private_key is incorrectly configured: {:?}", err);
                                return Err(Error::BadConfig);
                            }
                        },

                        None => {
                            error!("tls_certificate is set, but the tls_private_key is not");
                            return Err(Error::BadConfig);
                        }
                    };
                }

                Err(err) => {
                    error!("tls_certificate is incorrectly configured: {:?}", err);
                    return Err(Error::BadConfig);
                }
            }
        };

        for pool in self.pools.values_mut() {
            pool.validate()?;
        }

        Ok(())
    }
}

/// Get a read-only instance of the configuration
/// from anywhere in the app.
/// ArcSwap makes this cheap and quick.
pub fn get_config() -> Config {
    (*(*CONFIG.load())).clone()
}

pub fn get_idle_client_in_transaction_timeout() -> u64 {
    CONFIG.load().general.idle_client_in_transaction_timeout
}

/// Parse the configuration file located at the path.
pub async fn parse(path: &str) -> Result<(), Error> {
    let mut contents = String::new();
    let mut file = match File::open(path).await {
        Ok(file) => file,
        Err(err) => {
            error!("Could not open '{}': {}", path, err.to_string());
            return Err(Error::BadConfig);
        }
    };

    match file.read_to_string(&mut contents).await {
        Ok(_) => (),
        Err(err) => {
            error!("Could not read config file: {}", err.to_string());
            return Err(Error::BadConfig);
        }
    };

    let mut config: Config = match toml::from_str(&contents) {
        Ok(config) => config,
        Err(err) => {
            error!("Could not parse config file: {}", err.to_string());
            return Err(Error::BadConfig);
        }
    };

    config.fill_up_auth_query_config();
    config.validate()?;

    config.path = path.to_string();

    // Update the configuration globally.
    CONFIG.store(Arc::new(config.clone()));

    Ok(())
}

pub async fn reload_config(client_server_map: ClientServerMap) -> Result<bool, Error> {
    let old_config = get_config();

    match parse(&old_config.path).await {
        Ok(()) => (),
        Err(err) => {
            error!("Config reload error: {:?}", err);
            return Err(Error::BadConfig);
        }
    };

    let new_config = get_config();

    match CachedResolver::from_config().await {
        Ok(_) => (),
        Err(err) => error!("DNS cache reinitialization error: {:?}", err),
    };

    if old_config != new_config {
        info!("Config changed, reloading");
        ConnectionPool::from_config(client_server_map).await?;
        Ok(true)
    } else {
        Ok(false)
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[tokio::test]
    async fn test_config() {
        parse("pgcat.toml").await.unwrap();

        assert_eq!(get_config().path, "pgcat.toml".to_string());

        assert_eq!(get_config().general.ban_time, 60);
        assert_eq!(get_config().general.idle_client_in_transaction_timeout, 0);
        assert_eq!(get_config().general.idle_timeout, 30000);
        assert_eq!(get_config().pools.len(), 2);
        assert_eq!(get_config().pools["sharded_db"].shards.len(), 3);
        assert_eq!(get_config().pools["sharded_db"].idle_timeout, Some(40000));
        assert_eq!(get_config().pools["simple_db"].shards.len(), 1);
        assert_eq!(get_config().pools["sharded_db"].users.len(), 2);
        assert_eq!(get_config().pools["simple_db"].users.len(), 1);

        assert_eq!(
            get_config().pools["sharded_db"].shards["0"].servers[0].host,
            "127.0.0.1"
        );
        assert_eq!(
            get_config().pools["sharded_db"].shards["1"].servers[0].role,
            Role::Primary
        );
        assert_eq!(
            get_config().pools["sharded_db"].shards["1"].database,
            "shard1"
        );
        assert_eq!(
            get_config().pools["sharded_db"].users["0"].username,
            "sharding_user"
        );
        assert_eq!(
            get_config().pools["sharded_db"].users["1"]
                .password
                .as_ref()
                .unwrap(),
            "other_user"
        );
        assert_eq!(get_config().pools["sharded_db"].users["1"].pool_size, 21);
        assert_eq!(get_config().pools["sharded_db"].default_role, "any");

        assert_eq!(
            get_config().pools["simple_db"].shards["0"].servers[0].host,
            "127.0.0.1"
        );
        assert_eq!(
            get_config().pools["simple_db"].shards["0"].servers[0].port,
            5432
        );
        assert_eq!(
            get_config().pools["simple_db"].shards["0"].database,
            "some_db"
        );
        assert_eq!(get_config().pools["simple_db"].default_role, "primary");

        assert_eq!(
            get_config().pools["simple_db"].users["0"].username,
            "simple_user"
        );
        assert_eq!(
            get_config().pools["simple_db"].users["0"]
                .password
                .as_ref()
                .unwrap(),
            "simple_user"
        );
        assert_eq!(get_config().pools["simple_db"].users["0"].pool_size, 5);
        assert_eq!(get_config().general.auth_query, None);
        assert_eq!(get_config().general.auth_query_user, None);
        assert_eq!(get_config().general.auth_query_password, None);
    }

    #[tokio::test]
    async fn test_serialize_configs() {
        parse("pgcat.toml").await.unwrap();
        print!("{}", toml::to_string(&get_config()).unwrap());
    }
}
