use arc_swap::ArcSwap;
use async_trait::async_trait;
use bb8::{ManageConnection, Pool, PooledConnection};
use bytes::BytesMut;
use log::{debug, error, info, warn};
use once_cell::sync::Lazy;
use parking_lot::Mutex;
use rand::seq::SliceRandom;
use rand::thread_rng;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;

use crate::bans::{self, BanReason, BanReporter};
use crate::config::{get_config, Address, Role, User};
use crate::errors::Error;

use crate::server::Server;
use crate::sharding::ShardingFunction;
use crate::stats::{get_reporter, Reporter};

pub type ClientServerMap = Arc<Mutex<HashMap<(i32, i32), (i32, i32, String, u16)>>>;
pub type PoolMap = HashMap<(String, String), ConnectionPool>;
/// The connection pool, globally available.
/// This is atomic and safe and read-optimized.
/// The pool is recreated dynamically when the config is reloaded.
pub static POOLS: Lazy<ArcSwap<PoolMap>> = Lazy::new(|| ArcSwap::from_pointee(HashMap::default()));
/// Pool mode:
/// - transaction: server serves one transaction,
/// - session: server is attached to the client.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum PoolMode {
    Session,
    Transaction,
}

impl std::fmt::Display for PoolMode {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match *self {
            PoolMode::Session => write!(f, "session"),
            PoolMode::Transaction => write!(f, "transaction"),
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

    pub name: String,

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
}

impl Default for PoolSettings {
    fn default() -> PoolSettings {
        PoolSettings {
            pool_mode: PoolMode::Transaction,
            shards: 1,
            user: User::default(),
            name: String::default(),
            default_role: None,
            query_parser_enabled: false,
            primary_reads_enabled: true,
            sharding_function: ShardingFunction::PgBigintHash,
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
    ban_reporter: BanReporter,

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

        for (pool_name, pool_config) in &config.pools {
            // There is one pool per database/user pair.
            for (_, user) in &pool_config.users {
                let mut shards = Vec::new();
                let mut addresses = Vec::new();
                let mut shard_ids = pool_config
                    .shards
                    .clone()
                    .into_keys()
                    .map(|x| x.to_string())
                    .collect::<Vec<String>>();

                // Sort by shard number to ensure consistency.
                shard_ids.sort_by_key(|k| k.parse::<i64>().unwrap());

                for shard_idx in &shard_ids {
                    let shard = &pool_config.shards[shard_idx];
                    let mut pools = Vec::new();
                    let mut servers = Vec::new();
                    let mut address_index = 0;
                    let mut replica_number = 0;

                    for server in shard.servers.iter() {
                        let role = match server.2.as_ref() {
                            "primary" => Role::Primary,
                            "replica" => Role::Replica,
                            _ => {
                                error!("Config error: server role can be 'primary' or 'replica', have: '{}'. Defaulting to 'replica'.", server.2);
                                Role::Replica
                            }
                        };

                        let address = Address {
                            id: address_id,
                            database: shard.database.clone(),
                            host: server.0.clone(),
                            port: server.1 as u16,
                            role: role,
                            address_index,
                            replica_number,
                            shard: shard_idx.parse::<usize>().unwrap(),
                            username: user.username.clone(),
                            pool_name: pool_name.clone(),
                        };

                        address_id += 1;
                        address_index += 1;

                        if role == Role::Replica {
                            replica_number += 1;
                        }

                        let manager = ServerPool::new(
                            address.clone(),
                            user.clone(),
                            &shard.database,
                            client_server_map.clone(),
                            get_reporter(),
                        );

                        let pool = Pool::builder()
                            .max_size(user.pool_size)
                            .connection_timeout(std::time::Duration::from_millis(
                                config.general.connect_timeout,
                            ))
                            .test_on_check_out(false)
                            .build(manager)
                            .await
                            .unwrap();

                        pools.push(pool);
                        servers.push(address);
                    }

                    shards.push(pools);
                    addresses.push(servers);
                }

                assert_eq!(shards.len(), addresses.len());

                let mut pool = ConnectionPool {
                    databases: shards,
                    addresses: addresses,
                    ban_reporter: bans::get_ban_handler(),
                    stats: get_reporter(),
                    server_info: BytesMut::new(),
                    settings: PoolSettings {
                        name: pool_name.clone(),
                        pool_mode: match pool_config.pool_mode.as_str() {
                            "transaction" => PoolMode::Transaction,
                            "session" => PoolMode::Session,
                            _ => unreachable!(),
                        },
                        // shards: pool_config.shards.clone(),
                        shards: shard_ids.len(),
                        user: user.clone(),
                        default_role: match pool_config.default_role.as_str() {
                            "any" => None,
                            "replica" => Some(Role::Replica),
                            "primary" => Some(Role::Primary),
                            _ => unreachable!(),
                        },
                        query_parser_enabled: pool_config.query_parser_enabled.clone(),
                        primary_reads_enabled: pool_config.primary_reads_enabled,
                        sharding_function: match pool_config.sharding_function.as_str() {
                            "pg_bigint_hash" => ShardingFunction::PgBigintHash,
                            "sha1" => ShardingFunction::Sha1,
                            _ => unreachable!(),
                        },
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
                new_pools.insert((pool_name.clone(), user.username.clone()), pool);
            }
        }

        POOLS.store(Arc::new(new_pools.clone()));

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

                if server_infos.len() > 0 {
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
        if server_infos.len() == 0 {
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
        let now = Instant::now();
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

            if self.is_banned(&address) {
                debug!("Address {:?} is banned", address);
                continue;
            }

            // Indicate we're waiting on a server connection from a pool.
            self.stats.client_waiting(process_id);

            // Check if we can connect
            let mut conn = match self.databases[address.shard][address.address_index]
                .get()
                .await
            {
                Ok(conn) => conn,
                Err(err) => {
                    error!("Banning instance {:?}, error: {:?}", address, err);
                    self.ban(&address, process_id, BanReason::FailedCheckout);
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
                server.query(";"), // Cheap query (query parser not used in PG)
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

                        self.ban(&address, process_id, BanReason::FailedHealthCheck);
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

                    self.ban(&address, process_id, BanReason::FailedHealthCheck);
                    continue;
                }
            }
        }

        Err(Error::AllServersDown)
    }

    /// Ban an address (i.e. replica). It no longer will serve
    /// traffic for any new transactions. If this call bans the last
    /// replica in a shard, we unban all replicas
    pub fn ban(&self, address: &Address, client_id: i32, reason: BanReason) {
        error!("Banning {:?}", address);
        self.stats.client_ban_error(client_id, address.id);

        // Primaries cannot be banned
        if address.role == Role::Primary {
            return;
        }

        // We check if banning this address will result in all replica being banned
        // If so, we unban all replicas instead
        let pool_banned_addresses = self.ban_reporter.banlist(
            self.settings.name.clone(),
            self.settings.user.username.clone(),
        );

        let unbanned_count = self.addresses[address.shard]
                                        .iter()
                                        .filter(|addr| addr.role == Role::Replica)
                                        .filter(|addr|
                                            // Return true if address is not banned
                                            match pool_banned_addresses.get(addr) {
                                                Some(ban_entry) => ban_entry.has_expired(),
                                                // We assume the address that is to be banned is already banned
                                                None => address != *addr,
                                            })
                                        .count();
        if unbanned_count == 0 {
            // All replicas are banned
            // Unban everything
            warn!("Unbanning all replicas.");
            self.addresses[address.shard]
                .iter()
                .filter(|addr| addr.role == Role::Replica)
                .for_each(|address| self.unban(address));
            return;
        }

        match reason {
            BanReason::FailedHealthCheck => self.ban_reporter.report_failed_healthcheck(
                self.settings.name.clone(),
                self.settings.user.username.clone(),
                address.clone(),
            ),
            BanReason::MessageSendFailed => self.ban_reporter.report_server_send_failed(
                self.settings.name.clone(),
                self.settings.user.username.clone(),
                address.clone(),
            ),
            BanReason::MessageReceiveFailed => self.ban_reporter.report_server_receive_failed(
                self.settings.name.clone(),
                self.settings.user.username.clone(),
                address.clone(),
            ),
            BanReason::StatementTimeout => self.ban_reporter.report_statement_timeout(
                self.settings.name.clone(),
                self.settings.user.username.clone(),
                address.clone(),
            ),
            BanReason::FailedCheckout => self.ban_reporter.report_failed_checkout(
                self.settings.name.clone(),
                self.settings.user.username.clone(),
                address.clone(),
            ),
            BanReason::ManualBan => unreachable!(),
        }
    }

    /// Clear the replica to receive traffic again. ban/unban operations
    /// are not synchronous but are typically very fast
    pub fn unban(&self, address: &Address) {
        self.ban_reporter.unban(
            self.settings.name.clone(),
            self.settings.user.username.clone(),
            address.clone(),
        );
    }

    /// Check if a replica can serve traffic.
    /// This is a hot codepath, called for each query during
    /// the routing phase, we should keep it as fast as possible
    pub fn is_banned(&self, address: &Address) -> bool {
        if address.role == Role::Primary {
            return false;
        }

        let pool_banned_addresses = self.ban_reporter.banlist(
            self.settings.name.clone(),
            self.settings.user.username.clone(),
        );
        if pool_banned_addresses.len() == 0 {
            // We should hit this branch most of the time
            return false;
        }

        return match pool_banned_addresses.get(address) {
            Some(ban_entry) => ban_entry.has_expired(),
            None => false,
        };
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
            address: address,
            user: user,
            database: database.to_string(),
            client_server_map: client_server_map,
            stats: stats,
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
    async fn is_valid(&self, _conn: &mut PooledConnection<'_, Self>) -> Result<(), Self::Error> {
        Ok(())
    }

    /// Synchronously determine if the connection is no longer usable, if possible.
    fn has_broken(&self, conn: &mut Self::Connection) -> bool {
        conn.is_bad()
    }
}

/// Get the connection pool
pub fn get_pool(db: String, user: String) -> Option<ConnectionPool> {
    match get_all_pools().get(&(db, user)) {
        Some(pool) => Some(pool.clone()),
        None => None,
    }
}

/// Get a pointer to all configured pools.
pub fn get_all_pools() -> HashMap<(String, String), ConnectionPool> {
    return (*(*POOLS.load())).clone();
}

/// How many total servers we have in the config.
pub fn get_number_of_addresses() -> usize {
    get_all_pools()
        .iter()
        .map(|(_, pool)| pool.databases())
        .sum()
}
