use arc_swap::ArcSwap;
use async_trait::async_trait;
use bb8::{ManageConnection, Pool, PooledConnection};
use bytes::BytesMut;
use chrono::naive::NaiveDateTime;
use log::{debug, error, info, warn};
use once_cell::sync::Lazy;
use parking_lot::{Mutex, RwLock};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;

use crate::config::{get_config, Address, Role, Shard, User};
use crate::errors::Error;

use crate::server::Server;
use crate::stats::{get_reporter, Reporter};

pub type BanList = Arc<RwLock<Vec<HashMap<Address, NaiveDateTime>>>>;
pub type ClientServerMap = Arc<Mutex<HashMap<(i32, i32), (i32, i32, String, String)>>>;
pub type PoolMap = HashMap<(String, String), ConnectionPool>;
/// The connection pool, globally available.
/// This is atomic and safe and read-optimized.
/// The pool is recreated dynamically when the config is reloaded.
pub static POOLS: Lazy<ArcSwap<PoolMap>> = Lazy::new(|| ArcSwap::from_pointee(HashMap::default()));

#[derive(Clone, Debug)]
pub struct PoolSettings {
    pub pool_mode: String,
    pub shards: HashMap<String, Shard>,
    pub user: User,
    pub default_role: String,
    pub query_parser_enabled: bool,
    pub primary_reads_enabled: bool,
    pub sharding_function: String,
}
impl Default for PoolSettings {
    fn default() -> PoolSettings {
        PoolSettings {
            pool_mode: String::from("transaction"),
            shards: HashMap::from([(String::from("1"), Shard::default())]),
            user: User::default(),
            default_role: String::from("any"),
            query_parser_enabled: false,
            primary_reads_enabled: true,
            sharding_function: "pg_bigint_hash".to_string(),
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

    pub settings: PoolSettings,
}

impl ConnectionPool {
    /// Construct the connection pool from the configuration.
    pub async fn from_config(client_server_map: ClientServerMap) -> Result<(), Error> {
        let config = get_config();
        let mut new_pools = PoolMap::default();

        let mut address_id = 0;
        for (pool_name, pool_config) in &config.pools {
            for (_user_index, user_info) in &pool_config.users {
                let mut shards = Vec::new();
                let mut addresses = Vec::new();
                let mut banlist = Vec::new();
                let mut shard_ids = pool_config
                    .shards
                    .clone()
                    .into_keys()
                    .map(|x| x.to_string())
                    .collect::<Vec<String>>();

                // Sort by shard number to ensure consistency.
                shard_ids.sort_by_key(|k| k.parse::<i64>().unwrap());

                for shard_idx in shard_ids {
                    let shard = &pool_config.shards[&shard_idx];
                    let mut pools = Vec::new();
                    let mut servers = Vec::new();
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
                            port: server.1.to_string(),
                            role: role,
                            replica_number,
                            shard: shard_idx.parse::<usize>().unwrap(),
                            username: user_info.username.clone(),
                            poolname: pool_name.clone(),
                        };

                        address_id += 1;

                        if role == Role::Replica {
                            replica_number += 1;
                        }

                        let manager = ServerPool::new(
                            address.clone(),
                            user_info.clone(),
                            &shard.database,
                            client_server_map.clone(),
                            get_reporter(),
                        );

                        let pool = Pool::builder()
                            .max_size(user_info.pool_size)
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
                    banlist.push(HashMap::new());
                }

                assert_eq!(shards.len(), addresses.len());

                let mut pool = ConnectionPool {
                    databases: shards,
                    addresses: addresses,
                    banlist: Arc::new(RwLock::new(banlist)),
                    stats: get_reporter(),
                    server_info: BytesMut::new(),
                    settings: PoolSettings {
                        pool_mode: pool_config.pool_mode.clone(),
                        shards: pool_config.shards.clone(),
                        user: user_info.clone(),
                        default_role: pool_config.default_role.clone(),
                        query_parser_enabled: pool_config.query_parser_enabled.clone(),
                        primary_reads_enabled: pool_config.primary_reads_enabled,
                        sharding_function: pool_config.sharding_function.clone(),
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
                new_pools.insert((pool_name.clone(), user_info.username.clone()), pool);
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
            for index in 0..self.servers(shard) {
                let connection = match self.databases[shard][index].get().await {
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
        let addresses = &self.addresses[shard];

        let mut allowed_attempts = match role {
            // Primary-specific queries get one attempt, if the primary is down,
            // nothing we should do about it I think. It's dangerous to retry
            // write queries.
            Some(Role::Primary) => 1,

            // Replicas get to try as many times as there are replicas
            // and connections in the pool.
            _ => addresses.len(),
        };

        debug!("Allowed attempts for {:?}: {}", role, allowed_attempts);

        let exists = match role {
            Some(role) => addresses.iter().filter(|addr| addr.role == role).count() > 0,
            None => true,
        };

        if !exists {
            error!("Requested role {:?}, but none are configured", role);
            return Err(Error::BadConfig);
        }

        let healthcheck_timeout = get_config().general.healthcheck_timeout;
        let healthcheck_delay = get_config().general.healthcheck_delay as u128;

        while allowed_attempts > 0 {
            let index = rand::random::<usize>() % addresses.len();
            let address = &addresses[index];

            // Make sure you're getting a primary or a replica
            // as per request. If no specific role is requested, the first
            // available will be chosen.
            if address.role != role {
                continue;
            }

            // Don't attempt to connect to banned servers.
            if self.is_banned(address, shard, role) {
                continue;
            }

            allowed_attempts -= 1;

            // Indicate we're waiting on a server connection from a pool.
            self.stats.client_waiting(process_id, address.id);

            // Check if we can connect
            let mut conn = match self.databases[shard][index].get().await {
                Ok(conn) => conn,
                Err(err) => {
                    error!("Banning replica {}, error: {:?}", index, err);
                    self.ban(address, shard, process_id);
                    self.stats.client_disconnecting(process_id, address.id);
                    self.stats
                        .checkout_time(now.elapsed().as_micros(), process_id, address.id);
                    continue;
                }
            };

            // // Check if this server is alive with a health check.
            let server = &mut *conn;

            // Will return error if timestamp is greater than current system time, which it should never be set to
            let require_healthcheck =
                server.last_activity().elapsed().unwrap().as_millis() > healthcheck_delay;

            if !require_healthcheck {
                self.stats
                    .checkout_time(now.elapsed().as_micros(), process_id, address.id);
                self.stats.server_active(conn.process_id(), address.id);
                return Ok((conn, address.clone()));
            }

            debug!("Running health check on server {:?}", address);

            self.stats.server_tested(server.process_id(), address.id);

            match tokio::time::timeout(
                tokio::time::Duration::from_millis(healthcheck_timeout),
                server.query(";"),
            )
            .await
            {
                // Check if health check succeeded.
                Ok(res) => match res {
                    Ok(_) => {
                        self.stats
                            .checkout_time(now.elapsed().as_micros(), process_id, address.id);
                        self.stats.server_active(conn.process_id(), address.id);
                        return Ok((conn, address.clone()));
                    }

                    // Health check failed.
                    Err(_) => {
                        error!("Banning replica {} because of failed health check", index);

                        // Don't leave a bad connection in the pool.
                        server.mark_bad();

                        self.ban(address, shard, process_id);
                        continue;
                    }
                },

                // Health check timed out.
                Err(_) => {
                    error!("Banning replica {} because of health check timeout", index);
                    // Don't leave a bad connection in the pool.
                    server.mark_bad();

                    self.ban(address, shard, process_id);
                    continue;
                }
            }
        }

        return Err(Error::AllServersDown);
    }

    /// Ban an address (i.e. replica). It no longer will serve
    /// traffic for any new transactions. Existing transactions on that replica
    /// will finish successfully or error out to the clients.
    pub fn ban(&self, address: &Address, shard: usize, process_id: i32) {
        self.stats.client_disconnecting(process_id, address.id);
        self.stats
            .checkout_time(Instant::now().elapsed().as_micros(), process_id, address.id);

        error!("Banning {:?}", address);
        let now = chrono::offset::Utc::now().naive_utc();
        let mut guard = self.banlist.write();
        guard[shard].insert(address.clone(), now);
    }

    /// Clear the replica to receive traffic again. Takes effect immediately
    /// for all new transactions.
    pub fn _unban(&self, address: &Address, shard: usize) {
        let mut guard = self.banlist.write();
        guard[shard].remove(address);
    }

    /// Check if a replica can serve traffic. If all replicas are banned,
    /// we unban all of them. Better to try then not to.
    pub fn is_banned(&self, address: &Address, shard: usize, role: Option<Role>) -> bool {
        let replicas_available = match role {
            Some(Role::Replica) => self.addresses[shard]
                .iter()
                .filter(|addr| addr.role == Role::Replica)
                .count(),
            None => self.addresses[shard].len(),
            Some(Role::Primary) => return false, // Primary cannot be banned.
        };

        debug!("Available targets for {:?}: {}", role, replicas_available);

        let guard = self.banlist.read();

        // Everything is banned = nothing is banned.
        if guard[shard].len() == replicas_available {
            drop(guard);
            let mut guard = self.banlist.write();
            guard[shard].clear();
            drop(guard);
            warn!("Unbanning all replicas.");
            return false;
        }

        // I expect this to miss 99.9999% of the time.
        match guard[shard].get(address) {
            Some(timestamp) => {
                let now = chrono::offset::Utc::now().naive_utc();
                let config = get_config();

                // Ban expired.
                if now.timestamp() - timestamp.timestamp() > config.general.ban_time {
                    drop(guard);
                    warn!("Unbanning {:?}", address);
                    let mut guard = self.banlist.write();
                    guard[shard].remove(address);
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
        info!(
            "Creating a new connection to {:?} using user {:?}",
            self.address.name(),
            self.user.username
        );

        // Put a temporary process_id into the stats
        // for server login.
        let process_id = rand::random::<i32>();
        self.stats.server_login(process_id, self.address.id);

        // Connect to the PostgreSQL server.
        match Server::startup(
            &self.address,
            &self.user,
            &self.database,
            self.client_server_map.clone(),
            self.stats.clone(),
        )
        .await
        {
            Ok(conn) => {
                // Remove the temporary process_id from the stats.
                self.stats.server_disconnecting(process_id, self.address.id);
                Ok(conn)
            }
            Err(err) => {
                // Remove the temporary process_id from the stats.
                self.stats.server_disconnecting(process_id, self.address.id);
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

pub fn get_number_of_addresses() -> usize {
    get_all_pools()
        .iter()
        .map(|(_, pool)| pool.databases())
        .sum()
}

pub fn get_all_pools() -> HashMap<(String, String), ConnectionPool> {
    return (*(*POOLS.load())).clone();
}

mod test {
    extern crate postgres;

    use super::super::config::Config;
    use super::super::config::Pool;
    use super::super::config::Shard;
    use super::super::config::User;

    use core::time;
    use log::error;
    use postgres::{Client, NoTls};
    use rand::{distributions::Alphanumeric, Rng};
    use std::{
        collections::HashMap,
        fs,
        io::Write,
        process::{Child, Command, Stdio},
        thread::sleep,
    };

    pub const BASE_CONFIG: &str = "./pgcat.toml";

    pub struct PgcatInstance {
        process: Child,
        port: i16,
        log_file: String,
        config_filename: String,
        last_query_count: usize,
    }
    impl PgcatInstance {
        pub fn start(log_level: log::Level, config: Option<Config>) -> PgcatInstance {
            let base_filename: String = rand::thread_rng()
                .sample_iter(&Alphanumeric)
                .take(7)
                .map(char::from)
                .collect();
            let config_filename = format!("/tmp/pgcat_{}.temp.toml", base_filename);
            let log_filename = format!("/tmp/pgcat_{}.out", base_filename);

            let file = fs::File::create(log_filename.clone()).unwrap();
            let stdio = Stdio::from(file);

            let port = match config {
                Some(mut cfg) => {
                    let mut file = fs::File::create(config_filename.clone()).unwrap();
                    cfg.path = config_filename.clone();
                    file.write_all(toml::to_string(&cfg).unwrap().as_bytes())
                        .unwrap();
                    cfg.general.port
                }
                None => {
                    fs::copy(BASE_CONFIG, config_filename.clone()).unwrap();
                    let mut cfg = toml::from_str::<Config>(
                        &fs::read_to_string(config_filename.clone()).unwrap(),
                    )
                    .unwrap();
                    cfg.path = config_filename.clone();
                    let mut file = fs::File::create(config_filename.clone()).unwrap();
                    file.write_all(toml::to_string(&cfg).unwrap().as_bytes())
                        .unwrap();
                    cfg.general.port
                }
            };

            let child = Command::new("./target/debug/pgcat")
                .env("RUST_LOG", log_level.as_str())
                .arg(config_filename.clone())
                .stderr(stdio)
                .spawn()
                .unwrap();
            println!(
                "Started pgcat instance PID({:?}), LogFile({:?}), ConfigFile({:?})",
                child.id(),
                log_filename,
                config_filename
            );

            let instance = PgcatInstance {
                process: child,
                port: port,
                log_file: log_filename,
                config_filename: config_filename.clone(),
                last_query_count: 0,
            };
            instance.wait_until_ready();
            return instance;
        }

        pub fn current_config(&self) -> Config {
            return toml::from_str(
                fs::read_to_string(self.config_filename.clone())
                    .unwrap()
                    .as_str(),
            )
            .unwrap();
        }

        pub fn begin_recording_query_count(&mut self) {
            self.last_query_count = self.get_logs().matches("Sending query").count();
        }

        pub fn end_recording_query_count(&mut self) -> usize {
            return self.get_logs().matches("Sending query").count() - self.last_query_count;
        }

        pub fn get_logs(&self) -> String {
            fs::read_to_string(self.log_file.clone()).unwrap()
        }

        pub fn update_config(&self, new_config: Config) {
            let mut file = fs::File::create(self.config_filename.clone()).unwrap();
            file.write_all(toml::to_string(&new_config).unwrap().as_bytes())
                .unwrap();
        }

        pub fn reload_config(&self) {
            Command::new("kill")
                .args(["-s", "HUP", self.process.id().to_string().as_str()])
                .spawn()
                .unwrap()
                .wait()
                .unwrap();
        }

        pub fn stop(&mut self) {
            match self.process.kill() {
                Ok(_) => {
                    self.process.try_wait().unwrap();
                    ()
                }
                Err(err) => {
                    error!("Failed to kill process {:?}, {:?}", self.process.id(), err);
                    ()
                }
            }
        }

        pub fn wait_until_ready(&self) {
            let conn_str = format!(
                "postgres://sharding_user:sharding_user@localhost:{}/shard0",
                self.port
            );
            for _ in 0..10 {
                if Client::connect(&conn_str, NoTls).is_ok() {
                    return;
                }
                sleep(time::Duration::from_millis(500));
            }
            panic!("Server was never ready!");
        }
    }
    impl Drop for PgcatInstance {
        fn drop(&mut self) {
            self.stop();
            //fs::remove_file(self.config_filename.clone()).unwrap();
            //fs::remove_file(self.log_file.clone()).unwrap();
        }
    }

    pub fn single_shard_setup(
        num_replica: u8,
        db: String,
        base_config: Option<Config>,
    ) -> (PgcatInstance, Vec<PgcatInstance>) {
        let mut rng = rand::thread_rng();
        let mut pgclowder = vec![];
        let mut main_server_entries = vec![];
        let primary_pg_address = (
            String::from("localhost"),
            5432 as u16,
            String::from("primary"),
        );
        let replica_pg_address = (
            String::from("localhost"),
            5432 as u16,
            String::from("replica"),
        );

        let mut base_cfg = match base_config {
            Some(cfg) => cfg,
            None => Config::default(),
        };
        let user = User {
            username: String::from("sharding_user"),
            password: String::from("sharding_user"),
            pool_size: 5,
            statement_timeout: 100000,
        };
        base_cfg.pools.insert(db.clone(), Pool::default());
        base_cfg
            .pools
            .get_mut(&db.clone())
            .unwrap()
            .users
            .insert(String::from("sharding_user"), user);

        for i in 0..num_replica + 1 {
            let mut cfg = base_cfg.clone();
            let port = rng.gen_range(10000..32000);

            cfg.general.port = port;
            let (local_address, main_address) = match i {
                0 => (
                    primary_pg_address.clone(),
                    (
                        String::from("localhost"),
                        cfg.general.port as u16,
                        String::from("primary"),
                    ),
                ),
                _ => (
                    replica_pg_address.clone(),
                    (
                        String::from("localhost"),
                        cfg.general.port as u16,
                        String::from("replica"),
                    ),
                ),
            };

            cfg.pools.get_mut(&db.clone()).unwrap().shards = HashMap::from([(
                String::from("0"),
                Shard {
                    database: db.clone(),
                    servers: vec![local_address],
                },
            )]);
            main_server_entries.push(main_address);
            pgclowder.push(PgcatInstance::start(log::Level::Debug, Some(cfg)))
        }
        let mut main_cfg = base_cfg.clone();
        main_cfg.general.port = rng.gen_range(10000..32000);
        main_cfg.pools.get_mut(&db.clone()).unwrap().shards = HashMap::from([(
            String::from("0"),
            Shard {
                database: String::from("shard0"),
                // Server entries point to other Pgcat processes
                servers: main_server_entries,
            },
        )]);
        return (
            PgcatInstance::start(log::Level::Trace, Some(main_cfg)),
            pgclowder,
        );
    }

    fn value_within_range(target_value: i32, value: i32) {
        if value == target_value {
            return;
        }
        // Allow a buffer of 15% around the target value
        let buffer = ((target_value as f32) * 0.15) as i32;
        let start = target_value - buffer;
        let end = target_value + buffer;
        let result = if (start..end).contains(&value) {
            "Pass"
        } else {
            "Fail"
        };

        println!(
            "Expecting {} to fall between {} and {} ... {}",
            value, start, end, result
        );
        assert!((start..end).contains(&value));
    }

    #[test]
    fn test_basic_load_balancing() {
        let (main_pgcat, mut proxy_instances) = single_shard_setup(3, String::from("shard0"), None);

        let conn_str = format!(
            "postgres://sharding_user:sharding_user@localhost:{}/shard0",
            main_pgcat.port
        );
        let mut client = Client::connect(&conn_str, NoTls).unwrap();

        for proxy in proxy_instances.iter_mut() {
            proxy.begin_recording_query_count();
        }
        let total_count = 5000;
        for _ in 0..total_count {
            if client.simple_query("SELECT 1").is_err() {
                client = Client::connect(&conn_str, NoTls).unwrap();
                continue;
            }
        }

        let expected_share = total_count / 4;
        for proxy in proxy_instances.iter_mut() {
            value_within_range(expected_share, proxy.end_recording_query_count() as i32);
        }
    }

    #[test]
    fn test_failover_load_balancing() {
        let (main_pgcat, mut proxy_instances) =
            single_shard_setup(3, String::from("shard0"), None);

        let mut replica2 = proxy_instances.pop().unwrap();
        let mut replica1 = proxy_instances.pop().unwrap();
        let mut replica0 = proxy_instances.pop().unwrap();
        let mut primary = proxy_instances.pop().unwrap();

        primary.begin_recording_query_count();
        replica0.begin_recording_query_count();
        replica1.begin_recording_query_count();
        replica2.begin_recording_query_count();

        replica1.stop();

        let conn_str = format!(
            "postgres://sharding_user:sharding_user@localhost:{}/shard0",
            main_pgcat.port
        );
        let total_count = 2000;
        let mut client = Client::connect(&conn_str, NoTls).unwrap();
        for _ in 0..total_count {
            if client.simple_query("SELECT 1").is_err() {
                client = Client::connect(&conn_str, NoTls).unwrap();
                continue;
            }
        }
        let expected_share = total_count / 3;

        value_within_range(expected_share, primary.end_recording_query_count() as i32);
        value_within_range(expected_share, replica0.end_recording_query_count() as i32);
        value_within_range(0, replica1.end_recording_query_count() as i32);
        value_within_range(expected_share, replica2.end_recording_query_count() as i32);
    }

    #[test]
    fn test_load_balancing_with_query_routing() {
        let (main_pgcat, mut proxy_instances) = single_shard_setup(3, String::from("shard0"), None);
        let mut cfg = main_pgcat.current_config();
        cfg.pools.get_mut("shard0").unwrap().primary_reads_enabled = false;
        cfg.pools.get_mut("shard0").unwrap().query_parser_enabled = true;
        cfg.pools.get_mut("shard0").unwrap().default_role = String::from("auto");
        main_pgcat.update_config(cfg);
        main_pgcat.reload_config();

        let mut replica2 = proxy_instances.pop().unwrap();
        let mut replica1 = proxy_instances.pop().unwrap();
        let mut replica0 = proxy_instances.pop().unwrap();
        let mut primary = proxy_instances.pop().unwrap();

        primary.begin_recording_query_count();
        replica0.begin_recording_query_count();
        replica1.begin_recording_query_count();
        replica2.begin_recording_query_count();

        let conn_str = format!(
            "postgres://sharding_user:sharding_user@localhost:{}/shard0",
            main_pgcat.port
        );
        let mut client = Client::connect(&conn_str, NoTls).unwrap();
        client.simple_query("SET SERVER ROLE TO 'auto'").unwrap();

        let total_count = 2000;
        for _ in 0..total_count {
            if client.simple_query("SELECT 1").is_err() {
                client = Client::connect(&conn_str, NoTls).unwrap();
                continue;
            }
        }
        let expected_share = total_count / 3;
        value_within_range(expected_share, replica0.end_recording_query_count() as i32);
        value_within_range(expected_share, replica1.end_recording_query_count() as i32);
        value_within_range(expected_share, replica2.end_recording_query_count() as i32);
        value_within_range(0, primary.end_recording_query_count() as i32);
    }
}
