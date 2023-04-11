use super::AddressStats;
use super::PoolStats;
use super::{get_reporter, Reporter};
use crate::config::Address;
use atomic_enum::atomic_enum;
use parking_lot::RwLock;
use std::sync::atomic::*;
use std::sync::Arc;
use tokio::time::Instant;

/// The various states that a server can be in
#[atomic_enum]
#[derive(PartialEq)]
pub enum ServerState {
    Login = 0,
    Active,
    Tested,
    Idle,
}
impl std::fmt::Display for ServerState {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match *self {
            ServerState::Login => write!(f, "login"),
            ServerState::Active => write!(f, "active"),
            ServerState::Tested => write!(f, "tested"),
            ServerState::Idle => write!(f, "idle"),
        }
    }
}

/// Information we keep track of which can be queried by SHOW SERVERS
#[derive(Debug, Clone)]
pub struct ServerStats {
    /// A random integer assigned to the server and used by stats to track the server
    server_id: i32,

    /// Context information, only to be read
    address: Address,
    connect_time: Instant,

    pool_stats: Arc<PoolStats>,
    reporter: Reporter,

    /// Data
    pub application_name: Arc<RwLock<String>>,
    pub state: Arc<AtomicServerState>,
    pub bytes_sent: Arc<AtomicU64>,
    pub bytes_received: Arc<AtomicU64>,
    pub transaction_count: Arc<AtomicU64>,
    pub query_count: Arc<AtomicU64>,
    pub error_count: Arc<AtomicU64>,
}

impl Default for ServerStats {
    fn default() -> Self {
        ServerStats {
            server_id: 0,
            application_name: Arc::new(RwLock::new(String::new())),
            address: Address::default(),
            pool_stats: Arc::new(PoolStats::default()),
            connect_time: Instant::now(),
            state: Arc::new(AtomicServerState::new(ServerState::Login)),
            bytes_sent: Arc::new(AtomicU64::new(0)),
            bytes_received: Arc::new(AtomicU64::new(0)),
            transaction_count: Arc::new(AtomicU64::new(0)),
            query_count: Arc::new(AtomicU64::new(0)),
            error_count: Arc::new(AtomicU64::new(0)),
            reporter: get_reporter(),
        }
    }
}

impl ServerStats {
    pub fn new(address: Address, pool_stats: Arc<PoolStats>, connect_time: Instant) -> Self {
        Self {
            address,
            pool_stats,
            connect_time,
            server_id: rand::random::<i32>(),
            ..Default::default()
        }
    }

    pub fn server_id(&self) -> i32 {
        self.server_id
    }

    /// Register a server connection with the stats system. The stats system uses server_id
    /// to track and aggregate statistics from all source that relate to that server
    // Delegates to reporter
    pub fn register(&self, stats: Arc<ServerStats>) {
        self.reporter.server_register(self.server_id, stats);
        self.login();
    }

    /// Reports a server connection is no longer assigned to a client
    /// and is available for the next client to pick it up
    pub fn idle(&self) {
        self.pool_stats
            .server_idle(self.state.load(Ordering::Relaxed));

        self.state.store(ServerState::Idle, Ordering::Relaxed);
    }

    /// Reports a server connection is disconnecting from the pooler.
    /// Also updates metrics on the pool regarding server usage.
    pub fn disconnect(&self) {
        self.reporter.server_disconnecting(self.server_id);
        self.pool_stats
            .server_disconnect(self.state.load(Ordering::Relaxed))
    }

    /// Reports a server connection is being tested before being given to a client.
    pub fn tested(&self) {
        self.set_undefined_application();
        self.pool_stats
            .server_tested(self.state.load(Ordering::Relaxed));
        self.state.store(ServerState::Tested, Ordering::Relaxed);
    }

    /// Reports a server connection is attempting to login.
    pub fn login(&self) {
        self.pool_stats
            .server_login(self.state.load(Ordering::Relaxed));
        self.state.store(ServerState::Login, Ordering::Relaxed);
        self.set_undefined_application();
    }

    /// Reports a server connection has been assigned to a client that
    /// is about to query the server
    pub fn active(&self, application_name: String) {
        self.pool_stats
            .server_active(self.state.load(Ordering::Relaxed));
        self.state.store(ServerState::Active, Ordering::Relaxed);
        self.set_application(application_name);
    }

    pub fn address_stats(&self) -> Arc<AddressStats> {
        self.address.stats.clone()
    }

    // Helper methods for show_servers
    pub fn pool_name(&self) -> String {
        self.pool_stats.database()
    }

    pub fn username(&self) -> String {
        self.pool_stats.user()
    }

    pub fn address_name(&self) -> String {
        self.address.name()
    }

    pub fn connect_time(&self) -> Instant {
        self.connect_time
    }

    fn set_application(&self, name: String) {
        let mut application_name = self.application_name.write();
        *application_name = name;
    }

    fn set_undefined_application(&self) {
        self.set_application(String::from("Undefined"))
    }

    pub fn checkout_time(&self, microseconds: u64, application_name: String) {
        // Update server stats and address aggergation stats
        self.set_application(application_name);
        self.address
            .stats
            .total_wait_time
            .fetch_add(microseconds, Ordering::Relaxed);
        self.pool_stats
            .maxwait
            .fetch_max(microseconds, Ordering::Relaxed);
    }

    /// Report a query executed by a client against a server
    pub fn query(&self, milliseconds: u64, application_name: &str) {
        self.set_application(application_name.to_string());
        let address_stats = self.address_stats();
        address_stats
            .total_query_count
            .fetch_add(1, Ordering::Relaxed);
        address_stats
            .total_query_time
            .fetch_add(milliseconds, Ordering::Relaxed);
    }

    /// Report a transaction executed by a client a server
    /// we report each individual queries outside a transaction as a transaction
    /// We only count the initial BEGIN as a transaction, all queries within do not
    /// count as transactions
    pub fn transaction(&self, application_name: &str) {
        self.set_application(application_name.to_string());

        self.transaction_count.fetch_add(1, Ordering::Relaxed);
        self.address
            .stats
            .total_xact_count
            .fetch_add(1, Ordering::Relaxed);
    }

    /// Report data sent to a server
    pub fn data_sent(&self, amount_bytes: usize) {
        self.bytes_sent
            .fetch_add(amount_bytes as u64, Ordering::Relaxed);
        self.address
            .stats
            .total_sent
            .fetch_add(amount_bytes as u64, Ordering::Relaxed);
    }

    /// Report data received from a server
    pub fn data_received(&self, amount_bytes: usize) {
        self.bytes_received
            .fetch_add(amount_bytes as u64, Ordering::Relaxed);
        self.address
            .stats
            .total_received
            .fetch_add(amount_bytes as u64, Ordering::Relaxed);
    }
}
