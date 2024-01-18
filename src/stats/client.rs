use super::{get_reporter, Reporter};
use atomic_enum::atomic_enum;
use std::sync::atomic::*;
use std::sync::Arc;
use tokio::time::Instant;
/// The various states that a client can be in
#[atomic_enum]
#[derive(PartialEq)]
pub enum ClientState {
    Idle = 0,
    Waiting,
    Active,
}
impl std::fmt::Display for ClientState {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match *self {
            ClientState::Idle => write!(f, "idle"),
            ClientState::Waiting => write!(f, "waiting"),
            ClientState::Active => write!(f, "active"),
        }
    }
}

#[derive(Debug, Clone)]
/// Information we keep track of which can be queried by SHOW CLIENTS
pub struct ClientStats {
    /// A random integer assigned to the client and used by stats to track the client
    client_id: i32,

    /// Data associated with the client, not writable, only set when we construct the ClientStat
    application_name: String,
    username: String,
    pool_name: String,
    connect_time: Instant,

    reporter: Reporter,

    /// Total time spent waiting for a connection from pool, measures in microseconds
    pub total_wait_time: Arc<AtomicU64>,

    /// Maximum time spent waiting for a connection from pool, measures in microseconds
    pub max_wait_time: Arc<AtomicU64>,

    // Time when the client started waiting for a connection from pool, measures in microseconds
    // We use connect_time as the reference point for this value
    // U64 can represent ~5850 centuries in microseconds, so we should be fine
    pub wait_start_us: Arc<AtomicU64>,

    /// Current state of the client
    pub state: Arc<AtomicClientState>,

    /// Number of transactions executed by this client
    pub transaction_count: Arc<AtomicU64>,

    /// Number of queries executed by this client
    pub query_count: Arc<AtomicU64>,

    /// Number of errors made by this client
    pub error_count: Arc<AtomicU64>,
}

impl Default for ClientStats {
    fn default() -> Self {
        ClientStats {
            client_id: 0,
            connect_time: Instant::now(),
            application_name: String::new(),
            username: String::new(),
            pool_name: String::new(),
            total_wait_time: Arc::new(AtomicU64::new(0)),
            max_wait_time: Arc::new(AtomicU64::new(0)),
            wait_start_us: Arc::new(AtomicU64::new(0)),
            state: Arc::new(AtomicClientState::new(ClientState::Idle)),
            transaction_count: Arc::new(AtomicU64::new(0)),
            query_count: Arc::new(AtomicU64::new(0)),
            error_count: Arc::new(AtomicU64::new(0)),
            reporter: get_reporter(),
        }
    }
}

impl ClientStats {
    pub fn new(
        client_id: i32,
        application_name: &str,
        username: &str,
        pool_name: &str,
        connect_time: Instant,
    ) -> Self {
        Self {
            client_id,
            connect_time,
            application_name: application_name.to_string(),
            username: username.to_string(),
            pool_name: pool_name.to_string(),
            ..Default::default()
        }
    }

    /// Reports a client is disconnecting from the pooler and
    /// update metrics on the corresponding pool.
    pub fn disconnect(&self) {
        self.reporter.client_disconnecting(self.client_id);
    }

    /// Register a client with the stats system. The stats system uses client_id
    /// to track and aggregate statistics from all source that relate to that client
    pub fn register(&self, stats: Arc<ClientStats>) {
        self.reporter.client_register(self.client_id, stats);
        self.state.store(ClientState::Idle, Ordering::Relaxed);
    }

    /// Reports a client is done querying the server and is no longer assigned a server connection
    pub fn idle(&self) {
        self.state.store(ClientState::Idle, Ordering::Relaxed);
    }

    /// Reports a client is waiting for a connection
    pub fn waiting(&self) {
        let wait_start = self.connect_time.elapsed().as_micros() as u64;

        self.wait_start_us.store(wait_start, Ordering::Relaxed);
        self.state.store(ClientState::Waiting, Ordering::Relaxed);
    }

    /// Reports a client is done waiting for a connection and is about to query the server.
    pub fn active(&self) {
        self.state.store(ClientState::Active, Ordering::Relaxed);
    }

    /// Reports a client has failed to obtain a connection from a connection pool
    pub fn checkout_error(&self) {
        self.state.store(ClientState::Idle, Ordering::Relaxed);
        self.update_wait_times();
    }

    /// Reports a client has succeeded in obtaining a connection from a connection pool
    pub fn checkout_success(&self) {
        self.state.store(ClientState::Active, Ordering::Relaxed);
        self.update_wait_times();
    }

    /// Reports a client has had the server assigned to it be banned
    pub fn ban_error(&self) {
        self.state.store(ClientState::Idle, Ordering::Relaxed);
        self.error_count.fetch_add(1, Ordering::Relaxed);
    }

    fn update_wait_times(&self) {
        if self.wait_start_us.load(Ordering::Relaxed) == 0 {
            return;
        }

        let wait_time_us = self.get_current_wait_time_us();
        self.total_wait_time
            .fetch_add(wait_time_us, Ordering::Relaxed);
        self.max_wait_time
            .fetch_max(wait_time_us, Ordering::Relaxed);
        self.wait_start_us.store(0, Ordering::Relaxed);
    }

    pub fn get_current_wait_time_us(&self) -> u64 {
        let wait_start_us = self.wait_start_us.load(Ordering::Relaxed);
        let microseconds_since_connection_epoch = self.connect_time.elapsed().as_micros() as u64;
        if wait_start_us == 0 || microseconds_since_connection_epoch < wait_start_us {
            return 0;
        }
        microseconds_since_connection_epoch - wait_start_us
    }

    /// Report a query executed by a client against a server
    pub fn query(&self) {
        self.query_count.fetch_add(1, Ordering::Relaxed);
    }

    /// Report a transaction executed by a client a server
    /// we report each individual queries outside a transaction as a transaction
    /// We only count the initial BEGIN as a transaction, all queries within do not
    /// count as transactions
    pub fn transaction(&self) {
        self.transaction_count.fetch_add(1, Ordering::Relaxed);
    }

    // Helper methods for show clients
    pub fn connect_time(&self) -> Instant {
        self.connect_time
    }

    pub fn client_id(&self) -> i32 {
        self.client_id
    }

    pub fn application_name(&self) -> String {
        self.application_name.clone()
    }

    pub fn username(&self) -> String {
        self.username.clone()
    }

    pub fn pool_name(&self) -> String {
        self.pool_name.clone()
    }
}
