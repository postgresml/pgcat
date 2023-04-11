use crate::pool::PoolIdentifier;
/// Statistics and reporting.
use arc_swap::ArcSwap;

use log::{info, warn};
use once_cell::sync::Lazy;
use parking_lot::RwLock;
use std::collections::HashMap;

use std::sync::Arc;

// Structs that hold stats for different resources
pub mod address;
pub mod client;
pub mod pool;
pub mod server;
pub use address::AddressStats;
pub use client::{ClientState, ClientStats};
pub use pool::PoolStats;
pub use server::{ServerState, ServerStats};

/// Convenience types for various stats
type ClientStatesLookup = HashMap<i32, Arc<ClientStats>>;
type ServerStatesLookup = HashMap<i32, Arc<ServerStats>>;
type PoolStatsLookup = HashMap<(String, String), Arc<PoolStats>>;

/// Stats for individual client connections
/// Used in SHOW CLIENTS.
static CLIENT_STATS: Lazy<Arc<RwLock<ClientStatesLookup>>> =
    Lazy::new(|| Arc::new(RwLock::new(ClientStatesLookup::default())));

/// Stats for individual server connections
/// Used in SHOW SERVERS.
static SERVER_STATS: Lazy<Arc<RwLock<ServerStatesLookup>>> =
    Lazy::new(|| Arc::new(RwLock::new(ServerStatesLookup::default())));

/// Aggregate stats for each pool (a pool is identified by database name and username)
/// Used in SHOW POOLS.
static POOL_STATS: Lazy<Arc<RwLock<PoolStatsLookup>>> =
    Lazy::new(|| Arc::new(RwLock::new(PoolStatsLookup::default())));

/// The statistics reporter. An instance is given to each possible source of statistics,
/// e.g. client stats, server stats, connection pool stats.
pub static REPORTER: Lazy<ArcSwap<Reporter>> =
    Lazy::new(|| ArcSwap::from_pointee(Reporter::default()));

/// Statistics period used for average calculations.
/// 15 seconds.
static STAT_PERIOD: u64 = 15000;

/// The statistics reporter. An instance is given
/// to each possible source of statistics,
/// e.g. clients, servers, connection pool.
#[derive(Clone, Debug, Default)]
pub struct Reporter {}

impl Reporter {
    /// Register a client with the stats system. The stats system uses client_id
    /// to track and aggregate statistics from all source that relate to that client
    fn client_register(&self, client_id: i32, stats: Arc<ClientStats>) {
        if CLIENT_STATS.read().get(&client_id).is_some() {
            warn!("Client {:?} was double registered!", client_id);
            return;
        }

        CLIENT_STATS.write().insert(client_id, stats);
    }

    /// Reports a client is disconnecting from the pooler.
    fn client_disconnecting(&self, client_id: i32) {
        CLIENT_STATS.write().remove(&client_id);
    }

    /// Register a server connection with the stats system. The stats system uses server_id
    /// to track and aggregate statistics from all source that relate to that server
    fn server_register(&self, server_id: i32, stats: Arc<ServerStats>) {
        SERVER_STATS.write().insert(server_id, stats);
    }
    /// Reports a server connection is disconnecting from the pooler.
    fn server_disconnecting(&self, server_id: i32) {
        SERVER_STATS.write().remove(&server_id);
    }

    /// Register a pool with the stats system.
    fn pool_register(&self, identifier: PoolIdentifier, stats: Arc<PoolStats>) {
        POOL_STATS
            .write()
            .insert((identifier.db, identifier.user), stats);
    }
}

/// The statistics collector which used for calculating averages
/// There is only one collector (kind of like a singleton)
/// it updates averages every 15 seconds.
#[derive(Default)]
pub struct Collector {}

impl Collector {
    /// The statistics collection handler. It will collect statistics
    /// for `address_id`s starting at 0 up to `addresses`.
    pub async fn collect(&mut self) {
        info!("Events reporter started");

        tokio::task::spawn(async move {
            let mut interval =
                tokio::time::interval(tokio::time::Duration::from_millis(STAT_PERIOD));
            loop {
                interval.tick().await;

                for stats in SERVER_STATS.read().values() {
                    stats.address_stats().update_averages();
                }
            }
        });
    }
}

/// Get a snapshot of client statistics.
/// by the `Collector`.
pub fn get_client_stats() -> ClientStatesLookup {
    CLIENT_STATS.read().clone()
}

/// Get a snapshot of server statistics.
/// by the `Collector`.
pub fn get_server_stats() -> ServerStatesLookup {
    SERVER_STATS.read().clone()
}

/// Get a snapshot of pool statistics.
/// by the `Collector`.
pub fn get_pool_stats() -> PoolStatsLookup {
    POOL_STATS.read().clone()
}

/// Get the statistics reporter used to update stats across the pools/clients.
pub fn get_reporter() -> Reporter {
    (*(*REPORTER.load())).clone()
}
