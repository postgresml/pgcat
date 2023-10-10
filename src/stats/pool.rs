use log::debug;

use super::{ClientState, ServerState};
use crate::{config::PoolMode, messages::DataType, pool::PoolIdentifier};
use std::collections::HashMap;
use std::sync::atomic::*;

use crate::pool::get_all_pools;

#[derive(Debug, Clone)]
/// A struct that holds information about a Pool .
pub struct PoolStats {
    pub identifier: PoolIdentifier,
    pub mode: PoolMode,
    pub cl_idle: u64,
    pub cl_active: u64,
    pub cl_waiting: u64,
    pub cl_cancel_req: u64,
    pub sv_active: u64,
    pub sv_idle: u64,
    pub sv_used: u64,
    pub sv_tested: u64,
    pub sv_login: u64,
    pub maxwait: u64,
}
impl PoolStats {
    pub fn new(identifier: PoolIdentifier, mode: PoolMode) -> Self {
        PoolStats {
            identifier,
            mode,
            cl_idle: 0,
            cl_active: 0,
            cl_waiting: 0,
            cl_cancel_req: 0,
            sv_active: 0,
            sv_idle: 0,
            sv_used: 0,
            sv_tested: 0,
            sv_login: 0,
            maxwait: 0,
        }
    }

    pub fn construct_pool_lookup() -> HashMap<PoolIdentifier, PoolStats> {
        let mut map: HashMap<PoolIdentifier, PoolStats> = HashMap::new();
        let client_map = super::get_client_stats();
        let server_map = super::get_server_stats();

        for (identifier, pool) in get_all_pools() {
            map.insert(
                identifier.clone(),
                PoolStats::new(identifier, pool.settings.pool_mode),
            );
        }

        for client in client_map.values() {
            match map.get_mut(&PoolIdentifier {
                db: client.pool_name(),
                user: client.username(),
            }) {
                Some(pool_stats) => {
                    match client.state.load(Ordering::Relaxed) {
                        ClientState::Active => pool_stats.cl_active += 1,
                        ClientState::Idle => pool_stats.cl_idle += 1,
                        ClientState::Waiting => pool_stats.cl_waiting += 1,
                    }
                    let max_wait = client.max_wait_time.load(Ordering::Relaxed);
                    pool_stats.maxwait = std::cmp::max(pool_stats.maxwait, max_wait);
                }
                None => debug!("Client from an obselete pool"),
            }
        }

        for server in server_map.values() {
            match map.get_mut(&PoolIdentifier {
                db: server.pool_name(),
                user: server.username(),
            }) {
                Some(pool_stats) => match server.state.load(Ordering::Relaxed) {
                    ServerState::Active => pool_stats.sv_active += 1,
                    ServerState::Idle => pool_stats.sv_idle += 1,
                    ServerState::Login => pool_stats.sv_login += 1,
                    ServerState::Tested => pool_stats.sv_tested += 1,
                },
                None => debug!("Server from an obselete pool"),
            }
        }

        map
    }

    pub fn generate_header() -> Vec<(&'static str, DataType)> {
        vec![
            ("database", DataType::Text),
            ("user", DataType::Text),
            ("pool_mode", DataType::Text),
            ("cl_idle", DataType::Numeric),
            ("cl_active", DataType::Numeric),
            ("cl_waiting", DataType::Numeric),
            ("cl_cancel_req", DataType::Numeric),
            ("sv_active", DataType::Numeric),
            ("sv_idle", DataType::Numeric),
            ("sv_used", DataType::Numeric),
            ("sv_tested", DataType::Numeric),
            ("sv_login", DataType::Numeric),
            ("maxwait", DataType::Numeric),
            ("maxwait_us", DataType::Numeric),
        ]
    }

    pub fn generate_row(&self) -> Vec<String> {
        vec![
            self.identifier.db.clone(),
            self.identifier.user.clone(),
            self.mode.to_string(),
            self.cl_idle.to_string(),
            self.cl_active.to_string(),
            self.cl_waiting.to_string(),
            self.cl_cancel_req.to_string(),
            self.sv_active.to_string(),
            self.sv_idle.to_string(),
            self.sv_used.to_string(),
            self.sv_tested.to_string(),
            self.sv_login.to_string(),
            (self.maxwait / 1_000_000).to_string(),
            (self.maxwait % 1_000_000).to_string(),
        ]
    }
}

impl IntoIterator for PoolStats {
    type Item = (String, u64);
    type IntoIter = std::vec::IntoIter<Self::Item>;

    fn into_iter(self) -> Self::IntoIter {
        vec![
            ("cl_idle".to_string(), self.cl_idle),
            ("cl_active".to_string(), self.cl_active),
            ("cl_waiting".to_string(), self.cl_waiting),
            ("cl_cancel_req".to_string(), self.cl_cancel_req),
            ("sv_active".to_string(), self.sv_active),
            ("sv_idle".to_string(), self.sv_idle),
            ("sv_used".to_string(), self.sv_used),
            ("sv_tested".to_string(), self.sv_tested),
            ("sv_login".to_string(), self.sv_login),
            ("maxwait".to_string(), self.maxwait / 1_000_000),
            ("maxwait_us".to_string(), self.maxwait % 1_000_000),
        ]
        .into_iter()
    }
}
