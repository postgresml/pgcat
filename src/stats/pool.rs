use crate::config::Pool;
use crate::config::PoolMode;
use crate::pool::PoolIdentifier;
use std::sync::atomic::*;
use std::sync::Arc;

use super::get_reporter;
use super::Reporter;
use super::{ClientState, ServerState};

#[derive(Debug, Clone, Default)]
/// A struct that holds information about a Pool .
pub struct PoolStats {
    // Pool identifier, cannot be changed after creating the instance
    identifier: PoolIdentifier,

    // Pool Config, cannot be changed after creating the instance
    config: Pool,

    // A reference to the global reporter.
    reporter: Reporter,

    /// Counters (atomics)
    pub cl_idle: Arc<AtomicU64>,
    pub cl_active: Arc<AtomicU64>,
    pub cl_waiting: Arc<AtomicU64>,
    pub cl_cancel_req: Arc<AtomicU64>,
    pub sv_active: Arc<AtomicU64>,
    pub sv_idle: Arc<AtomicU64>,
    pub sv_used: Arc<AtomicU64>,
    pub sv_tested: Arc<AtomicU64>,
    pub sv_login: Arc<AtomicU64>,
    pub maxwait: Arc<AtomicU64>,
}

impl IntoIterator for PoolStats {
    type Item = (String, u64);
    type IntoIter = std::vec::IntoIter<Self::Item>;

    fn into_iter(self) -> Self::IntoIter {
        vec![
            ("cl_idle".to_string(), self.cl_idle.load(Ordering::Relaxed)),
            (
                "cl_active".to_string(),
                self.cl_active.load(Ordering::Relaxed),
            ),
            (
                "cl_waiting".to_string(),
                self.cl_waiting.load(Ordering::Relaxed),
            ),
            (
                "cl_cancel_req".to_string(),
                self.cl_cancel_req.load(Ordering::Relaxed),
            ),
            (
                "sv_active".to_string(),
                self.sv_active.load(Ordering::Relaxed),
            ),
            ("sv_idle".to_string(), self.sv_idle.load(Ordering::Relaxed)),
            ("sv_used".to_string(), self.sv_used.load(Ordering::Relaxed)),
            (
                "sv_tested".to_string(),
                self.sv_tested.load(Ordering::Relaxed),
            ),
            (
                "sv_login".to_string(),
                self.sv_login.load(Ordering::Relaxed),
            ),
            (
                "maxwait".to_string(),
                self.maxwait.load(Ordering::Relaxed) / 1_000_000,
            ),
            (
                "maxwait_us".to_string(),
                self.maxwait.load(Ordering::Relaxed) % 1_000_000,
            ),
        ]
        .into_iter()
    }
}

impl PoolStats {
    pub fn new(identifier: PoolIdentifier, config: Pool) -> Self {
        Self {
            identifier,
            config,
            reporter: get_reporter(),
            ..Default::default()
        }
    }

    // Getters
    pub fn register(&self, stats: Arc<PoolStats>) {
        self.reporter.pool_register(self.identifier.clone(), stats);
    }

    pub fn database(&self) -> String {
        self.identifier.db.clone()
    }

    pub fn user(&self) -> String {
        self.identifier.user.clone()
    }

    pub fn pool_mode(&self) -> PoolMode {
        self.config.pool_mode
    }

    /// Populates an array of strings with counters (used by admin in show pools)
    pub fn populate_row(&self, row: &mut Vec<String>) {
        for (_key, value) in self.clone() {
            row.push(value.to_string());
        }
    }

    /// Deletes the maxwait counter, this is done everytime we obtain metrics
    pub fn clear_maxwait(&self) {
        self.maxwait.store(0, Ordering::Relaxed);
    }

    /// Notified when a server of the pool enters login state.
    ///
    /// Arguments:
    ///
    /// `from`: The state of the server that notifies.
    pub fn server_login(&self, from: ServerState) {
        self.sv_login.fetch_add(1, Ordering::Relaxed);
        if from != ServerState::Login {
            self.decrease_from_server_state(from);
        }
    }

    /// Notified when a server of the pool become 'active'
    ///
    /// Arguments:
    ///
    /// `from`: The state of the server that notifies.
    pub fn server_active(&self, from: ServerState) {
        self.sv_active.fetch_add(1, Ordering::Relaxed);
        if from != ServerState::Active {
            self.decrease_from_server_state(from);
        }
    }

    /// Notified when a server of the pool become 'tested'
    ///
    /// Arguments:
    ///
    /// `from`: The state of the server that notifies.
    pub fn server_tested(&self, from: ServerState) {
        self.sv_tested.fetch_add(1, Ordering::Relaxed);
        if from != ServerState::Tested {
            self.decrease_from_server_state(from);
        }
    }

    /// Notified when a server of the pool become 'idle'
    ///
    /// Arguments:
    ///
    /// `from`: The state of the server that notifies.
    pub fn server_idle(&self, from: ServerState) {
        self.sv_idle.fetch_add(1, Ordering::Relaxed);
        if from != ServerState::Idle {
            self.decrease_from_server_state(from);
        }
    }

    /// Notified when a client of the pool become 'waiting'
    ///
    /// Arguments:
    ///
    /// `from`: The state of the client that notifies.
    pub fn client_waiting(&self, from: ClientState) {
        if from != ClientState::Waiting {
            self.cl_waiting.fetch_add(1, Ordering::Relaxed);
            self.decrease_from_client_state(from);
        }
    }

    /// Notified when a client of the pool become 'active'
    ///
    /// Arguments:
    ///
    /// `from`: The state of the client that notifies.
    pub fn client_active(&self, from: ClientState) {
        if from != ClientState::Active {
            self.cl_active.fetch_add(1, Ordering::Relaxed);
            self.decrease_from_client_state(from);
        }
    }

    /// Notified when a client of the pool become 'idle'
    ///
    /// Arguments:
    ///
    /// `from`: The state of the client that notifies.
    pub fn client_idle(&self, from: ClientState) {
        if from != ClientState::Idle {
            self.cl_idle.fetch_add(1, Ordering::Relaxed);
            self.decrease_from_client_state(from);
        }
    }

    /// Notified when a client disconnects.
    ///
    /// Arguments:
    ///
    /// `from`: The state of the client that notifies.
    pub fn client_disconnect(&self, from: ClientState) {
        let counter = match from {
            ClientState::Idle => &self.cl_idle,
            ClientState::Waiting => &self.cl_waiting,
            ClientState::Active => &self.cl_active,
        };

        Self::decrease_counter(counter.clone());
    }

    /// Notified when a server disconnects.
    ///
    /// Arguments:
    ///
    /// `from`: The state of the client that notifies.
    pub fn server_disconnect(&self, from: ServerState) {
        let counter = match from {
            ServerState::Active => &self.sv_active,
            ServerState::Idle => &self.sv_idle,
            ServerState::Login => &self.sv_login,
            ServerState::Tested => &self.sv_tested,
        };
        Self::decrease_counter(counter.clone());
    }

    // helpers for counter decrease
    fn decrease_from_server_state(&self, from: ServerState) {
        let counter = match from {
            ServerState::Tested => &self.sv_tested,
            ServerState::Active => &self.sv_active,
            ServerState::Idle => &self.sv_idle,
            ServerState::Login => &self.sv_login,
        };
        Self::decrease_counter(counter.clone());
    }

    fn decrease_from_client_state(&self, from: ClientState) {
        let counter = match from {
            ClientState::Active => &self.cl_active,
            ClientState::Idle => &self.cl_idle,
            ClientState::Waiting => &self.cl_waiting,
        };
        Self::decrease_counter(counter.clone());
    }

    fn decrease_counter(value: Arc<AtomicU64>) {
        if value.load(Ordering::Relaxed) > 0 {
            value.fetch_sub(1, Ordering::Relaxed);
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_decrease() {
        let stat: PoolStats = PoolStats::default();
        stat.server_login(ServerState::Login);
        stat.server_idle(ServerState::Login);
        assert_eq!(stat.sv_login.load(Ordering::Relaxed), 0);
        assert_eq!(stat.sv_idle.load(Ordering::Relaxed), 1);
    }
}
