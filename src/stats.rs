use arc_swap::ArcSwap;
/// Statistics and reporting.
use log::{error, info, trace, warn};
use once_cell::sync::Lazy;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::mpsc::error::TrySendError;
use tokio::sync::mpsc::{channel, Receiver, Sender};
use tokio::time::Instant;

use crate::pool::{get_all_pools, get_number_of_addresses};

type ClientStatesLookup = HashMap<i32, ClientInformation>;
type ServerStatesLookup = HashMap<i32, ServerInformation>;
type PoolStatsLookup = HashMap<(String, String), HashMap<String, i64>>;
type AddressStatsLookup = HashMap<usize, HashMap<String, i64>>;

/// Latest client stats updated every second; used in SHOW CLIENTS.
static LATEST_CLIENT_STATS: Lazy<ArcSwap<ClientStatesLookup>> =
    Lazy::new(|| ArcSwap::from_pointee(ClientStatesLookup::default()));

/// Latest client stats updated every second; used in SHOW CLIENTS.
static LATEST_SERVER_STATS: Lazy<ArcSwap<ServerStatesLookup>> =
    Lazy::new(|| ArcSwap::from_pointee(ServerStatesLookup::default()));

static LATEST_POOL_STATS: Lazy<ArcSwap<PoolStatsLookup>> =
    Lazy::new(|| ArcSwap::from_pointee(PoolStatsLookup::default()));

static LATEST_ADDRESS_STATS: Lazy<ArcSwap<AddressStatsLookup>> =
    Lazy::new(|| ArcSwap::from_pointee(AddressStatsLookup::default()));

pub static REPORTER: Lazy<ArcSwap<Reporter>> =
    Lazy::new(|| ArcSwap::from_pointee(Reporter::default()));

/// Statistics period used for average calculations.
/// 15 seconds.
static STAT_PERIOD: u64 = 15000;

/// The various states that a client can be in
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum ClientState {
    Idle,
    Waiting,
    Active,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum ServerState {
    Login,
    Active,
    Tested,
    Idle,
}

/// Information we keep track off which can be queried by SHOW CLIENTS
#[derive(Debug, Clone)]
pub struct ClientInformation {
    pub state: ClientState,
    pub connect_time: Instant,
    pub client_id: i32,

    pub application_name: String,
    pub username: String,
    pub pool_name: String,

    pub total_wait_time: u64,

    pub transaction_count: u64,
    pub query_count: u64,
    pub error_count: u64,
}

#[derive(Debug, Clone)]
pub struct ServerInformation {
    pub state: ServerState,
    pub connect_time: Instant,
    pub address_name: String,
    pub address_id: usize,
    pub server_id: i32,

    pub username: String,
    pub pool_name: String,

    pub bytes_sent: u64,
    pub bytes_received: u64,

    pub transaction_count: u64,
    pub query_count: u64,
    pub error_count: u64,
}

/// The names for the events reported
/// to the statistics collector.
#[derive(Debug, Clone)]
enum EventName {
    CheckoutTime {
        client_id: i32,
        server_id: i32,
    },
    Query {
        client_id: i32,
        server_id: i32,
    },
    Transaction {
        client_id: i32,
        server_id: i32,
    },

    DataSentToServer {
        server_id: i32,
    },

    DataReceivedFromServer {
        server_id: i32,
    },

    ClientRegistered {
        client_id: i32,
        pool_name: String,
        username: String,
        application_name: String,
    },
    ClientIdle {
        client_id: i32,
    },
    ClientWaiting {
        client_id: i32,
    },
    ClientActive {
        client_id: i32,
        #[allow(dead_code)]
        server_id: i32,
    },
    ClientDisconnecting {
        client_id: i32,
    },

    ClientCheckoutError {
        client_id: i32,
        #[allow(dead_code)]
        address_id: usize,
    },

    ClientBanError {
        client_id: i32,
        #[allow(dead_code)]
        address_id: usize,
    },

    ServerRegistered {
        server_id: i32,
        address_id: usize,
        address_name: String,
        pool_name: String,
        username: String,
    },
    ServerLogin {
        server_id: i32,
    },
    ServerIdle {
        server_id: i32,
    },
    ServerTested {
        server_id: i32,
    },
    ServerActive {
        #[allow(dead_code)]
        client_id: i32,
        server_id: i32,
    },
    ServerDisconnecting {
        server_id: i32,
    },

    UpdateStats {
        pool_name: String,
        username: String,
    },

    UpdateAverages {
        address_id: usize,
    },
}

/// Event data sent to the collector
/// from clients and servers.
#[derive(Debug, Clone)]
pub struct Event {
    /// The name of the event being reported.
    name: EventName,

    /// The value being reported. Meaning differs based on event name.
    value: i64,
}

/// The statistics reporter. An instance is given
/// to each possible source of statistics,
/// e.g. clients, servers, connection pool.
#[derive(Clone, Debug)]
pub struct Reporter {
    tx: Sender<Event>,
}

impl Default for Reporter {
    fn default() -> Reporter {
        let (tx, _rx) = channel(5);
        Reporter { tx }
    }
}

impl Reporter {
    /// Create a new Reporter instance.
    pub fn new(tx: Sender<Event>) -> Reporter {
        Reporter { tx: tx }
    }

    /// Send statistics to the task keeping track of stats.
    fn send(&self, event: Event) {
        let name = event.name.clone();
        let result = self.tx.try_send(event.clone());

        match result {
            Ok(_) => trace!(
                "{:?} event reported successfully, capacity: {} {:?}",
                name,
                self.tx.capacity(),
                event
            ),

            Err(err) => match err {
                TrySendError::Full { .. } => error!("{:?} event dropped, buffer full", name),
                TrySendError::Closed { .. } => error!("{:?} event dropped, channel closed", name),
            },
        };
    }

    /// Report a query executed by a client against
    /// a server identified by the `address_id`.
    pub fn query(&self, client_id: i32, server_id: i32) {
        let event = Event {
            name: EventName::Query {
                client_id,
                server_id,
            },
            value: 1,
        };
        self.send(event);
    }

    /// Report a transaction executed by a client against
    /// a server identified by the `address_id`.
    pub fn transaction(&self, client_id: i32, server_id: i32) {
        let event = Event {
            name: EventName::Transaction {
                client_id,
                server_id,
            },
            value: 1,
        };
        self.send(event);
    }

    /// Report data sent to a server identified by `address_id`.
    /// The `amount` is measured in bytes.
    pub fn data_sent(&self, amount: usize, server_id: i32) {
        let event = Event {
            name: EventName::DataSentToServer { server_id },
            value: amount as i64,
        };
        self.send(event)
    }

    /// Report data received from a server identified by `address_id`.
    /// The `amount` is measured in bytes.
    pub fn data_received(&self, amount: usize, server_id: i32) {
        let event = Event {
            name: EventName::DataReceivedFromServer { server_id },
            value: amount as i64,
        };
        self.send(event)
    }

    /// Time spent waiting to get a healthy connection from the pool
    /// for a server identified by `address_id`.
    /// Measured in milliseconds.
    pub fn checkout_time(&self, ms: u128, client_id: i32, server_id: i32) {
        let event = Event {
            name: EventName::CheckoutTime {
                client_id,
                server_id,
            },
            value: ms as i64,
        };
        self.send(event)
    }

    pub fn client_register(
        &self,
        client_id: i32,
        pool_name: String,
        username: String,
        app_name: String,
    ) {
        let event = Event {
            name: EventName::ClientRegistered {
                client_id,
                pool_name: pool_name.clone(),
                username: username.clone(),
                application_name: app_name.clone(),
            },
            value: 1,
        };
        self.send(event);
    }

    /// Reports a client identified by `client_id` waiting for a connection
    /// to a server identified by `address_id`.
    pub fn client_waiting(&self, client_id: i32) {
        let event = Event {
            name: EventName::ClientWaiting { client_id },
            value: 1,
        };
        self.send(event)
    }

    /// Reports a client identified by `process_id` is done waiting for a connection
    /// to a server identified by `address_id` and is about to query the server.
    pub fn client_ban_error(&self, client_id: i32, address_id: usize) {
        let event = Event {
            name: EventName::ClientBanError {
                client_id,
                address_id,
            },
            value: 1,
        };
        self.send(event)
    }

    /// Reports a client identified by `process_id` is done waiting for a connection
    /// to a server identified by `address_id` and is about to query the server.
    pub fn client_checkout_error(&self, client_id: i32, address_id: usize) {
        let event = Event {
            name: EventName::ClientCheckoutError {
                client_id,
                address_id,
            },
            value: 1,
        };
        self.send(event)
    }

    /// Reports a client identified by `process_id` is done waiting for a connection
    /// to a server identified by `address_id` and is about to query the server.
    pub fn client_active(&self, client_id: i32, server_id: i32) {
        let event = Event {
            name: EventName::ClientActive {
                client_id,
                server_id,
            },
            value: 1,
        };
        self.send(event)
    }

    /// Reports a client identified by `process_id` is done querying the server
    /// identified by `address_id` and is no longer active.
    pub fn client_idle(&self, client_id: i32) {
        let event = Event {
            name: EventName::ClientIdle { client_id },
            value: 1,
        };
        self.send(event)
    }

    /// Reports a client identified by `process_id` is disconecting from the pooler.
    /// The last server it was connected to is identified by `address_id`.
    pub fn client_disconnecting(&self, client_id: i32) {
        let event = Event {
            name: EventName::ClientDisconnecting { client_id },
            value: 1,
        };
        self.send(event)
    }

    pub fn server_register(
        &self,
        server_id: i32,
        address_id: usize,
        address_name: String,
        pool_name: String,
        username: String,
    ) {
        let event = Event {
            name: EventName::ServerRegistered {
                server_id,
                address_id,
                address_name,
                pool_name,
                username,
            },
            value: 1,
        };
        self.send(event);
    }

    /// Reports a server connection identified by `process_id` for
    /// a configured server identified by `address_id` is actively used
    /// by a client.
    pub fn server_active(&self, client_id: i32, server_id: i32) {
        let event = Event {
            name: EventName::ServerActive {
                client_id,
                server_id,
            },
            value: 1,
        };
        self.send(event)
    }

    /// Reports a server connection identified by `process_id` for
    /// a configured server identified by `address_id` is no longer
    /// actively used by a client and is now idle.
    pub fn server_idle(&self, server_id: i32) {
        let event = Event {
            name: EventName::ServerIdle { server_id },
            value: 1,
        };
        self.send(event)
    }

    /// Reports a server connection identified by `process_id` for
    /// a configured server identified by `address_id` is attempting
    /// to login.
    pub fn server_login(&self, server_id: i32) {
        let event = Event {
            name: EventName::ServerLogin { server_id },
            value: 1,
        };
        self.send(event)
    }

    /// Reports a server connection identified by `process_id` for
    /// a configured server identified by `address_id` is being
    /// tested before being given to a client.
    pub fn server_tested(&self, server_id: i32) {
        let event = Event {
            name: EventName::ServerTested { server_id },
            value: 1,
        };

        self.send(event)
    }

    /// Reports a server connection identified by `process_id` is disconecting from the pooler.
    /// The configured server it was connected to is identified by `address_id`.
    pub fn server_disconnecting(&self, server_id: i32) {
        let event = Event {
            name: EventName::ServerDisconnecting { server_id },
            value: 1,
        };
        self.send(event)
    }
}

/// The statistics collector which is receiving statistics
/// from clients, servers, and the connection pool. There is
/// only one collector (kind of like a singleton).
/// The collector can trigger events on its own, e.g.
/// it updates aggregates every second and averages every
/// 15 seconds.
pub struct Collector {
    rx: Receiver<Event>,
    tx: Sender<Event>,
}

impl Collector {
    /// Create a new collector instance. There should only be one instance
    /// at a time. This is ensured by mpsc which allows only one receiver.
    pub fn new(rx: Receiver<Event>, tx: Sender<Event>) -> Collector {
        Collector { rx, tx }
    }

    /// The statistics collection handler. It will collect statistics
    /// for `address_id`s starting at 0 up to `addresses`.
    pub async fn collect(&mut self) {
        info!("Events reporter started");

        let mut client_states = ClientStatesLookup::default();
        let mut server_states = ServerStatesLookup::default();
        let mut pool_stat_lookup = PoolStatsLookup::default();

        let mut address_stat_lookup = AddressStatsLookup::default();
        let mut address_old_stat_lookup = AddressStatsLookup::default();

        let tx = self.tx.clone();
        tokio::task::spawn(async move {
            let mut interval =
                tokio::time::interval(tokio::time::Duration::from_millis(STAT_PERIOD / 15));
            loop {
                interval.tick().await;
                for ((pool_name, username), _pool) in get_all_pools() {
                    let _ = tx.try_send(Event {
                        name: EventName::UpdateStats {
                            pool_name,
                            username,
                        },
                        value: 0,
                    });
                }
            }
        });

        let tx = self.tx.clone();
        tokio::task::spawn(async move {
            let mut interval =
                tokio::time::interval(tokio::time::Duration::from_millis(STAT_PERIOD));
            loop {
                interval.tick().await;
                for address_id in 0..get_number_of_addresses() {
                    let _ = tx.try_send(Event {
                        name: EventName::UpdateAverages { address_id },
                        value: 0,
                    });
                }
            }
        });

        // The collector loop
        loop {
            let stat = match self.rx.recv().await {
                Some(stat) => stat,
                None => {
                    info!("Events collector is shutting down");
                    return;
                }
            };

            // Some are counters, some are gauges...
            match stat.name {
                EventName::Query {
                    client_id,
                    server_id,
                } => {
                    // Update client stats
                    match client_states.get_mut(&client_id) {
                        Some(client_info) => client_info.query_count += stat.value as u64,
                        None => (),
                    }

                    // Update server stats and pool aggergation stats
                    match server_states.get_mut(&server_id) {
                        Some(server_info) => {
                            server_info.query_count += stat.value as u64;

                            let pool_stats = address_stat_lookup
                                .entry(server_info.address_id)
                                .or_insert(HashMap::default());
                            let counter = pool_stats
                                .entry("total_query_count".to_string())
                                .or_insert(0);
                            *counter += stat.value;
                        }
                        None => (),
                    }
                }

                EventName::Transaction {
                    client_id,
                    server_id,
                } => {
                    // Update client stats
                    match client_states.get_mut(&client_id) {
                        Some(client_info) => client_info.transaction_count += stat.value as u64,
                        None => (),
                    }
                    // Update server stats and pool aggergation stats
                    match server_states.get_mut(&server_id) {
                        Some(server_info) => {
                            server_info.transaction_count += stat.value as u64;

                            let address_stats = address_stat_lookup
                                .entry(server_info.address_id)
                                .or_insert(HashMap::default());
                            let counter = address_stats
                                .entry("total_xact_count".to_string())
                                .or_insert(0);
                            *counter += stat.value;
                        }
                        None => (),
                    }
                }

                EventName::DataSentToServer { server_id } => {
                    // Update server stats and pool aggergation stats
                    match server_states.get_mut(&server_id) {
                        Some(server_info) => {
                            server_info.bytes_sent += stat.value as u64;

                            let address_stats = address_stat_lookup
                                .entry(server_info.address_id)
                                .or_insert(HashMap::default());
                            let counter =
                                address_stats.entry("total_sent".to_string()).or_insert(0);
                            *counter += stat.value;
                        }
                        None => (),
                    }
                }

                EventName::DataReceivedFromServer { server_id } => {
                    // Update server states and server aggergation stats
                    match server_states.get_mut(&server_id) {
                        Some(server_info) => {
                            server_info.bytes_received += stat.value as u64;

                            let address_stats = address_stat_lookup
                                .entry(server_info.address_id)
                                .or_insert(HashMap::default());
                            let counter = address_stats
                                .entry("total_received".to_string())
                                .or_insert(0);
                            *counter += stat.value;
                        }
                        None => (),
                    }
                }

                EventName::CheckoutTime {
                    client_id,
                    server_id,
                } => {
                    // Update client stats
                    match client_states.get_mut(&client_id) {
                        Some(client_info) => client_info.total_wait_time += stat.value as u64,
                        None => (),
                    }
                    // Update server stats and pool aggergation stats
                    match server_states.get_mut(&server_id) {
                        Some(server_info) => {
                            let pool_stats = address_stat_lookup
                                .entry(server_info.address_id)
                                .or_insert(HashMap::default());
                            let counter =
                                pool_stats.entry("total_wait_time".to_string()).or_insert(0);
                            *counter += stat.value;

                            let counter = pool_stats.entry("maxwait_us".to_string()).or_insert(0);
                            let mic_part = stat.value % 1_000_000;

                            // Report max time here
                            if mic_part > *counter {
                                *counter = mic_part;
                            }

                            let counter = pool_stats.entry("maxwait".to_string()).or_insert(0);
                            let seconds = *counter / 1_000_000;

                            if seconds > *counter {
                                *counter = seconds;
                            }
                        }
                        None => (),
                    }
                }

                EventName::ClientRegistered {
                    client_id,
                    pool_name,
                    username,
                    application_name,
                } => {
                    match client_states.get_mut(&client_id) {
                        Some(_) => {
                            warn!("Client double registered!");
                        }

                        None => {
                            client_states.insert(
                                client_id,
                                ClientInformation {
                                    state: ClientState::Idle,
                                    connect_time: Instant::now(),
                                    client_id,
                                    pool_name: pool_name.clone(),
                                    username: username.clone(),
                                    application_name: application_name.clone(),
                                    total_wait_time: 0,
                                    transaction_count: 0,
                                    query_count: 0,
                                    error_count: 0,
                                },
                            );
                        }
                    };
                }

                EventName::ClientBanError {
                    client_id,
                    address_id,
                } => {
                    match client_states.get_mut(&client_id) {
                        Some(client_info) => client_info.error_count += stat.value as u64,
                        None => (),
                    }

                    let address_stats = address_stat_lookup
                        .entry(address_id)
                        .or_insert(HashMap::default());
                    let counter = address_stats.entry("total_errors".to_string()).or_insert(0);
                    *counter += stat.value;
                }

                EventName::ClientCheckoutError {
                    client_id,
                    address_id,
                } => {
                    match client_states.get_mut(&client_id) {
                        Some(client_info) => client_info.error_count += stat.value as u64,
                        None => (),
                    }
                    let address_stats = address_stat_lookup
                        .entry(address_id)
                        .or_insert(HashMap::default());
                    let counter = address_stats.entry("total_errors".to_string()).or_insert(0);
                    *counter += stat.value;
                }

                EventName::ClientIdle { client_id } => {
                    match client_states.get_mut(&client_id) {
                        Some(client_state) => client_state.state = ClientState::Idle,
                        None => warn!("Stats on unregistered client!"),
                    };
                }

                EventName::ClientWaiting { client_id } => {
                    match client_states.get_mut(&client_id) {
                        Some(client_state) => client_state.state = ClientState::Waiting,
                        None => warn!("Stats on unregistered client!"),
                    };
                }

                EventName::ClientActive {
                    client_id,
                    server_id: _,
                } => {
                    match client_states.get_mut(&client_id) {
                        Some(client_state) => client_state.state = ClientState::Active,
                        None => warn!("Stats on unregistered client!"),
                    };
                }

                EventName::ClientDisconnecting { client_id } => {
                    client_states.remove(&client_id);
                }

                EventName::ServerRegistered {
                    address_name,
                    server_id,
                    address_id,
                    pool_name,
                    username,
                } => {
                    server_states.insert(
                        server_id,
                        ServerInformation {
                            address_id,
                            address_name,
                            server_id,
                            username,
                            pool_name,

                            state: ServerState::Idle,
                            connect_time: Instant::now(),
                            bytes_sent: 0,
                            bytes_received: 0,
                            transaction_count: 0,
                            query_count: 0,
                            error_count: 0,
                        },
                    );
                }

                EventName::ServerLogin { server_id } => {
                    match server_states.get_mut(&server_id) {
                        Some(server_state) => server_state.state = ServerState::Login,
                        None => warn!("Stats on unregistered Server!"),
                    };
                }

                EventName::ServerTested { server_id } => {
                    match server_states.get_mut(&server_id) {
                        Some(server_state) => server_state.state = ServerState::Tested,
                        None => warn!("Stats on unregistered Server!"),
                    };
                }

                EventName::ServerIdle { server_id } => {
                    match server_states.get_mut(&server_id) {
                        Some(server_state) => server_state.state = ServerState::Idle,
                        None => warn!("Stats on unregistered Server!"),
                    };
                }

                EventName::ServerActive {
                    client_id: _,
                    server_id,
                } => {
                    match server_states.get_mut(&server_id) {
                        Some(server_state) => server_state.state = ServerState::Active,
                        None => warn!("Stats on unregistered Server!"),
                    };
                }

                EventName::ServerDisconnecting { server_id } => {
                    server_states.remove(&server_id);
                }

                EventName::UpdateStats {
                    pool_name,
                    username,
                } => {
                    let pool_stats = pool_stat_lookup
                        .entry((pool_name.clone(), username.clone()))
                        .or_insert(HashMap::default());

                    // These are re-calculated every iteration of the loop, so we don't want to add values
                    // from the last iteration.
                    for stat in &[
                        "cl_active",
                        "cl_waiting",
                        "cl_idle",
                        "sv_idle",
                        "sv_active",
                        "sv_tested",
                        "sv_login",
                        "maxwait",
                        "maxwait_us",
                    ] {
                        pool_stats.insert(stat.to_string(), 0);
                    }

                    for (_, client_info) in client_states.iter() {
                        if client_info.pool_name != pool_name || client_info.username != username {
                            continue;
                        }
                        match client_info.state {
                            ClientState::Idle => {
                                let counter = pool_stats.entry("cl_idle".to_string()).or_insert(0);
                                *counter += 1;
                            }
                            ClientState::Waiting => {
                                let counter =
                                    pool_stats.entry("cl_waiting".to_string()).or_insert(0);
                                *counter += 1;
                            }
                            ClientState::Active => {
                                let counter =
                                    pool_stats.entry("cl_active".to_string()).or_insert(0);
                                *counter += 1;
                            }
                        };
                    }

                    for (_, server_info) in server_states.iter() {
                        if server_info.pool_name != pool_name || server_info.username != username {
                            continue;
                        }
                        match server_info.state {
                            ServerState::Login => {
                                let counter = pool_stats.entry("sv_login".to_string()).or_insert(0);
                                *counter += 1;
                            }
                            ServerState::Tested => {
                                let counter =
                                    pool_stats.entry("sv_tested".to_string()).or_insert(0);
                                *counter += 1;
                            }
                            ServerState::Active => {
                                let counter =
                                    pool_stats.entry("sv_active".to_string()).or_insert(0);
                                *counter += 1;
                            }
                            ServerState::Idle => {
                                let counter = pool_stats.entry("sv_idle".to_string()).or_insert(0);
                                *counter += 1;
                            }
                        };
                    }

                    LATEST_CLIENT_STATS.store(Arc::new(client_states.clone()));
                    LATEST_SERVER_STATS.store(Arc::new(server_states.clone()));
                    LATEST_POOL_STATS.store(Arc::new(pool_stat_lookup.clone()));
                }

                EventName::UpdateAverages { address_id } => {
                    let stats = address_stat_lookup
                        .entry(address_id)
                        .or_insert(HashMap::default());
                    let old_stats = address_old_stat_lookup
                        .entry(address_id)
                        .or_insert(HashMap::default());

                    // Calculate averages
                    for stat in &[
                        "avg_query_count",
                        "avg_query_time",
                        "avg_recv",
                        "avg_sent",
                        "avg_errors",
                        "avg_xact_time",
                        "avg_xact_count",
                        "avg_wait_time",
                    ] {
                        let total_name = match stat {
                            &"avg_recv" => "total_received".to_string(), // Because PgBouncer is saving bytes
                            _ => stat.replace("avg_", "total_"),
                        };

                        let old_value = old_stats.entry(total_name.clone()).or_insert(0);
                        let new_value = stats.get(total_name.as_str()).unwrap_or(&0).to_owned();
                        let avg = (new_value - *old_value) / (STAT_PERIOD as i64 / 1_000); // Avg / second

                        stats.insert(stat.to_string(), avg);
                        *old_value = new_value;
                    }
                    LATEST_ADDRESS_STATS.store(Arc::new(address_stat_lookup.clone()));
                }
            };
        }
    }
}

/// Get a snapshot of client statistics. Updated once a second
/// by the `Collector`.
pub fn get_client_stats() -> ClientStatesLookup {
    (*(*LATEST_CLIENT_STATS.load())).clone()
}

pub fn get_server_stats() -> ServerStatesLookup {
    (*(*LATEST_SERVER_STATS.load())).clone()
}

pub fn get_pool_stats() -> PoolStatsLookup {
    (*(*LATEST_POOL_STATS.load())).clone()
}

pub fn get_address_stats() -> AddressStatsLookup {
    (*(*LATEST_ADDRESS_STATS.load())).clone()
}

/// Get the statistics reporter used to update stats across the pools/clients.
pub fn get_reporter() -> Reporter {
    (*(*REPORTER.load())).clone()
}
