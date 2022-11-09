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

/// Convenience types for various stats
type ClientStatesLookup = HashMap<i32, ClientInformation>;
type ServerStatesLookup = HashMap<i32, ServerInformation>;
type PoolStatsLookup = HashMap<(String, String), HashMap<String, i64>>;
type AddressStatsLookup = HashMap<usize, HashMap<String, i64>>;

/// Stats for individual client connections updated every second
/// Used in SHOW CLIENTS.
static LATEST_CLIENT_STATS: Lazy<ArcSwap<ClientStatesLookup>> =
    Lazy::new(|| ArcSwap::from_pointee(ClientStatesLookup::default()));

/// Stats for individual server connections updated every second
/// Used in SHOW SERVERS.
static LATEST_SERVER_STATS: Lazy<ArcSwap<ServerStatesLookup>> =
    Lazy::new(|| ArcSwap::from_pointee(ServerStatesLookup::default()));

/// Aggregate stats for each pool (a pool is identified by database name and username) updated every second
/// Used in SHOW POOLS.
static LATEST_POOL_STATS: Lazy<ArcSwap<PoolStatsLookup>> =
    Lazy::new(|| ArcSwap::from_pointee(PoolStatsLookup::default()));

/// Aggregate stats for individual database instances, updated every second, averages are calculated every 15
/// Used in SHOW STATS.
static LATEST_ADDRESS_STATS: Lazy<ArcSwap<AddressStatsLookup>> =
    Lazy::new(|| ArcSwap::from_pointee(AddressStatsLookup::default()));

/// The statistics reporter. An instance is given to each possible source of statistics,
/// e.g. clients, servers, connection pool.
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
impl std::fmt::Display for ClientState {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match *self {
            ClientState::Idle => write!(f, "idle"),
            ClientState::Waiting => write!(f, "waiting"),
            ClientState::Active => write!(f, "active"),
        }
    }
}

/// The various states that a server can be in
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum ServerState {
    Login,
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

/// Information we keep track off which can be queried by SHOW CLIENTS
#[derive(Debug, Clone)]
pub struct ClientInformation {
    pub state: ClientState,
    pub connect_time: Instant,

    /// A random integer assigned to the client and used by stats to track the client
    pub client_id: i32,

    pub application_name: String,
    pub username: String,
    pub pool_name: String,

    /// Total time spent waiting for a connection from pool, measures in microseconds
    pub total_wait_time: u64,

    pub transaction_count: u64,
    pub query_count: u64,
    pub error_count: u64,
}

/// Information we keep track off which can be queried by SHOW SERVERS
#[derive(Debug, Clone)]
pub struct ServerInformation {
    pub state: ServerState,
    pub connect_time: Instant,

    /// A random integer assigned to the server and used by stats to track the server
    pub server_id: i32,

    pub address_name: String,
    pub address_id: usize,

    pub username: String,
    pub pool_name: String,
    pub application_name: String,

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
        duration_ms: u128,
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
        Reporter { tx }
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

    /// Report a query executed by a client against a server
    pub fn query(&self, client_id: i32, server_id: i32, duration_ms: u128) {
        let event = Event {
            name: EventName::Query {
                client_id,
                server_id,
                duration_ms,
            },
            value: 1,
        };
        self.send(event);
    }

    /// Report a transaction executed by a client a server
    /// we report each individual queries outside a transaction as a transaction
    /// We only count the initial BEGIN as a transaction, all queries within do not
    /// count as transactions
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

    /// Report data sent to a server
    pub fn data_sent(&self, amount_bytes: usize, server_id: i32) {
        let event = Event {
            name: EventName::DataSentToServer { server_id },
            value: amount_bytes as i64,
        };
        self.send(event)
    }

    /// Report data received from a server
    pub fn data_received(&self, amount_bytes: usize, server_id: i32) {
        let event = Event {
            name: EventName::DataReceivedFromServer { server_id },
            value: amount_bytes as i64,
        };
        self.send(event)
    }

    /// Reportes the time spent by a client waiting to get a healthy connection from the pool
    pub fn checkout_time(&self, microseconds: u128, client_id: i32, server_id: i32) {
        let event = Event {
            name: EventName::CheckoutTime {
                client_id,
                server_id,
            },
            value: microseconds as i64,
        };
        self.send(event)
    }

    /// Register a client with the stats system. The stats system uses client_id
    /// to track and aggregate statistics from all source that relate to that client
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
                pool_name,
                username,
                application_name: app_name,
            },
            value: 1,
        };
        self.send(event);
    }

    /// Reports a client is waiting for a connection
    pub fn client_waiting(&self, client_id: i32) {
        let event = Event {
            name: EventName::ClientWaiting { client_id },
            value: 1,
        };
        self.send(event)
    }

    /// Reports a client has had the server assigned to it be banned
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

    /// Reports a client has failed to obtain a connection from a connection pool
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

    /// Reports a client is done waiting for a connection and is about to query the server.
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

    /// Reports a client is done querying the server and is no longer assigned a server connection
    pub fn client_idle(&self, client_id: i32) {
        let event = Event {
            name: EventName::ClientIdle { client_id },
            value: 1,
        };
        self.send(event)
    }

    /// Reports a client is disconecting from the pooler.
    pub fn client_disconnecting(&self, client_id: i32) {
        let event = Event {
            name: EventName::ClientDisconnecting { client_id },
            value: 1,
        };
        self.send(event)
    }

    /// Register a server connection with the stats system. The stats system uses server_id
    /// to track and aggregate statistics from all source that relate to that server
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

    /// Reports a server connection has been assigned to a client that
    /// is about to query the server
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

    /// Reports a server connection is no longer assigned to a client
    /// and is available for the next client to pick it up
    pub fn server_idle(&self, server_id: i32) {
        let event = Event {
            name: EventName::ServerIdle { server_id },
            value: 1,
        };
        self.send(event)
    }

    /// Reports a server connection is attempting to login.
    pub fn server_login(&self, server_id: i32) {
        let event = Event {
            name: EventName::ServerLogin { server_id },
            value: 1,
        };
        self.send(event)
    }

    /// Reports a server connection is being tested before being given to a client.
    pub fn server_tested(&self, server_id: i32) {
        let event = Event {
            name: EventName::ServerTested { server_id },
            value: 1,
        };

        self.send(event)
    }

    /// Reports a server connection is disconecting from the pooler.
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
                for (user_pool, _) in get_all_pools() {
                    let _ = tx.try_send(Event {
                        name: EventName::UpdateStats {
                            pool_name: user_pool.db,
                            username: user_pool.user,
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
                    duration_ms,
                } => {
                    // Update client stats
                    let app_name = match client_states.get_mut(&client_id) {
                        Some(client_info) => {
                            client_info.query_count += stat.value as u64;
                            client_info.application_name.to_string()
                        }
                        None => String::from("Undefined"),
                    };

                    // Update server stats and pool aggergation stats
                    match server_states.get_mut(&server_id) {
                        Some(server_info) => {
                            server_info.query_count += stat.value as u64;
                            server_info.application_name = app_name;

                            let address_stats = address_stat_lookup
                                .entry(server_info.address_id)
                                .or_insert_with(HashMap::default);
                            let counter = address_stats
                                .entry("total_query_count".to_string())
                                .or_insert(0);
                            *counter += stat.value;

                            let duration = address_stats
                                .entry("total_query_time".to_string())
                                .or_insert(0);
                            *duration += duration_ms as i64;
                        }
                        None => (),
                    }
                }

                EventName::Transaction {
                    client_id,
                    server_id,
                } => {
                    // Update client stats
                    let app_name = match client_states.get_mut(&client_id) {
                        Some(client_info) => {
                            client_info.transaction_count += stat.value as u64;
                            client_info.application_name.to_string()
                        }
                        None => String::from("Undefined"),
                    };

                    // Update server stats and pool aggergation stats
                    match server_states.get_mut(&server_id) {
                        Some(server_info) => {
                            server_info.transaction_count += stat.value as u64;
                            server_info.application_name = app_name;

                            let address_stats = address_stat_lookup
                                .entry(server_info.address_id)
                                .or_insert_with(HashMap::default);
                            let counter = address_stats
                                .entry("total_xact_count".to_string())
                                .or_insert(0);
                            *counter += stat.value;
                        }
                        None => (),
                    }
                }

                EventName::DataSentToServer { server_id } => {
                    // Update server stats and address aggergation stats
                    match server_states.get_mut(&server_id) {
                        Some(server_info) => {
                            server_info.bytes_sent += stat.value as u64;

                            let address_stats = address_stat_lookup
                                .entry(server_info.address_id)
                                .or_insert_with(HashMap::default);
                            let counter =
                                address_stats.entry("total_sent".to_string()).or_insert(0);
                            *counter += stat.value;
                        }
                        None => (),
                    }
                }

                EventName::DataReceivedFromServer { server_id } => {
                    // Update server states and address aggergation stats
                    match server_states.get_mut(&server_id) {
                        Some(server_info) => {
                            server_info.bytes_received += stat.value as u64;

                            let address_stats = address_stat_lookup
                                .entry(server_info.address_id)
                                .or_insert_with(HashMap::default);
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
                    let app_name = match client_states.get_mut(&client_id) {
                        Some(client_info) => {
                            client_info.total_wait_time += stat.value as u64;
                            client_info.application_name.to_string()
                        }
                        None => String::from("Undefined"),
                    };

                    // Update server stats and address aggergation stats
                    match server_states.get_mut(&server_id) {
                        Some(server_info) => {
                            server_info.application_name = app_name;

                            let address_stats = address_stat_lookup
                                .entry(server_info.address_id)
                                .or_insert_with(HashMap::default);
                            let counter = address_stats
                                .entry("total_wait_time".to_string())
                                .or_insert(0);
                            *counter += stat.value;

                            let pool_stats = pool_stat_lookup
                                .entry((
                                    server_info.pool_name.clone(),
                                    server_info.username.clone(),
                                ))
                                .or_insert_with(HashMap::default);

                            // We record max wait in microseconds, we do the pgbouncer second/microsecond split on admin
                            let old_microseconds =
                                pool_stats.entry("maxwait_us".to_string()).or_insert(0);
                            if stat.value > *old_microseconds {
                                *old_microseconds = stat.value;
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
                        Some(_) => warn!("Client {:?} was double registered!", client_id),
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
                        Some(client_info) => {
                            client_info.state = ClientState::Idle;
                            client_info.error_count += stat.value as u64;
                        }
                        None => warn!("Got event {:?} for unregistered client", stat.name),
                    }

                    // Update address aggregation stats
                    let address_stats = address_stat_lookup
                        .entry(address_id)
                        .or_insert_with(HashMap::default);
                    let counter = address_stats.entry("total_errors".to_string()).or_insert(0);
                    *counter += stat.value;
                }

                EventName::ClientCheckoutError {
                    client_id,
                    address_id,
                } => {
                    match client_states.get_mut(&client_id) {
                        Some(client_info) => {
                            client_info.state = ClientState::Idle;
                            client_info.error_count += stat.value as u64;
                        }
                        None => warn!("Got event {:?} for unregistered client", stat.name),
                    }

                    // Update address aggregation stats
                    let address_stats = address_stat_lookup
                        .entry(address_id)
                        .or_insert_with(HashMap::default);
                    let counter = address_stats.entry("total_errors".to_string()).or_insert(0);
                    *counter += stat.value;
                }

                EventName::ClientIdle { client_id } => {
                    match client_states.get_mut(&client_id) {
                        Some(client_state) => client_state.state = ClientState::Idle,
                        None => warn!("Got event {:?} for unregistered client", stat.name),
                    };
                }

                EventName::ClientWaiting { client_id } => {
                    match client_states.get_mut(&client_id) {
                        Some(client_state) => client_state.state = ClientState::Waiting,
                        None => warn!("Got event {:?} for unregistered client", stat.name),
                    };
                }

                EventName::ClientActive {
                    client_id,
                    server_id: _,
                } => {
                    match client_states.get_mut(&client_id) {
                        Some(client_state) => client_state.state = ClientState::Active,
                        None => warn!("Got event {:?} for unregistered client", stat.name),
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
                            application_name: String::from("Undefined"),
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
                        Some(server_state) => {
                            server_state.state = ServerState::Login;
                            server_state.application_name = String::from("Undefined");
                        }
                        None => warn!("Got event {:?} for unregistered server", stat.name),
                    };
                }

                EventName::ServerTested { server_id } => {
                    match server_states.get_mut(&server_id) {
                        Some(server_state) => {
                            server_state.state = ServerState::Tested;
                            server_state.application_name = String::from("Undefined");
                        }
                        None => warn!("Got event {:?} for unregistered server", stat.name),
                    };
                }

                EventName::ServerIdle { server_id } => {
                    match server_states.get_mut(&server_id) {
                        Some(server_state) => {
                            server_state.state = ServerState::Idle;
                            server_state.application_name = String::from("Undefined");
                        }
                        None => warn!("Got event {:?} for unregistered server", stat.name),
                    };
                }

                EventName::ServerActive {
                    client_id,
                    server_id,
                } => {
                    // Update client stats
                    let app_name = match client_states.get_mut(&client_id) {
                        Some(client_info) => client_info.application_name.to_string(),
                        None => String::from("Undefined"),
                    };

                    // Update server stats
                    match server_states.get_mut(&server_id) {
                        Some(server_state) => {
                            server_state.state = ServerState::Active;
                            server_state.application_name = app_name;
                        }
                        None => warn!("Got event {:?} for unregistered server", stat.name),
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
                        .or_insert_with(HashMap::default);

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

                    // The following calls publish the internal stats making it visible
                    // to clients using admin database to issue queries like `SHOW STATS`
                    LATEST_CLIENT_STATS.store(Arc::new(client_states.clone()));
                    LATEST_SERVER_STATS.store(Arc::new(server_states.clone()));
                    LATEST_POOL_STATS.store(Arc::new(pool_stat_lookup.clone()));

                    // Clear maxwait after reporting
                    pool_stat_lookup
                        .entry((pool_name.clone(), username.clone()))
                        .or_insert_with(HashMap::default)
                        .insert("maxwait_us".to_string(), 0);
                }

                EventName::UpdateAverages { address_id } => {
                    let stats = address_stat_lookup
                        .entry(address_id)
                        .or_insert_with(HashMap::default);
                    let old_stats = address_old_stat_lookup
                        .entry(address_id)
                        .or_insert_with(HashMap::default);

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

/// Get a snapshot of server statistics. Updated once a second
/// by the `Collector`.
pub fn get_server_stats() -> ServerStatesLookup {
    (*(*LATEST_SERVER_STATS.load())).clone()
}

/// Get a snapshot of pool statistics. Updated once a second
/// by the `Collector`.
pub fn get_pool_stats() -> PoolStatsLookup {
    (*(*LATEST_POOL_STATS.load())).clone()
}

/// Get a snapshot of address statistics. Updated once a second
/// by the `Collector`.
pub fn get_address_stats() -> AddressStatsLookup {
    (*(*LATEST_ADDRESS_STATS.load())).clone()
}

/// Get the statistics reporter used to update stats across the pools/clients.
pub fn get_reporter() -> Reporter {
    (*(*REPORTER.load())).clone()
}
