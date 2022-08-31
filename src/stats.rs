use arc_swap::ArcSwap;
use cadence::{
    prelude::*, BufferedUdpMetricSink, BufferedUnixMetricSink, MetricBuilder, NopMetricSink,
    QueuingMetricSink, StatsdClient,
};
/// Statistics and reporting.
use log::{error, info, trace, warn};
use once_cell::sync::Lazy;
use parking_lot::Mutex;
use std::collections::HashMap;
use std::net::UdpSocket;
use std::os::unix::net::UnixDatagram;
use tokio::sync::mpsc::error::TrySendError;
use tokio::sync::mpsc::{channel, Receiver, Sender};

use crate::config::Address;
use crate::pool::get_all_pools;
use crate::{config::get_config, pool::get_number_of_addresses};

pub static REPORTER: Lazy<ArcSwap<Reporter>> =
    Lazy::new(|| ArcSwap::from_pointee(Reporter::default()));

/// Latest stats updated every second; used in SHOW STATS and other admin commands.
static LATEST_STATS: Lazy<Mutex<HashMap<usize, HashMap<String, i64>>>> =
    Lazy::new(|| Mutex::new(HashMap::new()));

/// Statistics period used for average calculations.
/// 15 seconds.
static STAT_PERIOD: u64 = 15000;

use serde_derive::{Deserialize, Serialize};

#[derive(Clone, PartialEq, Serialize, Deserialize, Debug)]
#[serde(tag = "type", content = "args")]
pub enum StatsDMode {
    UnixSocket {
        prefix: String,
        path: String,
    },
    Udp {
        prefix: String,
        host: String,
        port: u16,
    },
}

#[derive(Debug, Clone)]
pub struct ClientMetadata {
    pub username: String,
    pub application_name: String,
}

impl ClientMetadata {
    pub fn new(username: String, application_name: String) -> ClientMetadata {
        ClientMetadata {
            username,
            application_name,
        }
    }
}

#[derive(Debug, Clone)]
pub struct ServerMetadata {
    address: Address,
}

impl ServerMetadata {
    pub fn new(address: Address) -> ServerMetadata {
        ServerMetadata { address }
    }

    pub fn dummy_with_address_id(address_id: usize) -> ServerMetadata {
        let mut address = Address::default();
        address.id = address_id;

        ServerMetadata::new(address)
    }
}

/// The names for the events reported
/// to the statistics collector.
#[derive(Debug, Clone, Copy)]
enum EventName {
    CheckoutTime,
    Query,
    Transaction,
    DataSent,
    DataReceived,
    ClientWaiting,
    ClientActive,
    ClientIdle,
    ClientDisconnecting,
    ServerActive,
    ServerIdle,
    ServerTested,
    ServerLogin,
    ServerDisconnecting,
    UpdateStats,
    UpdateAverages,
}

/// Event data sent to the collector
/// from clients and servers.
#[derive(Debug, Clone)]
pub struct Event {
    /// The name of the event being reported.
    name: EventName,

    /// The value being reported. Meaning differs based on event name.
    value: i64,

    /// The client or server connection reporting the event.
    process_id: i32,

    /// Metadata about the client on the event
    client_metadata: Option<ClientMetadata>,

    /// Metadata about the server on the event
    server_metadata: ServerMetadata,
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
        let name = event.name;
        let result = self.tx.try_send(event);

        match result {
            Ok(_) => trace!(
                "{:?} event reported successfully, capacity: {}",
                name,
                self.tx.capacity()
            ),

            Err(err) => match err {
                TrySendError::Full { .. } => error!("{:?} event dropped, buffer full", name),
                TrySendError::Closed { .. } => error!("{:?} event dropped, channel closed", name),
            },
        };
    }

    /// Report a query executed by a client against
    /// a server identified by the `address_id`.
    pub fn query(
        &self,
        process_id: i32,
        client_metadata: &ClientMetadata,
        server_metadata: &ServerMetadata,
    ) {
        let event = Event {
            name: EventName::Query,
            value: 1,
            process_id,
            client_metadata: Some(client_metadata.clone()),
            server_metadata: server_metadata.clone(),
        };

        self.send(event);
    }

    /// Report a transaction executed by a client against
    /// a server identified by the `address_id`.
    pub fn transaction(
        &self,
        process_id: i32,
        client_metadata: &ClientMetadata,
        server_metadata: &ServerMetadata,
    ) {
        let event = Event {
            name: EventName::Transaction,
            value: 1,
            process_id,
            client_metadata: Some(client_metadata.clone()),
            server_metadata: server_metadata.clone(),
        };

        self.send(event)
    }

    /// Report data sent to a server identified by `address_id`.
    /// The `amount` is measured in bytes.
    pub fn data_sent(&self, amount: usize, process_id: i32, server_metadata: &ServerMetadata) {
        let event = Event {
            name: EventName::DataSent,
            value: amount as i64,
            process_id,
            client_metadata: None,
            server_metadata: server_metadata.clone(),
        };

        self.send(event)
    }

    /// Report data received from a server identified by `address_id`.
    /// The `amount` is measured in bytes.
    pub fn data_received(&self, amount: usize, process_id: i32, server_metadata: &ServerMetadata) {
        let event = Event {
            name: EventName::DataReceived,
            value: amount as i64,
            process_id,
            client_metadata: None,
            server_metadata: server_metadata.clone(),
        };

        self.send(event)
    }

    /// Time spent waiting to get a healthy connection from the pool
    /// for a server identified by `address_id`.
    /// Measured in microseconds.
    pub fn checkout_time(&self, ms: u128, process_id: i32, server_metadata: &ServerMetadata) {
        let event = Event {
            name: EventName::CheckoutTime,
            value: ms as i64,
            process_id,
            client_metadata: None,
            server_metadata: server_metadata.clone(),
        };

        self.send(event)
    }

    /// Reports a client identified by `process_id` waiting for a connection
    /// to a server identified by `address_id`.
    pub fn client_waiting(
        &self,
        process_id: i32,
        client_metadata: &ClientMetadata,
        server_metadata: &ServerMetadata,
    ) {
        let event = Event {
            name: EventName::ClientWaiting,
            value: 1,
            process_id,
            client_metadata: Some(client_metadata.clone()),
            server_metadata: server_metadata.clone(),
        };

        self.send(event)
    }

    /// Reports a client identified by `process_id` is done waiting for a connection
    /// to a server identified by `address_id` and is about to query the server.
    pub fn client_active(
        &self,
        process_id: i32,
        client_metadata: &ClientMetadata,
        server_metadata: &ServerMetadata,
    ) {
        let event = Event {
            name: EventName::ClientActive,
            value: 1,
            process_id,
            client_metadata: Some(client_metadata.clone()),
            server_metadata: server_metadata.clone(),
        };

        self.send(event)
    }

    /// Reports a client identified by `process_id` is done querying the server
    /// identified by `address_id` and is no longer active.
    pub fn client_idle(
        &self,
        process_id: i32,
        client_metadata: &ClientMetadata,
        server_metadata: &ServerMetadata,
    ) {
        let event = Event {
            name: EventName::ClientIdle,
            value: 1,
            process_id,
            client_metadata: Some(client_metadata.clone()),
            server_metadata: server_metadata.clone(),
        };

        self.send(event)
    }

    /// Reports a client identified by `process_id` is disconnecting from the pooler.
    /// The last server it was connected to is identified by `address_id`.
    pub fn client_disconnecting(&self, process_id: i32, server_metadata: &ServerMetadata) {
        let event = Event {
            name: EventName::ClientDisconnecting,
            value: 1,
            process_id,
            client_metadata: None,
            server_metadata: server_metadata.clone(),
        };

        self.send(event)
    }

    /// Reports a server connection identified by `process_id` for
    /// a configured server identified by `address_id` is actively used
    /// by a client.
    pub fn server_active(&self, process_id: i32, server_metadata: &ServerMetadata) {
        let event = Event {
            name: EventName::ServerActive,
            value: 1,
            process_id,
            client_metadata: None,
            server_metadata: server_metadata.clone(),
        };

        self.send(event)
    }

    /// Reports a server connection identified by `process_id` for
    /// a configured server identified by `address_id` is no longer
    /// actively used by a client and is now idle.
    pub fn server_idle(&self, process_id: i32, server_metadata: &ServerMetadata) {
        let event = Event {
            name: EventName::ServerIdle,
            value: 1,
            process_id,
            client_metadata: None,
            server_metadata: server_metadata.clone(),
        };

        self.send(event)
    }

    /// Reports a server connection identified by `process_id` for
    /// a configured server identified by `address_id` is attempting
    /// to login.
    pub fn server_login(&self, process_id: i32, server_metadata: &ServerMetadata) {
        let event = Event {
            name: EventName::ServerLogin,
            value: 1,
            process_id,
            client_metadata: None,
            server_metadata: server_metadata.clone(),
        };

        self.send(event)
    }

    /// Reports a server connection identified by `process_id` for
    /// a configured server identified by `address_id` is being
    /// tested before being given to a client.
    pub fn server_tested(&self, process_id: i32, server_metadata: &ServerMetadata) {
        let event = Event {
            name: EventName::ServerTested,
            value: 1,
            process_id,
            client_metadata: None,
            server_metadata: server_metadata.clone(),
        };

        self.send(event)
    }

    /// Reports a server connection identified by `process_id` is disconnecting from the pooler.
    /// The configured server it was connected to is identified by `address_id`.
    pub fn server_disconnecting(&self, process_id: i32, server_metadata: &ServerMetadata) {
        let event = Event {
            name: EventName::ServerDisconnecting,
            value: 1,
            process_id,
            client_metadata: None,
            server_metadata: server_metadata.clone(),
        };

        self.send(event)
    }
}

trait StatSubmitter<T>
where
    T: cadence::Metric + From<String>,
{
    fn submit_stat(&self, metric_builder: MetricBuilder<T>);
}

impl<T> StatSubmitter<T> for StatsdClient
where
    T: cadence::Metric + From<String>,
{
    fn submit_stat(&self, metric_builder: cadence::MetricBuilder<T>) {
        // TODO: Move tagging logic here
        if metric_builder.try_send().is_err() {
            warn!("Error sending query metrics to client");
        }
    }
}

trait StatCreator {
    fn send_count(&self, name: &str, count: i64, tags: HashMap<String, String>);

    fn send_time(&self, name: &str, time: u64, tags: HashMap<String, String>);

    fn send_gauge(&self, name: &str, value: u64, tags: HashMap<String, String>);
}

impl StatCreator for StatsdClient {
    fn send_count(&self, name: &str, count: i64, tags: HashMap<String, String>) {
        let mut metric_builder = self.count_with_tags(name, count);

        for (k, v) in tags.iter() {
            metric_builder = metric_builder.with_tag(k, v);
        }

        self.submit_stat(metric_builder);
    }

    fn send_time(&self, name: &str, time: u64, tags: HashMap<String, String>) {
        let mut metric_builder = self.time_with_tags(name, time);

        for (k, v) in tags.iter() {
            metric_builder = metric_builder.with_tag(k, v);
        }

        self.submit_stat(metric_builder);
    }

    fn send_gauge(&self, name: &str, value: u64, tags: HashMap<String, String>) {
        let mut metric_builder = self.gauge_with_tags(name, value);

        for (k, v) in tags.iter() {
            metric_builder = metric_builder.with_tag(k, v);
        }

        self.submit_stat(metric_builder);
    }
}

fn new_statsd_client() -> StatsdClient {
    let config = get_config();

    // Queue with a maximum capacity of 128K elements
    const QUEUE_SIZE: usize = 128 * 1024;

    if let Some(statsd_mode) = config.general.statsd {
        let (prefix, sink) = match statsd_mode {
            StatsDMode::UnixSocket { prefix, path } => {
                let socket = UnixDatagram::unbound().unwrap();
                socket.set_nonblocking(true).unwrap();
                let buffered_sink = BufferedUnixMetricSink::from(path, socket);
                (
                    prefix,
                    QueuingMetricSink::with_capacity(buffered_sink, QUEUE_SIZE),
                )
            }
            StatsDMode::Udp { prefix, host, port } => {
                // Try to create
                let socket = UdpSocket::bind("0.0.0.0:0").unwrap();
                socket.set_nonblocking(true).unwrap();
                let buffered_sink = BufferedUdpMetricSink::from((host, port), socket).unwrap();
                (
                    prefix,
                    QueuingMetricSink::with_capacity(buffered_sink, QUEUE_SIZE),
                )
            }
        };

        info!("Started Statsd Client");
        let statsd_builder = StatsdClient::builder(&prefix, sink);
        // TODO: Add default tags for statsd client
        statsd_builder.build()
    } else {
        // No-op client
        StatsdClient::from_sink("prefix", NopMetricSink)
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
    statsd_client: StatsdClient,
}

impl Collector {
    /// Create a new collector instance. There should only be one instance
    /// at a time. This is ensured by mpsc which allows only one receiver.
    pub fn new(rx: Receiver<Event>, tx: Sender<Event>) -> Collector {
        Collector {
            rx,
            tx,
            statsd_client: new_statsd_client(),
        }
    }

    /// The statistics collection handler. It will collect statistics
    /// for `address_id`s starting at 0 up to `addresses`.
    pub async fn collect(&mut self) {
        info!("Events reporter started");

        let stats_template = HashMap::from([
            ("total_query_count", 0),
            ("total_query_time", 0),
            ("total_received", 0),
            ("total_sent", 0),
            ("total_xact_count", 0),
            ("total_xact_time", 0),
            ("total_wait_time", 0),
            ("avg_query_count", 0),
            ("avg_query_time", 0),
            ("avg_recv", 0),
            ("avg_sent", 0),
            ("avg_xact_count", 0),
            ("avg_xact_time", 0),
            ("avg_wait_time", 0),
            ("maxwait_us", 0),
            ("maxwait", 0),
            ("cl_waiting", 0),
            ("cl_active", 0),
            ("cl_idle", 0),
            ("sv_idle", 0),
            ("sv_active", 0),
            ("sv_login", 0),
            ("sv_tested", 0),
        ]);

        let mut stats = HashMap::new();

        // Stats saved after each iteration of the flush event. Used in calculation
        // of averages in the last flush period.
        let mut old_stats: HashMap<usize, HashMap<String, i64>> = HashMap::new();

        // Track which state the client and server are at any given time.
        let mut client_server_states: HashMap<usize, HashMap<i32, EventName>> = HashMap::new();

        // Flush stats to StatsD and calculate averages every 15 seconds.
        let tx = self.tx.clone();
        tokio::task::spawn(async move {
            let mut interval =
                tokio::time::interval(tokio::time::Duration::from_millis(STAT_PERIOD / 15));
            loop {
                interval.tick().await;
                for pool in get_all_pools().iter().map(|(_, pool)| pool) {
                    for address_vec in pool.addresses.iter() {
                        for address in address_vec {
                            let _ = tx.try_send(Event {
                                name: EventName::UpdateStats,
                                value: 0,       // unused for this event
                                process_id: -1, // unused for this event
                                client_metadata: None,
                                server_metadata: ServerMetadata::new(address.clone()),
                            });
                        }
                    }
                }
            }
        });

        let tx = self.tx.clone();
        tokio::task::spawn(async move {
            let mut interval =
                tokio::time::interval(tokio::time::Duration::from_millis(STAT_PERIOD));
            loop {
                interval.tick().await;
                let address_count = get_number_of_addresses();
                for address_id in 0..address_count {
                    let _ = tx.try_send(Event {
                        name: EventName::UpdateAverages,
                        value: 0,       // unused for this event
                        process_id: -1, // unused for this event
                        client_metadata: None,
                        server_metadata: ServerMetadata::dummy_with_address_id(address_id),
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

            let stats = stats
                .entry(stat.server_metadata.address.id)
                .or_insert(stats_template.clone());
            let client_server_states = client_server_states
                .entry(stat.server_metadata.address.id)
                .or_insert(HashMap::new());
            let old_stats = old_stats
                .entry(stat.server_metadata.address.id)
                .or_insert(HashMap::new());

            // Some are counters, some are gauges...
            match stat.name {
                EventName::Query | EventName::Transaction => {
                    let mut tags = HashMap::new();

                    // Client details
                    tags = add_client_tags(tags, &stat.client_metadata.unwrap());

                    // Server details
                    tags = add_server_tags(tags, &stat.server_metadata);

                    let (statsd_metric_name, metric_name) = match stat.name {
                        EventName::Query => ("query_count", "total_query_count"),
                        EventName::Transaction => ("tx_count", "total_xact_count"),
                        _ => unreachable!(),
                    };

                    self.statsd_client
                        .send_count(statsd_metric_name, stat.value, tags);
                    let counter = stats.entry(metric_name).or_insert(0);
                    *counter += stat.value;
                }

                EventName::DataSent => {
                    self.statsd_client
                        .send_count("bytes_sent", stat.value, HashMap::new());

                    let counter = stats.entry("total_sent").or_insert(0);
                    *counter += stat.value;
                }

                EventName::DataReceived => {
                    self.statsd_client
                        .send_count("bytes_received", stat.value, HashMap::new());

                    let counter = stats.entry("total_received").or_insert(0);
                    *counter += stat.value;
                }

                EventName::CheckoutTime => {
                    let mut tags = HashMap::new();

                    tags = add_server_tags(tags, &stat.server_metadata);

                    self.statsd_client
                        .send_time("server_checkout_time", stat.value as u64, tags);

                    let counter = stats.entry("total_wait_time").or_insert(0);
                    *counter += stat.value;

                    let counter = stats.entry("maxwait_us").or_insert(0);
                    let mic_part = stat.value % 1_000_000;

                    // Report max time here
                    if mic_part > *counter {
                        *counter = mic_part;
                    }

                    let counter = stats.entry("maxwait").or_insert(0);
                    let seconds = *counter / 1_000_000;

                    if seconds > *counter {
                        *counter = seconds;
                    }
                }

                EventName::ClientActive
                | EventName::ClientWaiting
                | EventName::ClientIdle
                | EventName::ServerActive
                | EventName::ServerIdle
                | EventName::ServerTested
                | EventName::ServerLogin => {
                    client_server_states.insert(stat.process_id, stat.name);
                }

                EventName::ClientDisconnecting | EventName::ServerDisconnecting => {
                    client_server_states.remove(&stat.process_id);
                }

                EventName::UpdateStats => {
                    // Calculate connection states
                    for (_, state) in client_server_states.iter() {
                        let stat_name = match state {
                            EventName::ClientActive => "cl_active",

                            EventName::ClientWaiting => "cl_waiting",

                            EventName::ClientIdle => "cl_idle",

                            EventName::ServerIdle => "sv_idle",

                            EventName::ServerActive => "sv_active",

                            EventName::ServerTested => "sv_tested",

                            EventName::ServerLogin => "sv_login",

                            _ => unreachable!(),
                        };

                        let counter = stats.entry(stat_name).or_insert(0);
                        *counter += 1;
                    }

                    let mut tags = HashMap::new();
                    tags = add_server_tags(tags, &stat.server_metadata);

                    // Send state stats to statsd
                    for (key, value) in stats.iter() {
                        self.statsd_client
                            .send_gauge(key, value.clone() as u64, tags.clone());
                    }

                    // Update latest stats used in SHOW STATS
                    let mut guard = LATEST_STATS.lock();
                    for (key, value) in stats.iter() {
                        let entry = guard
                            .entry(stat.server_metadata.address.id)
                            .or_insert(HashMap::new());
                        entry.insert(key.to_string(), value.clone());
                    }

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
                        stats.insert(stat, 0);
                    }
                }

                EventName::UpdateAverages => {
                    // Calculate averages
                    for stat in &[
                        "avg_query_count",
                        "avg_query_time",
                        "avg_recv",
                        "avg_sent",
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

                        stats.insert(stat, avg);
                        *old_value = new_value;
                    }
                }
            };
        }
    }
}

/// Get a snapshot of statistics. Updated once a second
/// by the `Collector`.
pub fn get_stats() -> HashMap<usize, HashMap<String, i64>> {
    LATEST_STATS.lock().clone()
}

/// Get the statistics reporter used to update stats across the pools/clients.
pub fn get_reporter() -> Reporter {
    (*(*REPORTER.load())).clone()
}

fn add_server_tags(
    mut tags: HashMap<String, String>,
    server_metadata: &ServerMetadata,
) -> HashMap<String, String> {
    tags.insert(String::from("name"), server_metadata.address.name());
    tags.insert(String::from("db_host_address"), server_metadata.address.host.clone());
    tags.insert(
        String::from("pool_name"),
        server_metadata.address.pool_name.to_string(),
    );
    tags.insert(
        String::from("port"),
        server_metadata.address.port.to_string(),
    );
    tags.insert(
        String::from("role"),
        server_metadata.address.role.to_string(),
    );
    tags.insert(
        String::from("username"),
        server_metadata.address.username.to_string(),
    );

    tags
}

fn add_client_tags(
    mut tags: HashMap<String, String>,
    client_metadata: &ClientMetadata,
) -> HashMap<String, String> {
    tags.insert(
        String::from("username"),
        client_metadata.username.to_string(),
    );
    tags.insert(
        String::from("application_name"),
        client_metadata.application_name.to_string(),
    );

    tags
}
