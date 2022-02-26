use log::{debug, error, info};
use once_cell::sync::OnceCell;
use statsd::Client;
/// Events collector and publisher.
use tokio::sync::mpsc::{Receiver, Sender};

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use crate::config::get_config;

static LATEST_STATS: OnceCell<Arc<Mutex<HashMap<String, i64>>>> = OnceCell::new();
static STAT_PERIOD: u64 = 15000; //15 seconds

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
    FlushStatsToStatsD,
}

#[derive(Debug)]
pub struct Event {
    name: EventName,
    value: i64,
    process_id: Option<i32>,
}

#[derive(Clone, Debug)]
pub struct Reporter {
    tx: Sender<Event>,
}

impl Reporter {
    pub fn new(tx: Sender<Event>) -> Reporter {
        Reporter { tx: tx }
    }

    pub fn query(&self) {
        let event = Event {
            name: EventName::Query,
            value: 1,
            process_id: None,
        };

        let _ = self.tx.try_send(event);
    }

    pub fn transaction(&self) {
        let event = Event {
            name: EventName::Transaction,
            value: 1,
            process_id: None,
        };

        let _ = self.tx.try_send(event);
    }

    pub fn data_sent(&self, amount: usize) {
        let event = Event {
            name: EventName::DataSent,
            value: amount as i64,
            process_id: None,
        };

        let _ = self.tx.try_send(event);
    }

    pub fn data_received(&self, amount: usize) {
        let event = Event {
            name: EventName::DataReceived,
            value: amount as i64,
            process_id: None,
        };

        let _ = self.tx.try_send(event);
    }

    pub fn checkout_time(&self, ms: u128) {
        let event = Event {
            name: EventName::CheckoutTime,
            value: ms as i64,
            process_id: None,
        };

        let _ = self.tx.try_send(event);
    }

    pub fn client_waiting(&self, process_id: i32) {
        let event = Event {
            name: EventName::ClientWaiting,
            value: 1,
            process_id: Some(process_id),
        };

        let _ = self.tx.try_send(event);
    }

    pub fn client_active(&self, process_id: i32) {
        let event = Event {
            name: EventName::ClientActive,
            value: 1,
            process_id: Some(process_id),
        };

        let _ = self.tx.try_send(event);
    }

    pub fn client_idle(&self, process_id: i32) {
        let event = Event {
            name: EventName::ClientIdle,
            value: 1,
            process_id: Some(process_id),
        };

        let _ = self.tx.try_send(event);
    }

    pub fn client_disconnecting(&self, process_id: i32) {
        let event = Event {
            name: EventName::ClientDisconnecting,
            value: 1,
            process_id: Some(process_id),
        };

        let _ = self.tx.try_send(event);
    }

    pub fn server_active(&self, process_id: i32) {
        let event = Event {
            name: EventName::ServerActive,
            value: 1,
            process_id: Some(process_id),
        };

        let _ = self.tx.try_send(event);
    }

    pub fn server_idle(&self, process_id: i32) {
        let event = Event {
            name: EventName::ServerIdle,
            value: 1,
            process_id: Some(process_id),
        };

        let _ = self.tx.try_send(event);
    }

    pub fn server_login(&self, process_id: i32) {
        let event = Event {
            name: EventName::ServerLogin,
            value: 1,
            process_id: Some(process_id),
        };

        let _ = self.tx.try_send(event);
    }

    pub fn server_tested(&self, process_id: i32) {
        let event = Event {
            name: EventName::ServerTested,
            value: 1,
            process_id: Some(process_id),
        };

        let _ = self.tx.try_send(event);
    }

    pub fn server_disconnecting(&self, process_id: i32) {
        let event = Event {
            name: EventName::ServerDisconnecting,
            value: 1,
            process_id: Some(process_id),
        };

        let _ = self.tx.try_send(event);
    }

    // pub fn flush_to_statsd(&self) {
    //     let event = Event {
    //         name: EventName::FlushStatsToStatsD,
    //         value: 0,
    //         process_id: None,
    //     };

    //     let _ = self.tx.try_send(event);
    // }
}

pub struct Collector {
    rx: Receiver<Event>,
    tx: Sender<Event>,
    client: Client,
}

impl Collector {
    pub fn new(rx: Receiver<Event>, tx: Sender<Event>) -> Collector {
        Collector {
            rx,
            tx,
            client: Client::new(&get_config().general.statsd_address, "pgcat").unwrap(),
        }
    }

    pub async fn collect(&mut self) {
        info!("Events reporter started");

        match LATEST_STATS.set(Arc::new(Mutex::new(HashMap::new()))) {
            Ok(_) => (),
            Err(_) => {
                error!("Latest stats will not be available");
            }
        };

        let mut stats = HashMap::from([
            ("total_query_count", 0),
            ("total_xact_count", 0),
            ("total_sent", 0),
            ("total_received", 0),
            ("total_xact_time", 0),
            ("total_query_time", 0),
            ("total_wait_time", 0),
            ("avg_xact_time", 0),
            ("avg_query_time", 0),
            ("avg_xact_count", 0),
            ("avg_sent", 0),
            ("avg_received", 0),
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

        // Stats saved after each iteration of the flush event. Used in calculation
        // of averages in the last flush period.
        let mut old_stats: HashMap<String, i64> = HashMap::new();

        // Track which state the client and server are at any given time.
        let mut client_server_states: HashMap<i32, EventName> = HashMap::new();

        // Flush stats to StatsD and calculate averages every 15 seconds.
        let tx = self.tx.clone();
        tokio::task::spawn(async move {
            let mut interval = tokio::time::interval(tokio::time::Duration::from_millis(STAT_PERIOD));
            loop {
                interval.tick().await;
                let _ = tx.try_send(Event {
                    name: EventName::FlushStatsToStatsD,
                    value: 0,
                    process_id: None,
                });
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
                EventName::Query => {
                    let counter = stats.entry("total_query_count").or_insert(0);
                    *counter += stat.value;
                }

                EventName::Transaction => {
                    let counter = stats.entry("total_xact_count").or_insert(0);
                    *counter += stat.value;
                }

                EventName::DataSent => {
                    let counter = stats.entry("total_sent").or_insert(0);
                    *counter += stat.value;
                }

                EventName::DataReceived => {
                    let counter = stats.entry("total_received").or_insert(0);
                    *counter += stat.value;
                }

                EventName::CheckoutTime => {
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
                    client_server_states.insert(stat.process_id.unwrap(), stat.name);
                }

                EventName::ClientDisconnecting | EventName::ServerDisconnecting => {
                    client_server_states.remove(&stat.process_id.unwrap());
                }

                EventName::FlushStatsToStatsD => {
                    // Calculate connection states
                    for (_, state) in &client_server_states {
                        match state {
                            EventName::ClientActive => {
                                let counter = stats.entry("cl_active").or_insert(0);
                                *counter += 1;
                            }

                            EventName::ClientWaiting => {
                                let counter = stats.entry("cl_waiting").or_insert(0);
                                *counter += 1;
                            }

                            EventName::ClientIdle => {
                                let counter = stats.entry("cl_idle").or_insert(0);
                                *counter += 1;
                            }

                            EventName::ServerIdle => {
                                let counter = stats.entry("sv_idle").or_insert(0);
                                *counter += 1;
                            }

                            EventName::ServerActive => {
                                let counter = stats.entry("sv_active").or_insert(0);
                                *counter += 1;
                            }

                            EventName::ServerTested => {
                                let counter = stats.entry("sv_tested").or_insert(0);
                                *counter += 1;
                            }

                            EventName::ServerLogin => {
                                let counter = stats.entry("sv_login").or_insert(0);
                                *counter += 1;
                            }

                            _ => unreachable!(),
                        };
                    }

                    // Calculate averages
                    for stat in &[
                        "avg_query_count",
                        "avgxact_count",
                        "avg_sent",
                        "avg_received",
                        "avg_wait_time",
                    ] {
                        let total_name = stat.replace("avg_", "total_");
                        let old_value = old_stats.entry(total_name.clone()).or_insert(0);
                        let new_value = stats.get(total_name.as_str()).unwrap_or(&0).to_owned();
                        let avg = (new_value - *old_value) / (STAT_PERIOD as i64 / 1_000); // Avg / second

                        stats.insert(stat, avg);
                        *old_value = new_value;
                    }

                    debug!("{:?}", stats);

                    // Update latest stats used in SHOW STATS
                    match LATEST_STATS.get() {
                        Some(arc) => {
                            let mut guard = arc.lock().unwrap();
                            for (key, value) in &stats {
                                guard.insert(key.to_string(), value.clone());
                            }
                        }

                        None => (),
                    };

                    let mut pipeline = self.client.pipeline();

                    for (key, value) in stats.iter() {
                        pipeline.gauge(key, *value as f64);
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

                    pipeline.send(&self.client);
                }
            };
        }
    }
}

pub fn get_stats() -> Option<HashMap<String, i64>> {
    match LATEST_STATS.get() {
        Some(arc) => {
            let guard = arc.lock().unwrap();
            Some(guard.clone())
        }

        None => None,
    }
}
