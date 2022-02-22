use log::info;
use statsd::Client;
/// Events collector and publisher.
use tokio::sync::mpsc::{Receiver, Sender};

use std::collections::HashMap;
use std::time::Instant;

use crate::config::get_config;

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
        let statistic = Event {
            name: EventName::Query,
            value: 1,
            process_id: None,
        };

        let _ = self.tx.try_send(statistic);
    }

    pub fn transaction(&self) {
        let statistic = Event {
            name: EventName::Transaction,
            value: 1,
            process_id: None,
        };

        let _ = self.tx.try_send(statistic);
    }

    pub fn data_sent(&self, amount: usize) {
        let statistic = Event {
            name: EventName::DataSent,
            value: amount as i64,
            process_id: None,
        };

        let _ = self.tx.try_send(statistic);
    }

    pub fn data_received(&self, amount: usize) {
        let statistic = Event {
            name: EventName::DataReceived,
            value: amount as i64,
            process_id: None,
        };

        let _ = self.tx.try_send(statistic);
    }

    pub fn checkout_time(&self, ms: u128) {
        let statistic = Event {
            name: EventName::CheckoutTime,
            value: ms as i64,
            process_id: None,
        };

        let _ = self.tx.try_send(statistic);
    }

    pub fn client_waiting(&self, process_id: i32) {
        let statistic = Event {
            name: EventName::ClientWaiting,
            value: 1,
            process_id: Some(process_id),
        };

        let _ = self.tx.try_send(statistic);
    }

    pub fn client_active(&self, process_id: i32) {
        let statistic = Event {
            name: EventName::ClientActive,
            value: 1,
            process_id: Some(process_id),
        };

        let _ = self.tx.try_send(statistic);
    }

    pub fn client_idle(&self, process_id: i32) {
        let statistic = Event {
            name: EventName::ClientIdle,
            value: 1,
            process_id: Some(process_id),
        };

        let _ = self.tx.try_send(statistic);
    }

    pub fn client_disconnecting(&self, process_id: i32) {
        let statistic = Event {
            name: EventName::ClientDisconnecting,
            value: 1,
            process_id: Some(process_id),
        };

        let _ = self.tx.try_send(statistic);
    }

    pub fn server_active(&self, process_id: i32) {
        let statistic = Event {
            name: EventName::ServerActive,
            value: 1,
            process_id: Some(process_id),
        };

        let _ = self.tx.try_send(statistic);
    }

    pub fn server_idle(&self, process_id: i32) {
        let statistic = Event {
            name: EventName::ServerIdle,
            value: 1,
            process_id: Some(process_id),
        };

        let _ = self.tx.try_send(statistic);
    }

    pub fn server_login(&self, process_id: i32) {
        let statistic = Event {
            name: EventName::ServerLogin,
            value: 1,
            process_id: Some(process_id),
        };

        let _ = self.tx.try_send(statistic);
    }

    pub fn server_tested(&self, process_id: i32) {
        let statistic = Event {
            name: EventName::ServerTested,
            value: 1,
            process_id: Some(process_id),
        };

        let _ = self.tx.try_send(statistic);
    }

    pub fn server_disconnecting(&self, process_id: i32) {
        let statistic = Event {
            name: EventName::ServerDisconnecting,
            value: 1,
            process_id: Some(process_id),
        };

        let _ = self.tx.try_send(statistic);
    }
}

pub struct Collector {
    rx: Receiver<Event>,
    client: Client,
}

impl Collector {
    pub fn new(rx: Receiver<Event>) -> Collector {
        Collector {
            rx: rx,
            client: Client::new(&get_config().general.statsd_address, "pgcat").unwrap(),
        }
    }

    pub async fn collect(&mut self) {
        info!("Events reporter started");

        let mut stats = HashMap::from([
            ("total_query_count", 0),
            ("total_xact_count", 0),
            ("total_sent", 0),
            ("total_received", 0),
            ("total_wait_time", 0),
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

        let mut client_server_states: HashMap<i32, EventName> = HashMap::new();

        let mut now = Instant::now();

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

                    // Report max time here
                    if stat.value > *counter {
                        *counter = stat.value;
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
            };

            // It's been 15 seconds. If there is no traffic, it won't publish anything,
            // but it also doesn't matter then.
            if now.elapsed().as_secs() > 15 {
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

                info!("{:?}", stats);

                let mut pipeline = self.client.pipeline();

                for (key, value) in stats.iter_mut() {
                    pipeline.gauge(key, *value as f64);
                    *value = 0;
                }

                pipeline.send(&self.client);

                now = Instant::now();
            }
        }
    }
}
