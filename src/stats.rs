use statsd::Client;
/// Statistics collector and publisher.
use tokio::sync::mpsc::{Receiver, Sender};

use std::collections::HashMap;
use std::time::Instant;

use crate::config::get_config;

#[derive(Debug, Clone, Copy)]
pub enum StatisticName {
    CheckoutTime,
    //QueryRuntime,
    //TransactionTime,
    Queries,
    Transactions,
    DataSent,
    DataReceived,
    ClientWaiting,
    ClientActive,
    ClientIdle,
    ClientDisconnecting,
}

#[derive(Debug)]
pub struct Statistic {
    pub name: StatisticName,
    pub value: i64,
    pub process_id: Option<i32>,
}

#[derive(Clone, Debug)]
pub struct Reporter {
    tx: Sender<Statistic>,
}

impl Reporter {
    pub fn new(tx: Sender<Statistic>) -> Reporter {
        Reporter { tx: tx }
    }

    pub fn query(&mut self) {
        let statistic = Statistic {
            name: StatisticName::Queries,
            value: 1,
            process_id: None,
        };

        let _ = self.tx.try_send(statistic);
    }

    pub fn transaction(&mut self) {
        let statistic = Statistic {
            name: StatisticName::Transactions,
            value: 1,
            process_id: None,
        };

        let _ = self.tx.try_send(statistic);
    }

    pub fn data_sent(&mut self, amount: usize) {
        let statistic = Statistic {
            name: StatisticName::DataSent,
            value: amount as i64,
            process_id: None,
        };

        let _ = self.tx.try_send(statistic);
    }

    pub fn data_received(&mut self, amount: usize) {
        let statistic = Statistic {
            name: StatisticName::DataReceived,
            value: amount as i64,
            process_id: None,
        };

        let _ = self.tx.try_send(statistic);
    }

    pub fn checkout_time(&mut self, ms: u128) {
        let statistic = Statistic {
            name: StatisticName::CheckoutTime,
            value: ms as i64,
            process_id: None,
        };

        let _ = self.tx.try_send(statistic);
    }

    pub fn client_waiting(&mut self, process_id: i32) {
        let statistic = Statistic {
            name: StatisticName::ClientWaiting,
            value: 1,
            process_id: Some(process_id),
        };

        let _ = self.tx.try_send(statistic);
    }

    pub fn client_active(&mut self, process_id: i32) {

        let statistic = Statistic {
            name: StatisticName::ClientActive,
            value: 1,
            process_id: Some(process_id),
        };

        let _ = self.tx.try_send(statistic);
    }

    pub fn client_idle(&mut self, process_id: i32) {
        let statistic = Statistic {
            name: StatisticName::ClientIdle,
            value: 1,
            process_id: Some(process_id),
        };

        let _ = self.tx.try_send(statistic);
    }

    pub fn client_disconnecting(&mut self, process_id: i32) {
        let statistic = Statistic {
            name: StatisticName::ClientDisconnecting,
            value: 1,
            process_id: Some(process_id),
        };

        let _ = self.tx.try_send(statistic);
    }
}

pub struct Collector {
    rx: Receiver<Statistic>,
    client: Client,
}

impl Collector {
    pub fn new(rx: Receiver<Statistic>) -> Collector {
        Collector {
            rx: rx,
            client: Client::new(&get_config().general.statsd_address, "pgcat").unwrap(),
        }
    }

    pub async fn collect(&mut self) {
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
        ]);

        let mut client_states: HashMap<i32, StatisticName> = HashMap::new();

        let mut now = Instant::now();

        loop {
            let stat = match self.rx.recv().await {
                Some(stat) => stat,
                None => {
                    println!(">> Statistics collector is shutting down.");
                    return;
                }
            };

            // Some are counters, some are gauges...
            match stat.name {
                StatisticName::Queries => {
                    let counter = stats.entry("total_query_count").or_insert(0);
                    *counter += stat.value;
                }

                StatisticName::Transactions => {
                    let counter = stats.entry("total_xact_count").or_insert(0);
                    *counter += stat.value;
                }

                StatisticName::DataSent => {
                    let counter = stats.entry("total_sent").or_insert(0);
                    *counter += stat.value;
                }

                StatisticName::DataReceived => {
                    let counter = stats.entry("total_received").or_insert(0);
                    *counter += stat.value;
                }

                StatisticName::CheckoutTime => {
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

                StatisticName::ClientActive | StatisticName::ClientWaiting | StatisticName::ClientIdle => {
                    client_states.insert(stat.process_id.unwrap(), stat.name);
                }

                StatisticName::ClientDisconnecting => {
                    client_states.remove(&stat.process_id.unwrap());
                }
            };


            // It's been 15 seconds. If there is no traffic, it won't publish anything,
            // but it also doesn't matter then.
            if now.elapsed().as_secs() > 15 {
                for (_, state) in &client_states {
                    match state {
                        StatisticName::ClientActive => {
                            let counter = stats.entry("cl_active").or_insert(0);
                            *counter += 1;
                        }

                        StatisticName::ClientWaiting => {
                            let counter = stats.entry("cl_waiting").or_insert(0);
                            *counter += 1;
                        }

                        StatisticName::ClientIdle => {
                            let counter = stats.entry("cl_idle").or_insert(0);
                            *counter += 1;
                        }

                        _ => unreachable!(),
                    };
                }

                println!(">> Reporting to StatsD: {:?}", stats);

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
