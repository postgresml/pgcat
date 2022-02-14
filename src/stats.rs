use statsd::Client;
/// Statistics collector and publisher.
use tokio::sync::mpsc::{Receiver, Sender};

use std::collections::HashMap;
use std::time::Instant;

#[derive(Debug)]
pub enum StatisticName {
    CheckoutTime,
    //QueryRuntime,
    //TransactionTime,
    Queries,
    Transactions,
    DataSent,
    DataReceived,
}

#[derive(Debug)]
pub struct Statistic {
    pub name: StatisticName,
    pub value: i64,
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
        };

        let _ = self.tx.try_send(statistic);
    }

    pub fn transaction(&mut self) {
        let statistic = Statistic {
            name: StatisticName::Transactions,
            value: 1,
        };

        let _ = self.tx.try_send(statistic);
    }

    pub fn data_sent(&mut self, amount: usize) {
        let statistic = Statistic {
            name: StatisticName::DataSent,
            value: amount as i64,
        };

        let _ = self.tx.try_send(statistic);
    }

    pub fn data_received(&mut self, amount: usize) {
        let statistic = Statistic {
            name: StatisticName::DataReceived,
            value: amount as i64,
        };

        let _ = self.tx.try_send(statistic);
    }

    pub fn checkout_time(&mut self, ms: u128) {
        let statistic = Statistic {
            name: StatisticName::CheckoutTime,
            value: ms as i64,
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
            client: Client::new("127.0.0.1:8125", "pgcat").unwrap(),
        }
    }

    pub async fn collect(&mut self) {
        let mut stats = HashMap::from([
            ("queries", 0),
            ("transactions", 0),
            ("data_sent", 0),
            ("data_received", 0),
            ("checkout_time", 0),
        ]);
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
                    let counter = stats.entry("queries").or_insert(0);
                    *counter += stat.value;
                }

                StatisticName::Transactions => {
                    let counter = stats.entry("transactions").or_insert(0);
                    *counter += stat.value;
                }

                StatisticName::DataSent => {
                    let counter = stats.entry("data_sent").or_insert(0);
                    *counter += stat.value;
                }

                StatisticName::DataReceived => {
                    let counter = stats.entry("data_received").or_insert(0);
                    *counter += stat.value;
                }

                StatisticName::CheckoutTime => {
                    let counter = stats.entry("checkout_time").or_insert(0);

                    // Report max time here
                    if stat.value > *counter {
                        *counter = stat.value;
                    }
                }
            };

            // It's been 15 seconds. If there is no traffic, it won't publish anything,
            // but it also doesn't matter then.
            if now.elapsed().as_secs() > 15 {
                let mut pipeline = self.client.pipeline();

                println!(">> Publishing statistics to StatsD: {:?}", stats);

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
