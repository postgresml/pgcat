use arc_swap::ArcSwap;
use log::error;
use once_cell::sync::Lazy;
use std::collections::HashMap;
use std::sync::Arc;

use crate::config::get_config;
use crate::config::Address;
use tokio::sync::mpsc;
use tokio::sync::mpsc::error::TrySendError;
use tokio::sync::mpsc::{channel, Receiver, Sender};
use tokio::time::Duration;
use tokio::time::Instant;

pub type GBanList = HashMap<(String, String), HashMap<Address, BanEntry>>;

#[derive(Debug, Clone, Copy)]
pub enum BanReason {
    FailedHealthCheck,
    MessageSendFailed,
    MessageReceiveFailed,
    FailedCheckout,
    StatementTimeout,
    #[allow(dead_code)]
    ManualBan,
}
#[derive(Debug, Clone)]
pub struct BanEntry {
    reason: BanReason,
    time: Instant,
    duration: Duration,
}

#[derive(Debug, Clone)]
pub enum BanManagerEvent {
    Ban {
        address: Address,
        pool_name: String,
        username: String,
        reason: BanReason,
    },
    Unban {
        address: Address,
        pool_name: String,
        username: String,
    },
    CleanUpBanList,
}

impl BanEntry {
    pub fn has_expired(&self) -> bool {
        return Instant::now().duration_since(self.time) > self.duration;
    }

    pub fn is_active(&self) -> bool {
        !self.has_expired()
    }
}
static BANLIST: Lazy<ArcSwap<GBanList>> = Lazy::new(|| ArcSwap::from_pointee(GBanList::default()));

static BAN_REPORTER: Lazy<ArcSwap<BanReporter>> =
    Lazy::new(|| ArcSwap::from_pointee(BanReporter::default()));

#[derive(Clone, Debug)]
pub struct BanReporter {
    channel_to_worker: Sender<BanManagerEvent>,
}

impl Default for BanReporter {
    fn default() -> BanReporter {
        let (channel_to_worker, _rx) = channel(1000);
        BanReporter { channel_to_worker }
    }
}

impl BanReporter {
    /// Create a new Reporter instance.
    pub fn new(channel_to_worker: Sender<BanManagerEvent>) -> BanReporter {
        BanReporter { channel_to_worker }
    }

    /// Send statistics to the task keeping track of stats.
    fn send(&self, event: BanManagerEvent) {
        let result = self.channel_to_worker.try_send(event.clone());

        match result {
            Ok(_) => (()),
            Err(err) => match err {
                TrySendError::Full { .. } => error!("event dropped, buffer full"),
                TrySendError::Closed { .. } => error!("event dropped, channel closed"),
            },
        };
    }

    pub fn report_failed_checkout(&self, pool_name: String, username: String, address: Address) {
        let event = BanManagerEvent::Ban {
            address: address,
            pool_name: pool_name,
            username: username,
            reason: BanReason::FailedCheckout,
        };
        self.send(event);
    }


    pub fn report_failed_healthcheck(&self, pool_name: String, username: String, address: Address) {
        let event = BanManagerEvent::Ban {
            address: address,
            pool_name: pool_name,
            username: username,
            reason: BanReason::FailedHealthCheck,
        };
        self.send(event);
    }

    pub fn report_server_send_failed(&self, pool_name: String, username: String, address: Address) {
        let event = BanManagerEvent::Ban {
            address: address,
            pool_name: pool_name,
            username: username,
            reason: BanReason::MessageSendFailed,
        };
        self.send(event);
    }

    pub fn report_server_receive_failed(&self, pool_name: String, username: String, address: Address) {
        let event = BanManagerEvent::Ban {
            address: address,
            pool_name: pool_name,
            username: username,
            reason: BanReason::MessageReceiveFailed,
        };
        self.send(event);
    }

    pub fn report_statement_timeout(&self, pool_name: String, username: String, address: Address) {
        let event = BanManagerEvent::Ban {
            address: address,
            pool_name: pool_name,
            username: username,
            reason: BanReason::StatementTimeout,
        };
        self.send(event);
    }

    #[allow(dead_code)]
    pub fn report_manual_ban(&self, pool_name: String, username: String, address: Address) {
        let event = BanManagerEvent::Ban {
            address: address,
            pool_name: pool_name,
            username: username,
            reason: BanReason::ManualBan,
        };
        self.send(event);
    }

    pub fn unban(&self, pool_name: String, username: String, address: Address) {
        let event = BanManagerEvent::Unban {
            address: address,
            pool_name: pool_name,
            username: username,
        };
        self.send(event);
    }

    pub fn banlist(&self, pool_name: String, username: String) -> HashMap<Address, BanEntry> {
        let k = (pool_name, username);
        match (*(*BANLIST.load())).get(&k) {
            Some(banlist) => banlist.clone(),
            None => HashMap::default(),
        }
    }

    #[allow(dead_code)]
    pub fn is_banned(&self, pool_name: String, username: String, address: Address) -> bool {
        let k = (pool_name, username);
        match (*(*BANLIST.load())).get(&k) {
            Some(pool_banlist) => match pool_banlist.get(&address) {
                Some(ban_entry) => ban_entry.is_active(),
                None => false,
            },
            None => false,
        }
    }
}

pub struct BanWorker {
    work_queue_tx: Sender<BanManagerEvent>,
    work_queue_rx: Receiver<BanManagerEvent>,
}

impl BanWorker {
    pub fn new() -> BanWorker {
        let (work_queue_tx, work_queue_rx) = mpsc::channel(100_000);
        BanWorker {
            work_queue_tx,
            work_queue_rx,
        }
    }

    pub fn get_reporter(&self) -> BanReporter {
        BanReporter::new(self.work_queue_tx.clone())
    }

    pub async fn start(&mut self) {
        let mut internal_ban_list: GBanList = GBanList::default();
        let tx = self.work_queue_tx.clone();

        tokio::task::spawn(async move {
            let mut interval = tokio::time::interval(tokio::time::Duration::from_secs(60));
            loop {
                interval.tick().await;
                match tx.try_send(BanManagerEvent::CleanUpBanList) {
                    Ok(_) => (),
                    Err(err) => match err {
                        TrySendError::Full(_) => (),
                        TrySendError::Closed(_) => (),
                    },
                }
            }
        });

        loop {
            let event = match self.work_queue_rx.recv().await {
                Some(stat) => stat,
                None => {
                    return;
                }
            };

            match event {
                BanManagerEvent::Ban {
                    address,
                    pool_name,
                    username,
                    reason,
                } => {
                    if self.ban(&mut internal_ban_list, address, pool_name, username, reason) {
                        // Ban list was changed, let's publish a new one
                        self.publish_banlist(&internal_ban_list);
                    }
                }
                BanManagerEvent::Unban {
                    address,
                    pool_name,
                    username,
                } => {
                    if self.unban(&mut internal_ban_list, address, pool_name, username) {
                        // Ban list was changed, let's publish a new one
                        self.publish_banlist(&internal_ban_list);
                    }
                }
                BanManagerEvent::CleanUpBanList => {
                    self.cleanup_ban_list(&mut internal_ban_list);
                }
            };
        }
    }

    fn publish_banlist(&self, internal_ban_list: &GBanList) {
        BANLIST.store(Arc::new(internal_ban_list.clone()));
    }

    fn cleanup_ban_list(&self, internal_ban_list: &mut GBanList) {
        for (_, v) in internal_ban_list {
            v.retain(|_k, v| v.is_active());
        }
    }

    fn unban(
        &self,
        internal_ban_list: &mut GBanList,
        address: Address,
        pool_name: String,
        username: String,
    ) -> bool {
        let k = (pool_name, username);
        match internal_ban_list.get_mut(&k) {
            Some(banlist) => {
                if banlist.remove(&address).is_none() {
                    // Was Already not banned? Let's avoid publishing a new list
                    return false;
                }
            }
            None => return false, // Was Already not banned? Let's avoid publishing a new list
        }
        return true;
    }

    fn ban(
        &self,
        internal_ban_list: &mut GBanList,
        address: Address,
        pool_name: String,
        username: String,
        reason: BanReason,
    ) -> bool {
        let k = (pool_name.clone(), username.clone());
        let ban_time = Instant::now(); // Technically, ban time is when client made the call but this should be close enough
        let config = get_config();
        let ban_duration = match reason {
            BanReason::FailedHealthCheck
            | BanReason::MessageReceiveFailed
            | BanReason::MessageSendFailed
            | BanReason::FailedCheckout
            | BanReason::StatementTimeout => {
                Duration::from_secs(config.general.ban_time.try_into().unwrap())
            }
            BanReason::ManualBan => Duration::from_secs(86400),
        };

        let pool_banlist = internal_ban_list.entry(k).or_insert(HashMap::default());

        let ban_entry = pool_banlist.entry(address.clone()).or_insert(BanEntry {
            reason: reason,
            time: ban_time,
            duration: ban_duration,
        });

        let old_banned_until = ban_entry.time + ban_entry.duration;
        let new_banned_until = ban_time + ban_duration;
        if new_banned_until >= old_banned_until {
            ban_entry.duration = ban_duration;
            ban_entry.time = ban_time;
            ban_entry.reason = reason;
        }

        return true;
    }
}

pub fn start_ban_manager() {
    let mut worker = BanWorker::new();
    BAN_REPORTER.store(Arc::new(worker.get_reporter()));

    tokio::task::spawn(async move { worker.start().await });
}

pub fn get_ban_handler() -> BanReporter {
    return (*(*BAN_REPORTER.load())).clone();
}
