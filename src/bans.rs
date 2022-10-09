use arc_swap::ArcSwap;
use log::error;
use once_cell::sync::Lazy;
use std::collections::HashMap;
use std::sync::Arc;

use crate::config::get_ban_time;
use crate::config::Address;
use crate::pool::PoolIdentifier;
use tokio::sync::mpsc;
use tokio::sync::mpsc::error::TrySendError;
use tokio::sync::mpsc::{channel, Receiver, Sender};
use tokio::time::Duration;
use tokio::time::Instant;

pub type BanList = HashMap<PoolIdentifier, HashMap<Address, BanEntry>>;

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
pub enum BanEvent {
    Ban {
        pool_id: PoolIdentifier,
        address: Address,
        reason: BanReason,
    },
    Unban {
        pool_id: PoolIdentifier,
        address: Address,
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
static BANLIST: Lazy<ArcSwap<BanList>> = Lazy::new(|| ArcSwap::from_pointee(BanList::default()));

static BAN_MANAGER: Lazy<ArcSwap<BanManager>> =
    Lazy::new(|| ArcSwap::from_pointee(BanManager::default()));

#[derive(Clone, Debug)]
pub struct BanManager {
    channel_to_worker: Sender<BanEvent>,
}

impl Default for BanManager {
    fn default() -> BanManager {
        let (channel_to_worker, _rx) = channel(1000);
        BanManager { channel_to_worker }
    }
}

impl BanManager {
    /// Create a new Reporter instance.
    pub fn new(channel_to_worker: Sender<BanEvent>) -> BanManager {
        BanManager { channel_to_worker }
    }

    /// Send statistics to the task keeping track of stats.
    async fn send(&self, event: BanEvent) {
        let result = self.channel_to_worker.send(event.clone()).await;

        match result {
            Ok(_) => (()),
            Err(err) => error!("Failed to send ban event {:?}", err),
        };
    }

    pub async fn report_failed_checkout(&self, pool_id: &PoolIdentifier, address: &Address) {
        let event = BanEvent::Ban {
            pool_id: pool_id.clone(),
            address: address.clone(),
            reason: BanReason::FailedCheckout,
        };
        self.send(event).await
    }

    pub async fn report_failed_healthcheck(&self, pool_id: &PoolIdentifier, address: &Address) {
        let event = BanEvent::Ban {
            pool_id: pool_id.clone(),
            address: address.clone(),
            reason: BanReason::FailedHealthCheck,
        };
        self.send(event).await
    }

    pub async fn report_server_send_failed(&self, pool_id: &PoolIdentifier, address: &Address) {
        let event = BanEvent::Ban {
            pool_id: pool_id.clone(),
            address: address.clone(),
            reason: BanReason::MessageSendFailed,
        };
        self.send(event).await
    }

    pub async fn report_server_receive_failed(&self, pool_id: &PoolIdentifier, address: &Address) {
        let event = BanEvent::Ban {
            pool_id: pool_id.clone(),
            address: address.clone(),
            reason: BanReason::MessageReceiveFailed,
        };
        self.send(event).await
    }

    pub async fn report_statement_timeout(&self, pool_id: &PoolIdentifier, address: &Address) {
        let event = BanEvent::Ban {
            pool_id: pool_id.clone(),
            address: address.clone(),
            reason: BanReason::StatementTimeout,
        };
        self.send(event).await
    }

    #[allow(dead_code)]
    pub async fn report_manual_ban(&self, pool_id: &PoolIdentifier, address: &Address) {
        let event = BanEvent::Ban {
            pool_id: pool_id.clone(),
            address: address.clone(),
            reason: BanReason::ManualBan,
        };
        self.send(event).await
    }

    pub async fn unban(&self, pool_id: &PoolIdentifier, address: &Address) {
        let event = BanEvent::Unban {
            pool_id: pool_id.clone(),
            address: address.clone(),
        };
        self.send(event).await;
    }

    pub fn banlist(&self, pool_id: &PoolIdentifier) -> HashMap<Address, BanEntry> {
        match (*(*BANLIST.load())).get(pool_id) {
            Some(banlist) => banlist.clone(),
            None => HashMap::default(),
        }
    }

    #[allow(dead_code)]
    pub fn is_banned(&self, pool_id: &PoolIdentifier, address: &Address) -> bool {
        match (*(*BANLIST.load())).get(pool_id) {
            Some(pool_banlist) => match pool_banlist.get(address) {
                Some(ban_entry) => ban_entry.is_active(),
                None => false,
            },
            None => false,
        }
    }
}

pub struct BanWorker {
    work_queue_tx: Sender<BanEvent>,
    work_queue_rx: Receiver<BanEvent>,
}

impl BanWorker {
    pub fn new() -> BanWorker {
        let (work_queue_tx, work_queue_rx) = mpsc::channel(100_000);
        BanWorker {
            work_queue_tx,
            work_queue_rx,
        }
    }

    pub fn get_reporter(&self) -> BanManager {
        BanManager::new(self.work_queue_tx.clone())
    }

    pub async fn start(&mut self) {
        let mut internal_ban_list: BanList = BanList::default();
        let tx = self.work_queue_tx.clone();

        tokio::task::spawn(async move {
            let mut interval = tokio::time::interval(tokio::time::Duration::from_secs(60));
            loop {
                interval.tick().await;
                match tx.try_send(BanEvent::CleanUpBanList) {
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
                BanEvent::Ban {
                    pool_id,
                    address,
                    reason,
                } => {
                    if self.ban(&mut internal_ban_list, &pool_id, &address, reason) {
                        // Ban list was changed, let's publish a new one
                        self.publish_banlist(&internal_ban_list);
                    }
                }
                BanEvent::Unban { pool_id, address } => {
                    if self.unban(&mut internal_ban_list, &pool_id, &address) {
                        // Ban list was changed, let's publish a new one
                        self.publish_banlist(&internal_ban_list);
                    }
                }
                BanEvent::CleanUpBanList => {
                    self.cleanup_ban_list(&mut internal_ban_list);
                }
            };
        }
    }

    fn publish_banlist(&self, internal_ban_list: &BanList) {
        BANLIST.store(Arc::new(internal_ban_list.clone()));
    }

    fn cleanup_ban_list(&self, internal_ban_list: &mut BanList) {
        for (_, v) in internal_ban_list {
            v.retain(|_k, v| v.is_active());
        }
    }

    fn unban(
        &self,
        internal_ban_list: &mut BanList,
        pool_id: &PoolIdentifier,
        address: &Address,
    ) -> bool {
        match internal_ban_list.get_mut(pool_id) {
            Some(banlist) => {
                if banlist.remove(&address).is_none() {
                    // Was already not banned? Let's avoid publishing a new list
                    return false;
                }
            }
            None => return false, // Was already not banned? Let's avoid publishing a new list
        }
        return true;
    }

    fn ban(
        &self,
        internal_ban_list: &mut BanList,
        pool_id: &PoolIdentifier,
        address: &Address,
        reason: BanReason,
    ) -> bool {
        let ban_duration_from_conf = get_ban_time();
        let ban_duration = match reason {
            BanReason::FailedHealthCheck
            | BanReason::MessageReceiveFailed
            | BanReason::MessageSendFailed
            | BanReason::FailedCheckout
            | BanReason::StatementTimeout => {
                Duration::from_secs(ban_duration_from_conf.try_into().unwrap())
            }
            BanReason::ManualBan => Duration::from_secs(86400),
        };

        // Technically, ban time is when client made the call but this should be close enough
        let ban_time = Instant::now();

        let pool_banlist = internal_ban_list
            .entry(pool_id.clone())
            .or_insert(HashMap::default());

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

pub fn start_ban_worker() {
    let mut worker = BanWorker::new();
    BAN_MANAGER.store(Arc::new(worker.get_reporter()));

    tokio::task::spawn(async move { worker.start().await });
}

pub fn get_ban_manager() -> BanManager {
    return (*(*BAN_MANAGER.load())).clone();
}
