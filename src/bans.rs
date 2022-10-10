use arc_swap::ArcSwap;

use once_cell::sync::Lazy;
use parking_lot::Mutex;
use std::collections::HashMap;
use std::sync::Arc;

use crate::config::get_ban_time;
use crate::config::Address;
use crate::pool::PoolIdentifier;
use tokio::time::Duration;
use tokio::time::Instant;
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
impl BanEntry {
    pub fn has_expired(&self) -> bool {
        return Instant::now().duration_since(self.time) > self.duration;
    }

    pub fn is_active(&self) -> bool {
        !self.has_expired()
    }
}
type BanList = HashMap<PoolIdentifier, HashMap<Address, BanEntry>>;
static BANLIST: Lazy<ArcSwap<BanList>> = Lazy::new(|| ArcSwap::from_pointee(BanList::default()));
static BANLIST_MUTEX: Lazy<Mutex<u8>> = Lazy::new(|| Mutex::new(0));

pub fn unban(pool_id: &PoolIdentifier, address: &Address) {
    if !is_banned(pool_id, address) {
        // Already not banned? No need to do any work
        return;
    }
    let _guard = BANLIST_MUTEX.lock();
    if !is_banned(pool_id, address) {
        // Maybe it was unbanned between our initial check and locking the mutex
        // In that case, we don't need to do any work
        return;
    }

    let mut global_banlist = (**BANLIST.load()).clone();

    match global_banlist.get_mut(pool_id) {
        Some(pool_banlist) => {
            if pool_banlist.remove(&address).is_none() {
                // Was already not banned? Let's avoid publishing a new list
                return;
            } else {
                // Banlist was updated, let's publish a new version for readers
                BANLIST.store(Arc::new(global_banlist));
            }
        }
        None => return, // Was already not banned? Let's avoid publishing a new list
    }
}

fn ban(pool_id: &PoolIdentifier, address: &Address, reason: BanReason) {
    if is_banned(pool_id, address) {
        // Already banned? No need to do any work
        return;
    }
    let _guard = BANLIST_MUTEX.lock();
    if is_banned(pool_id, address) {
        // Maybe it was banned between our initial check and locking the mutex
        // In that case, we don't need to do any work
        return;
    }

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

    let ban_time = Instant::now();
    let mut global_banlist = (**BANLIST.load()).clone();
    let pool_banlist = global_banlist.entry(pool_id.clone()).or_insert(HashMap::default());

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

    // Clean up
    pool_banlist.retain(|_k, v| v.is_active());

    BANLIST.store(Arc::new(global_banlist));
}

pub fn report_failed_checkout(pool_id: &PoolIdentifier, address: &Address) {
    ban(pool_id, address, BanReason::FailedCheckout);
}

pub fn report_failed_healthcheck(pool_id: &PoolIdentifier, address: &Address) {
    ban(pool_id, address, BanReason::FailedHealthCheck);
}

pub fn report_server_send_failed(pool_id: &PoolIdentifier, address: &Address) {
    ban(pool_id, address, BanReason::MessageSendFailed);
}

pub fn report_server_receive_failed(pool_id: &PoolIdentifier, address: &Address) {
    ban(pool_id, address, BanReason::MessageReceiveFailed);
}

pub fn report_statement_timeout(pool_id: &PoolIdentifier, address: &Address) {
    ban(pool_id, address, BanReason::StatementTimeout);
}

#[allow(dead_code)]
pub fn report_manual_ban(pool_id: &PoolIdentifier, address: &Address) {
    ban(pool_id, address, BanReason::ManualBan);
}

pub fn banlist(pool_id: &PoolIdentifier) -> HashMap<Address, BanEntry> {
    match (**BANLIST.load()).get(pool_id) {
        Some(banlist) => banlist.clone(),
        None => HashMap::default(),
    }
}

pub fn is_banned(pool_id: &PoolIdentifier, address: &Address) -> bool {
    match (**BANLIST.load()).get(pool_id) {
        Some(pool_banlist) => match pool_banlist.get(address) {
            Some(ban_entry) => ban_entry.is_active(),
            None => false,
        },
        None => false,
    }
}
