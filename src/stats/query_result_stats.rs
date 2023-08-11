use crate::config::get_config;
use crate::errors::Error;
use crate::Query;
use rand::Rng;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

pub fn hash_string(hash: &[u8]) -> String {
    // TODO assert length == 256
    let first = u128::from_ne_bytes(hash[0..16].try_into().unwrap());
    let second = u128::from_ne_bytes(hash[0..16].try_into().unwrap());
    format!("{:x}{:x}", first, second)
}

#[derive(Debug, PartialEq, Eq, Hash, Clone)]
pub struct Key {
    pub normalized: String,
    pub fingerprint: u64,
    pub query_hash: Vec<u8>,
    pub result_hash: Vec<u8>,
}

#[derive(Debug)]
pub struct Value {
    pub count: Arc<AtomicU64>,
    pub first_seen: chrono::DateTime<chrono::Utc>,
    pub last_seen: chrono::DateTime<chrono::Utc>,
}

impl Value {
    pub fn duration(&self) -> chrono::Duration {
        self.last_seen - self.first_seen
    }
}

#[derive(Debug)]
pub struct QueryResultStats {
    is_enabled: bool,
    // between 0 and 100
    sampling_rate: usize,
    // TODO limit size of hash to top 50
    pub statistics: HashMap<Key, Value>,
}

impl Default for QueryResultStats {
    fn default() -> Self {
        QueryResultStats {
            sampling_rate: 100,
            is_enabled: false,
            statistics: HashMap::new(),
        }
    }
}


// TODO rename to QueryResultStats
impl QueryResultStats {
    pub(crate) fn new() -> QueryResultStats {
        let config = get_config();
        let is_enabled = config
            .query_cache
            .clone()
            .map(|c| c.collect_stats)
            .unwrap_or(false);
        let sampling_rate =
            (&config.query_cache.map(|c| c.sample_rate).unwrap_or(0.0) * 100.0) as usize;
        QueryResultStats {
            is_enabled,
            sampling_rate,
            statistics: HashMap::new(),
        }
    }

    fn should_sample(&self) -> bool {
        rand::thread_rng().gen_range(0..100) < self.sampling_rate
    }

    pub fn insert(&mut self, query: &Query, result_hash: Vec<u8>) -> Result<(), Error> {
        if !self.is_enabled {
            return Ok(());
        }
        if !query.is_read {
            return Ok(());
        }
        if !self.should_sample() {
            return Ok(());
        }

        let key = Key {
            normalized: query.normalized.clone(),
            fingerprint: query.fingerprint,
            query_hash: query.hash.clone(),
            result_hash,
        };

        self.statistics
            .entry(key)
            .and_modify(|v| {
                v.count.fetch_add(1, Ordering::Relaxed);
                v.last_seen = chrono::Utc::now();
            })
            .or_insert(Value {
                count: Arc::new(AtomicU64::new(1)),
                first_seen: chrono::Utc::now(),
                last_seen: chrono::Utc::now(),
            });

        Ok(())
    }
}
