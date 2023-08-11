use crate::query::Query;
use rand::Rng;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use log::debug;

#[derive(Debug, PartialEq, Eq, Hash, Clone)]
pub struct Key {
    pub query: Query,
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
    pub is_enabled: bool,
    pub capacity: usize,
    pub statistics: HashMap<Key, Value>,
}

impl Default for QueryResultStats {
    fn default() -> Self {
        QueryResultStats {
            is_enabled: true,
            capacity: 1000,
            statistics: HashMap::new(),
        }
    }
}

impl QueryResultStats {
    pub(crate) fn new(is_enabled: bool, capacity: usize) -> QueryResultStats {
        // TODO expose those as better config options
        QueryResultStats {
            is_enabled,
            capacity,
            statistics: HashMap::new(),
        }
    }

    fn random_entry(&mut self) -> Option<(Key, Value)> {
        let mut rng = rand::thread_rng();
        let index = rng.gen_range(0..self.statistics.len());
        let key = self.statistics.keys().nth(index).cloned();
        self.statistics.remove_entry(&key.unwrap())
    }

    fn evict(&mut self) {
        if self.statistics.len() < self.capacity {
            return;
        }

        let entry_to_keep = {
            // random-2 lru
            let first_entry = self.random_entry();
            let second_entry = self.random_entry();

            match (first_entry, second_entry) {
                (Some(first), Some(second)) => {
                    if first.1.last_seen < second.1.last_seen {
                        debug!("Evicting {:?}", second.0);
                        Some(first)
                    } else {
                        debug!("Evicting {:?}", first.0);
                        Some(second)
                    }
                }
                _ => None,
            }
        };

        if let Some(entry_to_keep) = entry_to_keep {
            self.statistics.insert(entry_to_keep.0, entry_to_keep.1);
        }
    }

    pub fn insert(&mut self, query: Query, result_hash: Vec<u8>) {
        if !self.is_enabled {
            return;
        }
        if !query.is_select() {
            return;
        }

        self.evict();

        self.statistics
            .entry(Key { query, result_hash })
            .and_modify(|v| {
                v.count.fetch_add(1, Ordering::Relaxed);
                v.last_seen = chrono::Utc::now();
            })
            .or_insert(Value {
                count: Arc::new(AtomicU64::new(1)),
                first_seen: chrono::Utc::now(),
                last_seen: chrono::Utc::now(),
            });
    }
}
