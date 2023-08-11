use crate::query::Query;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

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
    is_enabled: bool,
    // TODO limit size of hash to top 50
    pub statistics: HashMap<Key, Value>,
}

impl Default for QueryResultStats {
    fn default() -> Self {
        QueryResultStats {
            is_enabled: false,
            statistics: HashMap::new(),
        }
    }
}

impl QueryResultStats {
    pub(crate) fn new() -> QueryResultStats {
        QueryResultStats {
            is_enabled: true,
            statistics: HashMap::new(),
        }
    }

    pub fn insert(&mut self, query: Query, result_hash: Vec<u8>) {
        if !self.is_enabled {
            return;
        }
        if !query.is_select {
            return;
        }

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
