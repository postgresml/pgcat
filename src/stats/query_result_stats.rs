use crate::errors::Error;
use crate::query::Query;
use std::collections::HashMap;

#[derive(Debug, PartialEq, Eq, Hash, Clone)]
pub struct Key {
    pub fingerprint: u64,
    pub normalized: String,
    pub result_hash: Vec<u8>,
}

#[derive(Debug)]
pub struct Value {
    pub count: u64,
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
    pub statistics: HashMap<Key, Value>,
}

impl Default for QueryResultStats {
    fn default() -> Self {
        QueryResultStats {
            statistics: HashMap::new(),
        }
    }
}

impl QueryResultStats {
    pub(crate) fn new() -> QueryResultStats {
        QueryResultStats {
            statistics: HashMap::new(),
        }
    }

    pub fn insert(&mut self, query: Query, result_hash: Vec<u8>) -> Result<(), Error> {
        // TODO limit number of entries
        // self.evict();

        let fingerprint = query.fingerprint()?;
        let normalized = query.normalized()?;

        self.statistics
            .entry(Key {
                fingerprint,
                normalized,
                result_hash,
            })
            .and_modify(|v| {
                v.count += 1;
                v.last_seen = chrono::Utc::now();
            })
            .or_insert(Value {
                count: 1,
                first_seen: chrono::Utc::now(),
                last_seen: chrono::Utc::now(),
            });

        Ok(())
    }
}
