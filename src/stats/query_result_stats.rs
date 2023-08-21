use crate::errors::Error;
use crate::query::Query;
use lru::LruCache;
use std::num::NonZeroUsize;

#[derive(Debug, PartialEq, Eq, Hash, Clone)]
pub struct Key {
    pub fingerprint: u64,
    pub normalized: String,
    pub result_hash: Vec<u8>,
}

#[derive(Debug)]
pub struct Value {
    pub count: usize,
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
    pub statistics: LruCache<Key, Value>,
}

impl Default for QueryResultStats {
    fn default() -> Self {
        QueryResultStats {
            statistics: LruCache::new(NonZeroUsize::new(1000).unwrap()),
        }
    }
}

impl QueryResultStats {
    pub(crate) fn new() -> QueryResultStats {
        QueryResultStats {
            statistics: LruCache::new(NonZeroUsize::new(1000).unwrap()),
        }
    }

    pub fn insert(&mut self, query: Query, result_hash: Vec<u8>) -> Result<(), Error> {
        let fingerprint = query.fingerprint()?;
        let normalized = query.normalized()?;

        let value = self.statistics.get_or_insert_mut(
            Key {
                fingerprint,
                normalized,
                result_hash,
            },
            || Value {
                count: 0,
                first_seen: chrono::Utc::now(),
                last_seen: chrono::Utc::now(),
            },
        );

        value.count += 1;
        value.last_seen = chrono::Utc::now();

        Ok(())
    }
}
