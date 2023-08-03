use std::collections::{HashMap, HashSet};
use std::io::Cursor;
use bytes::{Buf, BytesMut};
use log::{debug};
use crate::config::{get_config, Role};
use crate::messages::BytesMutReader;

#[derive(Debug)]
struct Query {
    _query: String,
}

#[derive(Debug)]
pub struct QueryCacher {
    fingerprints: HashSet<String>,
    _cache: HashMap<String, Query>,
    is_enabled: bool,
}

impl Default for QueryCacher {
    fn default() -> Self {
        QueryCacher {
            fingerprints: HashSet::new(),
            _cache: HashMap::new(),
            is_enabled: false,
        }
    }
}

impl QueryCacher {
    pub(crate) fn new() -> QueryCacher {
        let config = get_config();
        let is_enabled = config.query_cache.clone().map(|c| c.enabled).unwrap_or(false);
        let mut fingerprints = HashSet::new();
        let queries = config.query_cache.clone().map(|c| c.queries).unwrap_or(Vec::new());
        for query in queries {
            if let Some(fingerprint) = query.fingerprint() {
                fingerprints.insert(fingerprint);
            }
        }
        QueryCacher {
            _cache: HashMap::new(),
            fingerprints,
            is_enabled,
        }
    }

    pub fn try_read_query_results_from_cache(&self, message: &BytesMut, _shard: usize, _role: Option<Role>) -> () {
        // look into using https://github.com/moka-rs/moka or a cache facade library
        if !self.is_enabled {
            return;
        }

        let mut message_cursor = Cursor::new(message);
        let char = message_cursor.get_u8() as char;
        let _len = message_cursor.get_i32() as usize;

        if char != 'Q' {
            return;
        }

        let query = message_cursor.read_string().unwrap();
        let fingerprint_result = pg_query::fingerprint(&query);
        if fingerprint_result.is_err() {
            return;
        }
        let fingerprint = fingerprint_result.unwrap();
        if !self.fingerprints.contains(&fingerprint.hex) {
            return;
        }

        debug!("Query is eligible to read from cache");
    }
}
