use crate::config::get_config;
use crate::messages::BytesMutReader;
use bytes::{Buf, BytesMut};
use log::debug;
use sha2::{Digest, Sha256};
use std::collections::{HashMap, HashSet};
use std::io::Cursor;

#[derive(Debug)]
struct Query {
    text: String,
    fingerprint: String,
    hash: Vec<u8>,
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
        let is_enabled = config
            .query_cache
            .clone()
            .map(|c| c.enabled)
            .unwrap_or(false);
        let fingerprints = config
            .query_cache
            .clone()
            .map(|c| c.fingerprints())
            .unwrap_or(HashSet::new());
        QueryCacher {
            _cache: HashMap::new(),
            fingerprints,
            is_enabled,
        }
    }

    fn fingerprint_and_hash(&self, message: &BytesMut) -> Option<Query> {
        let mut message_cursor = Cursor::new(message);
        let char = message_cursor.get_u8() as char;
        if char != 'Q' {
            return None;
        }

        let _len = message_cursor.get_i32() as usize;
        let text = message_cursor.read_string().unwrap();
        let fingerprint_result = pg_query::fingerprint(&text);
        if fingerprint_result.is_err() {
            return None;
        }
        let fingerprint = fingerprint_result.unwrap();
        let mut hasher = Sha256::default();
        hasher.update(&text);
        let hash = hasher.finalize().to_vec();
        Some(Query {
            text,
            fingerprint: fingerprint.hex,
            hash,
        })
    }

    pub fn try_read_query_results_from_cache(&self, message: &BytesMut) -> () {
        if !self.is_enabled {
            return;
        }

        let result = self.fingerprint_and_hash(message);
        if result.is_none() {
            return;
        }
        let query = result.unwrap();

        if !self.fingerprints.contains(&query.fingerprint) {
            return;
        }

        debug!("Query is eligible to read from cache");
    }

    pub fn try_write_query_results_to_cache(
        &mut self,
        message: &BytesMut,
        results: &BytesMut,
    ) -> () {
        if !self.is_enabled {
            return;
        }

        let fingerprint_option = self.fingerprint_and_hash(message);
        if fingerprint_option.is_none() {
            return;
        }
        let query = fingerprint_option.unwrap();

        debug!("Query {:?} partial results: {:?}", query, results);
    }
}
