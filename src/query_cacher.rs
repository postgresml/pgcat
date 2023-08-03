use std::collections::HashMap;
use std::io::Cursor;
use bytes::{Buf, BytesMut};
use log::debug;
use crate::config::{get_config, Role};
use crate::messages::BytesMutReader;

#[derive(Debug)]
struct Query {
    _query: String,
}

#[derive(Debug)]
pub struct QueryCacher {
    // TODO use
    _cache: HashMap<String, Query>,
    is_enabled: bool,
}

impl Default for QueryCacher {
    fn default() -> Self {
        QueryCacher {
            _cache: HashMap::new(),
            is_enabled: false,
        }
    }
}

impl QueryCacher {
    pub(crate) fn new() -> QueryCacher {
        let is_enabled = get_config().query_cache.map(|c| c.enabled).unwrap_or(false);
        QueryCacher {
            _cache: HashMap::new(),
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

        if let 'Q' = char {
            let query = message_cursor.read_string().unwrap();
            if let Ok(fingerprint) = pg_query::fingerprint(&query) {
                // store in class
                debug!("Query: '{}', Fingerprint: '{}'", query, fingerprint.hex);
            }
        }
    }
}
