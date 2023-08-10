use crate::config::get_config;
use crate::errors::Error;
use crate::messages::BytesMutReader;
use bytes::{Buf, BytesMut};
use rand::Rng;
use sha2::{Digest, Sha256};
use std::collections::HashMap;
use std::io::Cursor;

#[derive(Debug, PartialEq, Eq)]
pub struct Query {
    text: String,
    is_read: bool,
    fingerprint: u64,
    hash: Vec<u8>,
}

#[derive(Debug, PartialEq, Eq, Hash, Clone)]
pub struct Key {
    pub fingerprint: u64,
    pub query_hash: Vec<u8>,
    pub result_hash: Vec<u8>,
}

#[derive(Debug, PartialEq, Eq)]
pub struct Value {
    pub first_seen: chrono::DateTime<chrono::Utc>,
    pub last_seen: chrono::DateTime<chrono::Utc>,
}

impl Value {
    pub fn duration(&self) -> chrono::Duration {
        self.last_seen - self.first_seen
    }
}

#[derive(Debug)]
pub struct QueryCacheReporter {
    is_enabled: bool,
    // between 0 and 100
    sampling_rate: usize,
    // TODO limit size of hash to top 50
    query_result_hashes: HashMap<Vec<u8>, Sha256>,
    pub statistics: HashMap<Key, Value>,
}

impl Default for QueryCacheReporter {
    fn default() -> Self {
        QueryCacheReporter {
            sampling_rate: 100,
            is_enabled: false,
            query_result_hashes: HashMap::new(),
            statistics: HashMap::new(),
        }
    }
}

pub fn parse_query(message: &BytesMut) -> Result<Query, Error> {
    let mut message_cursor = Cursor::new(message);
    let char = message_cursor.get_u8() as char;
    if char != 'Q' {
        return Err(Error::BadQuery(
            "Query message does not start with 'Q'".to_string(),
        ));
    }

    let _len = message_cursor.get_i32() as usize;
    let text = message_cursor.read_string()?;
    let fingerprint_result = pg_query::fingerprint(&text);
    if let Err(e) = fingerprint_result {
        return Err(Error::BadQuery(format!(
            "Error fingerprinting query: {}",
            e
        )));
    }
    let fingerprint = fingerprint_result.unwrap();
    let mut hasher = Sha256::default();
    hasher.update(&text);
    let hash = hasher.finalize().to_vec();
    Ok(Query {
        text,
        // TODO parse selects as read
        is_read: true,
        fingerprint: fingerprint.value,
        hash,
    })
}

#[cfg(test)]
mod tests {
    use crate::query_cacher::{parse_query, Query};
    use bytes::BytesMut;

    #[test]
    fn test_parse_query() {
        let text = "select 1".to_string();
        let message = BytesMut::from(
            format!("Q{:04}{}", text.len() + 1, text)
                .into_bytes()
                .as_slice(),
        );
        assert_eq!(
            parse_query(&message),
            Ok(Query {
                text,
                is_read: true,
                fingerprint: 5836069208177285818,
                hash: vec![
                    131, 24, 219, 163, 207, 48, 13, 45, 153, 20, 131, 251, 81, 150, 90, 197, 225,
                    42, 20, 161, 105, 58, 66, 249, 38, 156, 99, 230, 126, 50, 124, 155
                ]
            })
        )
    }

    #[test]
    fn test_parse_query_ignores_comments() {
        let query = "select 1".to_string();
        let commented_query = format!("/* my comment */ {}", query);
        let query_message = BytesMut::from(
            format!("Q{:04}{}", query.len(), query)
                .into_bytes()
                .as_slice(),
        );
        let commented_query_message = BytesMut::from(
            format!("Q{:04}{}", commented_query.len(), commented_query)
                .into_bytes()
                .as_slice(),
        );
        let parsed_query = parse_query(&query_message).unwrap();
        let parsed_commented_query = parse_query(&commented_query_message).unwrap();
        assert_eq!(parsed_query.fingerprint, parsed_commented_query.fingerprint);
    }
}

impl QueryCacheReporter {
    pub(crate) fn new() -> QueryCacheReporter {
        let config = get_config();
        let is_enabled = config
            .query_cache
            .clone()
            .map(|c| c.collect_stats)
            .unwrap_or(false);
        let sampling_rate = (config
            .query_cache
            .clone()
            .map(|c| c.sample_rate)
            .unwrap_or(0.0)
            * 100.0) as usize;
        QueryCacheReporter {
            is_enabled,
            sampling_rate,
            query_result_hashes: HashMap::new(),
            statistics: HashMap::new(),
        }
    }

    fn should_sample(&self) -> bool {
        rand::thread_rng().gen_range(0..100) < self.sampling_rate
    }

    pub fn update_result_hash(&mut self, query: &Query, results: &BytesMut) -> Result<(), Error> {
        if !self.is_enabled {
            return Ok(());
        }
        if !query.is_read {
            return Ok(());
        }
        if !self.should_sample() {
            return Ok(());
        }

        // TODO transaction status
        // TODO extended protocol
        // we see `H` or `S` messages for extended protocol results
        // TODO prepared statements

        let entry = self
            .query_result_hashes
            .entry(query.hash.clone())
            .or_insert(Sha256::default());
        (*entry).update(results);

        Ok(())
    }

    pub fn finalize_result_hash(&mut self, query: &Query) -> Result<(), Error> {
        if !self.is_enabled {
            return Ok(());
        }
        if !query.is_read {
            return Ok(());
        }
        if !self.should_sample() {
            return Ok(());
        }

        let hasher_option = self.query_result_hashes.remove(&query.hash);
        if hasher_option.is_none() {
            return Ok(());
        }
        let hasher = hasher_option.unwrap();

        let key = Key {
            fingerprint: query.fingerprint.clone(),
            query_hash: query.hash.clone(),
            result_hash: hasher.finalize().to_vec(),
        };

        self
            .statistics
            .entry(key.clone())
            .and_modify(|v| {
                v.last_seen = chrono::Utc::now();
            })
            .or_insert(Value {
                first_seen: chrono::Utc::now(),
                last_seen: chrono::Utc::now(),
            });

        Ok(())
    }
}
