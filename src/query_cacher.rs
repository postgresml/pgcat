use crate::config::get_config;
use crate::errors::Error;
use crate::messages::BytesMutReader;
use bytes::{Buf, BytesMut};
use log::debug;
use sha2::{Digest, Sha256};
use std::collections::{HashMap, HashSet};
use std::io::Cursor;

#[derive(Debug, PartialEq, Eq)]
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

fn parse_query(message: &BytesMut) -> Result<Query, Error> {
    let mut message_cursor = Cursor::new(message);
    let char = message_cursor.get_u8() as char;
    if char != 'Q' {
        return Err(Error::BadQuery(
            "Query message does not start with 'Q'".to_string(),
        ));
    }

    let _len = message_cursor.get_i32() as usize;
    let text = message_cursor.read_string()?;
    println!("text: {}", text);
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
        fingerprint: fingerprint.hex,
        hash,
    })
}

#[cfg(test)]
mod tests {
    use crate::query_cacher::{parse_query, Query};
    use bytes::BytesMut;

    #[test]
    fn test_parse_query() {
        let text = "/* my comment */ select 1".to_string();
        let message = BytesMut::from(
            format!("Q{:04}{}", text.len(), text)
                .into_bytes()
                .as_slice(),
        );
        assert_eq!(
            parse_query(&message),
            Ok(Query {
                text,
                fingerprint: "50fde20626009aba".to_string(),
                hash: vec![
                    131, 24, 219, 163, 207, 48, 13, 45, 153, 20, 131, 251, 81, 150, 90, 197, 225,
                    42, 20, 161, 105, 58, 66, 249, 38, 156, 99, 230, 126, 50, 124, 155
                ]
            })
        )
    }

    #[test]
    fn test_parse_query_ignores_comments() {
        let query = r#"select 1"#.to_string();
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
        assert_eq!(
            parse_query(&query_message),
            parse_query(&commented_query_message)
        );
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

    pub fn try_read_query_results_from_cache(&self, message: &BytesMut) -> Result<(), Error> {
        if !self.is_enabled {
            return Ok(());
        }

        let query = parse_query(message)?;

        if !self.fingerprints.contains(&query.fingerprint) {
            return Ok(());
        }

        debug!("Query is eligible to read from cache");

        Ok(())
    }

    pub fn try_write_query_results_to_cache(
        &mut self,
        message: &BytesMut,
        results: &BytesMut,
    ) -> Result<(), Error> {
        if !self.is_enabled {
            return Ok(());
        }

        let query = parse_query(message)?;

        debug!("Query {:?} partial results: {:?}", query, results);

        Ok(())
    }
}
