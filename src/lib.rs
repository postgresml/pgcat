use crate::errors::Error;
use crate::messages::BytesMutReader;
use bytes::{Buf, BytesMut};
use hmac::digest::Digest;
use sha2::Sha256;
use std::io::Cursor;

pub mod admin;
pub mod auth_passthrough;
pub mod client;
pub mod cmd_args;
pub mod config;
pub mod constants;
pub mod dns_cache;
pub mod errors;
pub mod logger;
pub mod messages;
pub mod mirrors;
pub mod plugins;
pub mod pool;
pub mod prometheus;
pub mod query_router;
pub mod scram;
pub mod server;
pub mod sharding;
pub mod stats;
pub mod tls;

/// Format chrono::Duration to be more human-friendly.
///
/// # Arguments
///
/// * `duration` - A duration of time
pub fn format_duration(duration: &chrono::Duration) -> String {
    let milliseconds = format!("{:0>3}", duration.num_milliseconds() % 1000);

    let seconds = format!("{:0>2}", duration.num_seconds() % 60);

    let minutes = format!("{:0>2}", duration.num_minutes() % 60);

    let hours = format!("{:0>2}", duration.num_hours() % 24);

    let days = duration.num_days().to_string();

    format!(
        "{}d {}:{}:{}.{}",
        days, hours, minutes, seconds, milliseconds
    )
}

#[derive(Debug, PartialEq, Eq)]
pub struct Query {
    text: String,
    normalized: String,
    is_read: bool,
    fingerprint: u64,
    hash: Vec<u8>,
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

    let normalized_result = pg_query::normalize(&text);
    if let Err(e) = normalized_result {
        return Err(Error::BadQuery(format!("Error normalizing query: {}", e)));
    }
    let normalized = normalized_result.unwrap();

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
        normalized,
        // TODO parse selects as read
        is_read: true,
        fingerprint: fingerprint.value,
        hash,
    })
}

#[cfg(test)]
mod tests {
    use crate::{parse_query, Query};
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
                normalized: "select ?".to_string(),
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
