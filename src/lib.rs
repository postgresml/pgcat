use std::io::{BufRead, Cursor};

use bytes::BytesMut;
use errors::Error;

pub mod config;
pub mod constants;
pub mod errors;
pub mod messages;
pub mod pool;
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

pub trait BytesMutReader {
    fn read_string(&mut self) -> Result<String, Error>;
}

impl BytesMutReader for Cursor<&BytesMut> {
    fn read_string(&mut self) -> Result<String, Error> {
        let mut buf = vec![];
        match self.read_until(b'\0', &mut buf) {
            Ok(_) => Ok(String::from_utf8_lossy(&buf[..buf.len() - 1]).to_string()),
            Err(err) => return Err(Error::ParseBytesError(err.to_string())),
        }
    }
}
