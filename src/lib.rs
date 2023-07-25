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
    let milliseconds = duration.num_milliseconds() % 1000;
    let seconds = duration.num_seconds() % 60;
    let minutes = duration.num_minutes() % 60;
    let hours = duration.num_hours() % 24;
    let days = duration.num_days();

    format!("{days}d {hours:0>2}:{minutes:0>2}:{seconds:0>2}.{milliseconds:0>3}")
}
