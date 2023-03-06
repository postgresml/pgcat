pub mod auth_passthrough;
pub mod config;
pub mod constants;
pub mod errors;
pub mod messages;
pub mod mirrors;
pub mod multi_logger;
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
