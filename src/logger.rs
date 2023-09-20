use crate::cmd_args::{Args, LogFormat};
use tracing_subscriber;
use tracing_subscriber::EnvFilter;

pub fn init(args: &Args) {
    // Iniitalize a default filter, and then override the builtin default "warning" with our
    // commandline, (default: "info")
    let filter = EnvFilter::from_default_env().add_directive(args.log_level.into());

    let trace_sub = tracing_subscriber::fmt()
        .with_thread_ids(true)
        .with_env_filter(filter)
        .with_ansi(!args.no_color);

    match args.log_format {
        LogFormat::Structured => trace_sub.json().init(),
        LogFormat::Debug => trace_sub.pretty().init(),
        _ => trace_sub.init(),
    };
}
