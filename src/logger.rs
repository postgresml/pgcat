use crate::cmd_args::{Args, LogFormat};
use tracing_subscriber;

pub fn init(args: &Args) {
    let trace_sub = tracing_subscriber::fmt()
        .with_max_level(args.log_level)
        .with_ansi(!args.no_color);

    match args.log_format {
        LogFormat::Structured => trace_sub.json().init(),
        LogFormat::Debug => trace_sub.pretty().init(),
        _ => trace_sub.init(),
    };
}
