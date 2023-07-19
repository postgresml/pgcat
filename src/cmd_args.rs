use clap::{Parser, ValueEnum};
use tracing::Level;

/// PgCat: Nextgen PostgreSQL Pooler
#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
pub struct Args {
    #[arg(default_value_t = String::from("pgcat.toml"), env)]
    pub config_file: String,

    #[arg(short, long, default_value_t = tracing::Level::INFO, env)]
    pub log_level: Level,

    #[clap(short='F', long, value_enum, default_value_t=LogFormat::Text, env)]
    pub log_format: LogFormat,
}

pub fn parse() -> Args {
    return Args::parse();
}

#[derive(ValueEnum, Clone, Debug)]
pub enum LogFormat {
    Text,
    Structured,
    Debug,
}
