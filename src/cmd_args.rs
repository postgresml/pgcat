use clap::{Parser, Subcommand, ValueEnum};
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

    #[arg(
        short,
        long,
        default_value_t = false,
        env,
        help = "disable colors in the log output"
    )]
    pub no_color: bool,

    #[command(subcommand)]
    pub command: Option<Commands>,
}

pub fn parse() -> Args {
    Args::parse()
}

#[derive(ValueEnum, Clone, Debug)]
pub enum LogFormat {
    Text,
    Structured,
    Debug,
}

#[derive(Subcommand, Debug)]
pub enum Commands {
    #[command(subcommand)]
    Config(ConfigSubcommands),
}

#[derive(Subcommand, Debug)]
pub enum ConfigSubcommands {
    #[command(subcommand)]
    Pools(ConfigPoolsSubcommands),
    Shards,
}

#[derive(Subcommand, Debug)]
pub enum ConfigPoolsSubcommands {
    List,
    Add,
    Remove,
}
