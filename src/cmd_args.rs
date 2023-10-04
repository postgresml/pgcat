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

#[derive(ValueEnum, Clone, Debug)]
pub enum LogFormat {
    Text,
    Structured,
    Debug,
}

#[derive(Subcommand, Debug)]
pub enum Commands {
    /// Manages the configuration
    #[command(subcommand)]
    Config(ConfigSubcommands),
}

#[derive(Subcommand, Debug)]
pub enum ConfigSubcommands {
    /// Show general configuration
    Show,

    /// Manage pool's configuration
    #[command(subcommand)]
    Pools(ConfigPoolsSubcommands),
}

#[derive(Subcommand, Debug)]
pub enum ConfigPoolsSubcommands {
    /// List pools, users and shards
    List,
    // /// not implemented
    // Add,
    // /// not implemented
    // Remove,
}

pub fn parse() -> Args {
    return Args::parse();
}
