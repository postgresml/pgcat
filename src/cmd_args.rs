use clap::Parser;
use log::LevelFilter;

/// PgCat: Nextgen PostgreSQL Pooler
#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
pub(crate) struct Args {
    #[arg(default_value_t = String::from("pgcat.toml"), env)]
    pub config_file: String,

    #[arg(short, long, default_value_t = LevelFilter::Info, env)]
    pub log_level: log::LevelFilter,
}

pub(crate) fn parse() -> Args {
    return Args::parse();
}
