use crate::cmd_args::{Args, Commands, ConfigPoolsSubcommands, ConfigSubcommands};
use crate::config;
use prettytable::{format, row, Cell, Row, Table};

pub fn init(args: &Args) {
    match &args.command {
        Some(Commands::Config(command)) => match command {
            ConfigSubcommands::Pools(subcommand) => match subcommand {
                ConfigPoolsSubcommands::List => cmd_config_pools_list(),
                ConfigPoolsSubcommands::Add => cmd_config_pools_add(),
                ConfigPoolsSubcommands::Remove => cmd_config_pools_remove(),
            },
            ConfigSubcommands::Shards => println!("Shard"),
        },
        None => {}
    }
    std::process::exit(exitcode::OK);
}

fn cmd_config_pools_list() {
    println!("Pools list");

    let cfg = config::get_config();

    // Create the table
    let mut table = Table::new();

    // let format = format::FormatBuilder::new()
    //     .column_separator('|')
    //     .borders('|')
    //     .separators(
    //         &[format::LinePosition::Top, format::LinePosition::Bottom],
    //         format::LineSeparator::new('-', '+', '+', '+'),
    //     )
    //     .padding(1, 1)
    //     .build();
    // table.set_format(format);
    /* Pool { pool_mode: Session, load_balancing_mode: Random, default_role:
     * "primary", query_parser_enabled: true, query_parser_max_length: None,
     * query_parser_read_write_splitting: false, primary_reads_enabled: true,
     * connect_timeout: None, idle_timeout: None, server_lifetime: None,
     * sharding_function: PgBigintHash, automatic_sharding_key: None,
     * sharding_key_regex: None, shard_id_regex: None, regex_search_limit: None,
     * auth_query: None, auth_query_user: None, auth_query_password: None,
     * cleanup_server_connections: true, plugins: None, shards: {"0": Shard {
     * database: "some_db", mirrors: None, servers: [ServerConfig { host:
     * "127.0.0.1", port: 5432, role: Primary }, ServerConfig { host:
     * "localhost", port: 5432, role: Replica }] }}, users: {"0": User {
     * username: "simple_user", password: Some("simple_user"), server_username:
     * None, server_password: None, pool_size: 5, min_pool_size: Some(3),
     * pool_mode: None, server_lifetime: Some(60000), statement_timeout: 0 }} }
     */
    table.set_format(*format::consts::FORMAT_NO_LINESEP_WITH_TITLE);

    table.set_titles(row![
        "pool_name",
        "pool_mode",
        "load_balancing_mode",
        "default_role"
    ]);

    for (pool_name, pool) in cfg.pools {
        table.add_row(Row::new(vec![
            Cell::new(&pool_name),
            Cell::new(&format!("{}", pool.pool_mode.to_string())),
            Cell::new(&format!("{}", pool.load_balancing_mode.to_string())),
            Cell::new(&format!("{}", pool.default_role.to_string())),
        ]));
    }

    // Print the table to stdout
    table.printstd();
}

fn cmd_config_pools_add() {
    todo!("Add pools")
}

fn cmd_config_pools_remove() {
    todo!("Remove pools")
}
