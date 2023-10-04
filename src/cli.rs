use crate::cmd_args::{Args, Commands, ConfigPoolsSubcommands, ConfigSubcommands};
use crate::config;
use prettytable::{format, row, table, Table};

pub fn init(args: &Args) {
    match &args.command {
        Some(Commands::Config(command)) => match command {
            ConfigSubcommands::Pools(subcommand) => match subcommand {
                ConfigPoolsSubcommands::List => cmd_config_pools_list(),
                // ConfigPoolsSubcommands::Add => todo!("Pools add"),
                // ConfigPoolsSubcommands::Remove => todo!("Pools remove"),
            },
            ConfigSubcommands::Show => cmd_config_show(),
        },
        None => {}
    }
}

fn cmd_config_show() {
    println!("🛠️  General configuration");

    let cfg = config::get_config();

    let mut table_general = table!(
        ["host", &format!("{}", cfg.general.host.to_string())],
        ["port", &format!("{}", cfg.general.port.to_string())],
        [
            "connect_timeout",
            &format!("{}", cfg.general.connect_timeout.to_string())
        ],
        [
            "idle_timeout",
            &format!("{}", cfg.general.idle_timeout.to_string())
        ],
        [
            "idle_client_in_transaction_timeout",
            &format!(
                "{}",
                cfg.general.idle_client_in_transaction_timeout.to_string()
            )
        ],
        [
            "shutdown_timeout",
            &format!("{}", cfg.general.shutdown_timeout.to_string())
        ],
        [
            "tcp_user_timeout",
            &format!("{}", cfg.general.tcp_user_timeout.to_string())
        ],
        [
            "admin_username",
            &format!("{}", cfg.general.admin_username.to_string())
        ],
        [
            "tcp_keepalives_idle",
            &format!("{}", cfg.general.tcp_keepalives_idle.to_string())
        ],
        [
            "tcp_keepalives_count",
            &format!("{}", cfg.general.tcp_keepalives_count.to_string())
        ],
        [
            "tcp_keepalives_interval",
            &format!("{}", cfg.general.tcp_keepalives_interval.to_string())
        ],
        [
            "dns_cache_enabled",
            &format!("{}", cfg.general.dns_cache_enabled.to_string())
        ],
        [
            "dns_max_ttl",
            &format!("{}", cfg.general.dns_max_ttl.to_string())
        ],
        [
            "log_client_connections",
            &format!("{}", cfg.general.log_client_connections.to_string())
        ],
        [
            "log_client_disconnections",
            &format!("{}", cfg.general.log_client_disconnections.to_string())
        ],
        [
            "healthcheck_timeout",
            &format!("{}", cfg.general.healthcheck_timeout.to_string())
        ],
        [
            "healthcheck_delay",
            &format!("{}", cfg.general.healthcheck_delay.to_string())
        ],
        ["ban_time", &format!("{}", cfg.general.ban_time.to_string())],
        [
            "server_lifetime",
            &format!("{}", cfg.general.server_lifetime.to_string())
        ],
        [
            "server_round_robin",
            &format!("{}", cfg.general.server_round_robin.to_string())
        ],
        [
            "worker_threads",
            &format!("{}", cfg.general.worker_threads.to_string())
        ],
        [
            "autoreload",
            &format!("{}", cfg.general.autoreload.unwrap_or_default())
        ],
        [
            "tls_certificate",
            &format!("{}", cfg.general.tls_certificate.unwrap_or("".to_string()))
        ],
        [
            "tls_private_key",
            &format!("{}", cfg.general.tls_private_key.unwrap_or("".to_string()))
        ],
        [
            "server_tls",
            &format!("{}", cfg.general.server_tls.to_string())
        ],
        [
            "verify_server_certificate",
            &format!("{}", cfg.general.verify_server_certificate.to_string())
        ],
        [
            "auth_query",
            &format!("{}", cfg.general.auth_query.unwrap_or("".to_string()))
        ],
        [
            "auth_query_user",
            &format!("{}", cfg.general.auth_query_user.unwrap_or("".to_string()))
        ],
        [
            "auth_query_password",
            &format!(
                "{}",
                cfg.general.auth_query_password.unwrap_or("".to_string())
            )
        ],
        [
            "enable_prometheus_exporter",
            &format!(
                "{}",
                cfg.general.enable_prometheus_exporter.unwrap_or(false)
            )
        ],
        [
            "prometheus_exporter_port",
            &format!("{}", cfg.general.prometheus_exporter_port.to_string())
        ],
        [
            "validate_config",
            &format!("{}", cfg.general.validate_config.to_string())
        ]
    );

    table_general.set_titles(row!["Setting", "Value"]);

    table_general.set_format(*format::consts::FORMAT_BOX_CHARS);

    table_general.printstd();

    std::process::exit(exitcode::OK);
}

fn cmd_config_pools_list() {
    println!("🌀 Pools list");

    let cfg = config::get_config();

    let mut table_pools = Table::new();

    table_pools.set_format(*format::consts::FORMAT_BOX_CHARS);

    table_pools.set_titles(row![
        "name", "mode", "lb mode", "def role", "users", "shards"
    ]);

    for (pool_name, pool) in cfg.pools {
        let mut table_users = Table::new();

        table_users.set_format(*format::consts::FORMAT_BOX_CHARS);
        table_users.set_titles(row!["username", "p size", "stmt timeout"]);

        for (_, user) in pool.users {
            table_users.add_row(row![user.username, user.pool_size, user.statement_timeout]);
        }

        let mut table_shards = Table::new();

        table_shards.set_format(*format::consts::FORMAT_BOX_CHARS);
        table_shards.set_titles(row!["database", "servers"]);

        let mut table_servers = Table::new();

        table_servers.set_format(*format::consts::FORMAT_BOX_CHARS);
        table_servers.set_titles(row!["database", "servers"]);

        for (_, shard) in pool.shards {
            let servers = shard
                .servers
                .iter()
                .map(|server| format!("{}:{} ({:?})", server.host, server.port, server.role))
                .collect::<Vec<String>>()
                .join("\n");

            table_shards.add_row(row![shard.database, servers]);
        }

        table_pools.add_row(row![
            &pool_name,
            &format!("{}", pool.pool_mode.to_string()),
            &format!("{}", pool.load_balancing_mode.to_string()),
            &format!("{}", pool.default_role.to_string()),
            &table_users.to_string(),
            &table_shards.to_string(),
        ]);
    }

    table_pools.printstd();

    std::process::exit(exitcode::OK);
}

/* Pool {   pool_mode: Session,
           load_balancing_mode: Random,
           default_role: "primary",
           query_parser_enabled: true,
           query_parser_max_length: None,
           query_parser_read_write_splitting: false,
           primary_reads_enabled: true,
           connect_timeout: None,
           idle_timeout: None,
           server_lifetime: None,
           sharding_function: PgBigintHash,
           automatic_sharding_key: None,
           sharding_key_regex: None,
           shard_id_regex: None,
           regex_search_limit: None,
           auth_query: None,
           auth_query_user: None,
           auth_query_password: None,
           cleanup_server_connections: true,
           plugins: None,
           shards: {"0": Shard {
                           database: "some_db",
                           mirrors: None,
                           servers: [ServerConfig { host: "127.0.0.1",
                                                    port: 5432,
                                                    role: Primary },
                                     ServerConfig { host: "localhost",
                                                    port: 5432,
                                                    role: Replica }
                                     ]
                   }
           },
           users: {"0": User {
                           username: "simple_user",
                           password: Some("simple_user"),
                           server_username: None,
                           server_password: None,
                           pool_size: 5,
                           min_pool_size: Some(3),
                           pool_mode: None,
                           server_lifetime: Some(60000),
                           statement_timeout: 0
                   }
           }
   }
*/
