/// Admin database.
use bytes::{Buf, BufMut, BytesMut};
use log::{info, trace};
use std::collections::HashMap;
use tokio::time::Instant;

use crate::config::{get_config, reload_config, VERSION};
use crate::errors::Error;
use crate::messages::*;
use crate::pool::get_all_pools;
use crate::stats::{
    get_address_stats, get_client_stats, get_pool_stats, get_server_stats, ClientState, ServerState,
};
use crate::ClientServerMap;

pub fn generate_server_info_for_admin() -> BytesMut {
    let mut server_info = BytesMut::new();

    server_info.put(server_parameter_message("application_name", ""));
    server_info.put(server_parameter_message("client_encoding", "UTF8"));
    server_info.put(server_parameter_message("server_encoding", "UTF8"));
    server_info.put(server_parameter_message("server_version", VERSION));
    server_info.put(server_parameter_message("DateStyle", "ISO, MDY"));

    server_info
}

/// Handle admin client.
pub async fn handle_admin<T>(
    stream: &mut T,
    mut query: BytesMut,
    client_server_map: ClientServerMap,
) -> Result<(), Error>
where
    T: tokio::io::AsyncWrite + std::marker::Unpin,
{
    let code = query.get_u8() as char;

    if code != 'Q' {
        return Err(Error::ProtocolSyncError);
    }

    let len = query.get_i32() as usize;
    let query = String::from_utf8_lossy(&query[..len - 5])
        .to_string()
        .to_ascii_uppercase();

    trace!("Admin query: {}", query);

    let query_parts: Vec<&str> = query.trim_end_matches(';').split_whitespace().collect();

    match query_parts[0] {
        "RELOAD" => {
            trace!("RELOAD");
            reload(stream, client_server_map).await
        }
        "SET" => {
            trace!("SET");
            ignore_set(stream).await
        }
        "SHOW" => match query_parts[1] {
            "CONFIG" => {
                trace!("SHOW CONFIG");
                show_config(stream).await
            }
            "DATABASES" => {
                trace!("SHOW DATABASES");
                show_databases(stream).await
            }
            "LISTS" => {
                trace!("SHOW LISTS");
                show_lists(stream).await
            }
            "POOLS" => {
                trace!("SHOW POOLS");
                show_pools(stream).await
            }
            "CLIENTS" => {
                trace!("SHOW CLIENTS");
                show_clients(stream).await
            }
            "SERVERS" => {
                trace!("SHOW SERVERS");
                show_servers(stream).await
            }
            "STATS" => {
                trace!("SHOW STATS");
                show_stats(stream).await
            }
            "VERSION" => {
                trace!("SHOW VERSION");
                show_version(stream).await
            }
            _ => error_response(stream, "Unsupported SHOW query against the admin database").await,
        },
        _ => error_response(stream, "Unsupported query against the admin database").await,
    }
}

/// Column-oriented statistics.
async fn show_lists<T>(stream: &mut T) -> Result<(), Error>
where
    T: tokio::io::AsyncWrite + std::marker::Unpin,
{
    let client_stats = get_client_stats();
    let server_stats = get_server_stats();

    let columns = vec![("list", DataType::Text), ("items", DataType::Int4)];

    let mut users = 1;
    let mut databases = 1;
    for (_, pool) in get_all_pools() {
        databases += pool.databases();
        users += 1; // One user per pool
    }
    let mut res = BytesMut::new();
    res.put(row_description(&columns));
    res.put(data_row(&vec![
        "databases".to_string(),
        databases.to_string(),
    ]));
    res.put(data_row(&vec!["users".to_string(), users.to_string()]));
    res.put(data_row(&vec!["pools".to_string(), databases.to_string()]));
    res.put(data_row(&vec![
        "free_clients".to_string(),
        client_stats
            .keys()
            .filter(|client_id| client_stats.get(client_id).unwrap().state == ClientState::Idle)
            .count()
            .to_string(),
    ]));
    res.put(data_row(&vec![
        "used_clients".to_string(),
        client_stats
            .keys()
            .filter(|client_id| client_stats.get(client_id).unwrap().state == ClientState::Active)
            .count()
            .to_string(),
    ]));
    res.put(data_row(&vec![
        "login_clients".to_string(),
        "0".to_string(),
    ]));
    res.put(data_row(&vec![
        "free_servers".to_string(),
        server_stats
            .keys()
            .filter(|server_id| server_stats.get(server_id).unwrap().state == ServerState::Idle)
            .count()
            .to_string(),
    ]));
    res.put(data_row(&vec![
        "used_servers".to_string(),
        server_stats
            .keys()
            .filter(|server_id| server_stats.get(server_id).unwrap().state == ServerState::Active)
            .count()
            .to_string(),
    ]));
    res.put(data_row(&vec!["dns_names".to_string(), "0".to_string()]));
    res.put(data_row(&vec!["dns_zones".to_string(), "0".to_string()]));
    res.put(data_row(&vec!["dns_queries".to_string(), "0".to_string()]));
    res.put(data_row(&vec!["dns_pending".to_string(), "0".to_string()]));

    res.put(command_complete("SHOW"));

    res.put_u8(b'Z');
    res.put_i32(5);
    res.put_u8(b'I');

    write_all_half(stream, res).await
}

/// Show PgCat version.
async fn show_version<T>(stream: &mut T) -> Result<(), Error>
where
    T: tokio::io::AsyncWrite + std::marker::Unpin,
{
    let mut res = BytesMut::new();

    res.put(row_description(&vec![("version", DataType::Text)]));
    res.put(data_row(&vec![format!("PgCat {}", VERSION)]));
    res.put(command_complete("SHOW"));

    res.put_u8(b'Z');
    res.put_i32(5);
    res.put_u8(b'I');

    write_all_half(stream, res).await
}

/// Show utilization of connection pools for each shard and replicas.
async fn show_pools<T>(stream: &mut T) -> Result<(), Error>
where
    T: tokio::io::AsyncWrite + std::marker::Unpin,
{
    let all_pool_stats = get_pool_stats();

    let columns = vec![
        ("database", DataType::Text),
        ("user", DataType::Text),
        ("pool_mode", DataType::Text),
        ("cl_idle", DataType::Numeric),
        ("cl_active", DataType::Numeric),
        ("cl_waiting", DataType::Numeric),
        ("cl_cancel_req", DataType::Numeric),
        ("sv_active", DataType::Numeric),
        ("sv_idle", DataType::Numeric),
        ("sv_used", DataType::Numeric),
        ("sv_tested", DataType::Numeric),
        ("sv_login", DataType::Numeric),
        ("maxwait", DataType::Numeric),
        ("maxwait_us", DataType::Numeric),
    ];

    let mut res = BytesMut::new();
    res.put(row_description(&columns));
    for (user_pool, pool) in get_all_pools() {
        let def = HashMap::default();
        let pool_stats = all_pool_stats
            .get(&(user_pool.db.clone(), user_pool.user.clone()))
            .unwrap_or(&def);

        let pool_config = &pool.settings;
        let mut row = vec![
            user_pool.db.clone(),
            user_pool.user.clone(),
            pool_config.pool_mode.to_string(),
        ];
        for column in &columns[3..columns.len()] {
            let value = match column.0 {
                "maxwait" => (pool_stats.get("maxwait_us").unwrap_or(&0) / 1_000_000).to_string(),
                "maxwait_us" => {
                    (pool_stats.get("maxwait_us").unwrap_or(&0) % 1_000_000).to_string()
                }
                _other_values => pool_stats.get(column.0).unwrap_or(&0).to_string(),
            };
            row.push(value);
        }
        res.put(data_row(&row));
    }

    res.put(command_complete("SHOW"));

    // ReadyForQuery
    res.put_u8(b'Z');
    res.put_i32(5);
    res.put_u8(b'I');

    write_all_half(stream, res).await
}

/// Show shards and replicas.
async fn show_databases<T>(stream: &mut T) -> Result<(), Error>
where
    T: tokio::io::AsyncWrite + std::marker::Unpin,
{
    // Columns
    let columns = vec![
        ("name", DataType::Text),
        ("host", DataType::Text),
        ("port", DataType::Text),
        ("database", DataType::Text),
        ("force_user", DataType::Text),
        ("pool_size", DataType::Int4),
        ("min_pool_size", DataType::Int4),
        ("reserve_pool", DataType::Int4),
        ("pool_mode", DataType::Text),
        ("max_connections", DataType::Int4),
        ("current_connections", DataType::Int4),
        ("paused", DataType::Int4),
        ("disabled", DataType::Int4),
    ];

    let mut res = BytesMut::new();

    res.put(row_description(&columns));

    for (_, pool) in get_all_pools() {
        let pool_config = pool.settings.clone();
        for shard in 0..pool.shards() {
            let database_name = &pool.address(shard, 0).database;
            for server in 0..pool.servers(shard) {
                let address = pool.address(shard, server);
                let pool_state = pool.pool_state(shard, server);
                let banned = pool.is_banned(address, Some(address.role));

                res.put(data_row(&vec![
                    address.name(),                         // name
                    address.host.to_string(),               // host
                    address.port.to_string(),               // port
                    database_name.to_string(),              // database
                    pool_config.user.username.to_string(),  // force_user
                    pool_config.user.pool_size.to_string(), // pool_size
                    "0".to_string(),                        // min_pool_size
                    "0".to_string(),                        // reserve_pool
                    pool_config.pool_mode.to_string(),      // pool_mode
                    pool_config.user.pool_size.to_string(), // max_connections
                    pool_state.connections.to_string(),     // current_connections
                    "0".to_string(),                        // paused
                    match banned {
                        // disabled
                        true => "1".to_string(),
                        false => "0".to_string(),
                    },
                ]));
            }
        }
    }
    res.put(command_complete("SHOW"));

    // ReadyForQuery
    res.put_u8(b'Z');
    res.put_i32(5);
    res.put_u8(b'I');

    write_all_half(stream, res).await
}

/// Ignore any SET commands the client sends.
/// This is common initialization done by ORMs.
async fn ignore_set<T>(stream: &mut T) -> Result<(), Error>
where
    T: tokio::io::AsyncWrite + std::marker::Unpin,
{
    custom_protocol_response_ok(stream, "SET").await
}

/// Reload the configuration file without restarting the process.
async fn reload<T>(stream: &mut T, client_server_map: ClientServerMap) -> Result<(), Error>
where
    T: tokio::io::AsyncWrite + std::marker::Unpin,
{
    info!("Reloading config");

    reload_config(client_server_map).await?;

    get_config().show();

    let mut res = BytesMut::new();

    res.put(command_complete("RELOAD"));

    // ReadyForQuery
    res.put_u8(b'Z');
    res.put_i32(5);
    res.put_u8(b'I');

    write_all_half(stream, res).await
}

/// Shows current configuration.
async fn show_config<T>(stream: &mut T) -> Result<(), Error>
where
    T: tokio::io::AsyncWrite + std::marker::Unpin,
{
    let config = &get_config();
    let config: HashMap<String, String> = config.into();

    // Configs that cannot be changed without restarting.
    let immutables = ["host", "port", "connect_timeout"];

    // Columns
    let columns = vec![
        ("key", DataType::Text),
        ("value", DataType::Text),
        ("default", DataType::Text),
        ("changeable", DataType::Text),
    ];

    // Response data
    let mut res = BytesMut::new();
    res.put(row_description(&columns));

    // DataRow rows
    for (key, value) in config {
        let changeable = if immutables.iter().filter(|col| *col == &key).count() == 1 {
            "no".to_string()
        } else {
            "yes".to_string()
        };

        let row = vec![key, value, "-".to_string(), changeable];

        res.put(data_row(&row));
    }

    res.put(command_complete("SHOW"));

    // ReadyForQuery
    res.put_u8(b'Z');
    res.put_i32(5);
    res.put_u8(b'I');

    write_all_half(stream, res).await
}

/// Show shard and replicas statistics.
async fn show_stats<T>(stream: &mut T) -> Result<(), Error>
where
    T: tokio::io::AsyncWrite + std::marker::Unpin,
{
    let columns = vec![
        ("instance", DataType::Text),
        ("database", DataType::Text),
        ("user", DataType::Text),
        ("total_xact_count", DataType::Numeric),
        ("total_query_count", DataType::Numeric),
        ("total_received", DataType::Numeric),
        ("total_sent", DataType::Numeric),
        ("total_xact_time", DataType::Numeric),
        ("total_query_time", DataType::Numeric),
        ("total_wait_time", DataType::Numeric),
        ("total_errors", DataType::Numeric),
        ("avg_xact_count", DataType::Numeric),
        ("avg_query_count", DataType::Numeric),
        ("avg_recv", DataType::Numeric),
        ("avg_sent", DataType::Numeric),
        ("avg_errors", DataType::Numeric),
        ("avg_xact_time", DataType::Numeric),
        ("avg_query_time", DataType::Numeric),
        ("avg_wait_time", DataType::Numeric),
    ];

    let all_stats = get_address_stats();
    let mut res = BytesMut::new();
    res.put(row_description(&columns));

    for (user_pool, pool) in get_all_pools() {
        for shard in 0..pool.shards() {
            for server in 0..pool.servers(shard) {
                let address = pool.address(shard, server);
                let stats = match all_stats.get(&address.id) {
                    Some(stats) => stats.clone(),
                    None => HashMap::new(),
                };

                let mut row = vec![address.name(), user_pool.db.clone(), user_pool.user.clone()];
                for column in &columns[3..] {
                    row.push(stats.get(column.0).unwrap_or(&0).to_string());
                }

                res.put(data_row(&row));
            }
        }
    }

    res.put(command_complete("SHOW"));

    // ReadyForQuery
    res.put_u8(b'Z');
    res.put_i32(5);
    res.put_u8(b'I');

    write_all_half(stream, res).await
}

/// Show currently connected clients
async fn show_clients<T>(stream: &mut T) -> Result<(), Error>
where
    T: tokio::io::AsyncWrite + std::marker::Unpin,
{
    let columns = vec![
        ("client_id", DataType::Text),
        ("database", DataType::Text),
        ("user", DataType::Text),
        ("application_name", DataType::Text),
        ("state", DataType::Text),
        ("transaction_count", DataType::Numeric),
        ("query_count", DataType::Numeric),
        ("error_count", DataType::Numeric),
        ("age_seconds", DataType::Numeric),
    ];

    let new_map = get_client_stats();
    let mut res = BytesMut::new();
    res.put(row_description(&columns));

    for (_, client) in new_map {
        let row = vec![
            format!("{:#010X}", client.client_id),
            client.pool_name,
            client.username,
            client.application_name.clone(),
            client.state.to_string(),
            client.transaction_count.to_string(),
            client.query_count.to_string(),
            client.error_count.to_string(),
            Instant::now()
                .duration_since(client.connect_time)
                .as_secs()
                .to_string(),
        ];

        res.put(data_row(&row));
    }

    res.put(command_complete("SHOW"));

    // ReadyForQuery
    res.put_u8(b'Z');
    res.put_i32(5);
    res.put_u8(b'I');

    write_all_half(stream, res).await
}

/// Show currently connected servers
async fn show_servers<T>(stream: &mut T) -> Result<(), Error>
where
    T: tokio::io::AsyncWrite + std::marker::Unpin,
{
    let columns = vec![
        ("server_id", DataType::Text),
        ("database_name", DataType::Text),
        ("user", DataType::Text),
        ("address_id", DataType::Text),
        ("application_name", DataType::Text),
        ("state", DataType::Text),
        ("transaction_count", DataType::Numeric),
        ("query_count", DataType::Numeric),
        ("bytes_sent", DataType::Numeric),
        ("bytes_received", DataType::Numeric),
        ("age_seconds", DataType::Numeric),
    ];

    let new_map = get_server_stats();
    let mut res = BytesMut::new();
    res.put(row_description(&columns));

    for (_, server) in new_map {
        let row = vec![
            format!("{:#010X}", server.server_id),
            server.pool_name,
            server.username,
            server.address_name,
            server.application_name,
            server.state.to_string(),
            server.transaction_count.to_string(),
            server.query_count.to_string(),
            server.bytes_sent.to_string(),
            server.bytes_received.to_string(),
            Instant::now()
                .duration_since(server.connect_time)
                .as_secs()
                .to_string(),
        ];

        res.put(data_row(&row));
    }

    res.put(command_complete("SHOW"));

    // ReadyForQuery
    res.put_u8(b'Z');
    res.put_i32(5);
    res.put_u8(b'I');

    write_all_half(stream, res).await
}
