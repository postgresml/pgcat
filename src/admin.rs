/// Admin database.
use bytes::{Buf, BufMut, BytesMut};
use log::{info, trace};
use std::collections::HashMap;

use crate::config::{get_config, reload_config, VERSION};
use crate::errors::Error;
use crate::messages::*;
use crate::pool::get_all_pools;
use crate::stats::get_stats;
use crate::ClientServerMap;

pub fn generate_server_info_for_admin() -> BytesMut {
    let mut server_info = BytesMut::new();

    server_info.put(server_paramater_message("application_name", ""));
    server_info.put(server_paramater_message("client_encoding", "UTF8"));
    server_info.put(server_paramater_message("server_encoding", "UTF8"));
    server_info.put(server_paramater_message("server_version", VERSION));
    server_info.put(server_paramater_message("DateStyle", "ISO, MDY"));

    return server_info;
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

    if query.starts_with("SHOW STATS") {
        trace!("SHOW STATS");
        show_stats(stream).await
    } else if query.starts_with("RELOAD") {
        trace!("RELOAD");
        reload(stream, client_server_map).await
    } else if query.starts_with("SHOW CONFIG") {
        trace!("SHOW CONFIG");
        show_config(stream).await
    } else if query.starts_with("SHOW DATABASES") {
        trace!("SHOW DATABASES");
        show_databases(stream).await
    } else if query.starts_with("SHOW POOLS") {
        trace!("SHOW POOLS");
        show_pools(stream).await
    } else if query.starts_with("SHOW LISTS") {
        trace!("SHOW LISTS");
        show_lists(stream).await
    } else if query.starts_with("SHOW VERSION") {
        trace!("SHOW VERSION");
        show_version(stream).await
    } else if query.starts_with("SET ") {
        trace!("SET");
        ignore_set(stream).await
    } else {
        error_response(stream, "Unsupported query against the admin database").await
    }
}

/// Column-oriented statistics.
async fn show_lists<T>(stream: &mut T) -> Result<(), Error>
where
    T: tokio::io::AsyncWrite + std::marker::Unpin,
{
    let stats = get_stats();

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
        stats
            .keys()
            .map(|address_id| stats[&address_id]["cl_idle"])
            .sum::<i64>()
            .to_string(),
    ]));
    res.put(data_row(&vec![
        "used_clients".to_string(),
        stats
            .keys()
            .map(|address_id| stats[&address_id]["cl_active"])
            .sum::<i64>()
            .to_string(),
    ]));
    res.put(data_row(&vec![
        "login_clients".to_string(),
        "0".to_string(),
    ]));
    res.put(data_row(&vec![
        "free_servers".to_string(),
        stats
            .keys()
            .map(|address_id| stats[&address_id]["sv_idle"])
            .sum::<i64>()
            .to_string(),
    ]));
    res.put(data_row(&vec![
        "used_servers".to_string(),
        stats
            .keys()
            .map(|address_id| stats[&address_id]["sv_active"])
            .sum::<i64>()
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
    res.put(data_row(&vec![format!("PgCat {}", VERSION).to_string()]));
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
    let stats = get_stats();

    let columns = vec![
        ("database", DataType::Text),
        ("user", DataType::Text),
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
        ("pool_mode", DataType::Text),
    ];

    let mut res = BytesMut::new();
    res.put(row_description(&columns));
    for (_, pool) in get_all_pools() {
        let pool_config = &pool.settings;
        for shard in 0..pool.shards() {
            for server in 0..pool.servers(shard) {
                let address = pool.address(shard, server);
                let stats = match stats.get(&address.id) {
                    Some(stats) => stats.clone(),
                    None => HashMap::new(),
                };

                let mut row = vec![address.name(), pool_config.user.username.clone()];

                for column in &columns[2..columns.len() - 1] {
                    let value = stats.get(column.0).unwrap_or(&0).to_string();
                    row.push(value);
                }

                row.push(pool_config.pool_mode.to_string());
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
            let database_name = &pool_config.shards[&shard.to_string()].database;

            for server in 0..pool.servers(shard) {
                let address = pool.address(shard, server);
                let pool_state = pool.pool_state(shard, server);

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
                    "0".to_string(),                        // disabled
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
        ("database", DataType::Text),
        ("total_xact_count", DataType::Numeric),
        ("total_query_count", DataType::Numeric),
        ("total_received", DataType::Numeric),
        ("total_sent", DataType::Numeric),
        ("total_xact_time", DataType::Numeric),
        ("total_query_time", DataType::Numeric),
        ("total_wait_time", DataType::Numeric),
        ("avg_xact_count", DataType::Numeric),
        ("avg_query_count", DataType::Numeric),
        ("avg_recv", DataType::Numeric),
        ("avg_sent", DataType::Numeric),
        ("avg_xact_time", DataType::Numeric),
        ("avg_query_time", DataType::Numeric),
        ("avg_wait_time", DataType::Numeric),
    ];

    let stats = get_stats();
    let mut res = BytesMut::new();
    res.put(row_description(&columns));

    for (_, pool) in get_all_pools() {
        for shard in 0..pool.shards() {
            for server in 0..pool.servers(shard) {
                let address = pool.address(shard, server);
                let stats = match stats.get(&address.id) {
                    Some(stats) => stats.clone(),
                    None => HashMap::new(),
                };

                let mut row = vec![address.name()];

                for column in &columns[1..] {
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
