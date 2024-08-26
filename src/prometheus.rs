use http_body_util::Full;
use hyper::body;
use hyper::body::Bytes;

use hyper::server::conn::http1;
use hyper::service::service_fn;
use hyper::{Method, Request, Response, StatusCode};
use hyper_util::rt::TokioIo;
use log::{debug, error, info};
use phf::phf_map;
use std::collections::HashMap;
use std::fmt;
use std::net::SocketAddr;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use tokio::net::TcpListener;

use crate::config::Address;
use crate::pool::{get_all_pools, PoolIdentifier};
use crate::stats::pool::PoolStats;
use crate::stats::{get_server_stats, ServerStats};

struct MetricHelpType {
    help: &'static str,
    ty: &'static str,
}

// reference for metric types: https://prometheus.io/docs/concepts/metric_types/
// counters only increase
// gauges can arbitrarily increase or decrease
static METRIC_HELP_AND_TYPES_LOOKUP: phf::Map<&'static str, MetricHelpType> = phf_map! {
    "stats_total_query_count" => MetricHelpType {
        help: "Number of queries sent by all clients",
        ty: "counter",
    },
    "stats_total_query_time" => MetricHelpType {
        help: "Total amount of time for queries to execute",
        ty: "counter",
    },
    "stats_total_received" => MetricHelpType {
        help: "Number of bytes received from the server",
        ty: "counter",
    },
    "stats_total_sent" => MetricHelpType {
        help: "Number of bytes sent to the server",
        ty: "counter",
    },
    "stats_total_xact_count" => MetricHelpType {
        help: "Total number of transactions started by the client",
        ty: "counter",
    },
    "stats_total_xact_time" => MetricHelpType {
        help: "Total amount of time for all transactions to execute",
        ty: "counter",
    },
    "stats_total_wait_time" => MetricHelpType {
        help: "Total time client waited for a server connection",
        ty: "counter",
    },
    "stats_avg_query_count" => MetricHelpType {
        help: "Average of total_query_count every 15 seconds",
        ty: "gauge",
    },
    "stats_avg_query_time" => MetricHelpType {
        help: "Average time taken for queries to execute every 15 seconds",
        ty: "gauge",
    },
    "stats_avg_recv" => MetricHelpType {
        help: "Average of total_received bytes every 15 seconds",
        ty: "gauge",
    },
    "stats_avg_sent" => MetricHelpType {
        help: "Average of total_sent bytes every 15 seconds",
        ty: "gauge",
    },
    "stats_avg_errors" => MetricHelpType {
        help: "Average number of errors every 15 seconds",
        ty: "gauge",
    },
    "stats_avg_xact_count" => MetricHelpType {
        help: "Average of total_xact_count every 15 seconds",
        ty: "gauge",
    },
    "stats_avg_xact_time" => MetricHelpType {
        help: "Average of total_xact_time every 15 seconds",
        ty: "gauge",
    },
    "stats_avg_wait_time" => MetricHelpType {
        help: "Average of total_wait_time every 15 seconds",
        ty: "gauge",
    },
    "pools_maxwait_us" => MetricHelpType {
        help: "The time a client waited for a server connection in microseconds",
        ty: "gauge",
    },
    "pools_maxwait" => MetricHelpType {
        help: "The time a client waited for a server connection in seconds",
        ty: "gauge",
    },
    "pools_cl_waiting" => MetricHelpType {
        help: "How many clients are waiting for a connection from the pool",
        ty: "gauge",
    },
    "pools_cl_active" => MetricHelpType {
        help: "How many clients are actively communicating with a server",
        ty: "gauge",
    },
    "pools_cl_idle" => MetricHelpType {
        help: "How many clients are idle",
        ty: "gauge",
    },
    "pools_sv_idle" => MetricHelpType {
        help: "How many server connections are idle",
        ty: "gauge",
    },
    "pools_sv_active" => MetricHelpType {
        help: "How many server connections are actively communicating with a client",
        ty: "gauge",
    },
    "pools_sv_login" => MetricHelpType {
        help: "How many server connections are currently being created",
        ty: "gauge",
    },
    "pools_sv_tested" => MetricHelpType {
        help: "How many server connections are currently waiting on a health check to succeed",
        ty: "gauge",
    },
    "servers_bytes_received" => MetricHelpType {
        help: "Volume in bytes of network traffic received by server",
        ty: "gauge",
    },
    "servers_bytes_sent" => MetricHelpType {
        help: "Volume in bytes of network traffic sent by server",
        ty: "gauge",
    },
    "servers_transaction_count" => MetricHelpType {
        help: "Number of transactions executed by server",
        ty: "gauge",
    },
    "servers_query_count" => MetricHelpType {
        help: "Number of queries executed by server",
        ty: "gauge",
    },
    "servers_error_count" => MetricHelpType {
        help: "Number of errors",
        ty: "gauge",
    },
    "databases_pool_size" => MetricHelpType {
        help: "Maximum number of server connections",
        ty: "gauge",
    },
    "databases_current_connections" => MetricHelpType {
        help: "Current number of connections for this database",
        ty: "gauge",
    },
};

struct PrometheusMetric<Value: fmt::Display> {
    name: String,
    help: String,
    ty: String,
    labels: HashMap<&'static str, String>,
    value: Value,
}

impl<Value: fmt::Display> fmt::Display for PrometheusMetric<Value> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let formatted_labels = self
            .labels
            .iter()
            .map(|(key, value)| format!("{}=\"{}\"", key, value))
            .collect::<Vec<_>>()
            .join(",");
        write!(
            f,
            "# HELP {name} {help}\n# TYPE {name} {ty}\n{name}{{{formatted_labels}}} {value}\n",
            name = format_args!("pgcat_{}", self.name),
            help = self.help,
            ty = self.ty,
            formatted_labels = formatted_labels,
            value = self.value
        )
    }
}

impl<Value: fmt::Display> PrometheusMetric<Value> {
    fn from_name<V: fmt::Display>(
        name: &str,
        value: V,
        labels: HashMap<&'static str, String>,
    ) -> Option<PrometheusMetric<V>> {
        METRIC_HELP_AND_TYPES_LOOKUP
            .get(name)
            .map(|metric| PrometheusMetric::<V> {
                name: name.to_owned(),
                help: metric.help.to_owned(),
                ty: metric.ty.to_owned(),
                value,
                labels,
            })
    }

    fn from_database_info(
        address: &Address,
        name: &str,
        value: u32,
    ) -> Option<PrometheusMetric<u32>> {
        let mut labels = HashMap::new();
        labels.insert("host", address.host.clone());
        labels.insert("shard", address.shard.to_string());
        labels.insert("role", address.role.to_string());
        labels.insert("pool", address.pool_name.clone());
        labels.insert("database", address.database.to_string());

        Self::from_name(&format!("databases_{}", name), value, labels)
    }

    fn from_server_info(
        address: &Address,
        name: &str,
        value: u64,
    ) -> Option<PrometheusMetric<u64>> {
        let mut labels = HashMap::new();
        labels.insert("host", address.host.clone());
        labels.insert("shard", address.shard.to_string());
        labels.insert("role", address.role.to_string());
        labels.insert("pool", address.pool_name.clone());
        labels.insert("database", address.database.to_string());

        Self::from_name(&format!("servers_{}", name), value, labels)
    }

    fn from_address(address: &Address, name: &str, value: u64) -> Option<PrometheusMetric<u64>> {
        let mut labels = HashMap::new();
        labels.insert("host", address.host.clone());
        labels.insert("shard", address.shard.to_string());
        labels.insert("pool", address.pool_name.clone());
        labels.insert("role", address.role.to_string());
        labels.insert("database", address.database.to_string());

        Self::from_name(&format!("stats_{}", name), value, labels)
    }

    fn from_pool(pool_id: PoolIdentifier, name: &str, value: u64) -> Option<PrometheusMetric<u64>> {
        let mut labels = HashMap::new();
        labels.insert("pool", pool_id.db);
        labels.insert("user", pool_id.user);

        Self::from_name(&format!("pools_{}", name), value, labels)
    }
}

async fn prometheus_stats(
    request: Request<body::Incoming>,
) -> Result<Response<Full<Bytes>>, hyper::http::Error> {
    match (request.method(), request.uri().path()) {
        (&Method::GET, "/metrics") => {
            let mut lines = Vec::new();
            push_address_stats(&mut lines);
            push_pool_stats(&mut lines);
            push_server_stats(&mut lines);
            push_database_stats(&mut lines);

            Response::builder()
                .header("content-type", "text/plain; version=0.0.4")
                .body(lines.join("\n").into())
        }
        _ => Response::builder()
            .status(StatusCode::NOT_FOUND)
            .body("".into()),
    }
}

// Adds metrics shown in a SHOW STATS admin command.
fn push_address_stats(lines: &mut Vec<String>) {
    for (_, pool) in get_all_pools() {
        for shard in 0..pool.shards() {
            for server in 0..pool.servers(shard) {
                let address = pool.address(shard, server);
                let stats = &*address.stats;
                for (key, value) in stats.clone() {
                    if let Some(prometheus_metric) =
                        PrometheusMetric::<u64>::from_address(address, &key, value)
                    {
                        lines.push(prometheus_metric.to_string());
                    } else {
                        debug!("Metric {} not implemented for {}", key, address.name());
                    }
                }
            }
        }
    }
}

// Adds relevant metrics shown in a SHOW POOLS admin command.
fn push_pool_stats(lines: &mut Vec<String>) {
    let pool_stats = PoolStats::construct_pool_lookup();
    for (pool_id, stats) in pool_stats.iter() {
        for (name, value) in stats.clone() {
            if let Some(prometheus_metric) =
                PrometheusMetric::<u64>::from_pool(pool_id.clone(), &name, value)
            {
                lines.push(prometheus_metric.to_string());
            } else {
                debug!("Metric {} not implemented for ({})", name, *pool_id);
            }
        }
    }
}

// Adds relevant metrics shown in a SHOW DATABASES admin command.
fn push_database_stats(lines: &mut Vec<String>) {
    for (_, pool) in get_all_pools() {
        let pool_config = pool.settings.clone();
        for shard in 0..pool.shards() {
            for server in 0..pool.servers(shard) {
                let address = pool.address(shard, server);
                let pool_state = pool.pool_state(shard, server);

                let metrics = vec![
                    ("pool_size", pool_config.user.pool_size),
                    ("current_connections", pool_state.connections),
                ];
                for (key, value) in metrics {
                    if let Some(prometheus_metric) =
                        PrometheusMetric::<u32>::from_database_info(address, key, value)
                    {
                        lines.push(prometheus_metric.to_string());
                    } else {
                        debug!("Metric {} not implemented for {}", key, address.name());
                    }
                }
            }
        }
    }
}

// Adds relevant metrics shown in a SHOW SERVERS admin command.
fn push_server_stats(lines: &mut Vec<String>) {
    let server_stats = get_server_stats();
    let mut server_stats_by_addresses = HashMap::<String, Arc<ServerStats>>::new();
    for (_, stats) in server_stats {
        server_stats_by_addresses.insert(stats.address_name(), stats);
    }

    for (_, pool) in get_all_pools() {
        for shard in 0..pool.shards() {
            for server in 0..pool.servers(shard) {
                let address = pool.address(shard, server);
                if let Some(server_info) = server_stats_by_addresses.get(&address.name()) {
                    let metrics = [
                        (
                            "bytes_received",
                            server_info.bytes_received.load(Ordering::Relaxed),
                        ),
                        ("bytes_sent", server_info.bytes_sent.load(Ordering::Relaxed)),
                        (
                            "transaction_count",
                            server_info.transaction_count.load(Ordering::Relaxed),
                        ),
                        (
                            "query_count",
                            server_info.query_count.load(Ordering::Relaxed),
                        ),
                        (
                            "error_count",
                            server_info.error_count.load(Ordering::Relaxed),
                        ),
                    ];
                    for (key, value) in metrics {
                        if let Some(prometheus_metric) =
                            PrometheusMetric::<u64>::from_server_info(address, key, value)
                        {
                            lines.push(prometheus_metric.to_string());
                        } else {
                            debug!("Metric {} not implemented for {}", key, address.name());
                        }
                    }
                }
            }
        }
    }
}

pub async fn start_metric_server(http_addr: SocketAddr) {
    let listener = TcpListener::bind(http_addr);
    let listener = match listener.await {
        Ok(listener) => listener,
        Err(e) => {
            error!("Failed to bind prometheus server to HTTP address: {}.", e);
            return;
        }
    };
    info!(
        "Exposing prometheus metrics on http://{}/metrics.",
        http_addr
    );
    loop {
        let stream = match listener.accept().await {
            Ok((stream, _)) => stream,
            Err(e) => {
                error!("Error accepting connection: {}", e);
                continue;
            }
        };
        let io = TokioIo::new(stream);

        tokio::task::spawn(async move {
            if let Err(err) = http1::Builder::new()
                .serve_connection(io, service_fn(prometheus_stats))
                .await
            {
                eprintln!("Error serving HTTP connection for metrics: {:?}", err);
            }
        });
    }
}
