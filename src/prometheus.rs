use hyper::service::{make_service_fn, service_fn};
use hyper::{Body, Method, Request, Response, Server, StatusCode};
use log::{error, info, warn};
use phf::phf_map;
use std::collections::HashMap;
use std::fmt;
use std::net::SocketAddr;

use crate::config::Address;
use crate::pool::get_all_pools;
use crate::stats::get_address_stats;

struct MetricHelpType {
    help: &'static str,
    ty: &'static str,
}

// reference for metric types: https://prometheus.io/docs/concepts/metric_types/
// counters only increase
// gauges can arbitrarily increase or decrease
static METRIC_HELP_AND_TYPES_LOOKUP: phf::Map<&'static str, MetricHelpType> = phf_map! {
    "total_query_count" => MetricHelpType {
        help: "Number of queries sent by all clients",
        ty: "counter",
    },
    "total_query_time" => MetricHelpType {
        help: "Total amount of time for queries to execute",
        ty: "counter",
    },
    "total_received" => MetricHelpType {
        help: "Number of bytes received from the server",
        ty: "counter",
    },
    "total_sent" => MetricHelpType {
        help: "Number of bytes sent to the server",
        ty: "counter",
    },
    "total_xact_count" => MetricHelpType {
        help: "Total number of transactions started by the client",
        ty: "counter",
    },
    "total_xact_time" => MetricHelpType {
        help: "Total amount of time for all transactions to execute",
        ty: "counter",
    },
    "total_wait_time" => MetricHelpType {
        help: "Total time client waited for a server connection",
        ty: "counter",
    },
    "avg_query_count" => MetricHelpType {
        help: "Average of total_query_count every 15 seconds",
        ty: "gauge",
    },
    "avg_query_time" => MetricHelpType {
        help: "Average time taken for queries to execute every 15 seconds",
        ty: "gauge",
    },
    "avg_recv" => MetricHelpType {
        help: "Average of total_received bytes every 15 seconds",
        ty: "gauge",
    },
    "avg_sent" => MetricHelpType {
        help: "Average of total_sent bytes every 15 seconds",
        ty: "gauge",
    },
    "avg_errors" => MetricHelpType {
        help: "Average number of errors every 15 seconds",
        ty: "gauge",
    },
    "avg_xact_count" => MetricHelpType {
        help: "Average of total_xact_count every 15 seconds",
        ty: "gauge",
    },
    "avg_xact_time" => MetricHelpType {
        help: "Average of total_xact_time every 15 seconds",
        ty: "gauge",
    },
    "avg_wait_time" => MetricHelpType {
        help: "Average of total_wait_time every 15 seconds",
        ty: "gauge",
    },
    "maxwait_us" => MetricHelpType {
        help: "The time a client waited for a server connection in microseconds",
        ty: "gauge",
    },
    "maxwait" => MetricHelpType {
        help: "The time a client waited for a server connection in seconds",
        ty: "gauge",
    },
    "cl_waiting" => MetricHelpType {
        help: "How many clients are waiting for a connection from the pool",
        ty: "gauge",
    },
    "cl_active" => MetricHelpType {
        help: "How many clients are actively communicating with a server",
        ty: "gauge",
    },
    "cl_idle" => MetricHelpType {
        help: "How many clients are idle",
        ty: "gauge",
    },
    "sv_idle" => MetricHelpType {
        help: "How many server connections are idle",
        ty: "gauge",
    },
    "sv_active" => MetricHelpType {
        help: "How many server connections are actively communicating with a client",
        ty: "gauge",
    },
    "sv_login" => MetricHelpType {
        help: "How many server connections are currently being created",
        ty: "gauge",
    },
    "sv_tested" => MetricHelpType {
        help: "How many server connections are currently waiting on a health check to succeed",
        ty: "gauge",
    },
};

struct PrometheusMetric {
    name: String,
    help: String,
    ty: String,
    labels: HashMap<&'static str, String>,
    value: i64,
}

impl fmt::Display for PrometheusMetric {
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

impl PrometheusMetric {
    fn new(address: &Address, name: &str, value: i64) -> Option<PrometheusMetric> {
        let mut labels = HashMap::new();
        labels.insert("host", address.host.clone());
        labels.insert("shard", address.shard.to_string());
        labels.insert("role", address.role.to_string());
        labels.insert("database", address.database.to_string());

        METRIC_HELP_AND_TYPES_LOOKUP
            .get(name)
            .map(|metric| PrometheusMetric {
                name: name.to_owned(),
                help: metric.help.to_owned(),
                ty: metric.ty.to_owned(),
                labels,
                value,
            })
    }
}

async fn prometheus_stats(request: Request<Body>) -> Result<Response<Body>, hyper::http::Error> {
    match (request.method(), request.uri().path()) {
        (&Method::GET, "/metrics") => {
            let stats: HashMap<usize, HashMap<String, i64>> = get_address_stats();

            let mut lines = Vec::new();
            for (_, pool) in get_all_pools() {
                for shard in 0..pool.shards() {
                    for server in 0..pool.servers(shard) {
                        let address = pool.address(shard, server);
                        if let Some(address_stats) = stats.get(&address.id) {
                            for (key, value) in address_stats.iter() {
                                if let Some(prometheus_metric) =
                                    PrometheusMetric::new(address, key, *value)
                                {
                                    lines.push(prometheus_metric.to_string());
                                } else {
                                    warn!("Metric {} not implemented for {}", key, address.name());
                                }
                            }
                        }
                    }
                }
            }

            Response::builder()
                .header("content-type", "text/plain; version=0.0.4")
                .body(lines.join("\n").into())
        }
        _ => Response::builder()
            .status(StatusCode::NOT_FOUND)
            .body("".into()),
    }
}

pub async fn start_metric_server(http_addr: SocketAddr) {
    let http_service_factory =
        make_service_fn(|_conn| async { Ok::<_, hyper::Error>(service_fn(prometheus_stats)) });
    let server = Server::bind(&http_addr).serve(http_service_factory);
    info!(
        "Exposing prometheus metrics on http://{}/metrics.",
        http_addr
    );
    if let Err(e) = server.await {
        error!("Failed to run HTTP server: {}.", e);
    }
}
