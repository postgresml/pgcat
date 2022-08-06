use hyper::service::{make_service_fn, service_fn};
use hyper::{Body, Method, Request, Response, Server, StatusCode};
use log::{error, info};
use phf::phf_map;
use std::collections::HashMap;
use std::fmt;
use std::net::SocketAddr;

use crate::config::Address;
use crate::pool::get_all_pools;
use crate::stats::get_stats;

struct MetricHelpType {
    help: &'static str,
    ty: &'static str,
}

// reference for metric types: https://prometheus.io/docs/concepts/metric_types/
// counters only increase
// gauges can arbitrarily increase or decrease
static METRIC_HELP_AND_TYPES_LOOKUP: phf::Map<&'static str, MetricHelpType> = phf_map! {
    "total_query_count" => MetricHelpType {
        help: "Total number of queries emitted",
        ty: "counter",
    },
    "total_query_time" => MetricHelpType {
        help: "Unimplemented",
        ty: "unknown",
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
        help: "Unimplemented",
        ty: "unknown",
    },
    "total_wait_time" => MetricHelpType {
        help: "Total time client waited for a server connection",
        ty: "counter",
    },
    "avg_query_count" => MetricHelpType {
        help: "",
        ty: "",
    },
    "avg_query_time" => MetricHelpType {
        help: "",
        ty: "",
    },
    "avg_recv" => MetricHelpType {
        help: "",
        ty: "",
    },
    "avg_sent" => MetricHelpType {
        help: "",
        ty: "",
    },
    "avg_xact_count" => MetricHelpType {
        help: "",
        ty: "",
    },
    "avg_xact_time" => MetricHelpType {
        help: "",
        ty: "",
    },
    "avg_wait_time" => MetricHelpType {
        help: "",
        ty: "",
    },
    "maxwait_us" => MetricHelpType {
        help: "",
        ty: "",
    },
    "maxwait" => MetricHelpType {
        help: "",
        ty: "",
    },
    "cl_waiting" => MetricHelpType {
        help: "",
        ty: "",
    },
    "cl_active" => MetricHelpType {
        help: "",
        ty: "",
    },
    "cl_idle" => MetricHelpType {
        help: "",
        ty: "",
    },
    "sv_idle" => MetricHelpType {
        help: "",
        ty: "",
    },
    "sv_active" => MetricHelpType {
        help: "",
        ty: "",
    },
    "sv_login" => MetricHelpType {
        help: "",
        ty: "",
    },
    "sv_tested" => MetricHelpType {
        help: "",
        ty: "",
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
    fn new(address: &Address, name: &str, value: i64) -> PrometheusMetric {
        let mut labels = HashMap::new();
        labels.insert("host", address.host.clone());
        labels.insert("shard", address.shard.to_string());
        labels.insert("role", address.role.to_string());
        labels.insert("database", address.database.to_string());

        let metric_help_type = METRIC_HELP_AND_TYPES_LOOKUP.get(name);
        let help = metric_help_type
            .map(|v| v.help)
            .unwrap_or_default()
            .to_owned();
        let ty = metric_help_type
            .map(|v| v.ty)
            .unwrap_or_default()
            .to_owned();
        PrometheusMetric {
            name: name.to_owned(),
            help,
            ty,
            labels,
            value,
        }
    }
}

async fn prometheus_stats(request: Request<Body>) -> Result<Response<Body>, hyper::http::Error> {
    match (request.method(), request.uri().path()) {
        (&Method::Get, "/metrics") => {
            let stats = get_stats();

            let mut lines = Vec::new();
            for (_, pool) in get_all_pools() {
                for shard in 0..pool.shards() {
                    for server in 0..pool.servers(shard) {
                        let address = pool.address(shard, server);
                        if let Some(address_stats) = stats.get(&address.id) {
                            for (key, value) in address_stats.iter() {
                                let prometheus_metric = PrometheusMetric::new(address, key, *value);
                                lines.push(prometheus_metric.to_string());
                            }
                        }
                    }
                }
            }

            Response::builder().body(lines.join("\n").into())
        }
        _ => Response::builder()
            .status(StatusCode::NOT_FOUND)
            .body("".into()),
    }
}

pub async fn start_metric_server(http_addr: SocketAddr) {
    let http_service_factory =
        make_service_fn(|_conn| async { Ok::<_, hyper::Error>(service_fn(prometheus_stats)) });
    let server = Server::bind(&http_addr.into()).serve(http_service_factory);
    info!("Exposing prometheus metrics on {http_addr}.");
    if let Err(e) = server.await {
        error!("Failed to run HTTP server: {}.", e);
    }
}
