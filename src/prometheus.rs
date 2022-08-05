use hyper::service::{make_service_fn, service_fn};
use hyper::{Body, Request, Response, Server, StatusCode};
use log::{error, info};
use std::collections::HashMap;
use std::fmt;
use std::net::SocketAddr;

use crate::config::Address;
use crate::pool::get_all_pools;
use crate::stats::get_stats;

struct PrometheusMetric {
    help: String,
    ty: String,
    name: String,
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
        PrometheusMetric {
            help: "".to_owned(),
            ty: "".to_owned(),
            name: name.to_owned(),
            labels,
            value,
        }
    }
}

async fn prometheus_stats(request: Request<Body>) -> Result<Response<Body>, hyper::http::Error> {
    if request.uri().path() != "/metrics" {
        Response::builder()
            .status(StatusCode::NOT_FOUND)
            .body("".into())
    } else {
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
        Ok(Response::new(lines.join("\n").into()))
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
