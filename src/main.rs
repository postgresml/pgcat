// Copyright (c) 2022 Lev Kokotov <hi@levthe.dev>

// Permission is hereby granted, free of charge, to any person obtaining
// a copy of this software and associated documentation files (the
// "Software"), to deal in the Software without restriction, including
// without limitation the rights to use, copy, modify, merge, publish,
// distribute, sublicense, and/or sell copies of the Software, and to
// permit persons to whom the Software is furnished to do so, subject to
// the following conditions:

// The above copyright notice and this permission notice shall be
// included in all copies or substantial portions of the Software.

// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
// EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
// MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
// NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE
// LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION
// OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION
// WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

extern crate arc_swap;
extern crate async_trait;
extern crate bb8;
extern crate bytes;
extern crate env_logger;
extern crate log;
extern crate md5;
extern crate num_cpus;
extern crate once_cell;
extern crate rustls_pemfile;
extern crate serde;
extern crate serde_derive;
extern crate sqlparser;
extern crate tokio;
extern crate tokio_rustls;
extern crate toml;

use log::{debug, error, info};
use parking_lot::Mutex;
use tokio::net::TcpListener;
use tokio::{
    signal::unix::{signal as unix_signal, SignalKind},
    sync::mpsc,
};

use std::collections::HashMap;
use std::net::SocketAddr;
use std::str::FromStr;
use std::sync::Arc;
use tokio::sync::broadcast;

mod admin;
mod client;
mod config;
mod constants;
mod errors;
mod messages;
mod pool;
mod prometheus;
mod query_router;
mod scram;
mod server;
mod sharding;
mod stats;
mod tls;

use crate::config::{get_config, reload_config, VERSION};
use crate::pool::{ClientServerMap, ConnectionPool};
use crate::prometheus::start_metric_server;
use crate::stats::{Collector, Reporter, REPORTER};

#[tokio::main(worker_threads = 4)]
async fn main() {
    env_logger::init();
    info!("Welcome to PgCat! Meow. (Version {})", VERSION);

    if !query_router::QueryRouter::setup() {
        error!("Could not setup query router");
        return;
    }

    let args = std::env::args().collect::<Vec<String>>();

    let config_file = if args.len() == 2 {
        args[1].to_string()
    } else {
        String::from("pgcat.toml")
    };

    match config::parse(&config_file).await {
        Ok(_) => (),
        Err(err) => {
            error!("Config parse error: {:?}", err);
            return;
        }
    };

    let config = get_config();

    if let Some(true) = config.general.enable_prometheus_exporter {
        let http_addr_str = format!(
            "{}:{}",
            config.general.host, config.general.prometheus_exporter_port
        );
        let http_addr = match SocketAddr::from_str(&http_addr_str) {
            Ok(addr) => addr,
            Err(err) => {
                error!("Invalid http address: {}", err);
                return;
            }
        };
        tokio::task::spawn(async move {
            start_metric_server(http_addr).await;
        });
    }

    let addr = format!("{}:{}", config.general.host, config.general.port);

    let listener = match TcpListener::bind(&addr).await {
        Ok(sock) => sock,
        Err(err) => {
            error!("Listener socket error: {:?}", err);
            return;
        }
    };

    info!("Running on {}", addr);

    config.show();

    // Tracks which client is connected to which server for query cancellation.
    let client_server_map: ClientServerMap = Arc::new(Mutex::new(HashMap::new()));

    // Statistics reporting.
    let (tx, rx) = mpsc::channel(100_000);
    REPORTER.store(Arc::new(Reporter::new(tx.clone())));

    // Connection pool that allows to query all shards and replicas.
    match ConnectionPool::from_config(client_server_map.clone()).await {
        Ok(_) => (),
        Err(err) => {
            error!("Pool error: {:?}", err);
            return;
        }
    };

    // Statistics collector task.
    let collector_tx = tx.clone();

    // Save these for reloading
    let reload_client_server_map = client_server_map.clone();
    let autoreload_client_server_map = client_server_map.clone();

    tokio::task::spawn(async move {
        let mut stats_collector = Collector::new(rx, collector_tx);
        stats_collector.collect().await;
    });

    info!("Waiting for clients");

    let (shutdown_event_tx, mut shutdown_event_rx) = broadcast::channel::<()>(1);

    let shutdown_event_tx_clone = shutdown_event_tx.clone();

    // Client connection loop.
    tokio::task::spawn(async move {
        // Creates event subscriber for shutdown event, this is dropped when shutdown event is broadcast
        let mut listener_shutdown_event_rx = shutdown_event_tx_clone.subscribe();
        loop {
            let client_server_map = client_server_map.clone();

            // Listen for shutdown event and client connection at the same time
            let (socket, addr) = tokio::select! {
                _ = listener_shutdown_event_rx.recv() => {
                    // Exits client connection loop which drops listener, listener_shutdown_event_rx and shutdown_event_tx_clone
                    break;
                }

                listener_response = listener.accept() => {
                    match listener_response {
                        Ok((socket, addr)) => (socket, addr),
                        Err(err) => {
                            error!("{:?}", err);
                            continue;
                        }
                    }
                }
            };

            // Used to signal shutdown
            let client_shutdown_handler_rx = shutdown_event_tx_clone.subscribe();

            // Used to signal that the task has completed
            let dummy_tx = shutdown_event_tx_clone.clone();

            // Handle client.
            tokio::task::spawn(async move {
                let start = chrono::offset::Utc::now().naive_utc();

                match client::client_entrypoint(
                    socket,
                    client_server_map,
                    client_shutdown_handler_rx,
                )
                .await
                {
                    Ok(_) => {
                        let duration = chrono::offset::Utc::now().naive_utc() - start;

                        info!(
                            "Client {:?} disconnected, session duration: {}",
                            addr,
                            format_duration(&duration)
                        );
                    }

                    Err(err) => {
                        debug!("Client disconnected with error {:?}", err);
                    }
                };
                // Drop this transmitter so receiver knows that the task is completed
                drop(dummy_tx);
            });
        }
    });

    // Reload config:
    // kill -SIGHUP $(pgrep pgcat)
    tokio::task::spawn(async move {
        let mut stream = unix_signal(SignalKind::hangup()).unwrap();

        loop {
            stream.recv().await;

            info!("Reloading config");

            match reload_config(reload_client_server_map.clone()).await {
                Ok(_) => (),
                Err(_) => continue,
            };

            get_config().show();
        }
    });

    if config.general.autoreload {
        let mut interval = tokio::time::interval(tokio::time::Duration::from_millis(15_000));

        tokio::task::spawn(async move {
            info!("Config autoreloader started");

            loop {
                interval.tick().await;
                match reload_config(autoreload_client_server_map.clone()).await {
                    Ok(changed) => {
                        if changed {
                            get_config().show()
                        }
                    }
                    Err(_) => (),
                };
            }
        });
    }

    let mut term_signal = unix_signal(SignalKind::terminate()).unwrap();
    let mut interrupt_signal = unix_signal(SignalKind::interrupt()).unwrap();

    tokio::select! {
        // Initiate graceful shutdown sequence on sig int
        _ = interrupt_signal.recv() => {
            info!("Got SIGINT, waiting for client connection drain now");

            // Broadcast that client tasks need to finish
            shutdown_event_tx.send(()).unwrap();
            // Closes transmitter
            drop(shutdown_event_tx);

            // This is in a loop because the first event that the receiver receives will be the shutdown event
            // This is not what we are waiting for instead, we want the receiver to send an error once all senders are closed which is reached after the shutdown event is received
            loop {
                match tokio::time::timeout(
                    tokio::time::Duration::from_millis(config.general.shutdown_timeout),
                    shutdown_event_rx.recv(),
                )
                .await
                {
                    Ok(res) => match res {
                        Ok(_) => {}
                        Err(_) => break,
                    },
                    Err(_) => {
                        info!("Timed out while waiting for clients to shutdown");
                        break;
                    }
                }
            }
        },
        _ = term_signal.recv() => (),
    }

    info!("Shutting down...");
}

/// Format chrono::Duration to be more human-friendly.
///
/// # Arguments
///
/// * `duration` - A duration of time
fn format_duration(duration: &chrono::Duration) -> String {
    let seconds = {
        let seconds = duration.num_seconds() % 60;
        if seconds < 10 {
            format!("0{}", seconds)
        } else {
            format!("{}", seconds)
        }
    };

    let minutes = {
        let minutes = duration.num_minutes() % 60;
        if minutes < 10 {
            format!("0{}", minutes)
        } else {
            format!("{}", minutes)
        }
    };

    let hours = {
        let hours = duration.num_hours() % 24;
        if hours < 10 {
            format!("0{}", hours)
        } else {
            format!("{}", hours)
        }
    };

    let days = duration.num_days().to_string();

    format!("{}d {}:{}:{}", days, hours, minutes, seconds)
}

#[cfg(test)]
mod test {
    extern crate postgres;

    use super::config::Config;
    use super::config::Pool;
    use super::config::Shard;
    use super::config::User;

    use core::time;
    use log::error;
    use postgres::{Client, NoTls};
    use rand::{distributions::Alphanumeric, Rng};
    use std::{
        collections::HashMap,
        fs,
        io::Write,
        process::{Child, Command, Stdio},
        thread::sleep,
    };

    pub const BASE_CONFIG: &str = "./pgcat.toml";

    pub struct PgcatInstance {
        process: Child,
        port: i16,
        log_file: String,
        config_filename: String,
        last_query_count: usize,
    }
    impl PgcatInstance {
        pub fn start(log_level: log::Level, config: Option<Config>) -> PgcatInstance {
            let base_filename: String = rand::thread_rng()
                .sample_iter(&Alphanumeric)
                .take(7)
                .map(char::from)
                .collect();
            let config_filename = format!("/tmp/pgcat_{}.temp.toml", base_filename);
            let log_filename = format!("/tmp/pgcat_{}.out", base_filename);

            let file = fs::File::create(log_filename.clone()).unwrap();
            let stdio = Stdio::from(file);

            let port = match config {
                Some(mut cfg) => {
                    let mut file = fs::File::create(config_filename.clone()).unwrap();
                    cfg.path = config_filename.clone();
                    file.write_all(toml::to_string(&cfg).unwrap().as_bytes())
                        .unwrap();
                    cfg.general.port
                }
                None => {
                    fs::copy(BASE_CONFIG, config_filename.clone()).unwrap();
                    let mut cfg = toml::from_str::<Config>(
                        &fs::read_to_string(config_filename.clone()).unwrap(),
                    )
                    .unwrap();
                    cfg.path = config_filename.clone();
                    let mut file = fs::File::create(config_filename.clone()).unwrap();
                    file.write_all(toml::to_string(&cfg).unwrap().as_bytes())
                        .unwrap();
                    cfg.general.port
                }
            };

            let child = Command::new("./target/debug/pgcat")
                .env("RUST_LOG", log_level.as_str())
                .arg(config_filename.clone())
                .stderr(stdio)
                .spawn()
                .unwrap();
            println!(
                "Started pgcat instance PID({:?}), LogFile({:?}), ConfigFile({:?})",
                child.id(),
                log_filename,
                config_filename
            );

            let instance = PgcatInstance {
                process: child,
                port: port,
                log_file: log_filename,
                config_filename: config_filename.clone(),
                last_query_count: 0,
            };
            instance.wait_until_ready();
            return instance;
        }

        pub fn current_config(&self) -> Config {
            return toml::from_str(
                fs::read_to_string(self.config_filename.clone())
                    .unwrap()
                    .as_str(),
            )
            .unwrap();
        }

        pub fn begin_recording_query_count(&mut self) {
            self.last_query_count = self.get_logs().matches("Sending query").count();
        }

        pub fn end_recording_query_count(&mut self) -> usize {
            return self.get_logs().matches("Sending query").count() - self.last_query_count;
        }

        pub fn get_logs(&self) -> String {
            fs::read_to_string(self.log_file.clone()).unwrap()
        }

        pub fn update_config(&self, new_config: Config) {
            let mut file = fs::File::create(self.config_filename.clone()).unwrap();
            file.write_all(toml::to_string(&new_config).unwrap().as_bytes())
                .unwrap();
        }

        pub fn reload_config(&self) {
            Command::new("kill")
                .args(["-s", "HUP", self.process.id().to_string().as_str()])
                .spawn()
                .unwrap()
                .wait()
                .unwrap();
        }

        pub fn stop(&mut self) {
            match self.process.kill() {
                Ok(_) => {
                    self.process.try_wait().unwrap();
                    ()
                }
                Err(err) => {
                    error!("Failed to kill process {:?}, {:?}", self.process.id(), err);
                    ()
                }
            }
        }

        pub fn wait_until_ready(&self) {
            let conn_str = format!(
                "postgres://sharding_user:sharding_user@localhost:{}/shard0",
                self.port
            );
            for _ in 0..30 {
                if Client::connect(&conn_str, NoTls).is_ok() {
                    return;
                }
                sleep(time::Duration::from_millis(500));
            }
            println!("Logs from failed process {}", self.get_logs());
            panic!("Server was never ready!");
        }
    }
    impl Drop for PgcatInstance {
        fn drop(&mut self) {
            self.stop();
            fs::remove_file(self.config_filename.clone()).unwrap();
            fs::remove_file(self.log_file.clone()).unwrap();
        }
    }

    pub fn single_shard_setup(
        num_replica: u8,
        db: String,
        base_config: Option<Config>,
    ) -> (PgcatInstance, Vec<PgcatInstance>) {
        let mut rng = rand::thread_rng();
        let mut pgclowder = vec![];
        let mut main_server_entries = vec![];
        let primary_pg_address = (
            String::from("localhost"),
            5432 as u16,
            String::from("primary"),
        );
        let replica_pg_address = (
            String::from("localhost"),
            5432 as u16,
            String::from("replica"),
        );

        let mut base_cfg = match base_config {
            Some(cfg) => cfg,
            None => Config::default(),
        };
        let user = User {
            username: String::from("sharding_user"),
            password: String::from("sharding_user"),
            pool_size: 5,
            statement_timeout: 100000,
        };
        base_cfg.pools.insert(db.clone(), Pool::default());
        base_cfg
            .pools
            .get_mut(&db.clone())
            .unwrap()
            .users
            .insert(String::from("sharding_user"), user);

        for i in 0..num_replica + 1 {
            let mut cfg = base_cfg.clone();
            let port = rng.gen_range(10000..32000);

            cfg.general.port = port;
            let (local_address, main_address) = match i {
                0 => (
                    primary_pg_address.clone(),
                    (
                        String::from("localhost"),
                        cfg.general.port as u16,
                        String::from("primary"),
                    ),
                ),
                _ => (
                    replica_pg_address.clone(),
                    (
                        String::from("localhost"),
                        cfg.general.port as u16,
                        String::from("replica"),
                    ),
                ),
            };

            cfg.pools.get_mut(&db.clone()).unwrap().shards = HashMap::from([(
                String::from("0"),
                Shard {
                    database: db.clone(),
                    servers: vec![local_address],
                },
            )]);
            main_server_entries.push(main_address);
            pgclowder.push(PgcatInstance::start(log::Level::Debug, Some(cfg)))
        }
        let mut main_cfg = base_cfg.clone();
        main_cfg.general.port = rng.gen_range(10000..32000);
        main_cfg.pools.get_mut(&db.clone()).unwrap().shards = HashMap::from([(
            String::from("0"),
            Shard {
                database: String::from("shard0"),
                // Server entries point to other Pgcat processes
                servers: main_server_entries,
            },
        )]);
        return (
            PgcatInstance::start(log::Level::Trace, Some(main_cfg)),
            pgclowder,
        );
    }

    fn value_within_range(target_value: i32, value: i32) {
        if value == target_value {
            return;
        }
        // Allow a buffer of 15% around the target value
        let buffer = ((target_value as f32) * 0.15) as i32;
        let start = target_value - buffer;
        let end = target_value + buffer;
        let result = if (start..end).contains(&value) {
            "Pass"
        } else {
            "Fail"
        };

        println!(
            "Expecting {} to fall between {} and {} ... {}",
            value, start, end, result
        );
        assert!((start..end).contains(&value));
    }

    #[test]
    fn test_basic_load_balancing() {
        let num_replicas: i32 = 3;
        let (main_pgcat, mut proxy_instances) =
            single_shard_setup(num_replicas as u8, String::from("shard0"), None);

        let conn_str = format!(
            "postgres://sharding_user:sharding_user@localhost:{}/shard0",
            main_pgcat.port
        );
        let mut client = Client::connect(&conn_str, NoTls).unwrap();

        for proxy in proxy_instances.iter_mut() {
            proxy.begin_recording_query_count();
        }
        let total_count = 5000;
        for _ in 0..total_count {
            if client.simple_query("SELECT 1").is_err() {
                client = Client::connect(&conn_str, NoTls).unwrap();
                continue;
            }
        }

        let expected_share = total_count / (num_replicas + 1);
        for proxy in proxy_instances.iter_mut() {
            value_within_range(expected_share, proxy.end_recording_query_count() as i32);
        }
    }

    #[test]
    fn test_failover_load_balancing() {
        let num_replicas: i32 = 3;
        let (main_pgcat, mut proxy_instances) =
            single_shard_setup(num_replicas as u8, String::from("shard0"), None);

        let mut replica2 = proxy_instances.pop().unwrap();
        let mut replica1 = proxy_instances.pop().unwrap();
        let mut replica0 = proxy_instances.pop().unwrap();
        let mut primary = proxy_instances.pop().unwrap();

        primary.begin_recording_query_count();
        replica0.begin_recording_query_count();
        replica1.begin_recording_query_count();
        replica2.begin_recording_query_count();

        replica1.stop();

        let conn_str = format!(
            "postgres://sharding_user:sharding_user@localhost:{}/shard0",
            main_pgcat.port
        );
        let total_count = 2000;
        let mut client = Client::connect(&conn_str, NoTls).unwrap();
        for _ in 0..total_count {
            if client.simple_query("SELECT 1").is_err() {
                client = Client::connect(&conn_str, NoTls).unwrap();
                continue;
            }
        }
        let expected_share = total_count / num_replicas;

        value_within_range(expected_share, primary.end_recording_query_count() as i32);
        value_within_range(expected_share, replica0.end_recording_query_count() as i32);
        value_within_range(0, replica1.end_recording_query_count() as i32);
        value_within_range(expected_share, replica2.end_recording_query_count() as i32);
    }

    #[test]
    fn test_load_balancing_with_query_routing() {
        let num_replicas: i32 = 3;
        let (main_pgcat, mut proxy_instances) = single_shard_setup(3, String::from("shard0"), None);
        let mut cfg = main_pgcat.current_config();
        cfg.pools.get_mut("shard0").unwrap().primary_reads_enabled = false;
        cfg.pools.get_mut("shard0").unwrap().query_parser_enabled = true;
        cfg.pools.get_mut("shard0").unwrap().default_role = String::from("auto");
        main_pgcat.update_config(cfg);
        main_pgcat.reload_config();

        let mut replica2 = proxy_instances.pop().unwrap();
        let mut replica1 = proxy_instances.pop().unwrap();
        let mut replica0 = proxy_instances.pop().unwrap();
        let mut primary = proxy_instances.pop().unwrap();

        primary.begin_recording_query_count();
        replica0.begin_recording_query_count();
        replica1.begin_recording_query_count();
        replica2.begin_recording_query_count();

        let conn_str = format!(
            "postgres://sharding_user:sharding_user@localhost:{}/shard0",
            main_pgcat.port
        );
        let mut client = Client::connect(&conn_str, NoTls).unwrap();
        client.simple_query("SET SERVER ROLE TO 'auto'").unwrap();

        let total_count = 2000;
        for _ in 0..total_count {
            if client.simple_query("SELECT 1").is_err() {
                client = Client::connect(&conn_str, NoTls).unwrap();
                continue;
            }
        }
        let expected_share = total_count / num_replicas;
        value_within_range(expected_share, replica0.end_recording_query_count() as i32);
        value_within_range(expected_share, replica1.end_recording_query_count() as i32);
        value_within_range(expected_share, replica2.end_recording_query_count() as i32);
        value_within_range(0, primary.end_recording_query_count() as i32);
    }
}
