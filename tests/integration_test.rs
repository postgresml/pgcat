mod config;
mod errors;
mod tls;

use config::Config;
use core::time;
use postgres::{Client, NoTls};
use rand::{distributions::Alphanumeric, Rng};
use std::{
    collections::HashMap,
    fmt, fs,
    io::Write,
    process::{Child, Command, Stdio},
    thread::sleep,
};

pub const BASE_CONFIG: &str = "./pgcat.toml";

pub fn get_base_config() -> Config {
    return toml::from_str(fs::read_to_string(BASE_CONFIG).unwrap().as_str()).unwrap();
}

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
                let mut cfg =
                    toml::from_str::<Config>(&fs::read_to_string(config_filename.clone()).unwrap())
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

        let mut instance = PgcatInstance {
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

    pub fn get_total_query_count(&self) -> usize {
        self.get_logs().matches("talking to server Address").count()
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
        self.process.kill();
    }

    pub fn wait_until_ready(&self) {
        let conn_str = format!(
            "postgres://sharding_user:sharding_user@localhost:{}/shard0",
            self.port
        );
        for _ in 0..10 {
            if Client::connect(&conn_str, NoTls).is_ok() {
                return;
            }
            sleep(time::Duration::from_millis(500));
        }
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
    let user = config::User {
        username: String::from("sharding_user"),
        password: String::from("sharding_user"),
        pool_size: 5,
        statement_timeout: 100000,
    };
    base_cfg.pools.insert(db.clone(), config::Pool::default());
    base_cfg
        .pools
        .get_mut(&db.clone())
        .unwrap()
        .users
        .insert(String::from("sharding_user"), user);

    for i in 0..num_replica + 1 {
        let mut cfg = base_cfg.clone();
        cfg.general.port = 10000 + i as i16;
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
            config::Shard {
                database: db.clone(),
                servers: vec![local_address],
            },
        )]);
        main_server_entries.push(main_address);
        pgclowder.push(PgcatInstance::start(log::Level::Debug, Some(cfg)))
    }
    let mut main_cfg = base_cfg.clone();
    main_cfg.general.port = 6432;
    main_cfg.pools.get_mut(&db.clone()).unwrap().shards = HashMap::from([(
        String::from("0"),
        config::Shard {
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
    let (_main_pgcat, mut proxy_instances) = single_shard_setup(3, String::from("shard0"), None);
    let mut client = Client::connect(
        "postgres://sharding_user:sharding_user@localhost:6432/shard0",
        NoTls,
    )
    .unwrap();

    for proxy in proxy_instances.iter_mut() {
        proxy.begin_recording_query_count();
    }
    let total_count = 5000;
    for _ in 0..total_count {
        if client.simple_query("SELECT 1").is_err() {
            client = Client::connect(
                "postgres://sharding_user:sharding_user@localhost:6432/shard0",
                NoTls,
            )
            .unwrap();
            continue;
        }
    }

    let expected_share = total_count / 4;
    for proxy in proxy_instances.iter_mut() {
        value_within_range(expected_share, proxy.end_recording_query_count() as i32);
    }
}

#[test]
fn test_failover_load_balancing() {
    let (_main_pgcat, mut proxy_instances) = single_shard_setup(3, String::from("shard0"), None);

    let mut replica2 = proxy_instances.pop().unwrap();
    let mut replica1 = proxy_instances.pop().unwrap();
    let mut replica0 = proxy_instances.pop().unwrap();
    let mut primary = proxy_instances.pop().unwrap();

    primary.begin_recording_query_count();
    replica0.begin_recording_query_count();
    replica1.begin_recording_query_count();
    replica2.begin_recording_query_count();

    replica1.stop();

    let total_count = 2000;
    let mut client = Client::connect(
        "postgres://sharding_user:sharding_user@localhost:6432/shard0",
        NoTls,
    )
    .unwrap();
    for _ in 0..total_count {
        if client.simple_query("SELECT 1").is_err() {
            client = Client::connect(
                "postgres://sharding_user:sharding_user@localhost:6432/shard0",
                NoTls,
            )
            .unwrap();
            continue;
        }
    }
    let expected_share = total_count / 3;

    value_within_range(expected_share, primary.end_recording_query_count() as i32);
    value_within_range(expected_share, replica0.end_recording_query_count() as i32);
    value_within_range(0, replica1.end_recording_query_count() as i32);
    value_within_range(expected_share, replica2.end_recording_query_count() as i32);
}

#[test]
fn test_load_balancing_with_query_routing() {
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

    let mut client = Client::connect(
        "postgres://sharding_user:sharding_user@localhost:6432/shard0",
        NoTls,
    )
    .unwrap();
    client.simple_query("SET SERVER ROLE TO 'auto'").unwrap();

    let total_count = 2000;
    for _ in 0..total_count {
        if client.simple_query("SELECT 1").is_err() {
            client = Client::connect(
                "postgres://sharding_user:sharding_user@localhost:6432/shard0",
                NoTls,
            )
            .unwrap();
            continue;
        }
    }
    let expected_share = total_count / 3;
    value_within_range(expected_share, replica0.end_recording_query_count() as i32);
    value_within_range(expected_share, replica1.end_recording_query_count() as i32);
    value_within_range(expected_share, replica2.end_recording_query_count() as i32);
    value_within_range(0, primary.end_recording_query_count() as i32);
}
