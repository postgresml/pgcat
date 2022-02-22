use arc_swap::{ArcSwap, Guard};
use log::{error, info};
use once_cell::sync::Lazy;
use serde_derive::Deserialize;
use tokio::fs::File;
use tokio::io::AsyncReadExt;
use toml;

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use crate::errors::Error;

static CONFIG: Lazy<ArcSwap<Config>> = Lazy::new(|| ArcSwap::from_pointee(Config::default()));

#[derive(Clone, PartialEq, Deserialize, Hash, std::cmp::Eq, Debug, Copy)]
pub enum Role {
    Primary,
    Replica,
}

impl PartialEq<Option<Role>> for Role {
    fn eq(&self, other: &Option<Role>) -> bool {
        match other {
            None => true,
            Some(role) => *self == *role,
        }
    }
}

impl PartialEq<Role> for Option<Role> {
    fn eq(&self, other: &Role) -> bool {
        match *self {
            None => true,
            Some(role) => role == *other,
        }
    }
}

#[derive(Clone, PartialEq, Hash, std::cmp::Eq, Debug)]
pub struct Address {
    pub host: String,
    pub port: String,
    pub shard: usize,
    pub role: Role,
}

impl Default for Address {
    fn default() -> Address {
        Address {
            host: String::from("127.0.0.1"),
            port: String::from("5432"),
            shard: 0,
            role: Role::Replica,
        }
    }
}

#[derive(Clone, PartialEq, Hash, std::cmp::Eq, Deserialize, Debug)]
pub struct User {
    pub name: String,
    pub password: String,
}

impl Default for User {
    fn default() -> User {
        User {
            name: String::from("postgres"),
            password: String::new(),
        }
    }
}

#[derive(Deserialize, Debug, Clone)]
pub struct General {
    pub host: String,
    pub port: i16,
    pub pool_size: u32,
    pub pool_mode: String,
    pub connect_timeout: u64,
    pub healthcheck_timeout: u64,
    pub ban_time: i64,
    pub statsd_address: String,
}

impl Default for General {
    fn default() -> General {
        General {
            host: String::from("localhost"),
            port: 5432,
            pool_size: 15,
            pool_mode: String::from("transaction"),
            connect_timeout: 5000,
            healthcheck_timeout: 1000,
            ban_time: 60,
            statsd_address: String::from("127.0.0.1:8125"),
        }
    }
}

#[derive(Deserialize, Debug, Clone)]
pub struct Shard {
    pub servers: Vec<(String, u16, String)>,
    pub database: String,
}

impl Default for Shard {
    fn default() -> Shard {
        Shard {
            servers: vec![(String::from("localhost"), 5432, String::from("primary"))],
            database: String::from("postgres"),
        }
    }
}

#[derive(Deserialize, Debug, Clone)]
pub struct QueryRouter {
    pub default_role: String,
    pub query_parser_enabled: bool,
    pub primary_reads_enabled: bool,
}

impl Default for QueryRouter {
    fn default() -> QueryRouter {
        QueryRouter {
            default_role: String::from("any"),
            query_parser_enabled: false,
            primary_reads_enabled: true,
        }
    }
}

#[derive(Deserialize, Debug, Clone)]
pub struct Config {
    pub general: General,
    pub user: User,
    pub shards: HashMap<String, Shard>,
    pub query_router: QueryRouter,
}

impl Default for Config {
    fn default() -> Config {
        Config {
            general: General::default(),
            user: User::default(),
            shards: HashMap::from([(String::from("1"), Shard::default())]),
            query_router: QueryRouter::default(),
        }
    }
}

impl Config {
    pub fn show(&self) {
        info!("Pool size: {}", self.general.pool_size);
        info!("Pool mode: {}", self.general.pool_mode);
        info!("Ban time: {}s", self.general.ban_time);
        info!(
            "Healthcheck timeout: {}ms",
            self.general.healthcheck_timeout
        );
        info!("Connection timeout: {}ms", self.general.connect_timeout);
    }
}

pub fn get_config() -> Guard<Arc<Config>> {
    CONFIG.load()
}

/// Parse the config.
pub async fn parse(path: &str) -> Result<(), Error> {
    let mut contents = String::new();
    let mut file = match File::open(path).await {
        Ok(file) => file,
        Err(err) => {
            error!("Could not open '{}': {}", path, err.to_string());
            return Err(Error::BadConfig);
        }
    };

    match file.read_to_string(&mut contents).await {
        Ok(_) => (),
        Err(err) => {
            error!("Could not read config file: {}", err.to_string());
            return Err(Error::BadConfig);
        }
    };

    let config: Config = match toml::from_str(&contents) {
        Ok(config) => config,
        Err(err) => {
            error!("Could not parse config file: {}", err.to_string());
            return Err(Error::BadConfig);
        }
    };

    // Quick config sanity check.
    for shard in &config.shards {
        // We use addresses as unique identifiers,
        // let's make sure they are unique in the config as well.
        let mut dup_check = HashSet::new();
        let mut primary_count = 0;

        match shard.0.parse::<usize>() {
            Ok(_) => (),
            Err(_) => {
                error!(
                    "Shard '{}' is not a valid number, shards must be numbered starting at 0",
                    shard.0
                );
                return Err(Error::BadConfig);
            }
        };

        if shard.1.servers.len() == 0 {
            error!("Shard {} has no servers configured", shard.0);
            return Err(Error::BadConfig);
        }

        for server in &shard.1.servers {
            dup_check.insert(server);

            // Check that we define only zero or one primary.
            match server.2.as_ref() {
                "primary" => primary_count += 1,
                _ => (),
            };

            // Check role spelling.
            match server.2.as_ref() {
                "primary" => (),
                "replica" => (),
                _ => {
                    error!(
                        "Shard {} server role must be either 'primary' or 'replica', got: '{}'",
                        shard.0, server.2
                    );
                    return Err(Error::BadConfig);
                }
            };
        }

        if primary_count > 1 {
            error!("Shard {} has more than on primary configured", &shard.0);
            return Err(Error::BadConfig);
        }

        if dup_check.len() != shard.1.servers.len() {
            error!("Shard {} contains duplicate server configs", &shard.0);
            return Err(Error::BadConfig);
        }
    }

    match config.query_router.default_role.as_ref() {
        "any" => (),
        "primary" => (),
        "replica" => (),
        other => {
            error!(
                "Query router default_role must be 'primary', 'replica', or 'any', got: '{}'",
                other
            );
            return Err(Error::BadConfig);
        }
    };

    CONFIG.store(Arc::new(config.clone()));

    Ok(())
}

#[cfg(test)]
mod test {
    use super::*;

    #[tokio::test]
    async fn test_config() {
        parse("pgcat.toml").await.unwrap();
        assert_eq!(get_config().general.pool_size, 15);
        assert_eq!(get_config().shards.len(), 3);
        assert_eq!(get_config().shards["1"].servers[0].0, "127.0.0.1");
        assert_eq!(get_config().shards["0"].servers[0].2, "primary");
        assert_eq!(get_config().query_router.default_role, "any");
    }
}
