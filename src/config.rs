use serde_derive::Deserialize;
use tokio::fs::File;
use tokio::io::AsyncReadExt;
use toml;

use std::collections::{HashMap, HashSet};

use crate::errors::Error;

#[derive(Clone, PartialEq, Deserialize, Hash, std::cmp::Eq, Debug, Copy)]
pub enum Role {
    Primary,
    Replica,
}

#[derive(Clone, PartialEq, Hash, std::cmp::Eq, Debug)]
pub struct Address {
    pub host: String,
    pub port: String,
    pub role: Role,
}

#[derive(Clone, PartialEq, Hash, std::cmp::Eq, Deserialize, Debug)]
pub struct User {
    pub name: String,
    pub password: String,
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
}

#[derive(Deserialize, Debug, Clone)]
pub struct Shard {
    pub servers: Vec<(String, u16, String)>,
    pub database: String,
}

#[derive(Deserialize, Debug, Clone)]
pub struct Config {
    pub general: General,
    pub user: User,
    pub shards: HashMap<String, Shard>,
}

/// Parse the config.
pub async fn parse(path: &str) -> Result<Config, Error> {
    let mut contents = String::new();
    let mut file = match File::open(path).await {
        Ok(file) => file,
        Err(err) => {
            println!("> Config error: {:?}", err);
            return Err(Error::BadConfig);
        }
    };

    match file.read_to_string(&mut contents).await {
        Ok(_) => (),
        Err(err) => {
            println!("> Config error: {:?}", err);
            return Err(Error::BadConfig);
        }
    };

    let config: Config = match toml::from_str(&contents) {
        Ok(config) => config,
        Err(err) => {
            println!("> Config error: {:?}", err);
            return Err(Error::BadConfig);
        }
    };

    // We use addresses as unique identifiers,
    // let's make sure they are unique in the config as well.
    for shard in &config.shards {
        let mut dup_check = HashSet::new();

        for server in &shard.1.servers {
            dup_check.insert(server);
        }

        if dup_check.len() != shard.1.servers.len() {
            println!("> Shard {} contains duplicate server configs.", &shard.0);
            return Err(Error::BadConfig);
        }
    }

    Ok(config)
}

#[cfg(test)]
mod test {
    use super::*;

    #[tokio::test]
    async fn test_config() {
        let config = parse("pgcat.toml").await.unwrap();
        assert_eq!(config.general.pool_size, 15);
        assert_eq!(config.shards.len(), 3);
        assert_eq!(config.shards["1"].servers[0].0, "127.0.0.1");
        assert_eq!(config.shards["0"].servers[0].2, "primary");
    }
}
