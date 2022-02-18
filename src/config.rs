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
pub struct QueryRouter {
    pub default_role: String,
    pub query_parser_enabled: bool,
    pub primary_reads_enabled: bool,
}

#[derive(Deserialize, Debug, Clone)]
pub struct Config {
    pub general: General,
    pub user: User,
    pub shards: HashMap<String, Shard>,
    pub query_router: QueryRouter,
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

    // Quick config sanity check.
    for shard in &config.shards {
        // We use addresses as unique identifiers,
        // let's make sure they are unique in the config as well.
        let mut dup_check = HashSet::new();
        let mut primary_count = 0;

        if shard.1.servers.len() == 0 {
            println!("> Shard {} has no servers configured", shard.0);
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
                    println!(
                        "> Shard {} server role must be either 'primary' or 'replica', got: '{}'",
                        shard.0, server.2
                    );
                    return Err(Error::BadConfig);
                }
            };
        }

        if primary_count > 1 {
            println!("> Shard {} has more than on primary configured.", &shard.0);
            return Err(Error::BadConfig);
        }

        if dup_check.len() != shard.1.servers.len() {
            println!("> Shard {} contains duplicate server configs.", &shard.0);
            return Err(Error::BadConfig);
        }
    }

    match config.query_router.default_role.as_ref() {
        "any" => (),
        "primary" => (),
        "replica" => (),
        other => {
            println!(
                "> Query router default_role must be 'primary', 'replica', or 'any', got: '{}'",
                other
            );
            return Err(Error::BadConfig);
        }
    };

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
        assert_eq!(config.query_router.default_role, "any");
    }
}
