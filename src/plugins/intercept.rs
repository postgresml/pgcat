//! The intercept plugin.
//!
//! It intercepts queries and returns fake results.

use arc_swap::ArcSwap;
use async_trait::async_trait;
use bytes::{BufMut, BytesMut};
use once_cell::sync::Lazy;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use sqlparser::ast::Statement;
use std::collections::HashMap;

use log::{debug, info};
use std::sync::Arc;

use crate::{
    config::Intercept as InterceptConfig,
    errors::Error,
    messages::{command_complete, data_row_nullable, row_description, DataType},
    plugins::{Plugin, PluginOutput},
    pool::{PoolIdentifier, PoolMap},
    query_router::QueryRouter,
};

pub static CONFIG: Lazy<ArcSwap<HashMap<PoolIdentifier, InterceptConfig>>> =
    Lazy::new(|| ArcSwap::from_pointee(HashMap::new()));

/// Check if the interceptor plugin has been enabled.
pub fn enabled() -> bool {
    !CONFIG.load().is_empty()
}

pub fn setup(intercept_config: &InterceptConfig, pools: &PoolMap) {
    let mut config = HashMap::new();
    for (identifier, _) in pools.iter() {
        let mut intercept_config = intercept_config.clone();
        intercept_config.substitute(&identifier.db, &identifier.user);
        config.insert(identifier.clone(), intercept_config);
    }

    CONFIG.store(Arc::new(config));

    info!("Intercepting {} queries", intercept_config.queries.len());
}

pub fn disable() {
    CONFIG.store(Arc::new(HashMap::new()));
}

// TODO: use these structs for deserialization
#[derive(Serialize, Deserialize)]
pub struct Rule {
    query: String,
    schema: Vec<Column>,
    result: Vec<Vec<String>>,
}

#[derive(Serialize, Deserialize)]
pub struct Column {
    name: String,
    data_type: String,
}

/// The intercept plugin.
pub struct Intercept;

#[async_trait]
impl Plugin for Intercept {
    async fn run(
        &mut self,
        query_router: &QueryRouter,
        ast: &Vec<Statement>,
    ) -> Result<PluginOutput, Error> {
        if ast.is_empty() {
            return Ok(PluginOutput::Allow);
        }

        let mut result = BytesMut::new();
        let query_map = match CONFIG.load().get(&PoolIdentifier::new(
            &query_router.pool_settings().db,
            &query_router.pool_settings().user.username,
        )) {
            Some(query_map) => query_map.clone(),
            None => return Ok(PluginOutput::Allow),
        };

        for q in ast {
            // Normalization
            let q = q.to_string().to_ascii_lowercase();

            for (_, target) in query_map.queries.iter() {
                if target.query.as_str() == q {
                    debug!("Intercepting query: {}", q);

                    let rd = target
                        .schema
                        .iter()
                        .map(|row| {
                            let name = &row[0];
                            let data_type = &row[1];
                            (
                                name.as_str(),
                                match data_type.as_str() {
                                    "text" => DataType::Text,
                                    "anyarray" => DataType::AnyArray,
                                    "oid" => DataType::Oid,
                                    "bool" => DataType::Bool,
                                    "int4" => DataType::Int4,
                                    _ => DataType::Any,
                                },
                            )
                        })
                        .collect::<Vec<(&str, DataType)>>();

                    result.put(row_description(&rd));

                    target.result.iter().for_each(|row| {
                        let row = row
                            .iter()
                            .map(|s| {
                                let s = s.as_str().to_string();

                                if s == "" {
                                    None
                                } else {
                                    Some(s)
                                }
                            })
                            .collect::<Vec<Option<String>>>();
                        result.put(data_row_nullable(&row));
                    });

                    result.put(command_complete("SELECT"));
                }
            }
        }

        if !result.is_empty() {
            result.put_u8(b'Z');
            result.put_i32(5);
            result.put_u8(b'I');

            return Ok(PluginOutput::Intercept(result));
        } else {
            Ok(PluginOutput::Allow)
        }
    }
}

/// Make IntelliJ SQL plugin believe it's talking to an actual database
/// instead of PgCat.
#[allow(dead_code)]
fn fool_datagrip(database: &str, user: &str) -> Value {
    json!([
        {
            "query": "select current_database() as a, current_schemas(false) as b",
            "schema": [
                {
                    "name": "a",
                    "data_type": "text",
                },
                {
                    "name": "b",
                    "data_type": "anyarray",
                },
            ],

            "result": [
                [database, "{public}"],
            ],
        },
        {
            "query": "select current_database(), current_schema(), current_user",
            "schema": [
                {
                    "name": "current_database",
                    "data_type": "text",
                },
                {
                    "name": "current_schema",
                    "data_type": "text",
                },
                {
                    "name": "current_user",
                    "data_type": "text",
                }
            ],

            "result": [
                ["sharded_db", "public", "sharding_user"],
            ],
        },
        {
            "query": "select cast(n.oid as bigint) as id, datname as name, d.description, datistemplate as is_template, datallowconn as allow_connections, pg_catalog.pg_get_userbyid(n.datdba) as \"owner\" from pg_catalog.pg_database as n left join pg_catalog.pg_shdescription as d on n.oid = d.objoid order by case when datname = pg_catalog.current_database() then -cast(1 as bigint) else cast(n.oid as bigint) end",
            "schema": [
                {
                    "name": "id",
                    "data_type": "oid",
                },
                {
                    "name": "name",
                    "data_type": "text",
                },
                {
                    "name": "description",
                    "data_type": "text",
                },
                {
                    "name": "is_template",
                    "data_type": "bool",
                },
                {
                    "name": "allow_connections",
                    "data_type": "bool",
                },
                {
                    "name": "owner",
                    "data_type": "text",
                }
            ],
            "result": [
                ["16387", database, "", "f", "t", user],
            ]
        },
        {
            "query": "select cast(r.oid as bigint) as role_id, rolname as role_name, rolsuper as is_super, rolinherit as is_inherit, rolcreaterole as can_createrole, rolcreatedb as can_createdb, rolcanlogin as can_login, rolreplication as is_replication, rolconnlimit as conn_limit, rolvaliduntil as valid_until, rolbypassrls as bypass_rls, rolconfig as config, d.description from pg_catalog.pg_roles as r left join pg_catalog.pg_shdescription as d on d.objoid = r.oid",
            "schema": [
                {
                    "name": "role_id",
                    "data_type": "oid",
                },
                {
                    "name": "role_name",
                    "data_type": "text",
                },
                {
                    "name": "is_super",
                    "data_type": "bool",
                },
                {
                    "name": "is_inherit",
                    "data_type": "bool",
                },
                {
                    "name": "can_createrole",
                    "data_type": "bool",
                },
                {
                    "name": "can_createdb",
                    "data_type": "bool",
                },
                {
                    "name": "can_login",
                    "data_type": "bool",
                },
                {
                    "name": "is_replication",
                    "data_type": "bool",
                },
                {
                    "name": "conn_limit",
                    "data_type": "int4",
                },
                {
                    "name": "valid_until",
                    "data_type": "text",
                },
                {
                    "name": "bypass_rls",
                    "data_type": "bool",
                },
                {
                    "name": "config",
                    "data_type": "text",
                },
                {
                    "name": "description",
                    "data_type": "text",
                },
            ],
            "result": [
                ["10", "postgres", "f", "t", "f", "f", "t", "f", "-1", "", "f", "", ""],
                ["16419", user, "f", "t", "f", "f", "t", "f", "-1", "", "f", "", ""],
            ]
        }
    ])
}
