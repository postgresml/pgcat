//! The intercept plugin.
//!
//! It intercepts queries and returns fake results.

use async_trait::async_trait;
use bytes::{BufMut, BytesMut};
use serde::{Deserialize, Serialize};
use sqlparser::ast::Statement;

use log::debug;

use crate::{
    config::Intercept as InterceptConfig,
    errors::Error,
    messages::{command_complete, data_row_nullable, row_description, DataType},
    plugins::{Plugin, PluginOutput},
    query_router::QueryRouter,
};

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
pub struct Intercept<'a> {
    pub enabled: bool,
    pub config: &'a InterceptConfig,
}

#[async_trait]
impl<'a> Plugin for Intercept<'a> {
    async fn run(
        &mut self,
        query_router: &QueryRouter,
        ast: &Vec<Statement>,
    ) -> Result<PluginOutput, Error> {
        if !self.enabled || ast.is_empty() {
            return Ok(PluginOutput::Allow);
        }

        let mut config = self.config.clone();
        config.substitute(
            &query_router.pool_settings().db,
            &query_router.pool_settings().user.username,
        );

        let mut result = BytesMut::new();

        for q in ast {
            // Normalization
            let q = q.to_string().to_ascii_lowercase();

            for (_, target) in config.queries.iter() {
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

                                if s.is_empty() {
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
