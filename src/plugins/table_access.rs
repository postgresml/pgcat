//! This query router plugin will check if the user can access a particular
//! table as part of their query. If they can't, the query will not be routed.

use async_trait::async_trait;
use sqlparser::ast::{visit_relations, Statement};

use crate::{
    errors::Error,
    plugins::{Plugin, PluginOutput},
    query_router::QueryRouter,
};

use log::debug;

use core::ops::ControlFlow;

pub struct TableAccess<'a> {
    pub enabled: bool,
    pub tables: &'a Vec<String>,
}

#[async_trait]
impl<'a> Plugin for TableAccess<'a> {
    async fn run(
        &mut self,
        _query_router: &QueryRouter,
        ast: &Vec<Statement>,
    ) -> Result<PluginOutput, Error> {
        if !self.enabled {
            return Ok(PluginOutput::Allow);
        }

        let mut found = None;

        visit_relations(ast, |relation| {
            let relation = relation.to_string();
            let parts = relation.split('.').collect::<Vec<&str>>();
            let table_name = parts.last().unwrap();

            if self.tables.contains(&table_name.to_string()) {
                found = Some(table_name.to_string());
                ControlFlow::<()>::Break(())
            } else {
                ControlFlow::<()>::Continue(())
            }
        });

        if let Some(found) = found {
            debug!("Blocking access to table \"{}\"", found);

            Ok(PluginOutput::Deny(format!(
                "permission for table \"{}\" denied",
                found
            )))
        } else {
            Ok(PluginOutput::Allow)
        }
    }
}
