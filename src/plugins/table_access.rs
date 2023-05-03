//! This query router plugin will check if the user can access a particular
//! table as part of their query. If they can't, the query will not be routed.

use async_trait::async_trait;
use sqlparser::ast::{visit_relations, Statement};

use crate::{
    config::TableAccess as TableAccessConfig,
    errors::Error,
    plugins::{Plugin, PluginOutput},
    query_router::QueryRouter,
};

use log::{debug, info};

use arc_swap::ArcSwap;
use core::ops::ControlFlow;
use once_cell::sync::Lazy;
use std::sync::Arc;

static CONFIG: Lazy<ArcSwap<Vec<String>>> = Lazy::new(|| ArcSwap::from_pointee(vec![]));

pub fn setup(config: &TableAccessConfig) {
    CONFIG.store(Arc::new(config.tables.clone()));

    info!("Blocking access to {} tables", config.tables.len());
}

pub fn enabled() -> bool {
    !CONFIG.load().is_empty()
}

pub fn disable() {
    CONFIG.store(Arc::new(vec![]));
}

pub struct TableAccess;

#[async_trait]
impl Plugin for TableAccess {
    async fn run(
        &mut self,
        _query_router: &QueryRouter,
        ast: &Vec<Statement>,
    ) -> Result<PluginOutput, Error> {
        let mut found = None;
        let forbidden_tables = CONFIG.load();

        visit_relations(ast, |relation| {
            let relation = relation.to_string();
            let parts = relation.split(".").collect::<Vec<&str>>();
            let table_name = parts.last().unwrap();

            if forbidden_tables.contains(&table_name.to_string()) {
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
