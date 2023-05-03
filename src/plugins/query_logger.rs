//! Log all queries to stdout (or somewhere else, why not).

use crate::{
    errors::Error,
    plugins::{Plugin, PluginOutput},
    query_router::QueryRouter,
};
use arc_swap::ArcSwap;
use async_trait::async_trait;
use log::info;
use once_cell::sync::Lazy;
use sqlparser::ast::Statement;
use std::sync::Arc;

static ENABLED: Lazy<ArcSwap<bool>> = Lazy::new(|| ArcSwap::from_pointee(false));

pub struct QueryLogger;

pub fn setup() {
    ENABLED.store(Arc::new(true));

    info!("Logging queries to stdout");
}

pub fn disable() {
    ENABLED.store(Arc::new(false));
}

pub fn enabled() -> bool {
    **ENABLED.load()
}

#[async_trait]
impl Plugin for QueryLogger {
    async fn run(
        &mut self,
        _query_router: &QueryRouter,
        ast: &Vec<Statement>,
    ) -> Result<PluginOutput, Error> {
        let query = ast
            .iter()
            .map(|q| q.to_string())
            .collect::<Vec<String>>()
            .join("; ");
        info!("{}", query);

        Ok(PluginOutput::Allow)
    }
}
