//! Log all queries to stdout (or somewhere else, why not).

use crate::{
    errors::Error,
    plugins::{Plugin, PluginOutput},
    query_router::QueryRouter,
};
use async_trait::async_trait;
use log::info;
use sqlparser::ast::Statement;

pub struct QueryLogger<'a> {
    pub enabled: bool,
    pub user: &'a str,
    pub db: &'a str,
}

#[async_trait]
impl<'a> Plugin for QueryLogger<'a> {
    async fn run(
        &mut self,
        _query_router: &QueryRouter,
        ast: &Vec<Statement>,
    ) -> Result<PluginOutput, Error> {
        if !self.enabled {
            return Ok(PluginOutput::Allow);
        }

        let query = ast
            .iter()
            .map(|q| q.to_string())
            .collect::<Vec<String>>()
            .join("; ");
        info!("[pool: {}][user: {}] {}", self.db, self.user, query);

        Ok(PluginOutput::Allow)
    }
}
