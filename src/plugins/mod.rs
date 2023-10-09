//! The plugin ecosystem.
//!
//! Currently plugins only grant access or deny access to the database for a particual query.
//! Example use cases:
//!   - block known bad queries
//!   - block access to system catalogs
//!   - block dangerous modifications like `DROP TABLE`
//!   - etc
//!

pub mod intercept;
pub mod prewarmer;
pub mod query_logger;
pub mod table_access;

use crate::{errors::Error, query_router::QueryRouter};
use async_trait::async_trait;
use bytes::BytesMut;
use sqlparser::ast::Statement;

pub use intercept::Intercept;
pub use query_logger::QueryLogger;
pub use table_access::TableAccess;

#[derive(Clone, Debug, PartialEq)]
pub enum PluginOutput {
    Allow,
    Deny(String),
    Overwrite(Vec<Statement>),
    Intercept(BytesMut),
}

#[async_trait]
pub trait Plugin {
    // Run before the query is sent to the server.
    #[allow(clippy::ptr_arg)]
    async fn run(
        &mut self,
        query_router: &QueryRouter,
        ast: &Vec<Statement>,
    ) -> Result<PluginOutput, Error>;

    // TODO: run after the result is returned
    // async fn callback(&mut self, query_router: &QueryRouter);
}
