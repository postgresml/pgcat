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
pub mod table_access;

use crate::{errors::Error, query_router::QueryRouter};
use async_trait::async_trait;
use bytes::BytesMut;
use sqlparser::ast::Statement;

pub use intercept::Intercept;
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
    // Custom output is allowed because we want to extend this system
    // to rewriting queries some day. So an output of a plugin could be
    // a rewritten AST.
    async fn run(
        &mut self,
        query_router: &QueryRouter,
        ast: &Vec<Statement>,
    ) -> Result<PluginOutput, Error>;
}
