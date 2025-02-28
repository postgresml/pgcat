//! Plugin for sanitizing and logging queries and their results
//! Replaces sensitive data matching configured regex patterns with <REDACTED>

use async_trait::async_trait;
use log::info;
use regex::Regex;
use sqlparser::ast::Statement;

use crate::{
    errors::Error,
    plugins::{Plugin, PluginOutput},
    query_router::QueryRouter,
};

#[derive(Clone)]
pub struct AuditLogger<'a> {
    pub enabled: bool,
    pub patterns: &'a Vec<String>,
    compiled_patterns: Vec<Regex>,
}

impl<'a> AuditLogger<'a> {
    pub fn new(enabled: bool, patterns: &'a Vec<String>) -> Result<Self, Error> {
        let compiled_patterns = patterns
            .iter()
            .map(|p| Regex::new(p))
            .collect::<Result<Vec<Regex>, regex::Error>>()
            .map_err(|_e| Error::BadConfig)?;

        Ok(AuditLogger {
            enabled,
            patterns,
            compiled_patterns,
        })
    }

    fn sanitize(&self, text: &str) -> String {
        let mut sanitized = text.to_string();
        for pattern in &self.compiled_patterns {
            sanitized = pattern.replace_all(&sanitized, "<REDACTED>").to_string();
        }
        sanitized
    }
}

#[async_trait]
impl<'a> Plugin for AuditLogger<'a> {
    async fn run(
        &mut self,
        query_router: &QueryRouter,
        ast: &Vec<Statement>,
    ) -> Result<PluginOutput, Error> {
        if !self.enabled {
            return Ok(PluginOutput::Allow);
        }

        // Log sanitized queries
        for stmt in ast {
            let query = stmt.to_string();
            let sanitized = self.sanitize(&query);
            info!(
                "[pool: {}][user: {}] Query: {}",
                query_router.pool_settings().db,
                query_router.pool_settings().user.username,
                sanitized
            );
        }

        Ok(PluginOutput::Allow)
    }
}
