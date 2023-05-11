//! Prewarm new connections before giving them to the client.
use crate::{errors::Error, server::Server};
use arc_swap::ArcSwap;
// use async_trait::async_trait;
use log::info;
use once_cell::sync::Lazy;
use std::sync::Arc;

static QUERIES: Lazy<ArcSwap<Vec<String>>> = Lazy::new(|| ArcSwap::from_pointee(Vec::new()));

pub struct Prewarmer;

pub fn setup(queries: &Vec<String>) {
    QUERIES.store(Arc::new(queries.clone()));

    info!("Query prewarmer enabled.");

    for query in queries {
        info!("Prewarming query: {}", query);
    }
}

pub fn disable() {
    QUERIES.store(Arc::new(vec![]));
}

pub fn enabled() -> bool {
    !QUERIES.load().is_empty()
}

// TODO: come up with a decent interface for server plugins.

pub async fn run(server: &mut Server) -> Result<(), Error> {
    let mut queries = Vec::new();

    // Don't want  to hold arcswap while we run these potentially slow queries.
    {
        for query in QUERIES.load().iter() {
            queries.push(query.clone());
        }
    }

    for query in queries {
        server.query(&query).await?;
    }

    Ok(())
}
