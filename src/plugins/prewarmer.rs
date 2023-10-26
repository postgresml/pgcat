//! Prewarm new connections before giving them to the client.
use crate::{errors::Error, server::Server};
use log::info;

pub struct Prewarmer<'a> {
    pub enabled: bool,
    pub server: &'a mut Server,
    pub queries: &'a Vec<String>,
}

impl<'a> Prewarmer<'a> {
    pub async fn run(&mut self) -> Result<(), Error> {
        if !self.enabled {
            return Ok(());
        }

        for query in self.queries {
            info!(
                "{} Prewarning with query: `{}`",
                self.server.address(),
                query
            );
            self.server.query(query).await?;
        }

        Ok(())
    }
}
