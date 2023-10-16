/// Implementation of the PostgreSQL server (database) protocol.
/// Here we are pretending to the a Postgres client.
use log::{debug, warn};
use once_cell::sync::Lazy;
use sqlparser::ast::TransactionIsolationLevel;
use std::collections::HashMap;

use crate::errors::Error;
use crate::server::{Server, ServerParameters};

/// The default transaction parameters that might be configured on the server.
pub static TRANSACTION_PARAMETERS: Lazy<Vec<String>> = Lazy::new(|| {
    vec![
        "default_transaction_isolation".to_string(),
        "default_transaction_read_only".to_string(),
        "default_transaction_deferrable".to_string(),
    ]
});

/// The default transaction parameters that are either configured on the server or set by the
/// BEGIN statement.
#[derive(Debug, Clone)]
pub struct CommonTxnParams {
    pub(crate) state: TransactionState,

    pub(crate) xact_gid: Option<String>,

    isolation_level: TransactionIsolationLevel,
    read_only: bool,
    deferrable: bool,
}

impl CommonTxnParams {
    pub fn new(
        isolation_level: TransactionIsolationLevel,
        read_only: bool,
        deferrable: bool,
    ) -> Self {
        Self {
            state: TransactionState::Idle,
            xact_gid: None,
            isolation_level,
            read_only,
            deferrable,
        }
    }

    pub fn get_isolation_level(&self) -> TransactionIsolationLevel {
        self.isolation_level
    }

    pub fn is_read_only(&self) -> bool {
        self.read_only
    }

    pub fn is_deferrable(&self) -> bool {
        self.deferrable
    }

    pub fn set_isolation_level(&mut self, isolation_level: TransactionIsolationLevel) {
        self.isolation_level = isolation_level;
    }

    pub fn set_read_only(&mut self, read_only: bool) {
        self.read_only = read_only;
    }

    pub fn set_deferrable(&mut self, deferrable: bool) {
        self.deferrable = deferrable;
    }

    pub fn is_serializable(&self) -> bool {
        matches!(
            self.get_isolation_level(),
            TransactionIsolationLevel::Serializable
        )
    }

    pub fn is_repeatable_read(&self) -> bool {
        matches!(
            self.get_isolation_level(),
            TransactionIsolationLevel::RepeatableRead
        )
    }

    pub fn is_repeatable_read_or_higher(&self) -> bool {
        self.is_serializable() || self.is_repeatable_read()
    }

    /// Sets the default transaction parameters on the given ServerParameters instance.
    pub fn set_default_server_parameters(sparams: &mut ServerParameters) {
        // TODO(MD): make these configurable
        sparams.set_param(
            "default_transaction_isolation".to_string(),
            "read committed".to_string(),
            false,
        );
        sparams.set_param(
            "default_transaction_read_only".to_string(),
            "off".to_string(),
            false,
        );
        sparams.set_param(
            "default_transaction_deferrable".to_string(),
            "off".to_string(),
            false,
        );
    }
}

impl Default for CommonTxnParams {
    fn default() -> Self {
        Self::new(TransactionIsolationLevel::ReadCommitted, false, false)
    }
}

/// The various states that a server transaction can be in.
#[derive(Debug, PartialEq, Clone, Copy)]
pub enum TransactionState {
    /// Server is idle.
    Idle,
    /// Server is in a transaction.
    InTransaction,
    /// Server is in a failed transaction.
    InFailedTransaction,
}

/// The metadata of a server transaction.
#[derive(Default, Debug, Clone)]
pub struct ServerTxnMetaData {
    is_prepared: bool,

    pub params: CommonTxnParams,
}

impl ServerTxnMetaData {
    pub fn set_state(&mut self, state: TransactionState) {
        self.params.state = state;
    }

    pub fn state(&self) -> TransactionState {
        self.params.state
    }

    pub fn is_idle(&self) -> bool {
        self.params.state == TransactionState::Idle
    }

    pub fn is_in_transaction(&self) -> bool {
        self.params.state == TransactionState::InTransaction
    }

    pub fn is_in_failed_transaction(&self) -> bool {
        self.params.state == TransactionState::InFailedTransaction
    }

    pub fn set_xact_gid(&mut self, xact_gid: Option<String>) {
        self.params.xact_gid = xact_gid;
    }

    pub fn get_xact_gid(&self) -> Option<String> {
        self.params.xact_gid.clone()
    }

    pub fn set_prepared(&mut self, is_prepared: bool) {
        self.is_prepared = is_prepared;
    }

    pub fn has_done_prepare_transaction(&self) -> bool {
        self.is_prepared
    }
}

impl ServerParameters {
    fn get_default_transaction_isolation(&self) -> TransactionIsolationLevel {
        // Can unwrap because we set it in the constructor
        if let Some(isolation_level) = self.parameters.get("default_transaction_isolation") {
            return match isolation_level.to_lowercase().as_str() {
                "read committed" => TransactionIsolationLevel::ReadCommitted,
                "repeatable read" => TransactionIsolationLevel::RepeatableRead,
                "serializable" => TransactionIsolationLevel::Serializable,
                "read uncommitted" => TransactionIsolationLevel::ReadUncommitted,
                _ => TransactionIsolationLevel::ReadCommitted,
            };
        }
        TransactionIsolationLevel::ReadCommitted
    }

    fn get_default_transaction_read_only(&self) -> bool {
        if let Some(is_readonly) = self.parameters.get("default_transaction_read_only") {
            return !is_readonly.to_lowercase().eq("off");
        }
        false
    }

    fn get_default_transaction_deferrable(&self) -> bool {
        if let Some(deferrable) = self.parameters.get("default_transaction_deferrable") {
            return !deferrable.to_lowercase().eq("off");
        }
        false
    }

    fn get_default_transaction_parameters(&self) -> CommonTxnParams {
        CommonTxnParams::new(
            self.get_default_transaction_isolation(),
            self.get_default_transaction_read_only(),
            self.get_default_transaction_deferrable(),
        )
    }
}

impl Server {
    pub fn server_default_transaction_parameters(&self) -> CommonTxnParams {
        self.server_parameters.get_default_transaction_parameters()
    }

    /// Sends some queries to the server to sync the given pramaters specified by 'keys'.
    pub async fn sync_given_parameter_keys(&mut self, keys: &[String]) -> Result<(), Error> {
        let mut key_values = HashMap::new();
        for key in keys {
            if let Some(value) = self.server_parameters.parameters.get(key) {
                key_values.insert(key.clone(), value.clone());
            }
        }
        self.sync_given_parameter_key_values(&key_values).await
    }

    /// Sends some queries to the server to sync the given pramaters specified by 'key_values'.
    pub async fn sync_given_parameter_key_values(
        &mut self,
        key_values: &HashMap<String, String>,
    ) -> Result<(), Error> {
        let mut query = String::from("");

        for (key, value) in key_values {
            query.push_str(&format!("SET {} TO '{}';", key, value));
        }

        let res = self.query(&query).await;

        self.cleanup_state.reset();

        match res {
            Ok(_) => Ok(()),
            Err(Error::ErrorResponse(err_res)) => {
                warn!(
                    "Error while syncing parameters (was dropped): {:?}",
                    err_res
                );
                Ok(())
            }
            Err(err) => Err(err),
        }
    }

    /// Returnes true if the given server is in a failed transaction state.
    pub fn in_failed_transaction(&self) -> bool {
        self.transaction_metadata.is_in_failed_transaction()
    }

    /// Sets the GID on the server. If we are in serializable mode, we need to register the GID to
    /// the remote postgres instance, too.
    pub async fn assign_xact_gid(&mut self, gid: &str) -> Result<(), Error> {
        self.transaction_metadata
            .set_xact_gid(Some(gid.to_string()));
        Ok(())
    }

    pub async fn local_server_prepare_transaction(&mut self) -> Result<(), Error> {
        debug!(
            "Called local_server_prepare_transaction on {}",
            self.address
        );

        let xact_gid = self.transaction_metadata.get_xact_gid();
        if xact_gid.is_none() {
            return Err(Error::BadQuery(format!(
                "There is no GID assigned to the current transaction while it's requested to be \
    prepared to commit on the server ({}).",
                self.address()
            )));
        }
        let xact_gid = xact_gid.unwrap();

        self.query(&format!("PREPARE TRANSACTION '{}'", xact_gid))
            .await?;

        self.transaction_metadata.set_prepared(true);
        Ok(())
    }

    pub async fn local_server_commit_prepared(&mut self) -> Result<(), Error> {
        debug!("Called local_server_commit_prepared on {}.", self.address);

        let xact_gid = self.transaction_metadata.get_xact_gid();
        if xact_gid.is_none() {
            return Err(Error::BadQuery(
                "The current connection is not attached to a \
        transaction while it's requested to be prepared to commit."
                    .to_string(),
            ));
        }
        let xact_gid = xact_gid.unwrap();

        self.query(&format!("COMMIT PREPARED '{}'", xact_gid)).await
    }
}
