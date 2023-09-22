/// Implementation of the PostgreSQL server (database) protocol.
/// Here we are pretending to the a Postgres client.
use bytes::Buf;
use chrono::NaiveDateTime;
use itertools::Either;
use log::{debug, warn};
use once_cell::sync::Lazy;
use sqlparser::ast::{Statement, TransactionIsolationLevel};
use std::collections::HashMap;
use std::time::Instant;

use crate::errors::Error;
use crate::messages::*;
use crate::query_messages::{DataRow, ErrorResponse, Message, QueryResponse, RowDescription};
use crate::server::{Server, ServerParameters};

/// The default transaction parameters that might be configured on the server.
pub static TRANSACTION_PARAMETERS: Lazy<Vec<String>> = Lazy::new(|| {
    let mut list = Vec::new();
    list.push("default_transaction_isolation".to_string());
    list.push("default_transaction_read_only".to_string());
    list.push("default_transaction_deferrable".to_string());
    list
});

/// The default transaction parameters that are either configured on the server or set by the
/// BEGIN statement.
#[derive(Debug, Clone)]
pub struct TransactionParameters {
    isolation_level: TransactionIsolationLevel,
    read_only: bool,
    deferrable: bool,
}

impl TransactionParameters {
    pub fn new(
        isolation_level: TransactionIsolationLevel,
        read_only: bool,
        deferrable: bool,
    ) -> Self {
        Self {
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

impl Default for TransactionParameters {
    fn default() -> Self {
        Self::new(TransactionIsolationLevel::ReadCommitted, false, false)
    }
}

fn get_default_transaction_isolation(sparams: &ServerParameters) -> TransactionIsolationLevel {
    // Can unwrap because we set it in the constructor
    if let Some(isolation_level) = sparams.parameters.get("default_transaction_isolation") {
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

fn get_default_transaction_read_only(sparams: &ServerParameters) -> bool {
    if let Some(is_readonly) = sparams.parameters.get("default_transaction_read_only") {
        return !is_readonly.to_lowercase().eq("off");
    }
    false
}

fn get_default_transaction_deferrable(sparams: &ServerParameters) -> bool {
    if let Some(deferrable) = sparams.parameters.get("default_transaction_deferrable") {
        return !deferrable.to_lowercase().eq("off");
    }
    false
}

fn get_default_transaction_parameters(sparams: &ServerParameters) -> TransactionParameters {
    TransactionParameters::new(
        get_default_transaction_isolation(sparams),
        get_default_transaction_read_only(sparams),
        get_default_transaction_deferrable(sparams),
    )
}

pub fn server_default_transaction_parameters(server: &Server) -> TransactionParameters {
    get_default_transaction_parameters(&server.server_parameters)
}

/// Sends some queries to the server to sync the given pramaters specified by 'keys'.
pub async fn sync_given_parameter_keys(server: &mut Server, keys: &[String]) -> Result<(), Error> {
    let mut key_values = HashMap::new();
    for key in keys {
        if let Some(value) = server.server_parameters.parameters.get(key) {
            key_values.insert(key.clone(), value.clone());
        }
    }
    sync_given_parameter_key_values(server, &key_values).await
}

/// Sends some queries to the server to sync the given pramaters specified by 'key_values'.
pub async fn sync_given_parameter_key_values(
    server: &mut Server,
    key_values: &HashMap<String, String>,
) -> Result<(), Error> {
    let mut query = String::from("");

    for (key, value) in key_values {
        query.push_str(&format!("SET {} TO '{}';", key, value));
    }

    let res = server.query(&query).await;

    server.cleanup_state.reset();

    match res {
        Ok(None) => Ok(()),
        Ok(Some(err_res)) => {
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
pub fn in_failed_transaction(server: &Server) -> bool {
    server.transaction_metadata.is_in_failed_transaction()
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
#[derive(Debug, Clone)]
pub struct TransactionMetaData {
    pub(crate) state: TransactionState,

    xact_gid: Option<String>,
    snapshot: Option<String>,
    prepared_timestamp: Option<NaiveDateTime>,

    begin_statement: Option<Statement>,
    commit_statement: Option<Statement>,
    abort_statement: Option<Statement>,

    pub params: TransactionParameters,
}

impl TransactionMetaData {
    pub fn set_state(&mut self, state: TransactionState) {
        match self.state {
            TransactionState::Idle => {
                self.state = state;
            }
            TransactionState::InTransaction => match state {
                TransactionState::Idle => {
                    warn!("Cannot go back to idle from a transaction.");
                }
                _ => {
                    self.state = state;
                }
            },
            TransactionState::InFailedTransaction => match state {
                TransactionState::Idle => {
                    warn!("Cannot go back to idle from a failed transaction.");
                }
                TransactionState::InTransaction => {
                    warn!("Cannot go back to a transaction from a failed transaction.")
                }
                _ => {
                    self.state = state;
                }
            },
        }
    }

    pub fn state(&self) -> TransactionState {
        self.state
    }

    pub fn is_idle(&self) -> bool {
        self.state == TransactionState::Idle
    }

    pub fn is_in_transaction(&self) -> bool {
        self.state == TransactionState::InTransaction
    }

    pub fn is_in_failed_transaction(&self) -> bool {
        self.state == TransactionState::InFailedTransaction
    }

    pub fn set_xact_gid(&mut self, xact_gid: Option<String>) {
        self.xact_gid = xact_gid;
    }

    pub fn get_xact_gid(&self) -> Option<String> {
        self.xact_gid.clone()
    }

    pub fn set_snapshot(&mut self, snapshot: Option<String>) {
        self.snapshot = snapshot;
    }

    pub fn get_snapshot(&self) -> Option<String> {
        self.snapshot.clone()
    }

    pub fn set_prepared_timestamp(&mut self, prepared_timestamp: Option<NaiveDateTime>) {
        self.prepared_timestamp = prepared_timestamp;
    }

    pub fn get_prepared_timestamp(&self) -> Option<NaiveDateTime> {
        self.prepared_timestamp
    }

    pub fn has_done_prepare_transaction(&self) -> bool {
        self.prepared_timestamp.is_some()
    }

    pub fn set_begin_statement(&mut self, begin_statement: Option<Statement>) {
        self.begin_statement = begin_statement;
    }

    pub fn get_begin_statement(&self) -> Option<&Statement> {
        self.begin_statement.as_ref()
    }

    pub fn set_commit_statement(&mut self, commit_statement: Option<Statement>) {
        self.commit_statement = commit_statement;
    }

    pub fn get_commit_statement(&self) -> Option<&Statement> {
        self.commit_statement.as_ref()
    }

    pub fn set_abort_statement(&mut self, abort_statement: Option<Statement>) {
        self.abort_statement = abort_statement;
    }

    pub fn get_abort_statement(&self) -> Option<&Statement> {
        self.abort_statement.as_ref()
    }
}

impl Default for TransactionMetaData {
    fn default() -> Self {
        Self {
            state: TransactionState::Idle,
            xact_gid: None,
            snapshot: None,
            prepared_timestamp: None,
            begin_statement: None,
            commit_statement: None,
            abort_statement: None,
            params: TransactionParameters::default(),
        }
    }
}

/// Represents a read-write conflict.
#[derive(Debug, Clone, Hash, PartialEq, Eq)]
pub struct RWConflict {
    pub source_gid: String,
    pub gid_in: String,
    pub gid_out: String,
}

impl RWConflict {
    pub fn new(source_gid: String, gid_in: String, gid_out: String) -> Self {
        Self {
            source_gid,
            gid_in,
            gid_out,
        }
    }
}

/// Given the query start time, this function registers the query execution time to the server
/// stats.
pub fn query_time_stats(server: &mut Server, query_start: Instant) {
    server.stats().query(
        Instant::now().duration_since(query_start).as_millis() as u64,
        &server.server_parameters.get_application_name(),
    );
}

/// Execute an arbitrary query against the server.
/// It will use the simple query protocol.
/// Result will be returned, so this is useful for things like `SELECT`.
async fn query_with_response(
    server: &mut Server,
    query: &str,
) -> Result<Either<QueryResponse, ErrorResponse>, Error> {
    debug!(
        "Running `{}` on server {} and capture response.",
        query, server.address
    );

    let query = simple_query(query);

    server.send(&query).await?;

    let query_start = Instant::now();
    // Read all data the server has to offer, which can be multiple messages
    // buffered in 8196 bytes chunks.
    let mut response = server.recv(None).await?;

    let query_response = match response[0] {
        b'T' => {
            let row_desc = RowDescription::decode(&mut response)?.unwrap();

            let mut data_rows = Vec::new();

            loop {
                if response.remaining() == 0 {
                    if server.is_data_available() {
                        response = server.recv(None).await?;
                    } else {
                        break;
                    }
                }
                if response[0] == b'C' {
                    break;
                }

                let data_row = DataRow::decode(&mut response)?.unwrap();
                data_rows.push(data_row);
            }

            QueryResponse::new(row_desc, data_rows)
        }

        b'E' => {
            let err = ErrorResponse::decode(&mut response)?.unwrap();
            return Ok(Either::Right(err));
        }

        _ => return Err(Error::ServerError),
    };

    query_time_stats(server, query_start);

    Ok(Either::Left(query_response))
}

/// Captures the snapshot from the server.
pub async fn acquire_xact_snapshot(
    server: &mut Server,
) -> Result<Either<String, ErrorResponse>, Error> {
    let qres = query_with_response(server, "select pg_export_snapshot()").await?;

    if qres.is_right() {
        return Ok(Either::Right(qres.right().unwrap()));
    }

    let qres = qres.left().unwrap();

    let qres_rows: &[DataRow] = qres.data_rows();
    assert!(qres.row_desc().fields().len() == 1);
    assert!(qres_rows.len() == 1);
    if let Some(snapshot) = qres_rows[0].fields().get(0).unwrap() {
        let snapshot = std::str::from_utf8(&snapshot).unwrap().to_string();

        debug!("Got snapshot: {}", snapshot);

        server
            .transaction_metadata
            .set_snapshot(Some(snapshot.clone()));

        Ok(Either::Left(snapshot))
    } else {
        Err(Error::BadQuery(
            "Could not get snapshot from server".to_string(),
        ))
    }
}

/// Sets the snapshot to the server (based on a previous snapshot acquired by the first server).
pub async fn assign_xact_snapshot(
    server: &mut Server,
    snapshot: &str,
) -> Result<Option<ErrorResponse>, Error> {
    server
        .query(&format!("set transaction snapshot '{snapshot}'"))
        .await
}

/// Sets the GID on the server. If we are in serializable mode, we need to register the GID to
/// the remote postgres instance, too.
pub async fn assign_xact_gid(
    server: &mut Server,
    gid: &str,
) -> Result<Option<ErrorResponse>, Error> {
    server
        .transaction_metadata
        .set_xact_gid(Some(gid.to_string()));
    Ok(None)
}

pub async fn local_server_prepare_transaction(
    server: &mut Server,
) -> Result<Option<ErrorResponse>, Error> {
    debug!(
        "Called local_server_prepare_transaction on {}",
        server.address,
    );

    let xact_gid = server.transaction_metadata.get_xact_gid();
    if xact_gid.is_none() {
        return Err(Error::BadQuery(format!(
            "There is no GID assigned to the current transaction while it's requested to be \
            prepared to commit on the server ({}).",
            server.address()
        )));
    }
    let xact_gid = xact_gid.unwrap();

    if let Some(prep_time) = server.transaction_metadata.get_prepared_timestamp() {
        return Err(Error::BadQuery(format!(
            "The server ({}) was prepared in the past: {} (with gid: {})",
            server.address(),
            prep_time,
            xact_gid,
        )));
    }

    let qres = server
        .query(&format!("PREPARE TRANSACTION '{}'", xact_gid))
        .await?;
    if qres.is_some() {
        return Ok(qres);
    }

    Ok(None)
}

pub async fn local_server_commit_prepared(
    server: &mut Server,
    commit_ts: NaiveDateTime,
) -> Result<Option<ErrorResponse>, Error> {
    debug!(
        "Called local_server_commit_prepared on {} with commit_ts: {:?}",
        server.address, commit_ts
    );

    let xact_gid = server.transaction_metadata.get_xact_gid();
    if xact_gid.is_none() {
        return Err(Error::BadQuery(
            "The current connection is not attached to a \
                transaction while it's requested to be prepared to commit."
                .to_string(),
        ));
    }
    let xact_gid = xact_gid.unwrap();

    let qres = server
        .query(&format!("COMMIT PREPARED '{}'", xact_gid))
        .await?;
    if qres.is_some() {
        return Ok(qres);
    }

    Ok(None)
}
