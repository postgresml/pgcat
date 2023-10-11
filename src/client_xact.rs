use crate::client::Client;
use crate::errors::Error;
use crate::query_messages::{ErrorInfo, ErrorResponse, Message};
use bytes::BytesMut;

use core::panic;
use futures::future::join_all;
use log::{debug, warn};
use sqlparser::ast::{Statement, TransactionAccessMode, TransactionMode};
use std::collections::HashMap;
use uuid::Uuid;

use crate::config::Address;
use crate::messages::*;
use crate::server::Server;
use crate::server_xact::*;

pub type ServerId = usize;

/// The metadata of a server transaction.
#[derive(Default, Debug, Clone)]
pub struct ClientTxnMetaData {
    begin_statement: Option<Statement>,
    commit_statement: Option<Statement>,
    abort_statement: Option<Statement>,

    pub params: CommonTxnParams,
}

impl ClientTxnMetaData {
    pub fn set_state(&mut self, state: TransactionState) {
        set_state_helper(&mut self.params.state, state);
    }

    pub fn state(&self) -> TransactionState {
        self.params.state
    }

    pub fn is_idle(&self) -> bool {
        self.params.state == TransactionState::Idle
    }

    pub fn set_xact_gid(&mut self, xact_gid: Option<String>) {
        self.params.xact_gid = xact_gid;
    }

    pub fn get_xact_gid(&self) -> Option<String> {
        self.params.xact_gid.clone()
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

impl<S, T> Client<S, T>
where
    S: tokio::io::AsyncRead + std::marker::Unpin,
    T: tokio::io::AsyncWrite + std::marker::Unpin,
{
    /// This function starts a distributed transaction by sending a BEGIN statement to the first server.
    /// It is called on the first server, as soon as client wants to interact with another server,
    /// which hints that the client wants to start a distributed transaction.
    pub async fn begin_distributed_xact(
        &mut self,
        server_key: ServerId,
        server: &mut Server,
    ) -> Result<bool, Error> {
        let begin_stmt = self.xact_info.get_begin_statement();
        assert!(begin_stmt.is_some());
        let res = server.query(&begin_stmt.unwrap().to_string()).await;
        if self.post_query_processing(server, res).await?.is_none() {
            return Ok(false);
        }

        // If we are in a distributed transaction, we need to assign a GID to the transaction.
        assert!(self.xact_info.get_xact_gid().is_some());
        let gid = self.xact_info.get_xact_gid().unwrap();

        debug!("Assigning GID ('{}') to server {}", gid, server.address(),);

        let gid_res = server
            .assign_xact_gid(&Self::gen_server_specific_gid(server_key, &gid))
            .await;
        if self.post_query_processing(server, gid_res).await?.is_none() {
            return Ok(false);
        }

        Ok(true)
    }

    /// This functions generates a GID for the current transaction and sends it to the server.
    pub async fn acquire_gid(
        &mut self,
        server_key: ServerId,
        server: &mut Server,
    ) -> Result<bool, Error> {
        assert!(self.xact_info.get_xact_gid().is_none());
        let gid = self.generate_xact_gid();

        debug!("Acquiring GID ('{}') from server {}", gid, server.address(),);

        // If we are in a distributed transaction, we need to assign a GID to the transaction.
        let gid_res = server
            .assign_xact_gid(&Self::gen_server_specific_gid(server_key, &gid))
            .await;
        if self.post_query_processing(server, gid_res).await?.is_none() {
            return Ok(false);
        }
        self.xact_info.set_xact_gid(Some(gid));
        Ok(true)
    }

    /// Generates a random GID (i.e., Global transaction ID) for a transaction.
    fn generate_xact_gid(&self) -> String {
        format!("txn_{}_{}", self.addr, Uuid::new_v4())
    }

    /// Generates a server-specific GID for a transaction. We need this, because it's possible that
    /// multiple servers might actually be the same server (which commonly happens in testing).
    fn gen_server_specific_gid(server_key: ServerId, gid: &str) -> String {
        format!("{}_{}", server_key, gid)
    }

    /// Assigns the transaction state based on the state of all servers.
    pub fn assign_client_transaction_state(
        &mut self,
        all_conns: &HashMap<
            ServerId,
            (bb8::PooledConnection<'_, crate::pool::ServerPool>, Address),
        >,
    ) {
        self.xact_info.set_state(if all_conns.is_empty() {
            // if there's no server, we're in idle mode.
            TransactionState::Idle
        } else if Self::is_any_server_in_failed_xact(all_conns) {
            // if any server is in failed transaction, we're in failed transaction.
            TransactionState::InFailedTransaction
        } else {
            // if we have at least one server and it is in a transaction, we're in a transaction.
            TransactionState::InTransaction
        });
    }

    /// Returns true if any server is in a failed transaction.
    fn is_any_server_in_failed_xact(
        all_conns: &HashMap<
            ServerId,
            (bb8::PooledConnection<'_, crate::pool::ServerPool>, Address),
        >,
    ) -> bool {
        all_conns
            .iter()
            .any(|(_, conn)| conn.0.in_failed_transaction())
    }

    /// This function initializes the transaction parameters based on the server's default.
    pub fn initialize_xact_params(&mut self, server: &mut Server, begin_stmt: &Statement) {
        if let Statement::StartTransaction { modes } = begin_stmt {
            // Initialize transaction parameters using the server's default.
            self.xact_info.params = server.server_default_transaction_parameters();
            for mode in modes {
                match mode {
                    TransactionMode::AccessMode(access_mode) => {
                        self.xact_info.params.set_read_only(match access_mode {
                            TransactionAccessMode::ReadOnly => true,
                            TransactionAccessMode::ReadWrite => false,
                        });
                    }
                    TransactionMode::IsolationLevel(isolation_level) => {
                        self.xact_info.params.set_isolation_level(*isolation_level);
                    }
                }
            }
            debug!(
                "Transaction paramaters after the first BEGIN statement: {:?}",
                self.xact_info.params
            );

            // Set the transaction parameters on the first server.
            server.transaction_metadata_mut().params = self.xact_info.params.clone();
        } else {
            // If it's not a BEGIN, then it's an irrecoverable error.
            panic!("The statement is not a BEGIN statement.");
        }
    }

    /// This function performs a distribted abort/commit if necessary, and also resets the transaction
    /// state. This is suppoed to be called before exiting the transaction loop. At that point, if
    /// either an abort or commit statement is set, we need to perform a distributed abort/commit. This
    /// is based on the logic that an abort or commit statement is only set if we are in a distributed
    /// transaction and we observe a commit or abort statement sent to the server. That is where we exit
    /// the transaction loop and expect this function to takeover and abort/commit the transaction.
    pub async fn distributed_commit_or_abort(
        &mut self,
        all_conns: &mut HashMap<
            ServerId,
            (bb8::PooledConnection<'_, crate::pool::ServerPool>, Address),
        >,
    ) -> Result<(), Error> {
        let dist_commit = self.xact_info.get_commit_statement();
        let dist_abort = self.xact_info.get_abort_statement();
        if dist_commit.is_some() || dist_abort.is_some() {
            // if either a commit or abort statement is set, we should be in a distributed transaction.
            assert!(!all_conns.is_empty());

            let is_chained = Self::should_be_chained(dist_commit, dist_abort);
            let dist_commit = dist_commit.map(|stmt| stmt.to_string());
            let mut dist_abort = dist_abort.map(|stmt| stmt.to_string());

            // Report transaction executed statistics.
            self.stats.transaction();

            let mut is_distributed_commit_failed = false;
            // We are in distributed transaction mode, and need to commit or abort on all servers.
            if let Some(commit_stmt) = dist_commit {
                // If two-phase commit was successful, we can send the COMMIT message to the client.
                // Otherwise, we need to  ROLLBACK on all servers.
                let dist_commit_res = self.distributed_commit(all_conns).await;
                if self
                    .communicate_err_response(dist_commit_res)
                    .await?
                    .is_none()
                {
                    // Currently, if a distributed commit fails, we send a ROLLBACK to all servers.
                    // However, this is different from how Postgres handles it. Postgres sends an
                    // error response to the client, and then does not accept any more queries from
                    // the client until the client explicitly sends a ROLLBACK.
                    dist_abort = Some("ROLLBACK".to_string());
                    is_distributed_commit_failed = true;
                } else {
                    custom_protocol_response_ok_with_state(
                        &mut self.write,
                        &commit_stmt,
                        TransactionState::Idle,
                    )
                    .await?;
                }
            }

            if let Some(abort_stmt) = dist_abort {
                let distributed_abort_res = self.distributed_abort(all_conns, &abort_stmt).await;
                if is_distributed_commit_failed {
                    // Nothing to do, as the error reponse is already sent before.
                } else if self
                    .communicate_err_response(distributed_abort_res)
                    .await?
                    .is_some()
                {
                    custom_protocol_response_ok_with_state(
                        &mut self.write,
                        &abort_stmt,
                        TransactionState::Idle,
                    )
                    .await?;
                }
            }

            let is_all_servers_in_non_copy_mode =
                all_conns.iter().all(|(_, conn)| !conn.0.in_copy_mode());

            // Release server back to the pool if we are in transaction or transparent modes.
            // If we are in session mode, we keep the server until the client disconnects.
            if (self.is_transaction_mode() || self.is_transparent_mode())
                && is_all_servers_in_non_copy_mode
            {
                self.stats.idle();
            }

            if is_chained {
                let last_conn = all_conns
                    .get_mut(self.last_server_key.as_ref().unwrap())
                    .unwrap();
                let last_server = &mut *last_conn.0;

                // TODO(MD): chained transaction should be implemented.
                // Here, we need to start a local transaction on the last server. However, here is
                // too late to start a transaction, as we are far from the transaction loop. We need to
                // rearrange the code (or add more complicated control flow) to make it possible.
                warn!(
                    "Chained transaction is not implemented yet. \
                    The last server {} will NOT be in transaction.",
                    last_server.address()
                );
            }
        }
        Ok(())
    }

    pub fn reset_client_xact(&mut self) {
        // Reset transaction state for safety reasons.
        self.xact_info = Default::default();
    }

    async fn distributed_commit(
        &mut self,
        all_conns: &mut HashMap<
            ServerId,
            (bb8::PooledConnection<'_, crate::pool::ServerPool>, Address),
        >,
    ) -> Result<(), Error> {
        debug!("Committing distributed transaction.");
        if Self::is_any_server_in_failed_xact(all_conns) {
            #[cfg(debug_assertions)]
            all_conns.iter().for_each(|(server_key, conn)| {
                let server = &*conn.0;
                if server.in_failed_transaction() {
                    debug!(
                    "Server {} (with server_key: {:?}) is in failed transaction. Skipping commit.",
                    server.address(),
                    server_key,
                );
                }
            });

            let err = ErrorInfo::new_brief(
                "Error".to_string(),
                "25P02".to_string(),
                "Cannot commit a transaction that is in failed state.".to_string(),
            );

            return Err(Error::ErrorResponse(ErrorResponse::from(err)));
        }
        self.distributed_prepare(all_conns).await?;

        let commit_prepared_results = join_all(all_conns.iter_mut().map(|(_, conn)| {
            let server = &mut *conn.0;
            server.local_server_commit_prepared()
        }))
        .await;

        all_conns.iter_mut().for_each(|(_, conn)| {
            let server = &mut *conn.0;
            self.set_post_query_state(server);
        });

        for commit_prepared_res in commit_prepared_results {
            // For now, we just return the first error we encounter.
            commit_prepared_res?;
        }

        Ok(())
    }

    /// After each interaction with the server, we need to set the transaction state based on the
    /// server's state.
    fn set_post_query_state(&mut self, server: &mut Server) {
        self.xact_info
            .set_state(server.transaction_metadata().state());
    }

    async fn distributed_abort(
        &mut self,
        all_conns: &mut HashMap<
            ServerId,
            (bb8::PooledConnection<'_, crate::pool::ServerPool>, Address),
        >,
        abort_stmt: &str,
    ) -> Result<(), Error> {
        debug!("Aborting distributed transaction");
        let abort_results = join_all(all_conns.iter_mut().map(|(_, conn)| {
            let server = &mut *conn.0;
            server.query(abort_stmt)
        }))
        .await;

        all_conns.iter_mut().for_each(|(_, conn)| {
            let server = &mut *conn.0;
            self.set_post_query_state(server);
            server
                .stats()
                .transaction(self.server_parameters.get_application_name());
        });

        for abort_res in abort_results {
            // For now, we just return the first error we encounter.
            abort_res?;
        }
        Ok(())
    }

    #[allow(clippy::type_complexity)]
    async fn distributed_prepare(
        &mut self,
        all_conns: &mut HashMap<
            ServerId,
            (bb8::PooledConnection<'_, crate::pool::ServerPool>, Address),
        >,
    ) -> Result<(), Error> {
        // Apply 'PREPARE TRANSACTION' on all involved servers.
        let prepare_results = join_all(all_conns.iter_mut().map(|(_, conn)| {
            let server = &mut *conn.0;
            self.set_post_query_state(server);
            server.local_server_prepare_transaction()
        }))
        .await;

        // Update the client state based on the server state.
        all_conns.iter_mut().for_each(|(_, conn)| {
            let server = &mut *conn.0;
            self.set_post_query_state(server);
        });

        // If there was any error, we need to abort the transaction.
        for prepare_res in prepare_results {
            prepare_res?;
        }
        Ok(())
    }

    /// Returns true if the statement is a commit or abort statement. Also, it sets the commit or abort
    /// statement on the client.
    pub fn set_commit_or_abort_statement(&mut self, ast: &Vec<Statement>) -> bool {
        if Self::is_commit_statement(ast) {
            self.xact_info.set_commit_statement(Some(ast[0].clone()));
            true
        } else if Self::is_abort_statement(ast) {
            self.xact_info.set_abort_statement(Some(ast[0].clone()));
            true
        } else {
            false
        }
    }

    /// Returns true if the statement is a commit statement.
    fn is_commit_statement(ast: &Vec<Statement>) -> bool {
        for statement in ast {
            if let Statement::Commit { .. } = *statement {
                assert_eq!(ast.len(), 1);
                return true;
            }
        }
        false
    }

    /// Returns true if the statement is an abort statement.
    fn is_abort_statement(ast: &Vec<Statement>) -> bool {
        for statement in ast {
            if let Statement::Rollback { .. } = *statement {
                assert_eq!(ast.len(), 1);
                return true;
            }
        }
        false
    }

    /// Returns true if the commit or abort statement should be chained.
    fn should_be_chained(dist_commit: Option<&Statement>, dist_abort: Option<&Statement>) -> bool {
        matches!(
            (dist_commit, dist_abort),
            (Some(Statement::Commit { chain: true }), _)
                | (_, Some(Statement::Rollback { chain: true }))
        )
    }
    async fn communicate_err_response<R>(
        &mut self,
        res: Result<R, Error>,
    ) -> Result<Option<R>, Error> {
        match res {
            Ok(res) => Ok(Some(res)),
            Err(Error::ErrorResponse(err)) => {
                error_response_stmt(&mut self.write, &err, self.xact_info.state()).await?;
                Ok(None)
            }
            Err(err) => Err(err),
        }
    }

    pub async fn post_query_processing<R>(
        &mut self,
        server: &mut Server,
        res: Result<R, Error>,
    ) -> Result<Option<R>, Error> {
        self.set_post_query_state(server);
        self.communicate_err_response(res).await
    }
}

/// Send an error response to the client.
pub async fn error_response_stmt<S>(
    stream: &mut S,
    err: &ErrorResponse,
    t_state: TransactionState,
) -> Result<(), Error>
where
    S: tokio::io::AsyncWrite + std::marker::Unpin,
{
    let mut err_bytes = BytesMut::new();
    err.encode(&mut err_bytes)?;
    write_all_half(stream, &err_bytes).await?;

    ready_for_query_with_state(stream, t_state).await
}
