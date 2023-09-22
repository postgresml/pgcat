use crate::client::Client;
use crate::errors::Error;
use crate::query_messages::{ErrorInfo, ErrorResponse, Message};
use bytes::BytesMut;
/// Handle clients by pretending to be a PostgreSQL server.
use chrono::NaiveDateTime;
use futures::future::join_all;
use itertools::Either;
use log::{debug, warn};
use sqlparser::ast::{Statement, TransactionAccessMode, TransactionMode};
use std::collections::HashMap;
use uuid::Uuid;

use crate::config::{Address, Role};
use crate::messages::*;
use crate::server::Server;
use crate::server_xact::*;

/// DistributedPrepareResult is an accumulator for the results of the 'PREPARE TRANSACTION's.
#[derive(Debug, Default)]
struct DistributedPrepareResult {
    max_prepare_timestamp: NaiveDateTime,
}

impl DistributedPrepareResult {
    /// Returns the maximum prepare timestamp of all servers.
    pub fn get_max_prepare_timestamp(&self) -> NaiveDateTime {
        self.max_prepare_timestamp
    }

    /// Accumulates the results of a 'PREPARE TRANSACTION'.
    /// The result is true if the server has a 'PREPARE TRANSACTION' timestamp.
    pub fn accumulate(&mut self, server: &Server) -> bool {
        let prep_timestamp = server.transaction_metadata().get_prepared_timestamp();
        if prep_timestamp.is_none() {
            false
        } else {
            self.max_prepare_timestamp =
                std::cmp::max(self.max_prepare_timestamp, prep_timestamp.unwrap());
            true
        }
    }
}

/// This function starts a distributed transaction by sending a BEGIN statement to the first server.
/// It is called on the first server, as soon as client wants to interact with another server,
/// which hints that the client wants to start a distributed transaction.
pub async fn begin_distributed_xact<S, T>(
    clnt: &mut Client<S, T>,
    server_key: &(usize, Option<Role>),
    server: &mut Server,
) -> Result<bool, Error>
where
    S: tokio::io::AsyncRead + std::marker::Unpin,
    T: tokio::io::AsyncWrite + std::marker::Unpin,
{
    let begin_stmt = clnt.xact_info.get_begin_statement();
    assert!(begin_stmt.is_some());
    if let Some(err) = query_server(clnt, server, &begin_stmt.unwrap().to_string()).await? {
        error_response_stmt(&mut clnt.write, &err, clnt.xact_info.state()).await?;
        return Ok(false);
    }

    if clnt.xact_info.params.is_repeatable_read_or_higher() {
        // If we are in a repeatable read or serializable transaction, we need to use the
        // snapshot we acquired from the first server.
        assert!(clnt.xact_info.get_snapshot().is_some());
        let snapshot = clnt.xact_info.get_snapshot().unwrap();

        debug!(
            "Assigning snapshot ('{}') to server {}",
            snapshot,
            server.address(),
        );

        let snapshot_res = assign_xact_snapshot(server, &snapshot).await?;
        set_post_query_state(clnt, server);

        if let Some(err) = snapshot_res {
            error_response_stmt(&mut clnt.write, &err, clnt.xact_info.state()).await?;
            return Ok(false);
        };
    }

    // If we are in a distributed transaction, we need to assign a GID to the transaction.
    assert!(clnt.xact_info.get_xact_gid().is_some());
    let gid = clnt.xact_info.get_xact_gid().unwrap();

    debug!("Assigning GID ('{}') to server {}", gid, server.address(),);

    let gid_res = assign_xact_gid(server, &gen_server_specific_gid(server_key, &gid)).await?;
    set_post_query_state(clnt, server);
    if let Some(err) = gid_res {
        error_response_stmt(&mut clnt.write, &err, clnt.xact_info.state()).await?;
        return Ok(false);
    }

    Ok(true)
}

/// This functions generates a GID for the current transaction and sends it to the server.
/// Also, if the transaction is repeatable read or higher, it acquires a snapshot from the server.
pub async fn acquire_gid_and_snapshot<S, T>(
    clnt: &mut Client<S, T>,
    server_key: &(usize, Option<Role>),
    server: &mut Server,
) -> Result<bool, Error>
where
    S: tokio::io::AsyncRead + std::marker::Unpin,
    T: tokio::io::AsyncWrite + std::marker::Unpin,
{
    assert!(clnt.xact_info.get_xact_gid().is_none());
    let gid = generate_xact_gid(clnt);

    debug!(
        "Acquiring GID ('{}') and snapshot from server {}",
        gid,
        server.address(),
    );

    // If we are in a distributed transaction, we need to assign a GID to the transaction.
    let gid_res = assign_xact_gid(server, &gen_server_specific_gid(server_key, &gid)).await?;
    set_post_query_state(clnt, server);
    if let Some(err) = gid_res {
        error_response_stmt(&mut clnt.write, &err, clnt.xact_info.state()).await?;
        return Ok(false);
    }
    clnt.xact_info.set_xact_gid(Some(gid));

    if clnt.xact_info.params.is_repeatable_read_or_higher() {
        // If we are in a repeatable read or serializable transaction, we need to acquire a
        // snapshot from the server.
        let snapshot_res = acquire_xact_snapshot(server).await?;
        set_post_query_state(clnt, server);

        match snapshot_res {
            Either::Left(snapshot) => {
                debug!(
                    "Got first server snapshot: {} (on {})",
                    snapshot,
                    server.address()
                );
                clnt.xact_info.set_snapshot(Some(snapshot));
            }
            Either::Right(err) => {
                error_response_stmt(&mut clnt.write, &err, clnt.xact_info.state()).await?;
                return Ok(false);
            }
        }
    }
    Ok(true)
}

/// Generates a random GID (i.e., Global transaction ID) for a transaction.
fn generate_xact_gid<S, T>(clnt: &Client<S, T>) -> String {
    format!(
        "txn_{}_{}",
        clnt.addr.to_string(),
        Uuid::new_v4().to_string()
    )
}

/// Generates a server-specific GID for a transaction. We need this, because it's possible that
/// multiple servers might actually be the same server (which commonly happens in testing).
fn gen_server_specific_gid(server_key: &(usize, Option<Role>), gid: &str) -> String {
    format!("{}_{}", server_key.0, gid)
}

/// Assigns the transaction state based on the state of all servers.
pub fn assign_client_transaction_state<S, T>(
    clnt: &mut Client<S, T>,
    all_conns: &HashMap<
        (usize, Option<Role>),
        (bb8::PooledConnection<'_, crate::pool::ServerPool>, Address),
    >,
) {
    clnt.xact_info.set_state(if all_conns.is_empty() {
        // if there's no server, we're in idle mode.
        TransactionState::Idle
    } else {
        if is_any_server_in_failed_xact(all_conns) {
            // if any server is in failed transaction, we're in failed transaction.
            TransactionState::InFailedTransaction
        } else {
            // if we have at least one server and it is in a transaction, we're in a transaction.
            TransactionState::InTransaction
        }
    });
}

/// Returns true if any server is in a failed transaction.
fn is_any_server_in_failed_xact(
    all_conns: &HashMap<
        (usize, Option<Role>),
        (bb8::PooledConnection<'_, crate::pool::ServerPool>, Address),
    >,
) -> bool {
    all_conns
        .iter()
        .any(|(_, conn)| in_failed_transaction(&*conn.0))
}

/// This function initializes the transaction parameters based on the first BEGIN statement.
pub fn initialize_xact_info<S, T>(
    clnt: &mut Client<S, T>,
    server: &mut Server,
    begin_stmt: &Statement,
) {
    if let Statement::StartTransaction { modes } = begin_stmt {
        // This is the first BEGIN statement. We need
        clnt.xact_info.set_begin_statement(Some(begin_stmt.clone()));

        // Initialize transaction parameters using the server's default.
        clnt.xact_info.params = server_default_transaction_parameters(server);
        for mode in modes {
            match mode {
                TransactionMode::AccessMode(access_mode) => {
                    clnt.xact_info.params.set_read_only(match access_mode {
                        TransactionAccessMode::ReadOnly => true,
                        TransactionAccessMode::ReadWrite => false,
                    });
                }
                TransactionMode::IsolationLevel(isolation_level) => {
                    clnt.xact_info
                        .params
                        .set_isolation_level(isolation_level.clone());
                }
            }
        }
        debug!(
            "Transaction paramaters after the first BEGIN statement: {:?}",
            clnt.xact_info.params
        );

        // Set the transaction parameters on the first server.
        server.transaction_metadata_mut().params = clnt.xact_info.params.clone();
    } else {
        // If we were not in a transaction and the first statement is
        // not a BEGIN, then it's an irrecovable error.
        assert!(false);
    }
}

/// This function performs a distribted abort/commit if necessary, and also resets the transaction
/// state. This is suppoed to be called before exiting the transaction loop. At that point, if
/// either an abort or commit statement is set, we need to perform a distributed abort/commit. This
/// is based on the logic that an abort or commit statement is only set if we are in a distributed
/// transaction and we observe a commit or abort statement sent to the server. That is where we exit
/// the transaction loop and expect this function to takeover and abort/commit the transaction.
pub async fn distributed_commit_or_abort<S, T>(
    clnt: &mut Client<S, T>,
    all_conns: &mut HashMap<
        (usize, Option<Role>),
        (bb8::PooledConnection<'_, crate::pool::ServerPool>, Address),
    >,
) -> Result<(), Error>
where
    S: tokio::io::AsyncRead + std::marker::Unpin,
    T: tokio::io::AsyncWrite + std::marker::Unpin,
{
    let dist_commit = clnt.xact_info.get_commit_statement();
    let dist_abort = clnt.xact_info.get_abort_statement();
    Ok(if dist_commit.is_some() || dist_abort.is_some() {
        // if either a commit or abort statement is set, we should be in a distributed transaction.
        assert!(all_conns.len() > 0);

        let is_chained = should_be_chained(dist_commit, dist_abort);
        let dist_commit = dist_commit.map(|stmt| stmt.to_string());
        let mut dist_abort = dist_abort.map(|stmt| stmt.to_string());

        // Report transaction executed statistics.
        clnt.stats.transaction();

        let mut is_distributed_commit_failed = false;
        // We are in distributed transaction mode, and need to commit or abort on all servers.
        if let Some(commit_stmt) = dist_commit {
            // If two-phase commit was successful, we can send the COMMIT message to the client.
            // Otherwise, we need to  ROLLBACK on all servers.
            if let Some(err) = distributed_commit(clnt, all_conns).await? {
                error_response_stmt(&mut clnt.write, &err, clnt.xact_info.state()).await?;

                // Currently, if a distributed commit fails, we send a ROLLBACK to all servers.
                // However, this is different from how Postgres handles it. Postgres sends an
                // error response to the client, and then does not accept any more queries from
                // the client until the client explicitly sends a ROLLBACK.
                dist_abort = Some("ROLLBACK".to_string());
                is_distributed_commit_failed = true;
            } else {
                custom_protocol_response_ok_with_state(
                    &mut clnt.write,
                    &commit_stmt,
                    TransactionState::Idle,
                )
                .await?;
            }
        }

        if let Some(abort_stmt) = dist_abort {
            let distributed_abort_res = distributed_abort(clnt, all_conns, &abort_stmt).await?;
            if is_distributed_commit_failed {
                // Nothing to do, as the error reponse is already sent before.
            } else if let Some(err) = distributed_abort_res {
                error_response_stmt(&mut clnt.write, &err, clnt.xact_info.state()).await?;
            } else {
                custom_protocol_response_ok_with_state(
                    &mut clnt.write,
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
        if (clnt.is_transaction_mode() || clnt.is_transparent_mode())
            && is_all_servers_in_non_copy_mode
        {
            clnt.stats.idle();
        }

        if is_chained {
            let last_conn = all_conns
                .get_mut(clnt.last_server_key.as_ref().unwrap())
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
    })
}

pub fn reset_client_xact<S, T>(clnt: &mut Client<S, T>) {
    // Reset transaction state for safety reasons.
    clnt.xact_info = Default::default();
}

async fn distributed_commit<S, T>(
    clnt: &mut Client<S, T>,
    all_conns: &mut HashMap<
        (usize, Option<Role>),
        (bb8::PooledConnection<'_, crate::pool::ServerPool>, Address),
    >,
) -> Result<Option<ErrorResponse>, Error>
where
    S: tokio::io::AsyncRead + std::marker::Unpin,
    T: tokio::io::AsyncWrite + std::marker::Unpin,
{
    debug!("Committing distributed transaction.");
    if is_any_server_in_failed_xact(all_conns) {
        #[cfg(debug_assertions)]
        all_conns.iter().for_each(|(server_key, conn)| {
            let server = &*conn.0;
            if in_failed_transaction(server) {
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

        return Ok(Some(ErrorResponse::from(err)));
    }
    let res = distributed_prepare(clnt, all_conns).await?;
    if res.is_right() {
        return Ok(res.right());
    }
    let res = res.left().unwrap();

    let commit_prepared_results = join_all(all_conns.iter_mut().map(|(_, conn)| {
        let server = &mut *conn.0;
        local_server_commit_prepared(server, res.get_max_prepare_timestamp())
    }))
    .await;

    all_conns.iter_mut().for_each(|(_, conn)| {
        let server = &mut *conn.0;
        set_post_query_state(clnt, server);
    });

    for commit_prepared_res in commit_prepared_results {
        if let Some(err) = commit_prepared_res? {
            // For now, we just return the first error we encounter.
            return Ok(Some(err));
        }
    }

    Ok(None)
}

/// After each interaction with the server, we need to set the transaction state based on the
/// server's state.
fn set_post_query_state<S, T>(clnt: &mut Client<S, T>, server: &mut Server) {
    clnt.xact_info
        .set_state(server.transaction_metadata().state());
}

async fn distributed_abort<S, T>(
    clnt: &mut Client<S, T>,
    all_conns: &mut HashMap<
        (usize, Option<Role>),
        (bb8::PooledConnection<'_, crate::pool::ServerPool>, Address),
    >,
    abort_stmt: &String,
) -> Result<Option<ErrorResponse>, Error>
where
    S: tokio::io::AsyncRead + std::marker::Unpin,
    T: tokio::io::AsyncWrite + std::marker::Unpin,
{
    debug!("Aborting distributed transaction");
    let abort_results = join_all(all_conns.iter_mut().map(|(_, conn)| {
        let server = &mut *conn.0;
        server.query(abort_stmt)
    }))
    .await;

    all_conns.iter_mut().for_each(|(_, conn)| {
        let server = &mut *conn.0;
        set_post_query_state(clnt, server);
        server
            .stats()
            .transaction(&clnt.server_parameters.get_application_name());
    });

    for abort_res in abort_results {
        if let Some(err) = abort_res? {
            // For now, we just return the first error we encounter.
            return Ok(Some(err));
        }
    }
    Ok(None)
}

async fn distributed_prepare<S, T>(
    clnt: &mut Client<S, T>,
    all_conns: &mut HashMap<
        (usize, Option<Role>),
        (bb8::PooledConnection<'_, crate::pool::ServerPool>, Address),
    >,
) -> Result<Either<DistributedPrepareResult, ErrorResponse>, Error>
where
    S: tokio::io::AsyncRead + std::marker::Unpin,
    T: tokio::io::AsyncWrite + std::marker::Unpin,
{
    // Apply 'PREPARE TRANSACTION' on all involved servers.
    let prepare_results = join_all(all_conns.iter_mut().map(|(_, conn)| {
        let server = &mut *conn.0;
        set_post_query_state(clnt, server);
        local_server_prepare_transaction(server)
    }))
    .await;

    // Update the client state based on the server state.
    all_conns.iter_mut().for_each(|(_, conn)| {
        let server = &mut *conn.0;
        set_post_query_state(clnt, server);
    });

    // If there was any error, we need to abort the transaction.
    let mut res = DistributedPrepareResult::default();
    for prepare_res in prepare_results {
        if let Some(err) = prepare_res? {
            // For now, we just return the first error we encounter.
            return Ok(Either::Right(err));
        }
    }

    // Otherwise, accumulate the results of 'PREPARE TRANSACTION'.
    all_conns.iter_mut().for_each(|(_, conn)| {
        let server = &mut *conn.0;
        res.accumulate(&server);
    });
    Ok(Either::Left(res))
}

/// This function is called when the client sends a query to the server without requiring an answer.
async fn query_server<S, T>(
    clnt: &mut Client<S, T>,
    server: &mut Server,
    stmt: &str,
) -> Result<Option<ErrorResponse>, Error>
where
    S: tokio::io::AsyncRead + std::marker::Unpin,
    T: tokio::io::AsyncWrite + std::marker::Unpin,
{
    let qres = server.query(stmt).await?;
    set_post_query_state(clnt, server);
    Ok(qres)
}

/// Returns true if the statement is a commit or abort statement. Also, it sets the commit or abort
/// statement on the client.
pub fn set_commit_or_abort_statement<S, T>(clnt: &mut Client<S, T>, ast: &Vec<Statement>) -> bool {
    if is_commit_statement(&ast) {
        clnt.xact_info.set_commit_statement(Some(ast[0].clone()));
        true
    } else if is_abort_statement(&ast) {
        clnt.xact_info.set_abort_statement(Some(ast[0].clone()));
        true
    } else {
        false
    }
}

/// Returns true if the statement is a commit statement.
fn is_commit_statement(ast: &Vec<Statement>) -> bool {
    for statement in ast {
        match *statement {
            Statement::Commit { .. } => {
                assert_eq!(ast.len(), 1);
                return true;
            }
            _ => (),
        }
    }
    false
}

/// Returns true if the statement is an abort statement.
fn is_abort_statement(ast: &Vec<Statement>) -> bool {
    for statement in ast {
        match *statement {
            Statement::Rollback { .. } => {
                assert_eq!(ast.len(), 1);
                return true;
            }
            _ => (),
        }
    }
    false
}

/// Returns true if the commit or abort statement should be chained.
fn should_be_chained(dist_commit: Option<&Statement>, dist_abort: Option<&Statement>) -> bool {
    dist_commit
        .map(|stmt| match stmt {
            Statement::Commit { chain } => *chain,
            _ => false,
        })
        .unwrap_or(false)
        || dist_abort
            .map(|stmt| match stmt {
                Statement::Rollback { chain } => *chain,
                _ => false,
            })
            .unwrap_or(false)
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
