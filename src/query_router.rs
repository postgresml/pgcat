/// Route queries automatically based on explicitly requested
/// or implied query characteristics.
use bytes::{Buf, BytesMut};
use log::{debug, error};
use mini_moka::sync::Cache;
use once_cell::sync::OnceCell;
use regex::{Regex, RegexSet};
use sqlparser::ast::Statement::{Delete, Insert, Query, StartTransaction, Update};
use sqlparser::ast::{
    Assignment, BinaryOperator, Expr, Ident, JoinConstraint, JoinOperator, SetExpr, Statement,
    TableFactor, TableWithJoins, Value,
};
use sqlparser::dialect::PostgreSqlDialect;
use sqlparser::parser::Parser;
use std::sync::OnceLock;

use crate::config::Role;
use crate::errors::Error;
use crate::messages::BytesMutReader;
use crate::plugins::{Intercept, Plugin, PluginOutput, QueryLogger, TableAccess};
use crate::pool::PoolSettings;
use crate::sharding::Sharder;

use std::collections::BTreeSet;
use std::io::Cursor;
use std::time::Duration;
use std::{cmp, mem};

/// Regexes used to parse custom commands.
const CUSTOM_SQL_REGEXES: [&str; 7] = [
    r"(?i)^ *SET SHARDING KEY TO '?([0-9]+)'? *;? *$",
    r"(?i)^ *SET SHARD TO '?([0-9]+|ANY)'? *;? *$",
    r"(?i)^ *SHOW SHARD *;? *$",
    r"(?i)^ *SET SERVER ROLE TO '(PRIMARY|REPLICA|ANY|AUTO|DEFAULT)' *;? *$",
    r"(?i)^ *SHOW SERVER ROLE *;? *$",
    r"(?i)^ *SET PRIMARY READS TO '?(on|off|default)'? *;? *$",
    r"(?i)^ *SHOW PRIMARY READS *;? *$",
];

/// Custom commands.
#[derive(PartialEq, Debug)]
pub enum Command {
    SetShardingKey,
    SetShard,
    ShowShard,
    SetServerRole,
    ShowServerRole,
    SetPrimaryReads,
    ShowPrimaryReads,
}

#[derive(PartialEq, Debug)]
pub enum ShardingKey {
    Value(i64),
    Placeholder(i16),
}

#[derive(Clone, Debug)]
enum ParameterFormat {
    Text,
    Binary,
    Uniform(Box<ParameterFormat>),
    Specified(Vec<ParameterFormat>),
}

/// Quickly test for match when a query is received.
static CUSTOM_SQL_REGEX_SET: OnceCell<RegexSet> = OnceCell::new();

// Get the value inside the custom command.
static CUSTOM_SQL_REGEX_LIST: OnceCell<Vec<Regex>> = OnceCell::new();

#[derive(Debug, Clone, PartialEq)]
enum DatabaseActivityState {
    Active,
    Initializing,
}

// A moka cache for the databases
// the key is the database name and the value is the database activity state
static DATABASE_ACTIVITY_CACHE: OnceLock<Cache<String, DatabaseActivityState>> = OnceLock::new();
// A moka cache for the tables, the key is the db_table.
static TABLE_MUTATIONS_CACHE: OnceLock<Cache<String, bool>> = OnceLock::new();

/// The query router.
pub struct QueryRouter {
    /// Which shard we should be talking to right now.
    active_shard: Option<usize>,

    /// Which server should we be talking to.
    active_role: Option<Role>,

    /// Should we try to parse queries to route them to replicas or primary automatically
    query_parser_enabled: Option<bool>,

    /// Include the primary into the replica pool for reads.
    primary_reads_enabled: Option<bool>,

    /// Pool configuration.
    pool_settings: PoolSettings,

    // Placeholders from prepared statement.
    placeholders: Vec<i16>,
}

struct ExtractedExprsAndTables<'a> {
    exprs: Vec<Expr>,
    table_names: Vec<Vec<Ident>>,
    assignments_opt: Option<&'a Vec<Assignment>>,
}

impl QueryRouter {
    /// One-time initialization of regexes
    /// that parse our custom SQL protocol.
    pub fn setup() -> bool {
        let set = match RegexSet::new(CUSTOM_SQL_REGEXES) {
            Ok(rgx) => rgx,
            Err(err) => {
                error!("QueryRouter::setup Could not compile regex set: {:?}", err);
                return false;
            }
        };

        let list: Vec<_> = CUSTOM_SQL_REGEXES
            .iter()
            .map(|rgx| Regex::new(rgx).unwrap())
            .collect();

        assert_eq!(list.len(), set.len());

        match CUSTOM_SQL_REGEX_LIST.set(list) {
            Ok(_) => true,
            Err(_) => return false,
        };

        CUSTOM_SQL_REGEX_SET.set(set).is_ok()
    }

    /// Create a new instance of the query router.
    /// Each client gets its own.
    pub fn new() -> QueryRouter {
        QueryRouter {
            active_shard: None,
            active_role: None,
            query_parser_enabled: None,
            primary_reads_enabled: None,
            pool_settings: PoolSettings::default(),
            placeholders: Vec::new(),
        }
    }

    /// Pool settings can change because of a config reload.
    pub fn update_pool_settings(&mut self, pool_settings: &PoolSettings) {
        self.pool_settings = pool_settings.clone();
    }

    pub fn pool_settings(&self) -> &PoolSettings {
        &self.pool_settings
    }

    /// Try to parse a command and execute it.
    pub fn try_execute_command(&mut self, message_buffer: &BytesMut) -> Option<(Command, String)> {
        let mut message_cursor = Cursor::new(message_buffer);

        let code = message_cursor.get_u8() as char;
        let len = message_cursor.get_i32() as usize;

        let comment_shard_routing_enabled = self.pool_settings.shard_id_regex.is_some()
            || self.pool_settings.sharding_key_regex.is_some();

        // Check for any sharding regex matches in any queries
        if comment_shard_routing_enabled {
            match code {
                // For Parse and Query messages peek to see if they specify a shard_id as a comment early in the statement
                'P' | 'Q' => {
                    // Check only the first block of bytes configured by the pool settings
                    let seg = cmp::min(len - 5, self.pool_settings.regex_search_limit);

                    let query_start_index = mem::size_of::<u8>() + mem::size_of::<i32>();

                    let initial_segment = String::from_utf8_lossy(
                        &message_buffer[query_start_index..query_start_index + seg],
                    );

                    // Check for a shard_id included in the query
                    if let Some(shard_id_regex) = &self.pool_settings.shard_id_regex {
                        let shard_id = shard_id_regex.captures(&initial_segment).and_then(|cap| {
                            cap.get(1).and_then(|id| id.as_str().parse::<usize>().ok())
                        });
                        if let Some(shard_id) = shard_id {
                            debug!("Setting shard to {:?}", shard_id);
                            self.set_shard(Some(shard_id));
                            // Skip other command processing since a sharding command was found
                            return None;
                        }
                    }

                    // Check for a sharding_key included in the query
                    if let Some(sharding_key_regex) = &self.pool_settings.sharding_key_regex {
                        let sharding_key =
                            sharding_key_regex
                                .captures(&initial_segment)
                                .and_then(|cap| {
                                    cap.get(1).and_then(|id| id.as_str().parse::<i64>().ok())
                                });
                        if let Some(sharding_key) = sharding_key {
                            debug!("Setting sharding_key to {:?}", sharding_key);
                            self.set_sharding_key(sharding_key);
                            // Skip other command processing since a sharding command was found
                            return None;
                        }
                    }
                }
                _ => {}
            }
        }

        // Only simple protocol supported for commands processed below
        if code != 'Q' {
            return None;
        }

        let query = message_cursor.read_string().unwrap();

        let regex_set = match CUSTOM_SQL_REGEX_SET.get() {
            Some(regex_set) => regex_set,
            None => return None,
        };

        let regex_list = match CUSTOM_SQL_REGEX_LIST.get() {
            Some(regex_list) => regex_list,
            None => return None,
        };

        let matches: Vec<_> = regex_set.matches(&query).into_iter().collect();

        // This is not a custom query, try to infer which
        // server it'll go to if the query parser is enabled.
        if matches.len() != 1 {
            debug!("Regular query, not a command");
            return None;
        }

        let command = match matches[0] {
            0 => Command::SetShardingKey,
            1 => Command::SetShard,
            2 => Command::ShowShard,
            3 => Command::SetServerRole,
            4 => Command::ShowServerRole,
            5 => Command::SetPrimaryReads,
            6 => Command::ShowPrimaryReads,
            _ => unreachable!(),
        };

        let mut value = match command {
            Command::SetShardingKey
            | Command::SetShard
            | Command::SetServerRole
            | Command::SetPrimaryReads => {
                // Capture value. I know this re-runs the regex engine, but I haven't
                // figured out a better way just yet. I think I can write a single Regex
                // that matches all 5 custom SQL patterns, but maybe that's not very legible?
                //
                // I think this is faster than running the Regex engine 5 times.
                match regex_list[matches[0]].captures(&query) {
                    Some(captures) => match captures.get(1) {
                        Some(value) => value.as_str().to_string(),
                        None => return None,
                    },
                    None => return None,
                }
            }

            Command::ShowShard => self
                .shard()
                .map_or_else(|| "unset".to_string(), |x| x.to_string()),
            Command::ShowServerRole => match self.active_role {
                Some(Role::Primary) => Role::Primary.to_string(),
                Some(Role::Replica) => Role::Replica.to_string(),
                Some(Role::Mirror) => Role::Mirror.to_string(),
                None => {
                    if self.query_parser_enabled() {
                        String::from("auto")
                    } else {
                        String::from("any")
                    }
                }
            },

            Command::ShowPrimaryReads => match self.primary_reads_enabled() {
                true => String::from("on"),
                false => String::from("off"),
            },
        };

        match command {
            Command::SetShardingKey => {
                // TODO: some error handling here
                value = self
                    .set_sharding_key(value.parse::<i64>().unwrap())
                    .unwrap()
                    .to_string();
            }

            Command::SetShard => {
                self.active_shard = match value.to_ascii_uppercase().as_ref() {
                    "ANY" => Some(rand::random::<usize>() % self.pool_settings.shards),
                    _ => Some(value.parse::<usize>().unwrap()),
                };
            }

            Command::SetServerRole => {
                self.active_role = match value.to_ascii_lowercase().as_ref() {
                    "primary" => {
                        self.query_parser_enabled = Some(false);
                        Some(Role::Primary)
                    }

                    "replica" => {
                        self.query_parser_enabled = Some(false);
                        Some(Role::Replica)
                    }

                    "any" => {
                        self.query_parser_enabled = Some(false);
                        None
                    }

                    "auto" => {
                        self.query_parser_enabled = Some(true);
                        None
                    }

                    "default" => {
                        self.active_role = self.pool_settings.default_role;
                        self.query_parser_enabled = None;
                        self.active_role
                    }

                    _ => unreachable!(),
                };
            }

            Command::SetPrimaryReads => {
                if value == "on" {
                    debug!("Setting primary reads to on");
                    self.primary_reads_enabled = Some(true);
                } else if value == "off" {
                    debug!("Setting primary reads to off");
                    self.primary_reads_enabled = Some(false);
                } else if value == "default" {
                    debug!("Setting primary reads to default");
                    self.primary_reads_enabled = None;
                }
            }

            _ => (),
        }

        Some((command, value))
    }

    pub fn parse(&self, message: &BytesMut) -> Result<Vec<Statement>, Error> {
        let mut message_cursor = Cursor::new(message);

        let code = message_cursor.get_u8() as char;
        let len = message_cursor.get_i32() as usize;

        if let Some(max_length) = self.pool_settings.query_parser_max_length {
            if len > max_length {
                return Err(Error::QueryRouterParserError(format!(
                    "Query too long for parser: {} > {}",
                    len, max_length
                )));
            }
        };

        let query = match code {
            // Query
            'Q' => {
                let query = message_cursor.read_string().unwrap();
                debug!("Query: '{}'", query);
                query
            }

            // Parse (prepared statement)
            'P' => {
                // Reads statement name
                let _name = message_cursor.read_string().unwrap();

                // Reads query string
                let query = message_cursor.read_string().unwrap();

                debug!("Prepared statement: '{}'", query);

                query
            }

            _ => return Err(Error::UnsupportedStatement),
        };

        match Parser::parse_sql(&PostgreSqlDialect {}, &query) {
            Ok(ast) => Ok(ast),
            Err(err) => {
                debug!("{}: {}", err, query);
                Err(Error::QueryRouterParserError(err.to_string()))
            }
        }
    }

    /// Determines if a query is a mutation or not.
    fn is_mutation_query(q: &sqlparser::ast::Query) -> bool {
        use sqlparser::ast::*;

        match q.body.as_ref() {
            SetExpr::Insert(_) => true,
            SetExpr::Update(_) => true,
            SetExpr::Query(q) => Self::is_mutation_query(q),
            _ => false,
        }
    }

    fn database_activity_cache(&self) -> Cache<String, DatabaseActivityState> {
        DATABASE_ACTIVITY_CACHE
            .get_or_init(|| {
                Cache::builder()
                    .time_to_idle(Duration::from_secs(self.pool_settings.db_activity_ttl))
                    .build()
            })
            .clone()
    }

    /// Check database activity state and reset it if necessary
    fn database_activity_state(&self, db: &String) -> DatabaseActivityState {
        let cache = self.database_activity_cache();

        // Exists in cache
        if cache.contains_key(db) {
            return cache.get(db).unwrap();
        }

        // Not in cache
        debug!("Adding database to cache: {}", db);

        cache.insert(db.to_string(), DatabaseActivityState::Initializing);

        // Set a timer to update the cache
        let db = db.clone();
        let db_activity_init_delay = self.pool_settings.db_activity_init_delay;
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(db_activity_init_delay)).await;
            cache.insert(db, DatabaseActivityState::Active);
        });

        DatabaseActivityState::Initializing
    }

    /// Try to infer which server to connect to based on the contents of the query.
    pub fn infer(&mut self, ast: &Vec<sqlparser::ast::Statement>) -> Result<(), Error> {
        if !self.pool_settings.query_parser_read_write_splitting {
            return Ok(()); // Nothing to do
        }

        debug!("Inferring role");

        if ast.is_empty() {
            // That's weird, no idea, let's go to primary
            self.active_role = Some(Role::Primary);
            return Err(Error::QueryRouterParserError("empty query".into()));
        }

        let mut primary_set_based_on_activity = false;
        let mut visited_write_statement = false;
        let mut prev_inferred_shard = None;

        if self.pool_settings.db_activity_based_routing {
            let db = self.pool_settings.db.clone();
            let state = self.database_activity_state(&db);
            debug!("Database activity state: {:?}", state);

            if let DatabaseActivityState::Initializing = state {
                debug!("Database is initializing, going to primary");

                self.active_role = Some(Role::Primary);
                primary_set_based_on_activity = true;
            }
        }

        for q in ast {
            match q {
                // All transactions go to the primary, probably a write.
                StartTransaction { .. } => {
                    self.active_role = Some(Role::Primary);
                    break;
                }

                // Likely a read-only query
                Query(query) => {
                    if primary_set_based_on_activity {
                        // If we already set the role based on activity, we don't need to do it again
                        continue;
                    }

                    if self.pool_settings.db_activity_based_routing {
                        // Check if the tables in the query have been written to recently
                        if self.query_handles_tables_in_mutation_cache(query) {
                            debug!("Query handles tables in mutation cache, going to primary");

                            self.active_role = Some(Role::Primary);
                            primary_set_based_on_activity = true;
                            continue;
                        }
                    }

                    match &self.pool_settings.automatic_sharding_key {
                        Some(_) => {
                            // TODO: if we have multiple queries in the same message,
                            // we can either split them and execute them individually
                            // or discard shard selection. If they point to the same shard though,
                            // we can let them through as-is.
                            // This is basically building a database now :)
                            let inferred_shard = self.infer_shard(query);
                            self.handle_inferred_shard(inferred_shard, &mut prev_inferred_shard)?;
                        }

                        None => (),
                    };

                    let has_locks = !query.locks.is_empty();
                    let has_mutation = Self::is_mutation_query(query);

                    if has_locks || has_mutation {
                        self.active_role = Some(Role::Primary);
                    } else if !visited_write_statement {
                        // If we already visited a write statement, we should be going to the primary.
                        self.active_role = match self.primary_reads_enabled() {
                            false => Some(Role::Replica), // If primary should not be receiving reads, use a replica.
                            true => None,                 // Any server role is fine in this case.
                        }
                    }
                }

                // Likely a write
                _ => {
                    debug!("Write statement found, going to primary");

                    if self.pool_settings.db_activity_based_routing {
                        // add all of the query tables to the mutation cache
                        self.update_mutation_cache_on_write(q);
                    }

                    match &self.pool_settings.automatic_sharding_key {
                        Some(_) => {
                            // TODO: similar to the above, if we have multiple queries in the
                            // same message, we can either split them and execute them individually
                            // or discard shard selection. If they point to the same shard though,
                            // we can let them through as-is.
                            let inferred_shard = self.infer_shard_on_write(q)?;
                            self.handle_inferred_shard(inferred_shard, &mut prev_inferred_shard)?;
                        }

                        None => (),
                    };
                    visited_write_statement = true;
                    self.active_role = Some(Role::Primary);
                }
            };
        }

        Ok(())
    }

    fn handle_inferred_shard(
        &mut self,
        inferred_shard: Option<usize>,
        prev_inferred_shard: &mut Option<usize>,
    ) -> Result<(), Error> {
        if let Some(shard) = inferred_shard {
            if let Some(prev_shard) = *prev_inferred_shard {
                if prev_shard != shard {
                    debug!("Found more than one shard in the query, not supported yet");
                    return Err(Error::QueryRouterParserError(
                        "multiple shards in query".into(),
                    ));
                }
            }
            *prev_inferred_shard = Some(shard);
            self.active_shard = Some(shard);
            debug!("Automatically using shard: {:?}", self.active_shard);
        };
        Ok(())
    }

    fn table_mutations_cache(&self) -> Cache<String, bool> {
        TABLE_MUTATIONS_CACHE
            .get_or_init(|| {
                Cache::builder()
                    .time_to_live(Duration::from_millis(
                        self.pool_settings.table_mutation_cache_ms_ttl,
                    ))
                    .build()
            })
            .clone()
    }

    fn query_handles_tables_in_mutation_cache(&self, query: &sqlparser::ast::Query) -> bool {
        let table_mutations_cache = self.table_mutations_cache();
        debug!("Checking if query handles tables in mutation cache");
        debug!("Table mutations cache: {:?}", table_mutations_cache);

        for tables in self.table_names(query) {
            for table in tables {
                if table_mutations_cache.contains_key(&self.table_mutation_cache_key(table)) {
                    return true;
                }
            }
        }

        false
    }
    fn extract_exprs_and_table_names<'a>(
        &'a self,
        q: &'a Statement,
    ) -> Option<ExtractedExprsAndTables<'a>> {
        let mut exprs = Vec::new();
        let mut table_names = Vec::new();
        let mut assignments_opt = None;

        match q {
            Insert(i) => {
                // Not supported in postgres.
                assert!(i.or.is_none());
                assert!(i.partitioned.is_none());
                assert!(i.after_columns.is_empty());

                Self::process_table(&i.table_name, &mut table_names);
                if let Some(source) = &i.source {
                    Self::process_query(source, &mut exprs, &mut table_names, &Some(&i.columns));
                }
            }
            Delete(d) => {
                if let Some(expr) = &d.selection {
                    exprs.push(expr.clone());
                }

                // Multi-tables delete are not supported in postgres.
                assert!(d.tables.is_empty());

                if let Some(using_tbl_with_join) = &d.using {
                    Self::process_tables_with_join(
                        using_tbl_with_join,
                        &mut exprs,
                        &mut table_names,
                    );
                }
                Self::process_selection(&d.selection, &mut exprs);
            }
            Update {
                table,
                assignments,
                from,
                selection,
                returning: _,
            } => {
                Self::process_table_with_join(table, &mut exprs, &mut table_names);
                if let Some(from_tbl) = from {
                    Self::process_table_with_join(from_tbl, &mut exprs, &mut table_names);
                }
                Self::process_selection(selection, &mut exprs);

                assignments_opt = Some(assignments);
            }
            _ => return None,
        };

        Some(ExtractedExprsAndTables {
            exprs,
            table_names,
            assignments_opt,
        })
    }

    fn infer_shard_on_write(&mut self, q: &Statement) -> Result<Option<usize>, Error> {
        if let Some(extracted) = self.extract_exprs_and_table_names(q) {
            let exprs = extracted.exprs;
            let table_names = extracted.table_names;
            let assignments_opt = extracted.assignments_opt;

            if let Some(assignments) = assignments_opt {
                self.assignment_parser(assignments)?;
            }

            Ok(self.infer_shard_from_exprs(exprs, table_names))
        } else {
            Ok(None)
        }
    }

    fn update_mutation_cache_on_write(&self, q: &Statement) {
        if let Some(extracted) = self.extract_exprs_and_table_names(q) {
            debug!("Updating mutation cache on write");

            let table_names = extracted.table_names;
            debug!("Table names in mutation query: {:?}", table_names);
            let table_mutations_cache = self.table_mutations_cache();
            for tables in table_names {
                for table in tables {
                    table_mutations_cache.insert(self.table_mutation_cache_key(table), true);
                }
            }
        }
    }

    // combines the database name and table name into a single string
    // to be used as the key in the table mutation cache
    // e.g. "mydb.mytable"
    fn table_mutation_cache_key(&self, table: Ident) -> String {
        format!("{}.{}", self.pool_settings.db, table.value)
    }

    fn process_query(
        query: &sqlparser::ast::Query,
        exprs: &mut Vec<Expr>,
        table_names: &mut Vec<Vec<Ident>>,
        columns: &Option<&Vec<Ident>>,
    ) {
        match &*query.body {
            SetExpr::Query(query) => {
                Self::process_query(query, exprs, table_names, columns);
            }

            // SELECT * FROM ...
            // We understand that pretty well.
            SetExpr::Select(select) => {
                Self::process_tables_with_join(&select.from, exprs, table_names);

                // Parse the actual "FROM ..."
                Self::process_selection(&select.selection, exprs);
            }

            SetExpr::Values(values) => {
                if let Some(cols) = columns {
                    for row in values.rows.iter() {
                        for (i, expr) in row.iter().enumerate() {
                            if cols.len() > i {
                                exprs.push(Expr::BinaryOp {
                                    left: Box::new(Expr::Identifier(cols[i].clone())),
                                    op: BinaryOperator::Eq,
                                    right: Box::new(expr.clone()),
                                });
                            }
                        }
                    }
                }
            }
            _ => (),
        };
    }

    fn process_selection(selection: &Option<Expr>, exprs: &mut Vec<Expr>) {
        match selection {
            Some(selection) => {
                exprs.push(selection.clone());
            }

            None => (),
        };
    }

    fn process_tables_with_join(
        tables: &[TableWithJoins],
        exprs: &mut Vec<Expr>,
        table_names: &mut Vec<Vec<Ident>>,
    ) {
        for table in tables.iter() {
            Self::process_table_with_join(table, exprs, table_names);
        }
    }

    fn process_table_with_join(
        table: &TableWithJoins,
        exprs: &mut Vec<Expr>,
        table_names: &mut Vec<Vec<Ident>>,
    ) {
        if let TableFactor::Table { name, .. } = &table.relation {
            Self::process_table(name, table_names);
        };

        // Get table names from all the joins.
        for join in table.joins.iter() {
            if let TableFactor::Table { name, .. } = &join.relation {
                Self::process_table(name, table_names);
            };

            // We can filter results based on join conditions, e.g.
            // SELECT * FROM t INNER JOIN B ON B.sharding_key = 5;
            if let JoinOperator::Inner(JoinConstraint::On(expr)) = &join.join_operator {
                // Parse the selection criteria later.
                exprs.push(expr.clone());
            };
        }
    }

    fn process_table(name: &sqlparser::ast::ObjectName, table_names: &mut Vec<Vec<Ident>>) {
        table_names.push(name.0.clone())
    }

    /// Parse the shard number from the Bind message
    /// which contains the arguments for a prepared statement.
    ///
    /// N.B.: Only supports anonymous prepared statements since we don't
    /// keep a cache of them in PgCat.
    pub fn infer_shard_from_bind(&mut self, message: &BytesMut) -> bool {
        if !self.pool_settings.query_parser_read_write_splitting {
            return false; // Nothing to do
        }

        debug!("Parsing bind message");

        let mut message_cursor = Cursor::new(message);

        let code = message_cursor.get_u8() as char;
        let len = message_cursor.get_i32();

        if code != 'B' {
            debug!("Not a bind packet");
            return false;
        }

        // Check message length
        if message.len() != len as usize + 1 {
            debug!(
                "Message has wrong length, expected {}, but have {}",
                len,
                message.len()
            );
            return false;
        }

        // There are no shard keys in the prepared statement.
        if self.placeholders.is_empty() {
            debug!("There are no placeholders in the prepared statement that matched the automatic sharding key");
            return false;
        }

        let sharder = Sharder::new(
            self.pool_settings.shards,
            self.pool_settings.sharding_function,
        );

        let mut shards = BTreeSet::new();

        let _portal = message_cursor.read_string();
        let _name = message_cursor.read_string();

        let num_params = message_cursor.get_i16();
        let parameter_format = match num_params {
            0 => ParameterFormat::Text, // Text
            1 => {
                let param_format = message_cursor.get_i16();
                ParameterFormat::Uniform(match param_format {
                    0 => Box::new(ParameterFormat::Text),
                    1 => Box::new(ParameterFormat::Binary),
                    _ => unreachable!(),
                })
            }
            n => {
                let mut v = Vec::with_capacity(n as usize);
                for _ in 0..n {
                    let param_format = message_cursor.get_i16();
                    v.push(match param_format {
                        0 => ParameterFormat::Text,
                        1 => ParameterFormat::Binary,
                        _ => unreachable!(),
                    });
                }
                ParameterFormat::Specified(v)
            }
        };

        let num_parameters = message_cursor.get_i16();

        for i in 0..num_parameters {
            let mut len = message_cursor.get_i32() as usize;
            let format = match &parameter_format {
                ParameterFormat::Text => ParameterFormat::Text,
                ParameterFormat::Uniform(format) => *format.clone(),
                ParameterFormat::Specified(formats) => formats[i as usize].clone(),
                _ => unreachable!(),
            };

            debug!("Parameter {} (len: {}): {:?}", i, len, format);

            // Postgres counts placeholders starting at 1
            let placeholder = i + 1;

            if self.placeholders.contains(&placeholder) {
                let value = match format {
                    ParameterFormat::Text => {
                        let mut value = String::new();
                        while len > 0 {
                            value.push(message_cursor.get_u8() as char);
                            len -= 1;
                        }

                        match value.parse::<i64>() {
                            Ok(value) => value,
                            Err(_) => {
                                debug!("Error parsing bind value: {}", value);
                                continue;
                            }
                        }
                    }

                    ParameterFormat::Binary => match len {
                        2 => message_cursor.get_i16() as i64,
                        4 => message_cursor.get_i32() as i64,
                        8 => message_cursor.get_i64(),
                        _ => {
                            error!(
                                "Got wrong length for integer type parameter in bind: {}",
                                len
                            );
                            continue;
                        }
                    },

                    _ => unreachable!(),
                };

                shards.insert(sharder.shard(value));
            }
        }

        self.placeholders.clear();
        self.placeholders.shrink_to_fit();

        // We only support querying one shard at a time.
        // TODO: Support multi-shard queries some day.
        if shards.len() == 1 {
            debug!("Found one sharding key");
            self.set_shard(Some(*shards.first().unwrap()));
            true
        } else {
            debug!("Found no sharding keys");
            false
        }
    }

    /// An `assignments` exists in the `UPDATE` statements. This parses the assignments and makes
    /// sure that we are not updating the sharding key. It's not supported yet.
    fn assignment_parser(&self, assignments: &Vec<Assignment>) -> Result<(), Error> {
        let sharding_key = self
            .pool_settings
            .automatic_sharding_key
            .as_ref()
            .unwrap()
            .split('.')
            .map(|ident| Ident::new(ident.to_lowercase()))
            .collect::<Vec<Ident>>();

        // Sharding key must be always fully qualified
        assert_eq!(sharding_key.len(), 2);

        for a in assignments {
            if sharding_key[0].value == "*"
                && sharding_key[1].value
                    == a.target
                        .to_string()
                        .split('.')
                        .last()
                        .unwrap()
                        .to_lowercase()
            {
                return Err(Error::QueryRouterParserError(
                    "Sharding key cannot be updated.".into(),
                ));
            }
        }
        Ok(())
    }

    /// A `selection` is the `WHERE` clause. This parses
    /// the clause and extracts the sharding key, if present.
    fn selection_parser(&self, expr: &Expr, table_names: &Vec<Vec<Ident>>) -> Vec<ShardingKey> {
        let mut result = Vec::new();
        let mut found = false;

        let sharding_key = self
            .pool_settings
            .automatic_sharding_key
            .as_ref()
            .unwrap()
            .split('.')
            .map(|ident| Ident::new(ident.to_lowercase()))
            .collect::<Vec<Ident>>();

        // Sharding key must be always fully qualified
        assert_eq!(sharding_key.len(), 2);

        // This parses `sharding_key = 5`. But it's technically
        // legal to write `5 = sharding_key`. I don't judge the people
        // who do that, but I think ORMs will still use the first variant,
        // so we can leave the second as a TODO.
        if let Expr::BinaryOp { left, op, right } = expr {
            match &**left {
                Expr::BinaryOp { .. } => result.extend(self.selection_parser(left, table_names)),
                Expr::Identifier(ident) => {
                    // Only if we're dealing with only one table
                    // and there is no ambiguity
                    if ident.value.to_lowercase() == sharding_key[1].value {
                        // Sharding key is unique enough, don't worry about
                        // table names.
                        if &sharding_key[0].value == "*" {
                            found = true;
                        } else if table_names.len() == 1 {
                            let table = &table_names[0];

                            if table.len() == 1 {
                                // Table is not fully qualified, e.g.
                                //      SELECT * FROM t WHERE sharding_key = 5
                                // Make sure the table name from the sharding key matches
                                // the table name from the query.
                                found = sharding_key[0].value == table[0].value.to_lowercase();
                            } else if table.len() == 2 {
                                // Table name is fully qualified with the schema: e.g.
                                //      SELECT * FROM public.t WHERE sharding_key = 5
                                // Ignore the schema (TODO: at some point, we want schema support)
                                // and use the table name only.
                                found = sharding_key[0].value == table[1].value.to_lowercase();
                            } else {
                                debug!("Got table name with more than two idents, which is not possible");
                            }
                        }
                    }
                }

                Expr::CompoundIdentifier(idents) => {
                    // The key is fully qualified in the query,
                    // it will exist or Postgres will throw an error.
                    if idents.len() == 2 {
                        found = (&sharding_key[0].value == "*"
                            || sharding_key[0].value == idents[0].value.to_lowercase())
                            && sharding_key[1].value == idents[1].value.to_lowercase();
                    }
                    // TODO: key can have schema as well, e.g. public.data.id (len == 3)
                }
                _ => (),
            };

            match op {
                BinaryOperator::Eq => (),
                BinaryOperator::Or => (),
                BinaryOperator::And => (),
                _ => {
                    // TODO: support other operators than equality.
                    debug!("Unsupported operation: {:?}", op);
                    return Vec::new();
                }
            };

            match &**right {
                Expr::BinaryOp { .. } => result.extend(self.selection_parser(right, table_names)),
                Expr::Value(Value::Number(value, ..)) => {
                    if found {
                        match value.parse::<i64>() {
                            Ok(value) => result.push(ShardingKey::Value(value)),
                            Err(_) => {
                                debug!("Sharding key was not an integer: {}", value);
                            }
                        };
                    }
                }

                Expr::Value(Value::Placeholder(placeholder)) => {
                    match placeholder.replace('$', "").parse::<i16>() {
                        Ok(placeholder) => result.push(ShardingKey::Placeholder(placeholder)),
                        Err(_) => {
                            debug!(
                                "Prepared statement didn't have integer placeholders: {}",
                                placeholder
                            );
                        }
                    }
                }
                _ => (),
            };
        }

        debug!("Sharding keys found: {:?}", result);

        result
    }

    /// Try to figure out which shard the query should go to.
    fn infer_shard(&mut self, query: &sqlparser::ast::Query) -> Option<usize> {
        let mut exprs = Vec::new();

        // Collect all table names from the query.
        let mut table_names = Vec::new();

        Self::process_query(query, &mut exprs, &mut table_names, &None);
        self.infer_shard_from_exprs(exprs, table_names)
    }

    /// get table names from query
    fn table_names(&self, query: &sqlparser::ast::Query) -> Vec<Vec<Ident>> {
        let mut exprs = Vec::new();

        let mut table_names = Vec::new();
        Self::process_query(query, &mut exprs, &mut table_names, &None);

        debug!("Table names in query: {:?}", table_names);

        table_names
    }

    fn infer_shard_from_exprs(
        &mut self,
        exprs: Vec<Expr>,
        table_names: Vec<Vec<Ident>>,
    ) -> Option<usize> {
        let mut shards = BTreeSet::new();

        let sharder = Sharder::new(
            self.pool_settings.shards,
            self.pool_settings.sharding_function,
        );

        // Look for sharding keys in either the join condition
        // or the selection.
        for expr in exprs.iter() {
            let sharding_keys = self.selection_parser(expr, &table_names);

            // TODO: Add support for prepared statements here.
            // This should just give us the position of the value in the `B` message.

            for value in sharding_keys {
                match value {
                    ShardingKey::Value(value) => {
                        let shard = sharder.shard(value);
                        shards.insert(shard);
                    }

                    ShardingKey::Placeholder(position) => {
                        self.placeholders.push(position);
                    }
                };
            }
        }
        match shards.len() {
            // Didn't find a sharding key, you're on your own.
            0 => {
                debug!("No sharding keys found");
                None
            }

            1 => Some(shards.into_iter().last().unwrap()),

            // TODO: support querying multiple shards (some day...)
            _ => {
                debug!("More than one sharding key found");
                None
            }
        }
    }

    /// Add your plugins here and execute them.
    pub async fn execute_plugins(&self, ast: &Vec<Statement>) -> Result<PluginOutput, Error> {
        let plugins = match self.pool_settings.plugins {
            Some(ref plugins) => plugins,
            None => return Ok(PluginOutput::Allow),
        };

        if let Some(ref query_logger) = plugins.query_logger {
            let mut query_logger = QueryLogger {
                enabled: query_logger.enabled,
                user: &self.pool_settings.user.username,
                db: &self.pool_settings.db,
            };

            let _ = query_logger.run(self, ast).await;
        }

        if let Some(ref intercept) = plugins.intercept {
            let mut intercept = Intercept {
                enabled: intercept.enabled,
                config: intercept,
            };

            let result = intercept.run(self, ast).await;

            if let Ok(PluginOutput::Intercept(output)) = result {
                return Ok(PluginOutput::Intercept(output));
            }
        }

        if let Some(ref table_access) = plugins.table_access {
            let mut table_access = TableAccess {
                enabled: table_access.enabled,
                tables: &table_access.tables,
            };

            let result = table_access.run(self, ast).await;

            if let Ok(PluginOutput::Deny(error)) = result {
                return Ok(PluginOutput::Deny(error));
            }
        }

        Ok(PluginOutput::Allow)
    }

    fn set_sharding_key(&mut self, sharding_key: i64) -> Option<usize> {
        let sharder = Sharder::new(
            self.pool_settings.shards,
            self.pool_settings.sharding_function,
        );
        let shard = sharder.shard(sharding_key);
        self.set_shard(Some(shard));
        self.active_shard
    }

    /// Set active_role as the default_role specified in the pool.
    pub fn set_default_role(&mut self) {
        self.active_role = self.pool_settings.default_role;
    }

    /// Get the current desired server role we should be talking to.
    pub fn role(&self) -> Option<Role> {
        self.active_role
    }

    /// Get desired shard we should be talking to.
    pub fn shard(&self) -> Option<usize> {
        self.active_shard
    }

    pub fn set_shard(&mut self, shard: Option<usize>) {
        self.active_shard = shard;
    }

    /// Should we attempt to parse queries?
    pub fn query_parser_enabled(&self) -> bool {
        match self.query_parser_enabled {
            None => {
                debug!(
                    "Using pool settings, query_parser_enabled: {}",
                    self.pool_settings.query_parser_enabled
                );
                self.pool_settings.query_parser_enabled
            }

            Some(value) => {
                debug!(
                    "Using query parser override, query_parser_enabled: {}",
                    value
                );
                value
            }
        }
    }

    pub fn primary_reads_enabled(&self) -> bool {
        match self.primary_reads_enabled {
            None => self.pool_settings.primary_reads_enabled,
            Some(value) => value,
        }
    }
}

impl Default for QueryRouter {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::config::PoolMode;
    use crate::messages::simple_query;
    use crate::sharding::ShardingFunction;
    use bytes::BufMut;
    use serial_test::serial;

    #[test]
    fn test_defaults() {
        QueryRouter::setup();
        let qr = QueryRouter::new();

        assert_eq!(qr.role(), None);
    }

    #[test]
    fn test_split_cte_queries() {
        QueryRouter::setup();
        let mut qr = QueryRouter::new();
        qr.pool_settings.query_parser_read_write_splitting = true;
        qr.pool_settings.query_parser_enabled = true;

        let query = simple_query(
            "WITH t AS (
                SELECT id FROM users WHERE name ILIKE '%ja%'
            )
            UPDATE user_languages
               SET settings = '{}'
              FROM t WHERE t.id = user_id;",
        );
        let ast = qr.parse(&query).unwrap();
        assert!(qr.infer(&ast).is_ok());
        assert_eq!(qr.role(), Some(Role::Primary));
    }

    #[test]
    fn test_infer_replica() {
        QueryRouter::setup();
        let mut qr = QueryRouter::new();
        qr.pool_settings.query_parser_read_write_splitting = true;
        assert!(qr
            .try_execute_command(&simple_query("SET SERVER ROLE TO 'auto'"))
            .is_some());
        assert!(qr.query_parser_enabled());

        assert!(qr
            .try_execute_command(&simple_query("SET PRIMARY READS TO off"))
            .is_some());

        let queries = vec![
            simple_query("SELECT * FROM items WHERE id = 5"),
            simple_query(
                "SELECT id, name, value FROM items INNER JOIN prices ON item.id = prices.item_id",
            ),
            simple_query("WITH t AS (SELECT * FROM items) SELECT * FROM t"),
        ];

        for query in queries {
            // It's a recognized query
            assert!(qr.infer(&qr.parse(&query).unwrap()).is_ok());
            assert_eq!(qr.role(), Some(Role::Replica));
        }
    }

    #[test]
    fn test_infer_primary() {
        QueryRouter::setup();
        let mut qr = QueryRouter::new();
        qr.pool_settings.query_parser_read_write_splitting = true;

        let queries = vec![
            simple_query("UPDATE items SET name = 'pumpkin' WHERE id = 5"),
            simple_query("INSERT INTO items (id, name) VALUES (5, 'pumpkin')"),
            simple_query("DELETE FROM items WHERE id = 5"),
            simple_query("BEGIN"), // Transaction start
        ];

        for query in queries {
            // It's a recognized query
            assert!(qr.infer(&qr.parse(&query).unwrap()).is_ok());
            assert_eq!(qr.role(), Some(Role::Primary));
        }
    }

    #[test]
    fn test_select_for_update() {
        QueryRouter::setup();
        let mut qr = QueryRouter::new();
        qr.pool_settings.query_parser_read_write_splitting = true;

        let queries_in_primary_role = vec![
            simple_query("BEGIN"), // Transaction start
            simple_query("SELECT * FROM items WHERE id = 5 FOR UPDATE"),
            simple_query("UPDATE items SET name = 'pumpkin' WHERE id = 5"),
        ];

        for query in queries_in_primary_role {
            assert!(qr.infer(&qr.parse(&query).unwrap()).is_ok());
            assert_eq!(qr.role(), Some(Role::Primary));
        }

        // query without lock do not change role
        let query = simple_query("SELECT * FROM items WHERE id = 5");
        assert!(qr.infer(&qr.parse(&query).unwrap()).is_ok());
        assert_eq!(qr.role(), None);
    }

    #[test]
    fn test_infer_primary_reads_enabled() {
        QueryRouter::setup();
        let mut qr = QueryRouter::new();
        let query = simple_query("SELECT * FROM items WHERE id = 5");
        assert!(qr
            .try_execute_command(&simple_query("SET PRIMARY READS TO on"))
            .is_some());

        assert!(qr.infer(&qr.parse(&query).unwrap()).is_ok());
        assert_eq!(qr.role(), None);
    }

    #[test]
    fn test_infer_parse_prepared() {
        QueryRouter::setup();
        let mut qr = QueryRouter::new();
        qr.pool_settings.query_parser_read_write_splitting = true;

        qr.try_execute_command(&simple_query("SET SERVER ROLE TO 'auto'"));
        assert!(qr
            .try_execute_command(&simple_query("SET PRIMARY READS TO off"))
            .is_some());

        let prepared_stmt = BytesMut::from(
            &b"WITH t AS (SELECT * FROM items WHERE name = $1) SELECT * FROM t WHERE id = $2\0"[..],
        );
        let mut res = BytesMut::from(&b"P"[..]);
        res.put_i32(prepared_stmt.len() as i32 + 4 + 1 + 2);
        res.put_u8(0);
        res.put(prepared_stmt);
        res.put_i16(0);

        assert!(qr.infer(&qr.parse(&res).unwrap()).is_ok());
        assert_eq!(qr.role(), Some(Role::Replica));
    }

    #[test]
    fn test_regex_set() {
        QueryRouter::setup();

        let tests = [
            // Upper case
            "SET SHARDING KEY TO '1'",
            "SET SHARD TO '1'",
            "SHOW SHARD",
            "SET SERVER ROLE TO 'replica'",
            "SET SERVER ROLE TO 'primary'",
            "SET SERVER ROLE TO 'any'",
            "SET SERVER ROLE TO 'auto'",
            "SHOW SERVER ROLE",
            "SET PRIMARY READS TO 'on'",
            "SET PRIMARY READS TO 'off'",
            "SET PRIMARY READS TO 'default'",
            "SHOW PRIMARY READS",
            // Lower case
            "set sharding key to '1'",
            "set shard to '1'",
            "show shard",
            "set server role to 'replica'",
            "set server role to 'primary'",
            "set server role to 'any'",
            "set server role to 'auto'",
            "show server role",
            "set primary reads to 'on'",
            "set primary reads to 'OFF'",
            "set primary reads to 'deFaUlt'",
            // No quotes
            "SET SHARDING KEY TO 11235",
            "SET SHARD TO 15",
            "SET PRIMARY READS TO off",
            // Spaces and semicolon
            "  SET SHARDING KEY TO 11235  ; ",
            "  SET SHARD TO 15;   ",
            "  SET SHARDING KEY TO 11235  ;",
            " SET SERVER ROLE TO 'primary';   ",
            "    SET SERVER ROLE TO 'primary'  ; ",
            "  SET SERVER ROLE TO 'primary'  ;",
            "  SET PRIMARY READS TO 'off'    ;",
        ];

        // Which regexes it'll match to in the list
        let matches = [
            0, 1, 2, 3, 3, 3, 3, 4, 5, 5, 5, 6, 0, 1, 2, 3, 3, 3, 3, 4, 5, 5, 5, 0, 1, 5, 0, 1, 0,
            3, 3, 3, 5,
        ];

        let list = CUSTOM_SQL_REGEX_LIST.get().unwrap();
        let set = CUSTOM_SQL_REGEX_SET.get().unwrap();

        for (i, test) in tests.iter().enumerate() {
            if !list[matches[i]].is_match(test) {
                println!("{} does not match {}", test, list[matches[i]]);
                panic!();
            }
            assert_eq!(set.matches(test).into_iter().count(), 1);
        }

        let bad = [
            "SELECT * FROM table",
            "SELECT * FROM table WHERE value = 'set sharding key to 5'", // Don't capture things in the middle of the query
        ];

        for query in &bad {
            assert_eq!(set.matches(query).into_iter().count(), 0);
        }
    }

    #[test]
    fn test_try_execute_command() {
        QueryRouter::setup();
        let mut qr = QueryRouter::new();

        // SetShardingKey
        let query = simple_query("SET SHARDING KEY TO 13");
        assert_eq!(
            qr.try_execute_command(&query),
            Some((Command::SetShardingKey, String::from("0")))
        );
        assert_eq!(qr.shard().unwrap(), 0);

        // SetShard
        let query = simple_query("SET SHARD TO '1'");
        assert_eq!(
            qr.try_execute_command(&query),
            Some((Command::SetShard, String::from("1")))
        );
        assert_eq!(qr.shard().unwrap(), 1);

        // ShowShard
        let query = simple_query("SHOW SHARD");
        assert_eq!(
            qr.try_execute_command(&query),
            Some((Command::ShowShard, String::from("1")))
        );

        // SetServerRole
        let roles = ["primary", "replica", "any", "auto", "primary"];
        let verify_roles = [
            Some(Role::Primary),
            Some(Role::Replica),
            None,
            None,
            Some(Role::Primary),
        ];
        let query_parser_enabled = [false, false, false, true, false];

        for (idx, role) in roles.iter().enumerate() {
            let query = simple_query(&format!("SET SERVER ROLE TO '{}'", role));
            assert_eq!(
                qr.try_execute_command(&query),
                Some((Command::SetServerRole, String::from(*role)))
            );
            assert_eq!(qr.role(), verify_roles[idx],);
            assert_eq!(qr.query_parser_enabled(), query_parser_enabled[idx],);

            // ShowServerRole
            let query = simple_query("SHOW SERVER ROLE");
            assert_eq!(
                qr.try_execute_command(&query),
                Some((Command::ShowServerRole, String::from(*role)))
            );
        }

        let primary_reads = ["on", "off", "default"];
        let primary_reads_enabled = ["on", "off", "on"];

        for (idx, primary_reads) in primary_reads.iter().enumerate() {
            assert_eq!(
                qr.try_execute_command(&simple_query(&format!(
                    "SET PRIMARY READS TO {}",
                    primary_reads
                ))),
                Some((Command::SetPrimaryReads, String::from(*primary_reads)))
            );
            assert_eq!(
                qr.try_execute_command(&simple_query("SHOW PRIMARY READS")),
                Some((
                    Command::ShowPrimaryReads,
                    String::from(primary_reads_enabled[idx])
                ))
            );
        }
    }

    #[test]
    fn test_enable_query_parser() {
        QueryRouter::setup();
        let mut qr = QueryRouter::new();
        qr.pool_settings.query_parser_read_write_splitting = true;

        let query = simple_query("SET SERVER ROLE TO 'auto'");
        assert!(qr
            .try_execute_command(&simple_query("SET PRIMARY READS TO off"))
            .is_some());

        assert!(qr.try_execute_command(&query).is_some());
        assert!(qr.query_parser_enabled());
        assert_eq!(qr.role(), None);

        let query = simple_query("INSERT INTO test_table VALUES (1)");
        assert!(qr.infer(&qr.parse(&query).unwrap()).is_ok());
        assert_eq!(qr.role(), Some(Role::Primary));

        let query = simple_query("SELECT * FROM test_table");
        assert!(qr.infer(&qr.parse(&query).unwrap()).is_ok());
        assert_eq!(qr.role(), Some(Role::Replica));

        assert!(qr.query_parser_enabled());
        let query = simple_query("SET SERVER ROLE TO 'default'");
        assert!(qr.try_execute_command(&query).is_some());
        assert!(!qr.query_parser_enabled());
    }

    #[test]
    fn test_query_parser() {
        QueryRouter::setup();
        let mut qr = QueryRouter::new();
        qr.pool_settings.query_parser_read_write_splitting = true;

        let query = simple_query("SELECT req_tab_0.*  FROM validation req_tab_0  WHERE  array['http://www.w3.org/ns/shacl#ValidationResult'] && req_tab_0.type::text[] AND ( (  (req_tab_0.focusnode = 'DataSource_Credilogic_DataSourceAddress_144959227') )  )");
        assert!(qr.infer(&qr.parse(&query).unwrap()).is_ok());

        let query = simple_query("WITH EmployeeSalaries AS (SELECT Department, Salary FROM Employees) SELECT Department, AVG(Salary) AS AverageSalary FROM EmployeeSalaries GROUP BY Department;");
        assert!(qr.infer(&qr.parse(&query).unwrap()).is_ok());
    }

    #[test]
    fn test_update_from_pool_settings() {
        QueryRouter::setup();

        let pool_settings = PoolSettings {
            pool_mode: PoolMode::Transaction,
            load_balancing_mode: crate::config::LoadBalancingMode::Random,
            shards: 2,
            user: crate::config::User::default(),
            default_role: Some(Role::Replica),
            query_parser_enabled: true,
            query_parser_max_length: None,
            query_parser_read_write_splitting: true,
            primary_reads_enabled: false,
            sharding_function: ShardingFunction::PgBigintHash,
            automatic_sharding_key: Some(String::from("test.id")),
            healthcheck_delay: PoolSettings::default().healthcheck_delay,
            healthcheck_timeout: PoolSettings::default().healthcheck_timeout,
            ban_time: PoolSettings::default().ban_time,
            sharding_key_regex: None,
            shard_id_regex: None,
            default_shard: crate::config::DefaultShard::Shard(0),
            regex_search_limit: 1000,
            auth_query: None,
            auth_query_password: None,
            auth_query_user: None,
            db: "test".to_string(),
            db_activity_based_routing: PoolSettings::default().db_activity_based_routing,
            db_activity_init_delay: PoolSettings::default().db_activity_init_delay,
            db_activity_ttl: PoolSettings::default().db_activity_ttl,
            table_mutation_cache_ms_ttl: PoolSettings::default().table_mutation_cache_ms_ttl,
            plugins: None,
        };
        let mut qr = QueryRouter::new();
        assert_eq!(qr.active_role, None);
        assert_eq!(qr.active_shard, None);
        assert_eq!(qr.query_parser_enabled, None);
        assert_eq!(qr.primary_reads_enabled, None);

        // Internal state must not be changed due to this, only defaults
        qr.update_pool_settings(&pool_settings);

        assert_eq!(qr.active_role, None);
        assert_eq!(qr.active_shard, None);
        assert!(qr.query_parser_enabled());
        assert!(!qr.primary_reads_enabled());

        let q1 = simple_query("SET SERVER ROLE TO 'primary'");
        assert!(qr.try_execute_command(&q1).is_some());
        assert_eq!(qr.active_role.unwrap(), Role::Primary);

        let q2 = simple_query("SET SERVER ROLE TO 'default'");
        assert!(qr.try_execute_command(&q2).is_some());
        assert_eq!(qr.active_role.unwrap(), pool_settings.default_role);
    }

    #[test]
    fn test_parse_multiple_queries() {
        QueryRouter::setup();

        let mut qr = QueryRouter::new();
        assert!(qr
            .infer(&qr.parse(&simple_query("BEGIN; SELECT 1; COMMIT;")).unwrap())
            .is_ok());
        assert_eq!(qr.role(), Role::Primary);

        assert!(qr
            .infer(&qr.parse(&simple_query("SELECT 1; SELECT 2;")).unwrap())
            .is_ok());
        assert_eq!(qr.role(), Role::Replica);

        assert!(qr
            .infer(
                &qr.parse(&simple_query(
                    "SELECT 123; INSERT INTO t VALUES (5); SELECT 1;"
                ))
                .unwrap()
            )
            .is_ok());
        assert_eq!(qr.role(), Role::Primary);
    }

    #[test]
    fn test_regex_shard_parsing() {
        QueryRouter::setup();

        let pool_settings = PoolSettings {
            pool_mode: PoolMode::Transaction,
            load_balancing_mode: crate::config::LoadBalancingMode::Random,
            shards: 5,
            user: crate::config::User::default(),
            default_role: Some(Role::Replica),
            query_parser_enabled: true,
            query_parser_max_length: None,
            query_parser_read_write_splitting: true,
            primary_reads_enabled: false,
            sharding_function: ShardingFunction::PgBigintHash,
            automatic_sharding_key: None,
            healthcheck_delay: PoolSettings::default().healthcheck_delay,
            healthcheck_timeout: PoolSettings::default().healthcheck_timeout,
            ban_time: PoolSettings::default().ban_time,
            sharding_key_regex: Some(Regex::new(r"/\* sharding_key: (\d+) \*/").unwrap()),
            shard_id_regex: Some(Regex::new(r"/\* shard_id: (\d+) \*/").unwrap()),
            default_shard: crate::config::DefaultShard::Shard(0),
            regex_search_limit: 1000,
            auth_query: None,
            auth_query_password: None,
            auth_query_user: None,
            db: "test".to_string(),
            db_activity_based_routing: PoolSettings::default().db_activity_based_routing,
            db_activity_init_delay: PoolSettings::default().db_activity_init_delay,
            db_activity_ttl: PoolSettings::default().db_activity_ttl,
            table_mutation_cache_ms_ttl: PoolSettings::default().table_mutation_cache_ms_ttl,
            plugins: None,
        };

        let mut qr = QueryRouter::new();
        qr.update_pool_settings(&pool_settings);

        // Shard should start out unset
        assert_eq!(qr.active_shard, None);

        // Don't panic when short query eg. ; is sent
        let q0 = simple_query(";");
        assert!(qr.try_execute_command(&q0).is_none());
        assert_eq!(qr.active_shard, None);

        // Make sure setting it works
        let q1 = simple_query("/* shard_id: 1 */ select 1 from foo;");
        assert!(qr.try_execute_command(&q1).is_none());
        assert_eq!(qr.active_shard, Some(1));

        // And make sure changing it works
        let q2 = simple_query("/* shard_id: 0 */ select 1 from foo;");
        assert!(qr.try_execute_command(&q2).is_none());
        assert_eq!(qr.active_shard, Some(0));

        // Validate setting by shard with expected shard copied from sharding.rs tests
        let q2 = simple_query("/* sharding_key: 6 */ select 1 from foo;");
        assert!(qr.try_execute_command(&q2).is_none());
        assert_eq!(qr.active_shard, Some(2));
    }

    #[test]
    fn test_automatic_sharding_key() {
        QueryRouter::setup();

        let mut qr = QueryRouter::new();
        qr.pool_settings.automatic_sharding_key = Some("data.id".to_string());
        qr.pool_settings.shards = 3;
        qr.pool_settings.query_parser_read_write_splitting = true;

        assert!(qr
            .infer(
                &qr.parse(&simple_query("SELECT * FROM data WHERE id = 5"))
                    .unwrap(),
            )
            .is_ok());
        assert_eq!(qr.shard().unwrap(), 2);

        assert!(qr
            .infer(
                &qr.parse(&simple_query(
                    "SELECT one, two, three FROM public.data WHERE id = 6"
                ))
                .unwrap()
            )
            .is_ok());
        assert_eq!(qr.shard().unwrap(), 0);

        assert!(qr
            .infer(
                &qr.parse(&simple_query(
                    "SELECT * FROM data
            INNER JOIN t2 ON data.id = 5
            AND t2.data_id = data.id
        WHERE data.id = 5"
                ))
                .unwrap()
            )
            .is_ok());
        assert_eq!(qr.shard().unwrap(), 2);

        // Shard did not move because we couldn't determine the sharding key since it could be ambiguous
        // in the query.
        assert!(qr
            .infer(
                &qr.parse(&simple_query(
                    "SELECT * FROM t2 INNER JOIN data ON id = 6 AND data.id = t2.data_id"
                ))
                .unwrap()
            )
            .is_ok());
        assert_eq!(qr.shard().unwrap(), 2);

        assert!(qr
            .infer(
                &qr.parse(&simple_query(
                    r#"SELECT * FROM "public"."data" WHERE "id" = 6"#
                ))
                .unwrap()
            )
            .is_ok());
        assert_eq!(qr.shard().unwrap(), 0);

        assert!(qr
            .infer(
                &qr.parse(&simple_query(
                    r#"SELECT * FROM "public"."data" WHERE "data"."id" = 5"#
                ))
                .unwrap()
            )
            .is_ok());
        assert_eq!(qr.shard().unwrap(), 2);

        // Super unique sharding key
        qr.pool_settings.automatic_sharding_key = Some("*.unique_enough_column_name".to_string());
        assert!(qr
            .infer(
                &qr.parse(&simple_query(
                    "SELECT * FROM table_x WHERE unique_enough_column_name = 6"
                ))
                .unwrap()
            )
            .is_ok());
        assert_eq!(qr.shard().unwrap(), 0);

        assert!(qr
            .infer(
                &qr.parse(&simple_query("SELECT * FROM table_y WHERE another_key = 5"))
                    .unwrap()
            )
            .is_ok());
        assert_eq!(qr.shard().unwrap(), 0);
    }

    fn auto_shard_wrapper(qry: &str, should_succeed: bool) -> Option<usize> {
        let mut qr = QueryRouter::new();
        qr.pool_settings.automatic_sharding_key = Some("*.w_id".to_string());
        qr.pool_settings.shards = 3;
        qr.pool_settings.query_parser_read_write_splitting = true;
        assert_eq!(qr.shard(), None);
        let infer_res = qr.infer(&qr.parse(&simple_query(qry)).unwrap());
        assert_eq!(infer_res.is_ok(), should_succeed);
        qr.shard()
    }

    fn auto_shard(qry: &str) -> Option<usize> {
        auto_shard_wrapper(qry, true)
    }

    fn auto_shard_fails(qry: &str) -> Option<usize> {
        auto_shard_wrapper(qry, false)
    }

    #[test]
    fn test_automatic_sharding_insert_update_delete() {
        QueryRouter::setup();

        assert_eq!(
            auto_shard_fails(
                "UPDATE ORDERS SET w_id = 3 WHERE O_ID = 3 AND O_D_ID = 3 AND W_ID = 5"
            ),
            None
        );

        assert_eq!(
            auto_shard_fails(
                "UPDATE ORDERS o SET o.W_ID = 3 WHERE o.O_ID = 3 AND o.O_D_ID = 3 AND o.W_ID = 5"
            ),
            None
        );

        assert_eq!(
            auto_shard(
                "UPDATE ORDERS o SET o.O_CARRIER_ID = 3 WHERE o.O_ID = 3 AND o.O_D_ID = 3 AND o.W_ID = 5"
            ),
            Some(2)
        );
    }

    #[test]
    fn test_automatic_sharding_key_tpcc() {
        QueryRouter::setup();

        assert_eq!(auto_shard("SELECT * FROM my_tbl WHERE w_id = 5"), Some(2));
        assert_eq!(
            auto_shard("BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ"),
            None
        );
        assert_eq!(auto_shard("COMMIT"), None);
        assert_eq!(auto_shard("ROLLBACK"), None);

        assert_eq!(auto_shard("SELECT NO_O_ID FROM NEW_ORDER WHERE NO_D_ID = 7 AND W_ID = 5 AND NO_O_ID > 3 LIMIT 3"), Some(2));
        assert_eq!(auto_shard("SELECT NO_O_ID FROM NEW_ORDER no WHERE no.NO_D_ID = 7 AND no.W_ID = 5 AND no.NO_O_ID > 3 LIMIT 3"), Some(2));

        assert_eq!(
            auto_shard("DELETE FROM NEW_ORDER WHERE NO_D_ID = 7 AND W_ID = 5 AND NO_O_ID = 3"),
            Some(2)
        );

        assert_eq!(
            auto_shard("SELECT O_C_ID FROM ORDERS WHERE O_ID = 3 AND O_D_ID = 3 AND W_ID = 5"),
            Some(2)
        );
        assert_eq!(
            auto_shard(
                "UPDATE ORDERS SET O_CARRIER_ID = 3 WHERE O_ID = 3 AND O_D_ID = 3 AND W_ID = 5"
            ),
            Some(2)
        );

        assert_eq!(
            auto_shard("UPDATE ORDER_LINE SET OL_DELIVERY_D = 3 WHERE OL_O_ID = 3 AND OL_D_ID = 3 AND W_ID = 5"),
            Some(2)
        );

        assert_eq!(
            auto_shard("SELECT SUM(OL_AMOUNT) FROM ORDER_LINE WHERE OL_O_ID = 3 AND OL_D_ID = 3 AND W_ID = 5"),
            Some(2)
        );

        assert_eq!(
            auto_shard("UPDATE CUSTOMER SET C_BALANCE = C_BALANCE + 3 WHERE C_ID = 3 AND C_D_ID = 3 AND W_ID = 5"),
            Some(2)
        );

        assert_eq!(
            auto_shard("SELECT W_TAX FROM WAREHOUSE WHERE W_ID = 5"),
            Some(2)
        );
        assert_eq!(
            auto_shard("SELECT D_TAX, D_NEXT_O_ID FROM DISTRICT WHERE D_ID = 3 AND W_ID = 5"),
            Some(2)
        );
        assert_eq!(
            auto_shard("UPDATE DISTRICT SET D_NEXT_O_ID = 3 WHERE D_ID = 3 AND W_ID = 5"),
            Some(2)
        );
        assert_eq!(
            auto_shard("SELECT C_DISCOUNT, C_LAST, C_CREDIT FROM CUSTOMER WHERE W_ID = 5 AND C_D_ID = 3 AND C_ID = 3"),
            Some(2)
        );
        assert_eq!(
            auto_shard("INSERT INTO ORDERS (O_ID, O_D_ID, W_ID, O_C_ID, O_ENTRY_D, O_CARRIER_ID, O_OL_CNT, O_ALL_LOCAL) VALUES (3, 3, 5, 3, 3, 3, 3, 3)"),
            Some(2)
        );
        assert_eq!(
            auto_shard("INSERT INTO NEW_ORDER (NO_O_ID, NO_D_ID, W_ID) VALUES (3, 3, 5)"),
            Some(2)
        );
        assert_eq!(
            auto_shard("SELECT I_PRICE, I_NAME, I_DATA FROM ITEM WHERE I_ID = 3"),
            None
        );
        assert_eq!(
            auto_shard("SELECT S_QUANTITY, S_DATA, S_YTD, S_ORDER_CNT, S_REMOTE_CNT, S_DIST_03 FROM STOCK WHERE S_I_ID = 3 AND W_ID = 5"),
            Some(2)
        );
        assert_eq!(
            auto_shard("UPDATE STOCK SET S_QUANTITY = 3, S_YTD = 3, S_ORDER_CNT = 3, S_REMOTE_CNT = 3 WHERE S_I_ID = 3 AND W_ID = 5"),
            Some(2)
        );
        assert_eq!(
            auto_shard("INSERT INTO ORDER_LINE (OL_O_ID, OL_D_ID, W_ID, OL_NUMBER, OL_I_ID, OL_SUPPLY_W_ID, OL_DELIVERY_D, OL_QUANTITY, OL_AMOUNT, OL_DIST_INFO) VALUES (3, 3, 5, 3, 3, 3, 3, 3, 3, 3)"),
            Some(2)
        );
        assert_eq!(
            auto_shard("SELECT C_ID, C_FIRST, C_MIDDLE, C_LAST, C_BALANCE FROM CUSTOMER WHERE W_ID = 5 AND C_D_ID = 3 AND C_ID = 3"),
            Some(2)
        );
        assert_eq!(
            auto_shard("SELECT C_ID, C_FIRST, C_MIDDLE, C_LAST, C_BALANCE FROM CUSTOMER WHERE W_ID = 5 AND C_D_ID = 3 AND C_LAST = '3' ORDER BY C_FIRST"),
            Some(2)
        );
        assert_eq!(
            auto_shard("SELECT O_ID, O_CARRIER_ID, O_ENTRY_D FROM ORDERS WHERE W_ID = 5 AND O_D_ID = 3 AND O_C_ID = 3 ORDER BY O_ID DESC LIMIT 3"),
            Some(2)
        );
        assert_eq!(
            auto_shard("SELECT OL_SUPPLY_W_ID, OL_I_ID, OL_QUANTITY, OL_AMOUNT, OL_DELIVERY_D FROM ORDER_LINE WHERE W_ID = 5 AND OL_D_ID = 3 AND OL_O_ID = 3"),
            Some(2)
        );
        assert_eq!(
            auto_shard("SELECT W_NAME, W_STREET_1, W_STREET_2, W_CITY, W_STATE, W_ZIP FROM WAREHOUSE WHERE W_ID = 5"),
            Some(2)
        );
        assert_eq!(
            auto_shard("UPDATE WAREHOUSE SET W_YTD = W_YTD + 3 WHERE W_ID = 5"),
            Some(2)
        );
        assert_eq!(
            auto_shard("SELECT D_NAME, D_STREET_1, D_STREET_2, D_CITY, D_STATE, D_ZIP FROM DISTRICT WHERE W_ID = 5 AND D_ID = 3"),
            Some(2)
        );
        assert_eq!(
            auto_shard("UPDATE DISTRICT SET D_YTD = D_YTD + 3 WHERE W_ID  = 5 AND D_ID = 3"),
            Some(2)
        );
        assert_eq!(
            auto_shard("SELECT C_ID, C_FIRST, C_MIDDLE, C_LAST, C_STREET_1, C_STREET_2, C_CITY, C_STATE, C_ZIP, C_PHONE, C_SINCE, C_CREDIT, C_CREDIT_LIM, C_DISCOUNT, C_BALANCE, C_YTD_PAYMENT, C_PAYMENT_CNT, C_DATA FROM CUSTOMER WHERE W_ID = 5 AND C_D_ID = 3 AND C_ID = 3"),
            Some(2)
        );
        assert_eq!(
            auto_shard("SELECT C_ID, C_FIRST, C_MIDDLE, C_LAST, C_STREET_1, C_STREET_2, C_CITY, C_STATE, C_ZIP, C_PHONE, C_SINCE, C_CREDIT, C_CREDIT_LIM, C_DISCOUNT, C_BALANCE, C_YTD_PAYMENT, C_PAYMENT_CNT, C_DATA FROM CUSTOMER WHERE W_ID = 5 AND C_D_ID = 3 AND C_LAST = '3' ORDER BY C_FIRST"),
            Some(2)
        );
        assert_eq!(
            auto_shard("UPDATE CUSTOMER SET C_BALANCE = 3, C_YTD_PAYMENT = 3, C_PAYMENT_CNT = 3, C_DATA = 3 WHERE W_ID = 5 AND C_D_ID = 3 AND C_ID = 3"),
            Some(2)
        );
        assert_eq!(
            auto_shard("UPDATE CUSTOMER SET C_BALANCE = 3, C_YTD_PAYMENT = 3, C_PAYMENT_CNT = 3 WHERE W_ID = 5 AND C_D_ID = 3 AND C_ID = 3"),
            Some(2)
        );

        assert_eq!(auto_shard("INSERT INTO HISTORY (H_C_ID, H_C_D_ID, H_C_W_ID, H_D_ID, W_ID, H_DATE, H_AMOUNT, H_DATA) VALUES (3, 3, 5, 3, 5, 3, 3, 3)"), Some(2));
        assert_eq!(
            auto_shard("SELECT D_NEXT_O_ID FROM DISTRICT WHERE W_ID = 5 AND D_ID = 3"),
            Some(2)
        );
        assert_eq!(
            auto_shard(
                "SELECT COUNT(DISTINCT(OL_I_ID)) FROM ORDER_LINE, STOCK
                 WHERE ORDER_LINE.W_ID = 5
                 AND OL_D_ID = 3
                 AND OL_O_ID < 3
                 AND OL_O_ID >= 3
                 AND STOCK.W_ID = 5
                 AND S_I_ID = OL_I_ID
                 AND S_QUANTITY < 3"
            ),
            Some(2)
        );

        // This is a distributed query and contains two shards
        assert_eq!(
            auto_shard(
                "SELECT COUNT(DISTINCT(OL_I_ID)) FROM ORDER_LINE, STOCK
                 WHERE ORDER_LINE.W_ID = 5
                 AND OL_D_ID = 3
                 AND OL_O_ID < 3
                 AND OL_O_ID >= 3
                 AND STOCK.W_ID = 7
                 AND S_I_ID = OL_I_ID
                 AND S_QUANTITY < 3"
            ),
            None
        );
    }

    #[test]
    fn test_prepared_statements() {
        let stmt = "SELECT * FROM data WHERE id = $1";

        let mut bind = BytesMut::from(&b"B"[..]);

        let mut payload = BytesMut::from(&b"\0\0"[..]);
        payload.put_i16(0);
        payload.put_i16(1);
        payload.put_i32(1);
        payload.put(&b"5"[..]);
        payload.put_i16(0);

        bind.put_i32(payload.len() as i32 + 4);
        bind.put(payload);

        let mut qr = QueryRouter::new();
        qr.pool_settings.automatic_sharding_key = Some("data.id".to_string());
        qr.pool_settings.shards = 3;
        qr.pool_settings.query_parser_read_write_splitting = true;

        assert!(qr.infer(&qr.parse(&simple_query(stmt)).unwrap()).is_ok());
        assert_eq!(qr.placeholders.len(), 1);

        assert!(qr.infer_shard_from_bind(&bind));
        assert_eq!(qr.shard().unwrap(), 2);
        assert!(qr.placeholders.is_empty());
    }

    #[tokio::test]
    async fn test_table_access_plugin() {
        use crate::config::{Plugins, TableAccess};
        let table_access = TableAccess {
            enabled: true,
            tables: vec![String::from("pg_database")],
        };
        let plugins = Plugins {
            table_access: Some(table_access),
            intercept: None,
            query_logger: None,
            prewarmer: None,
        };

        QueryRouter::setup();
        let pool_settings = PoolSettings {
            query_parser_enabled: true,
            plugins: Some(plugins),
            ..Default::default()
        };
        let mut qr = QueryRouter::new();
        qr.update_pool_settings(&pool_settings);

        let query = simple_query("SELECT * FROM pg_database");
        let ast = qr.parse(&query).unwrap();

        let res = qr.execute_plugins(&ast).await;

        assert_eq!(
            res,
            Ok(PluginOutput::Deny(
                "permission for table \"pg_database\" denied".to_string()
            ))
        );
    }

    #[tokio::test]
    async fn test_plugins_disabled_by_defaault() {
        QueryRouter::setup();
        let qr = QueryRouter::new();

        let query = simple_query("SELECT * FROM pg_database");
        let ast = qr.parse(&query).unwrap();

        let res = qr.execute_plugins(&ast).await;

        assert_eq!(res, Ok(PluginOutput::Allow));
    }

    #[tokio::test]
    #[serial]
    async fn test_db_activity_based_routing_initializing_state() {
        QueryRouter::setup();
        let mut qr = QueryRouter::new();
        qr.pool_settings.db_activity_based_routing = true;
        qr.pool_settings.query_parser_read_write_splitting = true;
        qr.pool_settings.query_parser_enabled = true;
        qr.pool_settings.db = "test_table_mutation_cache".to_string();

        qr.database_activity_cache()
            .invalidate(&qr.pool_settings.db.clone());

        let query = simple_query("SELECT * FROM some_table");
        let ast = qr.parse(&query).unwrap();

        // Initially, the database activity should be in the "Initializing" state
        let state = qr.database_activity_state(&qr.pool_settings.db.clone());
        assert_eq!(state, DatabaseActivityState::Initializing);

        // Check that the router chooses the primary role due to "Initializing" state
        assert!(qr.infer(&ast).is_ok());
        assert_eq!(qr.role(), Some(Role::Primary));
    }

    #[tokio::test]
    #[serial]
    async fn test_db_activity_based_routing_active_state() {
        QueryRouter::setup();
        let mut qr = QueryRouter::new();
        qr.pool_settings.db_activity_based_routing = true;
        qr.pool_settings.query_parser_read_write_splitting = true;
        qr.pool_settings.query_parser_enabled = true;
        qr.pool_settings.db = "test_table_mutation_cache".to_string();

        let db_name = qr.pool_settings.db.clone();
        let cache = qr.database_activity_cache();
        cache.insert(db_name.clone(), DatabaseActivityState::Active);

        let query = simple_query("SELECT * FROM some_table");
        let ast = qr.parse(&query).unwrap();

        // Check that the router can choose a replica role when in "Active" state
        assert!(qr.infer(&ast).is_ok());
        assert_eq!(qr.role(), None); // Default should allow replica due to active state
    }

    #[tokio::test]
    #[serial]
    async fn test_table_mutation_cache_on_write() {
        QueryRouter::setup();
        let mut qr = QueryRouter::new();
        qr.pool_settings.db_activity_based_routing = true;
        qr.pool_settings.table_mutation_cache_ms_ttl = 20_000; // 20 seconds in milliseconds
        qr.pool_settings.query_parser_enabled = true;
        qr.pool_settings.query_parser_read_write_splitting = true;
        qr.pool_settings.db = "test_table_mutation_cache".to_string();

        qr.database_activity_cache()
            .invalidate(&qr.pool_settings.db.clone());

        let query = simple_query("UPDATE some_table SET col1 = 'value' WHERE col2 = 1");
        let ast = qr.parse(&query).unwrap();

        // Simulate the mutation query which should populate the mutation cache
        assert!(qr.infer(&ast).is_ok());
        assert_eq!(qr.role(), Some(Role::Primary));

        let table_cache_key = qr.table_mutation_cache_key(Ident::new("some_table"));
        let cache = qr.table_mutations_cache();

        // Ensure the table mutation cache contains the table with recent write
        assert!(cache.contains_key(&table_cache_key));
    }

    #[tokio::test]
    #[serial]
    async fn test_db_activity_based_routing_multi_query() {
        use super::*;
        use crate::messages::simple_query;
        use tokio::time::Duration;

        QueryRouter::setup();
        let mut qr = QueryRouter::new();

        // Configure the pool settings for db_activity_based_routing
        qr.pool_settings.query_parser_read_write_splitting = true;
        qr.pool_settings.query_parser_enabled = true;
        qr.pool_settings.db_activity_based_routing = true;
        qr.pool_settings.db = "test_db_activity_routing".to_string();

        qr.database_activity_cache()
            .invalidate(&qr.pool_settings.db.clone());

        // First query when database is initializing
        let query = simple_query("SELECT * FROM test_table");
        let ast = qr.parse(&query).unwrap();
        assert!(qr.infer(&ast).is_ok());
        // Should route to primary because database is initializing
        assert_eq!(qr.role(), Some(Role::Primary));

        // Wait for the initialization delay to pass
        tokio::time::sleep(Duration::from_millis(
            qr.pool_settings.db_activity_init_delay * 2,
        ))
        .await;

        // Next query after database is active
        let query = simple_query("SELECT * FROM test_table");
        let ast = qr.parse(&query).unwrap();
        qr.active_role = None; // Reset the active_role
        assert!(qr.infer(&ast).is_ok());
        // Should route to replica because database is active and no recent mutations
        assert_eq!(qr.role(), None);

        // Simulate a write query to update the mutation cache
        let query = simple_query("INSERT INTO test_table (id, name) VALUES (1, 'test')");
        let ast = qr.parse(&query).unwrap();
        qr.active_role = None; // Reset the active_role
        assert!(qr.infer(&ast).is_ok());
        // Should route to primary because it's a write operation
        assert_eq!(qr.role(), Some(Role::Primary));

        // Immediately run a read query on the same table
        let query = simple_query("SELECT * FROM test_table WHERE id = 1");
        let ast = qr.parse(&query).unwrap();
        qr.active_role = None; // Reset the active_role
        assert!(qr.infer(&ast).is_ok());
        // Should route to primary because the table was recently mutated
        assert_eq!(qr.role(), Some(Role::Primary));

        // Wait for the mutation cache TTL to expire
        tokio::time::sleep(Duration::from_millis(
            qr.pool_settings.table_mutation_cache_ms_ttl * 2,
        ))
        .await;

        // Run the read query again after cache expiration
        let query = simple_query("SELECT * FROM test_table WHERE id = 1");
        let ast = qr.parse(&query).unwrap();
        qr.active_role = None; // Reset the active_role
        assert!(qr.infer(&ast).is_ok());
        // Should route to replica because mutation cache has expired
        assert_eq!(qr.role(), None);
    }
}
