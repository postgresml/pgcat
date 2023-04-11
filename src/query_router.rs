/// Route queries automatically based on explicitly requested
/// or implied query characteristics.
use bytes::{Buf, BytesMut};
use log::{debug, error};
use once_cell::sync::OnceCell;
use regex::{Regex, RegexSet};
use sqlparser::ast::Statement::{Query, StartTransaction};
use sqlparser::ast::{
    BinaryOperator, Expr, Ident, JoinConstraint, JoinOperator, SetExpr, TableFactor, Value,
};
use sqlparser::dialect::PostgreSqlDialect;
use sqlparser::parser::Parser;

use crate::config::Role;
use crate::messages::BytesMutReader;
use crate::pool::PoolSettings;
use crate::sharding::Sharder;

use std::cmp;
use std::collections::BTreeSet;
use std::io::Cursor;

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

impl QueryRouter {
    /// One-time initialization of regexes
    /// that parse our custom SQL protocol.
    pub fn setup() -> bool {
        let set = match RegexSet::new(&CUSTOM_SQL_REGEXES) {
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
    pub fn update_pool_settings(&mut self, pool_settings: PoolSettings) {
        self.pool_settings = pool_settings;
    }

    /// Try to parse a command and execute it.
    pub fn try_execute_command(&mut self, message_buffer: &BytesMut) -> Option<(Command, String)> {
        let mut message_cursor = Cursor::new(message_buffer);

        let code = message_cursor.get_u8() as char;

        // Check for any sharding regex matches in any queries
        match code as char {
            // For Parse and Query messages peek to see if they specify a shard_id as a comment early in the statement
            'P' | 'Q' => {
                if self.pool_settings.shard_id_regex.is_some()
                    || self.pool_settings.sharding_key_regex.is_some()
                {
                    // Check only the first block of bytes configured by the pool settings
                    let len = message_cursor.get_i32() as usize;
                    let seg = cmp::min(len - 5, self.pool_settings.regex_search_limit);
                    let initial_segment = String::from_utf8_lossy(&message_buffer[0..seg]);

                    // Check for a shard_id included in the query
                    if let Some(shard_id_regex) = &self.pool_settings.shard_id_regex {
                        let shard_id = shard_id_regex.captures(&initial_segment).and_then(|cap| {
                            cap.get(1).and_then(|id| id.as_str().parse::<usize>().ok())
                        });
                        if let Some(shard_id) = shard_id {
                            debug!("Setting shard to {:?}", shard_id);
                            self.set_shard(shard_id);
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
            }
            _ => {}
        }

        // Only simple protocol supported for commands processed below
        if code != 'Q' {
            return None;
        }

        let _len = message_cursor.get_i32() as usize;
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

            Command::ShowShard => self.shard().to_string(),
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

    /// Try to infer which server to connect to based on the contents of the query.
    pub fn infer(&mut self, message: &BytesMut) -> bool {
        debug!("Inferring role");

        let mut message_cursor = Cursor::new(message);

        let code = message_cursor.get_u8() as char;
        let _len = message_cursor.get_i32() as usize;

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
                message_cursor.read_string().unwrap();

                // Reads query string
                let query = message_cursor.read_string().unwrap();

                debug!("Prepared statement: '{}'", query);
                query
            }

            _ => return false,
        };

        let ast = match Parser::parse_sql(&PostgreSqlDialect {}, &query) {
            Ok(ast) => ast,
            Err(err) => {
                // SELECT ... FOR UPDATE won't get parsed correctly.
                debug!("{}: {}", err, query);
                self.active_role = Some(Role::Primary);
                return false;
            }
        };

        debug!("AST: {:?}", ast);

        if ast.is_empty() {
            // That's weird, no idea, let's go to primary
            self.active_role = Some(Role::Primary);
            return false;
        }

        for q in &ast {
            match q {
                // All transactions go to the primary, probably a write.
                StartTransaction { .. } => {
                    self.active_role = Some(Role::Primary);
                    break;
                }

                // Likely a read-only query
                Query(query) => {
                    match &self.pool_settings.automatic_sharding_key {
                        Some(_) => {
                            // TODO: if we have multiple queries in the same message,
                            // we can either split them and execute them individually
                            // or discard shard selection. If they point to the same shard though,
                            // we can let them through as-is.
                            // This is basically building a database now :)
                            match self.infer_shard(query) {
                                Some(shard) => {
                                    self.active_shard = Some(shard);
                                    debug!("Automatically using shard: {:?}", self.active_shard);
                                }

                                None => (),
                            };
                        }

                        None => (),
                    };

                    self.active_role = match self.primary_reads_enabled() {
                        false => Some(Role::Replica), // If primary should not be receiving reads, use a replica.
                        true => None,                 // Any server role is fine in this case.
                    }
                }

                // Likely a write
                _ => {
                    self.active_role = Some(Role::Primary);
                    break;
                }
            };
        }

        true
    }

    /// Parse the shard number from the Bind message
    /// which contains the arguments for a prepared statement.
    ///
    /// N.B.: Only supports anonymous prepared statements since we don't
    /// keep a cache of them in PgCat.
    pub fn infer_shard_from_bind(&mut self, message: &BytesMut) -> bool {
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
            self.set_shard(*shards.first().unwrap());
            true
        } else {
            debug!("Found no sharding keys");
            false
        }
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
            .split(".")
            .map(|ident| Ident::new(ident))
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
                    if &ident.value == &sharding_key[1].value {
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
                                found = &sharding_key[0].value == &table[0].value;
                            } else if table.len() == 2 {
                                // Table name is fully qualified with the schema: e.g.
                                //      SELECT * FROM public.t WHERE sharding_key = 5
                                // Ignore the schema (TODO: at some point, we want schema support)
                                // and use the table name only.
                                found = &sharding_key[0].value == &table[1].value;
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
                        found = &sharding_key[0].value == &idents[0].value
                            && &sharding_key[1].value == &idents[1].value;
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
                    match placeholder.replace("$", "").parse::<i16>() {
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
        let mut shards = BTreeSet::new();
        let mut exprs = Vec::new();

        match &*query.body {
            SetExpr::Query(query) => {
                match self.infer_shard(&*query) {
                    Some(shard) => {
                        shards.insert(shard);
                    }
                    None => (),
                };
            }

            // SELECT * FROM ...
            // We understand that pretty well.
            SetExpr::Select(select) => {
                // Collect all table names from the query.
                let mut table_names = Vec::new();

                for table in select.from.iter() {
                    match &table.relation {
                        TableFactor::Table { name, .. } => {
                            table_names.push(name.0.clone());
                        }

                        _ => (),
                    };

                    // Get table names from all the joins.
                    for join in table.joins.iter() {
                        match &join.relation {
                            TableFactor::Table { name, .. } => {
                                table_names.push(name.0.clone());
                            }

                            _ => (),
                        };

                        // We can filter results based on join conditions, e.g.
                        // SELECT * FROM t INNER JOIN B ON B.sharding_key = 5;
                        match &join.join_operator {
                            JoinOperator::Inner(inner_join) => match &inner_join {
                                JoinConstraint::On(expr) => {
                                    // Parse the selection criteria later.
                                    exprs.push(expr.clone());
                                }

                                _ => (),
                            },

                            _ => (),
                        };
                    }
                }

                // Parse the actual "FROM ..."
                match &select.selection {
                    Some(selection) => {
                        exprs.push(selection.clone());
                    }

                    None => (),
                };

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
            }
            _ => (),
        };

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

    fn set_sharding_key(&mut self, sharding_key: i64) -> Option<usize> {
        let sharder = Sharder::new(
            self.pool_settings.shards,
            self.pool_settings.sharding_function,
        );
        let shard = sharder.shard(sharding_key);
        self.set_shard(shard);
        self.active_shard
    }

    /// Get the current desired server role we should be talking to.
    pub fn role(&self) -> Option<Role> {
        self.active_role
    }

    /// Get desired shard we should be talking to.
    pub fn shard(&self) -> usize {
        self.active_shard.unwrap_or(0)
    }

    pub fn set_shard(&mut self, shard: usize) {
        self.active_shard = Some(shard);
    }

    /// Should we attempt to parse queries?
    pub fn query_parser_enabled(&self) -> bool {
        let enabled = match self.query_parser_enabled {
            None => self.pool_settings.query_parser_enabled,
            Some(value) => value,
        };

        debug!("Query parser enabled: {}", enabled);

        enabled
    }

    pub fn primary_reads_enabled(&self) -> bool {
        match self.primary_reads_enabled {
            None => self.pool_settings.primary_reads_enabled,
            Some(value) => value,
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::config::PoolMode;
    use crate::messages::simple_query;
    use crate::sharding::ShardingFunction;
    use bytes::BufMut;

    #[test]
    fn test_defaults() {
        QueryRouter::setup();
        let qr = QueryRouter::new();

        assert_eq!(qr.role(), None);
    }

    #[test]
    fn test_infer_replica() {
        QueryRouter::setup();
        let mut qr = QueryRouter::new();
        assert!(qr.try_execute_command(&simple_query("SET SERVER ROLE TO 'auto'")) != None);
        assert!(qr.query_parser_enabled());

        assert!(qr.try_execute_command(&simple_query("SET PRIMARY READS TO off")) != None);

        let queries = vec![
            simple_query("SELECT * FROM items WHERE id = 5"),
            simple_query(
                "SELECT id, name, value FROM items INNER JOIN prices ON item.id = prices.item_id",
            ),
            simple_query("WITH t AS (SELECT * FROM items) SELECT * FROM t"),
        ];

        for query in queries {
            // It's a recognized query
            assert!(qr.infer(&query));
            assert_eq!(qr.role(), Some(Role::Replica));
        }
    }

    #[test]
    fn test_infer_primary() {
        QueryRouter::setup();
        let mut qr = QueryRouter::new();

        let queries = vec![
            simple_query("UPDATE items SET name = 'pumpkin' WHERE id = 5"),
            simple_query("INSERT INTO items (id, name) VALUES (5, 'pumpkin')"),
            simple_query("DELETE FROM items WHERE id = 5"),
            simple_query("BEGIN"), // Transaction start
        ];

        for query in queries {
            // It's a recognized query
            assert!(qr.infer(&query));
            assert_eq!(qr.role(), Some(Role::Primary));
        }
    }

    #[test]
    fn test_infer_primary_reads_enabled() {
        QueryRouter::setup();
        let mut qr = QueryRouter::new();
        let query = simple_query("SELECT * FROM items WHERE id = 5");
        assert!(qr.try_execute_command(&simple_query("SET PRIMARY READS TO on")) != None);

        assert!(qr.infer(&query));
        assert_eq!(qr.role(), None);
    }

    #[test]
    fn test_infer_parse_prepared() {
        QueryRouter::setup();
        let mut qr = QueryRouter::new();
        qr.try_execute_command(&simple_query("SET SERVER ROLE TO 'auto'"));
        assert!(qr.try_execute_command(&simple_query("SET PRIMARY READS TO off")) != None);

        let prepared_stmt = BytesMut::from(
            &b"WITH t AS (SELECT * FROM items WHERE name = $1) SELECT * FROM t WHERE id = $2\0"[..],
        );
        let mut res = BytesMut::from(&b"P"[..]);
        res.put_i32(prepared_stmt.len() as i32 + 4 + 1 + 2);
        res.put_u8(0);
        res.put(prepared_stmt);
        res.put_i16(0);

        assert!(qr.infer(&res));
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
        assert_eq!(qr.shard(), 0);

        // SetShard
        let query = simple_query("SET SHARD TO '1'");
        assert_eq!(
            qr.try_execute_command(&query),
            Some((Command::SetShard, String::from("1")))
        );
        assert_eq!(qr.shard(), 1);

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
        let query = simple_query("SET SERVER ROLE TO 'auto'");
        assert!(qr.try_execute_command(&simple_query("SET PRIMARY READS TO off")) != None);

        assert!(qr.try_execute_command(&query) != None);
        assert!(qr.query_parser_enabled());
        assert_eq!(qr.role(), None);

        let query = simple_query("INSERT INTO test_table VALUES (1)");
        assert!(qr.infer(&query));
        assert_eq!(qr.role(), Some(Role::Primary));

        let query = simple_query("SELECT * FROM test_table");
        assert!(qr.infer(&query));
        assert_eq!(qr.role(), Some(Role::Replica));

        assert!(qr.query_parser_enabled());
        let query = simple_query("SET SERVER ROLE TO 'default'");
        assert!(qr.try_execute_command(&query) != None);
        assert!(!qr.query_parser_enabled());
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
            primary_reads_enabled: false,
            sharding_function: ShardingFunction::PgBigintHash,
            automatic_sharding_key: Some(String::from("test.id")),
            healthcheck_delay: PoolSettings::default().healthcheck_delay,
            healthcheck_timeout: PoolSettings::default().healthcheck_timeout,
            ban_time: PoolSettings::default().ban_time,
            sharding_key_regex: None,
            shard_id_regex: None,
            regex_search_limit: 1000,
            auth_query: None,
            auth_query_password: None,
            auth_query_user: None,
        };
        let mut qr = QueryRouter::new();
        assert_eq!(qr.active_role, None);
        assert_eq!(qr.active_shard, None);
        assert_eq!(qr.query_parser_enabled, None);
        assert_eq!(qr.primary_reads_enabled, None);

        // Internal state must not be changed due to this, only defaults
        qr.update_pool_settings(pool_settings.clone());

        assert_eq!(qr.active_role, None);
        assert_eq!(qr.active_shard, None);
        assert!(qr.query_parser_enabled());
        assert!(!qr.primary_reads_enabled());

        let q1 = simple_query("SET SERVER ROLE TO 'primary'");
        assert!(qr.try_execute_command(&q1) != None);
        assert_eq!(qr.active_role.unwrap(), Role::Primary);

        let q2 = simple_query("SET SERVER ROLE TO 'default'");
        assert!(qr.try_execute_command(&q2) != None);
        assert_eq!(qr.active_role.unwrap(), pool_settings.default_role);
    }

    #[test]
    fn test_parse_multiple_queries() {
        QueryRouter::setup();

        let mut qr = QueryRouter::new();
        assert!(qr.infer(&simple_query("BEGIN; SELECT 1; COMMIT;")));
        assert_eq!(qr.role(), Role::Primary);

        assert!(qr.infer(&simple_query("SELECT 1; SELECT 2;")));
        assert_eq!(qr.role(), Role::Replica);

        assert!(qr.infer(&simple_query(
            "SELECT 123; INSERT INTO t VALUES (5); SELECT 1;"
        )));
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
            primary_reads_enabled: false,
            sharding_function: ShardingFunction::PgBigintHash,
            automatic_sharding_key: None,
            healthcheck_delay: PoolSettings::default().healthcheck_delay,
            healthcheck_timeout: PoolSettings::default().healthcheck_timeout,
            ban_time: PoolSettings::default().ban_time,
            sharding_key_regex: Some(Regex::new(r"/\* sharding_key: (\d+) \*/").unwrap()),
            shard_id_regex: Some(Regex::new(r"/\* shard_id: (\d+) \*/").unwrap()),
            regex_search_limit: 1000,
            auth_query: None,
            auth_query_password: None,
            auth_query_user: None,
        };
        let mut qr = QueryRouter::new();
        qr.update_pool_settings(pool_settings.clone());

        // Shard should start out unset
        assert_eq!(qr.active_shard, None);

        // Make sure setting it works
        let q1 = simple_query("/* shard_id: 1 */ select 1 from foo;");
        assert!(qr.try_execute_command(&q1) == None);
        assert_eq!(qr.active_shard, Some(1));

        // And make sure changing it works
        let q2 = simple_query("/* shard_id: 0 */ select 1 from foo;");
        assert!(qr.try_execute_command(&q2) == None);
        assert_eq!(qr.active_shard, Some(0));

        // Validate setting by shard with expected shard copied from sharding.rs tests
        let q2 = simple_query("/* sharding_key: 6 */ select 1 from foo;");
        assert!(qr.try_execute_command(&q2) == None);
        assert_eq!(qr.active_shard, Some(2));
    }

    #[test]
    fn test_automatic_sharding_key() {
        QueryRouter::setup();

        let mut qr = QueryRouter::new();
        qr.pool_settings.automatic_sharding_key = Some("data.id".to_string());
        qr.pool_settings.shards = 3;

        assert!(qr.infer(&simple_query("SELECT * FROM data WHERE id = 5")));
        assert_eq!(qr.shard(), 2);

        assert!(qr.infer(&simple_query(
            "SELECT one, two, three FROM public.data WHERE id = 6"
        )));
        assert_eq!(qr.shard(), 0);

        assert!(qr.infer(&simple_query(
            "SELECT * FROM data
            INNER JOIN t2 ON data.id = 5
            AND t2.data_id = data.id
        WHERE data.id = 5"
        )));
        assert_eq!(qr.shard(), 2);

        // Shard did not move because we couldn't determine the sharding key since it could be ambiguous
        // in the query.
        assert!(qr.infer(&simple_query(
            "SELECT * FROM t2 INNER JOIN data ON id = 6 AND data.id = t2.data_id"
        )));
        assert_eq!(qr.shard(), 2);

        assert!(qr.infer(&simple_query(
            r#"SELECT * FROM "public"."data" WHERE "id" = 6"#
        )));
        assert_eq!(qr.shard(), 0);

        assert!(qr.infer(&simple_query(
            r#"SELECT * FROM "public"."data" WHERE "data"."id" = 5"#
        )));
        assert_eq!(qr.shard(), 2);

        // Super unique sharding key
        qr.pool_settings.automatic_sharding_key = Some("*.unique_enough_column_name".to_string());
        assert!(qr.infer(&simple_query(
            "SELECT * FROM table_x WHERE unique_enough_column_name = 6"
        )));
        assert_eq!(qr.shard(), 0);

        assert!(qr.infer(&simple_query("SELECT * FROM table_y WHERE another_key = 5")));
        assert_eq!(qr.shard(), 0);
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

        assert!(qr.infer(&simple_query(stmt)));
        assert_eq!(qr.placeholders.len(), 1);

        assert!(qr.infer_shard_from_bind(&bind));
        assert_eq!(qr.shard(), 2);
        assert!(qr.placeholders.is_empty());
    }
}
