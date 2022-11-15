/// Route queries automatically based on explicitely requested
/// or implied query characteristics.
use bytes::{Buf, BytesMut};
use log::{debug, error};
use once_cell::sync::OnceCell;
use regex::{Regex, RegexSet};
use sqlparser::ast::Statement::{Query, StartTransaction};
use sqlparser::ast::{BinaryOperator, Expr, SetExpr, Value};
use sqlparser::dialect::PostgreSqlDialect;
use sqlparser::parser::Parser;

use crate::config::Role;
use crate::pool::PoolSettings;
use crate::sharding::Sharder;

use std::collections::BTreeSet;

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
        }
    }

    /// Pool settings can change because of a config reload.
    pub fn update_pool_settings(&mut self, pool_settings: PoolSettings) {
        self.pool_settings = pool_settings;
    }

    /// Try to parse a command and execute it.
    pub fn try_execute_command(&mut self, mut buf: BytesMut) -> Option<(Command, String)> {
        let code = buf.get_u8() as char;

        // Only simple protocol supported for commands.
        if code != 'Q' {
            return None;
        }

        let len = buf.get_i32() as usize;
        let query = String::from_utf8_lossy(&buf[..len - 5]).to_string(); // Ignore the terminating NULL.

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
                let sharder = Sharder::new(
                    self.pool_settings.shards,
                    self.pool_settings.sharding_function,
                );
                let shard = sharder.shard(value.parse::<i64>().unwrap());
                self.active_shard = Some(shard);
                value = shard.to_string();
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
    pub fn infer(&mut self, mut buf: BytesMut) -> bool {
        debug!("Inferring role");

        let code = buf.get_u8() as char;
        let len = buf.get_i32() as usize;

        let query = match code {
            // Query
            'Q' => {
                let query = String::from_utf8_lossy(&buf[..len - 5]).to_string();
                debug!("Query: '{}'", query);
                query
            }

            // Parse (prepared statement)
            'P' => {
                let mut start = 0;

                // Skip the name of the prepared statement.
                while buf[start] != 0 && start < buf.len() {
                    start += 1;
                }
                start += 1; // Skip terminating null

                // Find the end of the prepared stmt (\0)
                let mut end = start;
                while buf[end] != 0 && end < buf.len() {
                    end += 1;
                }

                let query = String::from_utf8_lossy(&buf[start..end]).to_string();

                debug!("Prepared statement: '{}'", query);

                query.replace('$', "") // Remove placeholders turning them into "values"
            }

            _ => return false,
        };

        let ast = match Parser::parse_sql(&PostgreSqlDialect {}, &query) {
            Ok(ast) => ast,
            Err(err) => {
                // SELECT ... FOR UPDATE won't get parsed correctly.
                error!("{}: {}", err, query);
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

    /// A `selection` is the `WHERE` clause. This parses
    /// the clause and extracts the sharding key, if present.
    fn selection_parser(&self, expr: &Expr) -> Vec<i64> {
        let mut result = Vec::new();
        let mut found = false;

        // This parses `sharding_key = 5`. But it's technically
        // legal to write `5 = sharding_key`. I don't judge the people
        // who do that, but I think ORMs will still use the first variant,
        // so we can leave the second as a TODO.
        if let Expr::BinaryOp { left, op, right } = expr {
            match &**left {
                Expr::BinaryOp { .. } => result.extend(self.selection_parser(left)),
                Expr::Identifier(ident) => {
                    found =
                        ident.value == *self.pool_settings.automatic_sharding_key.as_ref().unwrap();
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
                Expr::BinaryOp { .. } => result.extend(self.selection_parser(right)),
                Expr::Value(Value::Number(value, ..)) => {
                    if found {
                        match value.parse::<i64>() {
                            Ok(value) => result.push(value),
                            Err(_) => {
                                debug!("Sharding key was not an integer: {}", value);
                            }
                        };
                    }
                }
                _ => (),
            };
        }

        debug!("Sharding keys found: {:?}", result);

        result
    }

    /// Try to figure out which shard the query should go to.
    fn infer_shard(&self, query: &sqlparser::ast::Query) -> Option<usize> {
        let mut shards = BTreeSet::new();

        match &*query.body {
            SetExpr::Query(query) => {
                match self.infer_shard(&*query) {
                    Some(shard) => {
                        shards.insert(shard);
                    }
                    None => (),
                };
            }

            SetExpr::Select(select) => {
                match &select.selection {
                    Some(selection) => {
                        let sharding_keys = self.selection_parser(selection);

                        // TODO: Add support for prepared statements here.
                        // This should just give us the position of the value in the `B` message.

                        let sharder = Sharder::new(
                            self.pool_settings.shards,
                            self.pool_settings.sharding_function,
                        );

                        for value in sharding_keys {
                            let shard = sharder.shard(value);
                            shards.insert(shard);
                        }
                    }

                    None => (),
                };
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
        match self.query_parser_enabled {
            None => self.pool_settings.query_parser_enabled,
            Some(value) => value,
        }
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
        assert!(qr.try_execute_command(simple_query("SET SERVER ROLE TO 'auto'")) != None);
        assert!(qr.query_parser_enabled());

        assert!(qr.try_execute_command(simple_query("SET PRIMARY READS TO off")) != None);

        let queries = vec![
            simple_query("SELECT * FROM items WHERE id = 5"),
            simple_query(
                "SELECT id, name, value FROM items INNER JOIN prices ON item.id = prices.item_id",
            ),
            simple_query("WITH t AS (SELECT * FROM items) SELECT * FROM t"),
        ];

        for query in queries {
            // It's a recognized query
            assert!(qr.infer(query));
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
            assert!(qr.infer(query));
            assert_eq!(qr.role(), Some(Role::Primary));
        }
    }

    #[test]
    fn test_infer_primary_reads_enabled() {
        QueryRouter::setup();
        let mut qr = QueryRouter::new();
        let query = simple_query("SELECT * FROM items WHERE id = 5");
        assert!(qr.try_execute_command(simple_query("SET PRIMARY READS TO on")) != None);

        assert!(qr.infer(query));
        assert_eq!(qr.role(), None);
    }

    #[test]
    fn test_infer_parse_prepared() {
        QueryRouter::setup();
        let mut qr = QueryRouter::new();
        qr.try_execute_command(simple_query("SET SERVER ROLE TO 'auto'"));
        assert!(qr.try_execute_command(simple_query("SET PRIMARY READS TO off")) != None);

        let prepared_stmt = BytesMut::from(
            &b"WITH t AS (SELECT * FROM items WHERE name = $1) SELECT * FROM t WHERE id = $2\0"[..],
        );
        let mut res = BytesMut::from(&b"P"[..]);
        res.put_i32(prepared_stmt.len() as i32 + 4 + 1 + 2);
        res.put_u8(0);
        res.put(prepared_stmt);
        res.put_i16(0);

        assert!(qr.infer(res));
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
            qr.try_execute_command(query),
            Some((Command::SetShardingKey, String::from("0")))
        );
        assert_eq!(qr.shard(), 0);

        // SetShard
        let query = simple_query("SET SHARD TO '1'");
        assert_eq!(
            qr.try_execute_command(query),
            Some((Command::SetShard, String::from("1")))
        );
        assert_eq!(qr.shard(), 1);

        // ShowShard
        let query = simple_query("SHOW SHARD");
        assert_eq!(
            qr.try_execute_command(query),
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
                qr.try_execute_command(query),
                Some((Command::SetServerRole, String::from(*role)))
            );
            assert_eq!(qr.role(), verify_roles[idx],);
            assert_eq!(qr.query_parser_enabled(), query_parser_enabled[idx],);

            // ShowServerRole
            let query = simple_query("SHOW SERVER ROLE");
            assert_eq!(
                qr.try_execute_command(query),
                Some((Command::ShowServerRole, String::from(*role)))
            );
        }

        let primary_reads = ["on", "off", "default"];
        let primary_reads_enabled = ["on", "off", "on"];

        for (idx, primary_reads) in primary_reads.iter().enumerate() {
            assert_eq!(
                qr.try_execute_command(simple_query(&format!(
                    "SET PRIMARY READS TO {}",
                    primary_reads
                ))),
                Some((Command::SetPrimaryReads, String::from(*primary_reads)))
            );
            assert_eq!(
                qr.try_execute_command(simple_query("SHOW PRIMARY READS")),
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
        assert!(qr.try_execute_command(simple_query("SET PRIMARY READS TO off")) != None);

        assert!(qr.try_execute_command(query) != None);
        assert!(qr.query_parser_enabled());
        assert_eq!(qr.role(), None);

        let query = simple_query("INSERT INTO test_table VALUES (1)");
        assert!(qr.infer(query));
        assert_eq!(qr.role(), Some(Role::Primary));

        let query = simple_query("SELECT * FROM test_table");
        assert!(qr.infer(query));
        assert_eq!(qr.role(), Some(Role::Replica));

        assert!(qr.query_parser_enabled());
        let query = simple_query("SET SERVER ROLE TO 'default'");
        assert!(qr.try_execute_command(query) != None);
        assert!(!qr.query_parser_enabled());
    }

    #[test]
    fn test_update_from_pool_settings() {
        QueryRouter::setup();

        let pool_settings = PoolSettings {
            pool_mode: PoolMode::Transaction,
            shards: 2,
            user: crate::config::User::default(),
            default_role: Some(Role::Replica),
            query_parser_enabled: true,
            primary_reads_enabled: false,
            sharding_function: ShardingFunction::PgBigintHash,
            automatic_sharding_key: Some(String::from("id")),
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
        assert!(qr.try_execute_command(q1) != None);
        assert_eq!(qr.active_role.unwrap(), Role::Primary);

        let q2 = simple_query("SET SERVER ROLE TO 'default'");
        assert!(qr.try_execute_command(q2) != None);
        assert_eq!(qr.active_role.unwrap(), pool_settings.default_role);

        // Here we go :)
        let q3 = simple_query("SELECT * FROM test WHERE id = 5 AND values IN (1, 2, 3)");
        assert!(qr.infer(q3));
        assert_eq!(qr.shard(), 1);
    }

    #[test]
    fn test_parse_multiple_queries() {
        QueryRouter::setup();

        let mut qr = QueryRouter::new();
        assert!(qr.infer(simple_query("BEGIN; SELECT 1; COMMIT;")));
        assert_eq!(qr.role(), Role::Primary);

        assert!(qr.infer(simple_query("SELECT 1; SELECT 2;")));
        assert_eq!(qr.role(), Role::Replica);

        assert!(qr.infer(simple_query(
            "SELECT 123; INSERT INTO t VALUES (5); SELECT 1;"
        )));
        assert_eq!(qr.role(), Role::Primary);
    }
}
