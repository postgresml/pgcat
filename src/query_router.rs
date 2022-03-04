use crate::config::{get_config, Role};
use crate::sharding::{Sharder, ShardingFunction};
/// Route queries automatically based on explicitely requested
/// or implied query characteristics.
use bytes::{Buf, BytesMut};
use log::{debug, error};
use once_cell::sync::OnceCell;
use regex::{Regex, RegexSet};
use sqlparser::ast::Statement::{Query, StartTransaction};
use sqlparser::dialect::PostgreSqlDialect;
use sqlparser::parser::Parser;

const CUSTOM_SQL_REGEXES: [&str; 5] = [
    r"(?i)^ *SET SHARDING KEY TO '?([0-9]+)'? *;? *$",
    r"(?i)^ *SET SHARD TO '?([0-9]+|ANY)'? *;? *$",
    r"(?i)^ *SHOW SHARD *;? *$",
    r"(?i)^ *SET SERVER ROLE TO '(PRIMARY|REPLICA|ANY|AUTO|DEFAULT)' *;? *$",
    r"(?i)^ *SHOW SERVER ROLE *;? *$",
];

#[derive(PartialEq, Debug)]
pub enum Command {
    SetShardingKey,
    SetShard,
    ShowShard,
    SetServerRole,
    ShowServerRole,
}

// Quick test
static CUSTOM_SQL_REGEX_SET: OnceCell<RegexSet> = OnceCell::new();

// Capture value
static CUSTOM_SQL_REGEX_LIST: OnceCell<Vec<Regex>> = OnceCell::new();

pub struct QueryRouter {
    // By default, queries go here, unless we have better information
    // about what the client wants.
    default_server_role: Option<Role>,

    // Number of shards in the cluster.
    shards: usize,

    // Which shard we should be talking to right now.
    active_shard: Option<usize>,

    // Should we be talking to a primary or a replica?
    active_role: Option<Role>,

    // Include the primary into the replica pool?
    primary_reads_enabled: bool,

    // Should we try to parse queries?
    query_parser_enabled: bool,

    // Which sharding function are we using?
    sharding_function: ShardingFunction,
}

impl QueryRouter {
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

        // Impossible
        if list.len() != set.len() {
            return false;
        }

        match CUSTOM_SQL_REGEX_LIST.set(list) {
            Ok(_) => true,
            Err(_) => return false,
        };

        match CUSTOM_SQL_REGEX_SET.set(set) {
            Ok(_) => true,
            Err(_) => false,
        }
    }

    pub fn new() -> QueryRouter {
        let config = get_config();

        let default_server_role = match config.query_router.default_role.as_ref() {
            "any" => None,
            "primary" => Some(Role::Primary),
            "replica" => Some(Role::Replica),
            _ => unreachable!(),
        };

        let sharding_function = match config.query_router.sharding_function.as_ref() {
            "pg_bigint_hash" => ShardingFunction::PgBigintHash,
            "sha1" => ShardingFunction::Sha1,
            _ => unreachable!(),
        };

        QueryRouter {
            default_server_role: default_server_role,
            shards: config.shards.len(),

            active_role: default_server_role,
            active_shard: None,
            primary_reads_enabled: config.query_router.primary_reads_enabled,
            query_parser_enabled: config.query_router.query_parser_enabled,
            sharding_function,
        }
    }

    /// Try to parse a command and execute it.
    pub fn try_execute_command(&mut self, mut buf: BytesMut) -> Option<(Command, String)> {
        let code = buf.get_u8() as char;

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

        if matches.len() != 1 {
            return None;
        }

        let command = match matches[0] {
            0 => Command::SetShardingKey,
            1 => Command::SetShard,
            2 => Command::ShowShard,
            3 => Command::SetServerRole,
            4 => Command::ShowServerRole,
            _ => unreachable!(),
        };

        let mut value = match command {
            Command::SetShardingKey | Command::SetShard | Command::SetServerRole => {
                // Capture value. I know this re-runs the regex engine, but I haven't
                // figured out a better way just yet. I think I can write a single Regex
                // that matches all 5 custom SQL patterns, but maybe that's not very legible?
                //
                // I think this is faster than running the Regex engine 5 times, so
                // this is a strong maybe for me so far.
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
                Some(Role::Primary) => String::from("primary"),
                Some(Role::Replica) => String::from("replica"),
                None => {
                    if self.query_parser_enabled {
                        String::from("auto")
                    } else {
                        String::from("any")
                    }
                }
            },
        };

        match command {
            Command::SetShardingKey => {
                let sharder = Sharder::new(self.shards, self.sharding_function);
                let shard = sharder.shard(value.parse::<i64>().unwrap());
                self.active_shard = Some(shard);
                value = shard.to_string();
            }

            Command::SetShard => {
                self.active_shard = match value.as_ref() {
                    "any" => Some(rand::random::<usize>() % self.shards),
                    _ => Some(value.parse::<usize>().unwrap()),
                };
            }

            Command::SetServerRole => {
                self.active_role = match value.to_ascii_lowercase().as_ref() {
                    "primary" => {
                        self.query_parser_enabled = false;
                        Some(Role::Primary)
                    }

                    "replica" => {
                        self.query_parser_enabled = false;
                        Some(Role::Replica)
                    }

                    "any" => {
                        self.query_parser_enabled = false;
                        None
                    }

                    "auto" => {
                        self.query_parser_enabled = true;
                        None
                    }

                    "default" => {
                        // TODO: reset query parser to default here.
                        self.active_role = self.default_server_role;
                        self.query_parser_enabled = get_config().query_router.query_parser_enabled;
                        self.active_role
                    }

                    _ => unreachable!(),
                };
            }

            _ => (),
        }

        Some((command, value))
    }

    /// Try to infer which server to connect to based on the contents of the query.
    pub fn infer_role(&mut self, mut buf: BytesMut) -> bool {
        let code = buf.get_u8() as char;
        let len = buf.get_i32() as usize;

        let query = match code {
            'Q' => {
                let query = String::from_utf8_lossy(&buf[..len - 5]).to_string();
                debug!("Query: '{}'", query);
                query
            }

            'P' => {
                let mut start = 0;
                let mut end;

                // Skip the name of the prepared statement.
                while buf[start] != 0 && start < buf.len() {
                    start += 1;
                }
                start += 1; // Skip terminating null

                // Find the end of the prepared stmt (\0)
                end = start;
                while buf[end] != 0 && end < buf.len() {
                    end += 1;
                }

                let query = String::from_utf8_lossy(&buf[start..end]).to_string();

                debug!("Prepared statement: '{}'", query);

                query.replace("$", "") // Remove placeholders turning them into "values"
            }
            _ => return false,
        };

        let ast = match Parser::parse_sql(&PostgreSqlDialect {}, &query) {
            Ok(ast) => ast,
            Err(err) => {
                debug!("{}", err.to_string());
                return false;
            }
        };

        if ast.len() == 0 {
            return false;
        }

        match ast[0] {
            // All transactions go to the primary, probably a write.
            StartTransaction { .. } => {
                self.active_role = Some(Role::Primary);
            }

            // Likely a read-only query
            Query { .. } => {
                self.active_role = match self.primary_reads_enabled {
                    false => Some(Role::Replica), // If primary should not be receiving reads, use a replica.
                    true => None,                 // Any server role is fine in this case.
                }
            }

            // Likely a write
            _ => {
                self.active_role = Some(Role::Primary);
            }
        };

        true
    }

    /// Get the current desired server role we should be talking to.
    pub fn role(&self) -> Option<Role> {
        self.active_role
    }

    /// Get desired shard we should be talking to.
    pub fn shard(&self) -> usize {
        match self.active_shard {
            Some(shard) => shard,
            None => 0,
        }
    }

    /// Reset the router back to defaults.
    /// This must be called at the end of every transaction in transaction mode.
    pub fn _reset(&mut self) {
        self.active_role = self.default_server_role;
        self.active_shard = None;
    }

    /// Should we attempt to parse queries?
    pub fn query_parser_enabled(&self) -> bool {
        self.query_parser_enabled
    }

    #[allow(dead_code)]
    pub fn toggle_primary_reads(&mut self, value: bool) {
        self.primary_reads_enabled = value;
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::messages::simple_query;
    use bytes::BufMut;

    #[test]
    fn test_defaults() {
        QueryRouter::setup();
        let qr = QueryRouter::new();

        assert_eq!(qr.role(), None);
    }

    #[test]
    fn test_infer_role_replica() {
        QueryRouter::setup();
        let mut qr = QueryRouter::new();
        assert!(qr.try_execute_command(simple_query("SET SERVER ROLE TO 'auto'")) != None);
        assert_eq!(qr.query_parser_enabled(), true);
        qr.toggle_primary_reads(false);

        let queries = vec![
            simple_query("SELECT * FROM items WHERE id = 5"),
            simple_query(
                "SELECT id, name, value FROM items INNER JOIN prices ON item.id = prices.item_id",
            ),
            simple_query("WITH t AS (SELECT * FROM items) SELECT * FROM t"),
        ];

        for query in queries {
            // It's a recognized query
            assert!(qr.infer_role(query));
            assert_eq!(qr.role(), Some(Role::Replica));
        }
    }

    #[test]
    fn test_infer_role_primary() {
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
            assert!(qr.infer_role(query));
            assert_eq!(qr.role(), Some(Role::Primary));
        }
    }

    #[test]
    fn test_infer_role_primary_reads_enabled() {
        QueryRouter::setup();
        let mut qr = QueryRouter::new();
        let query = simple_query("SELECT * FROM items WHERE id = 5");
        qr.toggle_primary_reads(true);

        assert!(qr.infer_role(query));
        assert_eq!(qr.role(), None);
    }

    #[test]
    fn test_infer_role_parse_prepared() {
        QueryRouter::setup();
        let mut qr = QueryRouter::new();
        qr.try_execute_command(simple_query("SET SERVER ROLE TO 'auto'"));
        qr.toggle_primary_reads(false);

        let prepared_stmt = BytesMut::from(
            &b"WITH t AS (SELECT * FROM items WHERE name = $1) SELECT * FROM t WHERE id = $2\0"[..],
        );
        let mut res = BytesMut::from(&b"P"[..]);
        res.put_i32(prepared_stmt.len() as i32 + 4 + 1 + 2);
        res.put_u8(0);
        res.put(prepared_stmt);
        res.put_i16(0);

        assert!(qr.infer_role(res));
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
            // Lower case
            "set sharding key to '1'",
            "set shard to '1'",
            "show shard",
            "set server role to 'replica'",
            "set server role to 'primary'",
            "set server role to 'any'",
            "set server role to 'auto'",
            "show server role",
            // No quotes
            "SET SHARDING KEY TO 11235",
            "SET SHARD TO 15",
            // Spaces and semicolon
            "  SET SHARDING KEY TO 11235  ; ",
            "  SET SHARD TO 15;   ",
            "  SET SHARDING KEY TO 11235  ;",
            " SET SERVER ROLE TO 'primary';   ",
            "    SET SERVER ROLE TO 'primary'  ; ",
            "  SET SERVER ROLE TO 'primary'  ;",
        ];

        // Which regexes it'll match to in the list
        let matches = [
            0, 1, 2, 3, 3, 3, 3, 4, 0, 1, 2, 3, 3, 3, 3, 4, 0, 1, 0, 1, 0, 3, 3, 3,
        ];

        let list = CUSTOM_SQL_REGEX_LIST.get().unwrap();
        let set = CUSTOM_SQL_REGEX_SET.get().unwrap();

        for (i, test) in tests.iter().enumerate() {
            assert!(list[matches[i]].is_match(test));
            assert_eq!(set.matches(test).into_iter().collect::<Vec<_>>().len(), 1);
        }

        let bad = [
            "SELECT * FROM table",
            "SELECT * FROM table WHERE value = 'set sharding key to 5'", // Don't capture things in the middle of the query
        ];

        for query in &bad {
            assert_eq!(set.matches(query).into_iter().collect::<Vec<_>>().len(), 0);
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
            Some((Command::SetShardingKey, String::from("1")))
        );
        assert_eq!(qr.shard(), 1);

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
    }

    #[test]
    fn test_enable_query_parser() {
        QueryRouter::setup();
        let mut qr = QueryRouter::new();
        let query = simple_query("SET SERVER ROLE TO 'auto'");
        qr.toggle_primary_reads(false);

        assert!(qr.try_execute_command(query) != None);
        assert!(qr.query_parser_enabled());
        assert_eq!(qr.role(), None);

        let query = simple_query("INSERT INTO test_table VALUES (1)");
        assert_eq!(qr.infer_role(query), true);
        assert_eq!(qr.role(), Some(Role::Primary));

        let query = simple_query("SELECT * FROM test_table");
        assert_eq!(qr.infer_role(query), true);
        assert_eq!(qr.role(), Some(Role::Replica));

        assert!(qr.query_parser_enabled());
        let query = simple_query("SET SERVER ROLE TO 'default'");
        assert!(qr.try_execute_command(query) != None);
        assert!(!qr.query_parser_enabled());
    }
}
