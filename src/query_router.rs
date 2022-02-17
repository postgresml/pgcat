/// Route queries automatically based on explicitely requested
/// or implied query characteristics.
use bytes::{Buf, BytesMut};
use once_cell::sync::OnceCell;
use regex::{Regex, RegexBuilder};
use sqlparser::ast::Statement::{Query, StartTransaction};
use sqlparser::dialect::PostgreSqlDialect;
use sqlparser::parser::Parser;

use crate::config::Role;
use crate::sharding::Sharder;

const SHARDING_REGEX: &str = r"SET SHARDING KEY TO '[0-9]+';";
const ROLE_REGEX: &str = r"SET SERVER ROLE TO '(PRIMARY|REPLICA)';";

static SHARDING_REGEX_RE: OnceCell<Regex> = OnceCell::new();
static ROLE_REGEX_RE: OnceCell<Regex> = OnceCell::new();

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
}

impl QueryRouter {
    pub fn setup() -> bool {
        // Compile our query routing regexes early, so we only do it once.
        let a = match SHARDING_REGEX_RE.set(
            RegexBuilder::new(SHARDING_REGEX)
                .case_insensitive(true)
                .build()
                .unwrap(),
        ) {
            Ok(_) => true,
            Err(_) => false,
        };

        let b = match ROLE_REGEX_RE.set(
            RegexBuilder::new(ROLE_REGEX)
                .case_insensitive(true)
                .build()
                .unwrap(),
        ) {
            Ok(_) => true,
            Err(_) => false,
        };

        a && b
    }

    pub fn new(
        default_server_role: Option<Role>,
        shards: usize,
        primary_reads_enabled: bool,
        query_parser_enabled: bool,
    ) -> QueryRouter {
        QueryRouter {
            default_server_role: default_server_role,
            shards: shards,

            active_role: default_server_role,
            active_shard: None,
            primary_reads_enabled: primary_reads_enabled,
            query_parser_enabled: query_parser_enabled,
        }
    }

    /// Determine if the query is part of our special syntax, extract
    /// the shard key, and return the shard to query based on Postgres'
    /// PARTITION BY HASH function.
    pub fn select_shard(&mut self, mut buf: BytesMut) -> bool {
        let code = buf.get_u8() as char;

        // Only supporting simpe protocol here, so
        // one would have to execute something like this:
        // psql -c "SET SHARDING KEY TO '1234'"
        // after sanitizing the value manually, which can be just done with an
        // int parser, e.g. `let key = "1234".parse::<i64>().unwrap()`.
        match code {
            'Q' => (),
            _ => return false,
        };

        let len = buf.get_i32();
        let query = String::from_utf8_lossy(&buf[..len as usize - 4 - 1]); // Don't read the ternminating null

        let rgx = match SHARDING_REGEX_RE.get() {
            Some(r) => r,
            None => return false,
        };

        if rgx.is_match(&query) {
            let shard = query.split("'").collect::<Vec<&str>>()[1];

            match shard.parse::<i64>() {
                Ok(shard) => {
                    let sharder = Sharder::new(self.shards);
                    self.active_shard = Some(sharder.pg_bigint_hash(shard));

                    true
                }

                // The shard must be a valid integer. Our regex won't let anything else pass,
                // so this code will never run, but Rust can't know that, so we have to handle this
                // case anyway.
                Err(_) => false,
            }
        } else {
            false
        }
    }

    /// Pick a primary or a replica from the pool.
    pub fn select_role(&mut self, mut buf: BytesMut) -> bool {
        let code = buf.get_u8() as char;

        // Same story as select_shard() above.
        match code {
            'Q' => (),
            _ => return false,
        };

        let len = buf.get_i32();
        let query = String::from_utf8_lossy(&buf[..len as usize - 4 - 1]).to_ascii_uppercase();

        let rgx = match ROLE_REGEX_RE.get() {
            Some(r) => r,
            None => return false,
        };

        // Copy / paste from above. If we get one more of these use cases,
        // it'll be time to abstract :).
        if rgx.is_match(&query) {
            let role = query.split("'").collect::<Vec<&str>>()[1];

            match role {
                "PRIMARY" => {
                    self.active_role = Some(Role::Primary);
                    true
                }
                "REPLICA" => {
                    self.active_role = Some(Role::Replica);
                    true
                }

                // Our regex won't let this case happen, but Rust can't know that.
                _ => false,
            }
        } else {
            false
        }
    }

    /// Try to infer which server to connect to based on the contents of the query.
    pub fn infer_role(&mut self, mut buf: BytesMut) -> bool {
        let code = buf.get_u8() as char;
        let len = buf.get_i32() as usize;

        let query = match code {
            'Q' => String::from_utf8_lossy(&buf[..len - 5]).to_string(),
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

                query.replace("$", "") // Remove placeholders turning them into "values"
            }
            _ => return false,
        };

        let ast = match Parser::parse_sql(&PostgreSqlDialect {}, &query) {
            Ok(ast) => ast,
            Err(_) => return false,
        };

        if ast.len() == 0 {
            return false;
        }

        match ast[0] {
            // All transactions go to the primary, probably a write.
            StartTransaction { .. } => {
                self.active_role = Some(Role::Primary);
            }

            // All SELECTs go to the replica
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
            None => 0, // TODO: pick random shard
        }
    }

    /// Reset the router back to defaults.
    /// This must be called at the end of every transaction in transaction mode.
    pub fn reset(&mut self) {
        self.active_role = self.default_server_role;
        self.active_shard = None;
    }

    /// Should we attempt to parse queries?
    pub fn query_parser_enabled(&self) -> bool {
        self.query_parser_enabled
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use bytes::BufMut;

    #[test]
    fn test_select_shard() {
        QueryRouter::setup();

        let default_server_role: Option<Role> = None;
        let shards = 5;
        let mut query_router = QueryRouter::new(default_server_role, shards);

        // Build the special syntax query.
        let mut message = BytesMut::new();
        let query = BytesMut::from(&b"SET SHARDING KEY TO '13';\0"[..]);

        message.put_u8(b'Q'); // Query
        message.put_i32(query.len() as i32 + 4);
        message.put_slice(&query[..]);

        assert!(query_router.select_shard(message));
        assert_eq!(query_router.shard(), 3); // See sharding.rs (we are using 5 shards on purpose in this test)

        query_router.reset();
        assert_eq!(query_router.shard(), 0);
    }

    #[test]
    fn test_select_replica() {
        QueryRouter::setup();

        let default_server_role: Option<Role> = None;
        let shards = 5;
        let mut query_router = QueryRouter::new(default_server_role, shards);

        // Build the special syntax query.
        let mut message = BytesMut::new();
        let query = BytesMut::from(&b"SET SERVER ROLE TO 'replica';\0"[..]);

        message.put_u8(b'Q'); // Query
        message.put_i32(query.len() as i32 + 4);
        message.put_slice(&query[..]);

        assert!(query_router.select_role(message));
        assert_eq!(query_router.role(), Some(Role::Replica));

        query_router.reset();

        assert_eq!(query_router.role(), default_server_role);
    }

    #[test]
    fn test_defaults() {
        QueryRouter::setup();

        let default_server_role: Option<Role> = None;
        let shards = 5;
        let query_router = QueryRouter::new(default_server_role, shards);

        assert_eq!(query_router.shard(), 0);
        assert_eq!(query_router.role(), None);
    }

    #[test]
    fn test_incorrect_syntax() {
        QueryRouter::setup();

        let default_server_role: Option<Role> = None;
        let shards = 5;
        let mut query_router = QueryRouter::new(default_server_role, shards);

        // Build the special syntax query.
        let mut message = BytesMut::new();

        // Typo!
        let query = BytesMut::from(&b"SET SERVER RLE TO 'replica';\0"[..]);

        message.put_u8(b'Q'); // Query
        message.put_i32(query.len() as i32 + 4);
        message.put_slice(&query[..]);

        assert_eq!(query_router.select_shard(message.clone()), false);
        assert_eq!(query_router.select_role(message.clone()), false);
    }
}
