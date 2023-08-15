use crate::errors::Error;
use crate::messages::BytesMutReader;
use bytes::{Buf, BytesMut};
use sqlparser::ast::Statement;
use sqlparser::dialect::PostgreSqlDialect;
use sqlparser::parser::Parser;
use std::io::Cursor;

#[derive(Debug, PartialEq, Eq, Clone, Hash)]
pub struct Query {
    pub text: String,
}

impl Query {
    pub fn new(message: &BytesMut) -> Option<Query> {
        let mut message_cursor = Cursor::new(message);
        let char = message_cursor.get_u8() as char;

        // TODO max_len
        let _len = message_cursor.get_i32() as usize;

        let text = match char {
            // Query
            'Q' => message_cursor.read_string().unwrap(),

            // Parse (prepared statement)
            'P' => {
                // Reads statement name
                let _name = message_cursor.read_string().unwrap();

                // Reads query string
                message_cursor.read_string().unwrap()
            }

            _ => return None,
        };

        Some(Query { text })
    }

    fn statements(&self) -> Result<Vec<Statement>, Error> {
        Parser::parse_sql(&PostgreSqlDialect {}, &self.text)
            .map_err(|e| Error::BadQuery(format!("Error parsing query: {}", e)))
    }

    pub fn is_select(&self) -> bool {
        self.statements()
            .map(|statements| {
                statements.iter().any(|s| match s {
                    Statement::Query(_) => true,
                    _ => false,
                })
            })
            .unwrap_or(false)
    }

    pub fn normalized(&self) -> Result<String, Error> {
        pg_query::normalize(&self.text)
            .map_err(|e| Error::BadQuery(format!("Error normalizing query: {}", e)))
    }

    pub fn fingerprint(&self) -> Result<u64, Error> {
        pg_query::fingerprint(&self.text)
            .map(|f| f.value)
            .map_err(|e| Error::BadQuery(format!("Error fingerprinting query: {}", e)))
    }
}

#[cfg(test)]
mod tests {
    use crate::query::Query;
    use bytes::BytesMut;

    #[test]
    fn test_parse_query() {
        let text = "select 1".to_string();
        let message = BytesMut::from(
            format!("Q{:04}{}\0", text.len(), text)
                .into_bytes()
                .as_slice(),
        );
        let query = Query::new(&message).unwrap();
        assert_eq!(query.text, text);
        assert!(query.is_select());
        assert_eq!(query.normalized(), "select $1".to_string());
        assert_eq!(query.fingerprint(), 5836069208177285818);
    }

    #[test]
    fn test_parse_query_prepared() {
        let text = "select 1".to_string();
        let message = BytesMut::from(
            format!("P{:04}some_statement_name\0{}\0", text.len(), text)
                .into_bytes()
                .as_slice(),
        );
        let query = Query::new(&message).unwrap();
        assert_eq!(query.text, text);
        assert!(query.is_select());
        assert_eq!(query.normalized, "select $1".to_string());
        assert_eq!(query.fingerprint, 5836069208177285818);
        assert_eq!(
            query.hash,
            vec![
                130, 42, 224, 125, 71, 131, 21, 139, 193, 145, 43, 182, 35, 229, 16, 124, 201, 0,
                45, 81, 158, 17, 67, 169, 194, 0, 237, 110, 225, 139, 109, 15
            ]
        );
    }

    #[test]
    fn test_parse_query_ignores_comments() {
        let query = "select 1".to_string();
        let commented_query = format!("/* my comment */ {}", query);
        let query_message = BytesMut::from(
            format!("Q{:04}{}\0", query.len(), query)
                .into_bytes()
                .as_slice(),
        );
        let commented_query_message = BytesMut::from(
            format!("Q{:04}{}\0", commented_query.len(), commented_query)
                .into_bytes()
                .as_slice(),
        );
        let parsed_query = parse_query(&query_message).unwrap();
        let parsed_commented_query = parse_query(&commented_query_message).unwrap();
        assert_eq!(parsed_query.fingerprint, parsed_commented_query.fingerprint);
    }
}
