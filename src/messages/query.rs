
#[derive(Debug)]
pub struct Query {
    len: i32,
    query: String,
}

impl crate::messages::Message for Query {
    fn len(&self) -> i32 {
        self.len
    }

    fn parse(buf: &[u8], len: i32) -> Option<Query> {
        // 'Q': 1 byte
        // Len: 4 bytes
        let buf = &buf[5..(len + 1) as usize];
        let query = String::from_utf8_lossy(&buf[..buf.len() - 1]).to_string();

        Some(Query {
            len: len,
            query: query,
        })
    }

    fn debug(&self) -> String {
        format!("Query = {:?}", self.query)
    }

    fn to_vec(&self) -> Vec<u8> {
        self.into()
    }
}

/// Convert to byte stream.
impl std::convert::Into<Vec<u8>> for Query {
    fn into(self) -> Vec<u8> {
        Vec::<u8>::from(&self)
    }
}

/// Convert to byte stream, actual implementation.
impl std::convert::From<&Query> for Vec<u8> {
    fn from(query: &Query) -> Vec<u8> {
        // Q
        let mut res = vec![b'Q'];

        // null-terminated and include itself
        let len = query.query.len() + 4 + 1;

        let mut query: Vec<u8> = query.query.chars().map(|x| x as u8).collect();

        query.push(0);

        // Length: 8
        res.extend(&(len as i32).to_be_bytes());
        res.extend(&query);

        res
    }
}
