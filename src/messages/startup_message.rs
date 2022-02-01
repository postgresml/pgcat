#[derive(Debug)]
pub struct StartupMessage {
    arguments: std::collections::HashMap<String, String>,
}

/// Convert to byte stream.
impl std::convert::Into<Vec<u8>> for StartupMessage {
    fn into(self) -> Vec<u8> {
        Vec::<u8>::from(&self)
    }
}

/// Convert to byte stream, actual implementation.
impl std::convert::From<&StartupMessage> for Vec<u8> {
    fn from(auth_ok: &StartupMessage) -> Vec<u8> {
        // R
        let mut res = Vec::new();
        let mut body = Vec::new();

        // Protocol number
        body.extend(&i32::to_be_bytes(196608i32));

        for (key, value) in &auth_ok.arguments {
            let mut key = key.chars().map(|x| x as u8).collect::<Vec<u8>>();
            let mut value = value.chars().map(|x| x as u8).collect::<Vec<u8>>();

            key.push(0);
            value.push(0);

            body.extend(&key);
            body.extend(&value);
        }

        let len = (body.len() + 4 + 1) as i32;

        res.extend(&i32::to_be_bytes(len));
        res.extend(&body);
        res.push(0u8);

        res
    }
}

impl StartupMessage {
    pub fn new(username: &str, database: &str) -> StartupMessage {
        let mut arguments = std::collections::HashMap::from([
            ("user".to_string(), username.to_string()),
            ("database".to_string(), database.to_string()),
        ]);

        StartupMessage {
            arguments: arguments,
        }
    }

    pub fn parse(buf: &[u8]) -> Option<StartupMessage> {
        let mut sbuf = String::new();
        let mut tuple = Vec::new();
        let mut args = std::collections::HashMap::new();
        for c in buf {
            // Strings are null-terminated
            if *c == 0 {
                // We have key
                if tuple.len() < 2 {
                    tuple.push(sbuf.clone());
                }
                // We have key and value
                else if tuple.len() == 2 {
                    args.insert(tuple[0].clone(), tuple[1].clone());
                    tuple.clear();
                    tuple.push(sbuf.clone());
                }

                sbuf.clear();
            }
            // Normal character
            else {
                sbuf.push(*c as char);
            }
        }

        Some(StartupMessage { arguments: args })
    }

    pub fn username(&self) -> String {
        self.arguments["user"].clone()
    }

    pub fn database(&self) -> String {
        self.arguments["database"].clone()
    }
}
