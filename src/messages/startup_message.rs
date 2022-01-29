
#[derive(Debug)]
pub struct StartupMessage {
    arguments: std::collections::HashMap<String, String>,
}


impl StartupMessage {
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

        Some(StartupMessage{
            arguments: args,
        })
    }

    pub fn username(&self) -> String {
        self.arguments["user"].clone()
    }

    pub fn database(&self) -> String {
        self.arguments["database"].clone()
    }
}