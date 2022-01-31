#[derive(Debug)]
pub struct ErrorResponse {
    len: i32,
    error: String,
}

impl crate::messages::Message for ErrorResponse {
    fn len(&self) -> i32 {
        self.len
    }

    fn parse(buf: &[u8], len: i32) -> Option<ErrorResponse> {
        // 'S': 1 byte
        // Len: 4 bytes
        let buf = &buf[5..(len + 1) as usize];
        let code = buf[0];

        let error = match code {
            0 => String::from(""),
            _ => {
                let buf: Vec<u8> = buf[1..buf.len()]
                    .iter()
                    .map(|x| if *x == 0 { 32 } else { *x })
                    .collect();

                String::from_utf8_lossy(&buf).to_string()
            }
        };

        Some(ErrorResponse {
            len: len,
            error: error,
        })
    }

    fn debug(&self) -> String {
        format!("Error: {}", self.error)
    }

    fn to_vec(&self) -> Vec<u8> {
        Vec::new()
    }
}
