use bytes::Buf;

#[derive(Debug)]
pub struct ErrorResponse {
    len: i32,
    error: String,
}

impl ErrorResponse {
    pub fn error(&self) -> String {
        self.error.clone()
    }
}

impl crate::messages::Message for ErrorResponse {
    fn len(&self) -> i32 {
        self.len
    }

    fn parse(buf: &mut bytes::BytesMut, len: i32) -> Option<ErrorResponse> {
        // 'E': 1 byte
        // Len: 4 bytes
        let _c = buf.get_u8();
        let len = buf.get_i32() as usize;
        let code = buf.get_u8();

        let error = match code {
            0 => String::from(""),
            _ => {
                let bytes = buf.copy_to_bytes(len - 4 - 1);
                String::from_utf8_lossy(&bytes).to_string()
            }
        };

        Some(ErrorResponse {
            len: len as i32,
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
