pub struct AuthenticationMD5Password {
    pub salt: Vec<u8>,
}

impl crate::messages::Message for AuthenticationMD5Password {
    fn len(&self) -> i32 {
        8
    }

    fn parse(buf: &mut bytes::BytesMut, _len: i32) -> Option<AuthenticationMD5Password> {
        Some(AuthenticationMD5Password {
            salt: buf[9..13].to_vec(),
        })
    }

    fn debug(&self) -> String {
        format!("AuthenticationOk")
    }

    fn to_vec(&self) -> Vec<u8> {
        Vec::new()
    }
}
