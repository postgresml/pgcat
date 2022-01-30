/// Client authentication was successful.
pub struct AuthenticationOk {}

/// Convert to byte stream.
impl std::convert::Into<Vec<u8>> for AuthenticationOk {
    fn into(self) -> Vec<u8> {
        Vec::<u8>::from(&self)
    }
}

/// Convert to byte stream, actual implementation.
impl std::convert::From<&AuthenticationOk> for Vec<u8> {
    fn from(auth_ok: &AuthenticationOk) -> Vec<u8> {
        // R
        let mut res = vec![b'R'];

        // Length: 8
        res.extend(&8i32.to_be_bytes());

        // Code 0 indicating success
        res.extend(&0i32.to_be_bytes());

        res
    }
}


impl crate::messages::Message for AuthenticationOk {
    fn len(&self) -> i32 {
        8
    }

    fn parse(buf: &[u8], len: i32) -> Option<AuthenticationOk> {
        Some(AuthenticationOk{})
    }

    fn debug(&self) -> String {
        format!("AuthenticationOk")
    }

    fn to_vec(&self) -> Vec<u8> {
		Vec::new()
	}
}