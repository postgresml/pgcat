pub struct Terminate {}

impl crate::messages::Message for Terminate {
    fn len(&self) -> i32 {
        4
    }

    fn parse(buf: &mut bytes::BytesMut, len: i32) -> Option<Terminate> {
        None
    }

    fn to_vec(&self) -> Vec<u8> {
        Vec::new()
    }

    fn debug(&self) -> String {
        String::from("Terminate")
    }
}

impl std::convert::Into<Vec<u8>> for Terminate {
    fn into(self) -> Vec<u8> {
        Vec::<u8>::from(&self)
    }
}

/// Convert to byte stream, actual implementation.
impl std::convert::From<&Terminate> for Vec<u8> {
    fn from(auth_ok: &Terminate) -> Vec<u8> {
        // X
        let mut res = vec![b'X'];
        res.extend(&i32::to_be_bytes(4i32));

        res
    }
}
