use bytes::Buf;

pub struct BackendKeyData {
    len: i32,
    process_id: i32,
    secret_key: i32,
}

impl crate::messages::Message for BackendKeyData {
    fn len(&self) -> i32 {
        self.len
    }

    fn parse(buf: &mut bytes::BytesMut, len: i32) -> Option<BackendKeyData> {
        // 'K': 1 byte
        // Len: 4 bytes
        let _c = buf.get_u8();
        let _len = buf.get_i32();

        let process_id = buf.get_i32();
        let secret_key = buf.get_i32();

        Some(BackendKeyData {
            len: len,
            process_id: process_id,
            secret_key: secret_key,
        })
    }

    fn debug(&self) -> String {
        format!(
            "process_id = {}, backend_secret = {}",
            self.process_id, self.secret_key
        )
    }

    fn to_vec(&self) -> Vec<u8> {
        Vec::new()
    }
}
