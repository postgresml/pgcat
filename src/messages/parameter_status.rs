use bytes::Buf;

/// ParameterStatus (B)
#[derive(Debug, Clone)]
pub struct ParameterStatus {
    len: i32,
    name: String,
    value: String,
}

impl crate::messages::Message for ParameterStatus {
    fn len(&self) -> i32 {
        self.len
    }

    fn parse(buf: &mut bytes::BytesMut, len: i32) -> Option<ParameterStatus> {
        // 'S': 1 byte
        // Len: 4 bytes
        let _c = buf.get_u8();
        let len = buf.get_i32() as usize;
        let mut args = buf.copy_to_bytes(len - 4);

        let args = crate::communication::parse_parameters(&mut args);

        let name = match args.clone().into_keys().next() {
            Some(n) => n,
            None => return None,
        };

        let value = match args.into_values().next() {
            Some(v) => v,
            None => return None,
        };

        Some(ParameterStatus {
            len: len as i32,
            name: name,
            value: value,
        })
    }

    fn debug(&self) -> String {
        format!("{} = {}", self.name, self.value)
    }

    fn to_vec(&self) -> Vec<u8> {
        Vec::new()
    }
}
