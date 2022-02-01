use crate::communication::parse_string;
use crate::messages::Message;

pub struct RowDescription {
    len: i32,
    num_fields: i16,
    row_fields: Vec<RowField>,
}

pub struct RowField {
    name: String,
    table_oid: i32,
    attr_num: i16,
    data_type_oid: i32,
    data_type_size: i16,
    type_modifier: i32,
    format_code: i16,
}

impl Message for RowDescription {
    fn len(&self) -> i32 {
        self.len
    }

    fn parse(buf: &mut bytes::BytesMut, len: i32) -> Option<RowDescription> {
        let mut buf = &buf[5..(len as usize + 1)];
        let num_fields = i16::from_be_bytes(buf[0..2].try_into().unwrap());

        let mut row_fields = Vec::new();

        for _ in 0..num_fields {
            let (mut len, name) = parse_string(&buf);
            len += 1;

            row_fields.push(RowField {
                name: name,
                table_oid: i32::from_be_bytes(buf[len..4].try_into().unwrap()),
                attr_num: i16::from_be_bytes(buf[len + 4..len + 6].try_into().unwrap()),
                data_type_oid: i32::from_be_bytes(buf[len + 6..len + 10].try_into().unwrap()),
                data_type_size: i16::from_be_bytes(buf[len + 10..len + 12].try_into().unwrap()),
                type_modifier: i32::from_be_bytes(buf[len + 12..len + 16].try_into().unwrap()),
                format_code: i16::from_be_bytes(buf[len + 16..len + 18].try_into().unwrap()),
            });

            buf = &buf[len + 18..];
        }

        Some(RowDescription {
            len: len,
            num_fields: num_fields,
            row_fields: row_fields,
        })
    }

    fn to_vec(&self) -> Vec<u8> {
        Vec::new()
    }

    fn debug(&self) -> String {
        format!("RowDescription (num_fields = {})", self.num_fields)
    }
}
