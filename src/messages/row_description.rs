use bytes::Buf;

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
        let _c = buf.get_u8();
        let _len = buf.get_u8();
        let num_fields = buf.get_i16();

        let mut row_fields = Vec::new();

        for _ in 0..num_fields {
            let name = parse_string(buf);

            row_fields.push(RowField {
                name: name,
                table_oid: buf.get_i32(),
                attr_num: buf.get_i16(),
                data_type_oid: buf.get_i32(),
                data_type_size: buf.get_i16(),
                type_modifier: buf.get_i32(),
                format_code: buf.get_i16(),
            });
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
