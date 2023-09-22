/// Helper functions to send one-off protocol messages
/// and handle TcpStream (TCP socket).
use bytes::{Buf, BufMut, Bytes, BytesMut};

use crate::errors::Error;

pub type Oid = u32;

#[derive(PartialEq, Eq, Debug, Default)]
pub struct FieldDescription {
    // the field name
    name: String,
    // the object ID of table, default to 0 if not a table
    table_id: i32,
    // the attribute number of the column, default to 0 if not a column from table
    column_id: i16,
    // the object ID of the data type
    type_id: Oid,
    // the size of data type, negative values denote variable-width types
    type_size: i16,
    // the type modifier
    type_modifier: i32,
    // the format code being used for the filed, will be 0 or 1 for now
    format_code: i16,
}

/// Get null-terminated string, returns None when empty cstring read.
///
/// Note that this implementation will also advance cursor by 1 after reading
/// empty cstring. This behaviour works for how postgres wire protocol handling
/// key-value pairs, which is ended by a single `\0`
pub(crate) fn get_cstring(buf: &mut BytesMut) -> Option<String> {
    let mut i = 0;

    // with bound check to prevent invalid format
    while i < buf.remaining() && buf[i] != b'\0' {
        i += 1;
    }

    // i+1: include the '\0'
    // move cursor to the end of cstring
    let string_buf = buf.split_to(i + 1);

    if i == 0 {
        None
    } else {
        Some(String::from_utf8_lossy(&string_buf[..i]).into_owned())
    }
}

/// Put null-termianted string
///
/// You can put empty string by giving `""` as input.
pub(crate) fn put_cstring(buf: &mut BytesMut, input: &str) {
    buf.put_slice(input.as_bytes());
    buf.put_u8(b'\0');
}

/// Try to read message length from buf, without actually move the cursor
pub(crate) fn get_length(buf: &BytesMut, offset: usize) -> Option<usize> {
    if buf.remaining() >= 4 + offset {
        Some((&buf[offset..4 + offset]).get_i32() as usize)
    } else {
        None
    }
}

/// Check if message_length matches and move the cursor to right position then
/// call the `decode_fn` for the body
pub(crate) fn decode_packet<T, F>(
    buf: &mut BytesMut,
    offset: usize,
    decode_fn: F,
) -> Result<Option<T>, Error>
where
    F: Fn(&mut BytesMut, usize) -> Result<T, Error>,
{
    if let Some(msg_len) = get_length(buf, offset) {
        if buf.remaining() >= msg_len + offset {
            buf.advance(offset + 4);
            return decode_fn(buf, msg_len).map(|r| Some(r));
        }
    }

    Ok(None)
}

/// Define how message encode and decoded.
pub trait Message: Sized {
    /// Return the type code of the message. In order to maintain backward
    /// compatibility, `Startup` has no message type.
    #[inline]
    fn message_type() -> Option<u8> {
        None
    }

    /// Return the length of the message, including the length integer itself.
    fn message_length(&self) -> usize;

    /// Encode body part of the message.
    fn encode_body(&self, buf: &mut BytesMut) -> Result<(), Error>;

    /// Decode body part of the message.
    fn decode_body(buf: &mut BytesMut, full_len: usize) -> Result<Self, Error>;

    /// Default implementation for encoding message.
    ///
    /// Message type and length are encoded in this implementation and it calls
    /// `encode_body` for remaining parts.
    fn encode(&self, buf: &mut BytesMut) -> Result<(), Error> {
        if let Some(mt) = Self::message_type() {
            buf.put_u8(mt);
        }

        buf.put_i32(self.message_length() as i32);
        self.encode_body(buf)
    }

    /// Default implementation for decoding message.
    ///
    /// Message type and length are decoded in this implementation and it calls
    /// `decode_body` for remaining parts. Return `None` if the packet is not
    /// complete for parsing.
    fn decode(buf: &mut BytesMut) -> Result<Option<Self>, Error> {
        let offset = Self::message_type().is_some().into();

        decode_packet(buf, offset, |buf, full_len| {
            Self::decode_body(buf, full_len)
        })
    }
}

pub const MESSAGE_TYPE_BYTE_ROW_DESCRITION: u8 = b'T';

#[derive(PartialEq, Eq, Debug, Default)]
pub struct RowDescription {
    fields: Vec<FieldDescription>,
}

impl RowDescription {
    pub fn fields(&self) -> &[FieldDescription] {
        &self.fields
    }
}

impl Message for RowDescription {
    fn message_type() -> Option<u8> {
        Some(MESSAGE_TYPE_BYTE_ROW_DESCRITION)
    }

    fn message_length(&self) -> usize {
        4 + 2
            + self
                .fields
                .iter()
                .map(|f| f.name.as_bytes().len() + 1 + 4 + 2 + 4 + 2 + 4 + 2)
                .sum::<usize>()
    }

    fn encode_body(&self, buf: &mut BytesMut) -> Result<(), Error> {
        buf.put_i16(self.fields.len() as i16);

        for field in &self.fields {
            put_cstring(buf, &field.name);
            buf.put_i32(field.table_id);
            buf.put_i16(field.column_id);
            buf.put_u32(field.type_id);
            buf.put_i16(field.type_size);
            buf.put_i32(field.type_modifier);
            buf.put_i16(field.format_code);
        }

        Ok(())
    }

    fn decode_body(buf: &mut BytesMut, _: usize) -> Result<Self, Error> {
        let fields_len = buf.get_i16();
        let mut fields = Vec::with_capacity(fields_len as usize);

        for _ in 0..fields_len {
            let field = FieldDescription {
                name: get_cstring(buf).unwrap_or_else(|| "".to_owned()),
                table_id: buf.get_i32(),
                column_id: buf.get_i16(),
                type_id: buf.get_u32(),
                type_size: buf.get_i16(),
                type_modifier: buf.get_i32(),
                format_code: buf.get_i16(),
            };

            fields.push(field);
        }

        Ok(RowDescription { fields })
    }
}

/// Data structure for postgresql wire protocol `DataRow` message.
///
/// Data can be represented as text or binary format as specified by format
/// codes from previous `RowDescription` message.
#[derive(PartialEq, Eq, Debug, Default, Clone)]
pub struct DataRow {
    fields: Vec<Option<Bytes>>,
}

impl DataRow {
    pub fn fields(&self) -> &[Option<Bytes>] {
        &self.fields
    }
}

pub const MESSAGE_TYPE_BYTE_DATA_ROW: u8 = b'D';

impl Message for DataRow {
    #[inline]
    fn message_type() -> Option<u8> {
        Some(MESSAGE_TYPE_BYTE_DATA_ROW)
    }

    fn message_length(&self) -> usize {
        4 + 2
            + self
                .fields
                .iter()
                .map(|b| b.as_ref().map(|b| b.len() + 4).unwrap_or(4))
                .sum::<usize>()
    }

    fn encode_body(&self, buf: &mut BytesMut) -> Result<(), Error> {
        buf.put_i16(self.fields.len() as i16);
        for field in &self.fields {
            if let Some(bytes) = field {
                buf.put_i32(bytes.len() as i32);
                buf.put_slice(bytes.as_ref());
            } else {
                buf.put_i32(-1);
            }
        }

        Ok(())
    }

    fn decode_body(buf: &mut BytesMut, _msg_len: usize) -> Result<Self, Error> {
        let field_count = buf.get_i16() as usize;

        let mut fields = Vec::with_capacity(field_count);
        for _ in 0..field_count {
            let field_len = buf.get_i32();
            if field_len >= 0 {
                fields.push(Some(buf.split_to(field_len as usize).freeze()));
            } else {
                fields.push(None);
            }
        }

        Ok(DataRow { fields })
    }
}

#[derive(PartialEq, Eq, Debug, Default)]
pub struct QueryResponse {
    row_desc: RowDescription,
    data_rows: Vec<DataRow>,
}

impl QueryResponse {
    pub fn new(row_desc: RowDescription, data_rows: Vec<DataRow>) -> Self {
        Self {
            row_desc,
            data_rows,
        }
    }

    pub fn row_desc(&self) -> &RowDescription {
        &self.row_desc
    }

    pub fn data_rows(&self) -> &[DataRow] {
        &self.data_rows
    }
}

// Postgres error and notice message fields
// This part of protocol is defined in
// https://www.postgresql.org/docs/8.2/protocol-error-fields.html
#[derive(Debug, Default)]
pub struct ErrorInfo {
    // severity can be one of `ERROR`, `FATAL`, or `PANIC` (in an error
    // message), or `WARNING`, `NOTICE`, `DEBUG`, `INFO`, or `LOG` (in a notice
    // message), or a localized translation of one of these.
    severity: String,
    // error code defined in
    // https://www.postgresql.org/docs/current/errcodes-appendix.html
    code: String,
    // readable message
    message: String,
    // optional secondary message
    detail: Option<String>,
    // optional suggestion for fixing the issue
    hint: Option<String>,
    // Position: the field value is a decimal ASCII integer, indicating an error
    // cursor position as an index into the original query string.
    position: Option<String>,
    // Internal position: this is defined the same as the P field, but it is
    // used when the cursor position refers to an internally generated command
    // rather than the one submitted by the client
    internal_position: Option<String>,
    // Internal query: the text of a failed internally-generated command.
    internal_query: Option<String>,
    // Where: an indication of the context in which the error occurred.
    where_context: Option<String>,
    // File: the file name of the source-code location where the error was
    // reported.
    file_name: Option<String>,
    // Line: the line number of the source-code location where the error was
    // reported.
    line: Option<usize>,
    // Routine: the name of the source-code routine reporting the error.
    routine: Option<String>,
}

impl ErrorInfo {
    pub fn new(
        severity: String,
        code: String,
        message: String,
        detail: Option<String>,
        hint: Option<String>,
        position: Option<String>,
        internal_position: Option<String>,
        internal_query: Option<String>,
        where_context: Option<String>,
        file_name: Option<String>,
        line: Option<usize>,
        routine: Option<String>,
    ) -> Self {
        Self {
            severity,
            code,
            message,
            detail,
            hint,
            position,
            internal_position,
            internal_query,
            where_context,
            file_name,
            line,
            routine,
        }
    }
    pub fn new_brief(severity: String, code: String, message: String) -> Self {
        Self::new(
            severity, code, message, None, None, None, None, None, None, None, None, None,
        )
    }
}

impl ErrorInfo {
    fn into_fields(self) -> Vec<(u8, String)> {
        let mut fields = Vec::with_capacity(11);

        fields.push((b'S', self.severity));
        fields.push((b'C', self.code));
        fields.push((b'M', self.message));
        if let Some(value) = self.detail {
            fields.push((b'D', value));
        }
        if let Some(value) = self.hint {
            fields.push((b'H', value));
        }
        if let Some(value) = self.position {
            fields.push((b'P', value));
        }
        if let Some(value) = self.internal_position {
            fields.push((b'p', value));
        }
        if let Some(value) = self.internal_query {
            fields.push((b'q', value));
        }
        if let Some(value) = self.where_context {
            fields.push((b'W', value));
        }
        if let Some(value) = self.file_name {
            fields.push((b'F', value));
        }
        if let Some(value) = self.line {
            fields.push((b'L', value.to_string()));
        }
        if let Some(value) = self.routine {
            fields.push((b'R', value));
        }

        fields
    }
}

impl From<ErrorInfo> for ErrorResponse {
    fn from(ei: ErrorInfo) -> ErrorResponse {
        ErrorResponse {
            fields: ei.into_fields(),
        }
    }
}

/// postgres error response, sent from backend to frontend
#[derive(PartialEq, Eq, Debug, Default)]
pub struct ErrorResponse {
    fields: Vec<(u8, String)>,
}

pub const MESSAGE_TYPE_BYTE_ERROR_RESPONSE: u8 = b'E';

impl Message for ErrorResponse {
    #[inline]
    fn message_type() -> Option<u8> {
        Some(MESSAGE_TYPE_BYTE_ERROR_RESPONSE)
    }

    fn message_length(&self) -> usize {
        4 + self
            .fields
            .iter()
            .map(|f| 1 + f.1.as_bytes().len() + 1)
            .sum::<usize>()
            + 1
    }

    fn encode_body(&self, buf: &mut BytesMut) -> Result<(), Error> {
        for (code, value) in &self.fields {
            buf.put_u8(*code);
            put_cstring(buf, value);
        }

        buf.put_u8(b'\0');

        Ok(())
    }

    fn decode_body(buf: &mut BytesMut, _: usize) -> Result<Self, Error> {
        let mut fields = Vec::new();
        loop {
            let code = buf.get_u8();

            if code == b'\0' {
                return Ok(ErrorResponse { fields });
            } else {
                let value = get_cstring(buf).unwrap_or_else(|| "".to_owned());
                fields.push((code, value));
            }
        }
    }
}
