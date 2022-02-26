use tokio::net::{
    tcp::{OwnedReadHalf, OwnedWriteHalf},
    TcpStream,
};

use bytes::{Buf, BufMut, BytesMut};
use log::trace;

use std::collections::HashMap;

use crate::constants::{OID_NUMERIC, OID_TEXT};
use crate::errors::Error;
use crate::messages::write_all_half;
use crate::stats::get_stats;

/// Handle admin client
pub async fn handle_admin(stream: &mut OwnedWriteHalf, mut query: BytesMut) -> Result<(), Error> {
    let code = query.get_u8() as char;

    if code != 'Q' {
        return Err(Error::ProtocolSyncError);
    }

    let len = query.get_i32() as usize;
    let query = String::from_utf8_lossy(&query[..len - 5])
        .to_string()
        .to_ascii_uppercase();

    if query.starts_with("SHOW STATS") {
        trace!("SHOW STATS");
        show_stats(stream).await
    } else {
        Err(Error::ProtocolSyncError)
    }
}

/// SHOW STATS
pub async fn show_stats(stream: &mut OwnedWriteHalf) -> Result<(), Error> {
    let columns = [
        "database",
        "total_xact_count",
        "total_query_count",
        "total_received",
        "total_sent",
        "total_xact_time",
        "total_query_time",
        "total_wait_time",
        "avg_xact_count",
        "avg_query_count",
        "avg_recv",
        "avg_sent",
        "avg_xact_time",
        "avg_query_time",
        "avg_wait_time",
    ];

    let stats = get_stats().unwrap_or(HashMap::new());
    let mut res = BytesMut::new();
    let mut row_desc = BytesMut::new();
    let mut data_row = BytesMut::new();

    // Number of columns: 1
    row_desc.put_i16(columns.len() as i16);
    data_row.put_i16(columns.len() as i16);

    for (i, column) in columns.iter().enumerate() {
        // RowDescription

        // Column name
        row_desc.put_slice(&format!("{}\0", column).as_bytes());

        // Doesn't belong to any table
        row_desc.put_i32(0);

        // Doesn't belong to any table
        row_desc.put_i16(0);

        // Data type
        row_desc.put_i32(if i == 0 { OID_TEXT } else { OID_NUMERIC });

        // Numeric/text size = variable (-1)
        row_desc.put_i16(-1);

        // Type modifier: none that I know
        row_desc.put_i32(0); //TODO maybe -1?

        // Format being used: text (0), binary (1)
        row_desc.put_i16(0);

        // DataRow
        let value = if i == 0 {
            String::from("all shards")
        } else {
            stats.get(&column.to_string()).unwrap_or(&0).to_string()
        };

        data_row.put_i32(value.len() as i32);
        data_row.put_slice(value.as_bytes());
    }

    let command_complete = BytesMut::from(&"SHOW\0"[..]);

    res.put_u8(b'T');
    res.put_i32(row_desc.len() as i32 + 4);
    res.put(row_desc);

    res.put_u8(b'D');
    res.put_i32(data_row.len() as i32 + 4);
    res.put(data_row);

    res.put_u8(b'C');
    res.put_i32(command_complete.len() as i32 + 4);
    res.put(command_complete);

    res.put_u8(b'Z');
    res.put_i32(5);
    res.put_u8(b'I');

    write_all_half(stream, res).await
}
