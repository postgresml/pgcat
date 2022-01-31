use tokio::io::AsyncReadExt;
use tokio::io::AsyncWriteExt;

use crate::messages::MessageName;

pub struct BufferResult {
    complete_messages: usize,
    bytes_checked: usize,
    bytes_read: usize,
}

impl BufferResult {
    pub fn complete(&self) -> bool {
        self.bytes_checked == self.bytes_read
    }

    pub fn bytes_checked(&self) -> usize {
        self.bytes_checked
    }

    pub fn bytes_read(&self) -> usize {
        self.bytes_read
    }
}

/// Help with socket communication
pub async fn buffer(
    stream: &mut tokio::net::TcpStream,
    buf: &mut [u8],
    bytes_start: usize,
    check_start: usize,
) -> Result<BufferResult, Box<dyn std::error::Error>> {
    let mut n = bytes_start;
    let mut checked = check_start;

    assert!(check_start >= bytes_start);

    n += stream.read(&mut buf[n..]).await?;

    let (c, complete_messages) = check(&buf[checked..n]);
    checked += c;

    Ok(BufferResult {
        complete_messages: complete_messages,
        bytes_checked: checked,
        bytes_read: n,
    })
}

pub async fn buffer_until_complete(
    stream: &mut tokio::net::TcpStream,
    buf: &mut [u8],
) -> Result<BufferResult, Box<dyn std::error::Error>> {
    let (mut bytes_checked, mut bytes_read, mut complete_messages) = (0, 0, 0);

    let mut buffer_result = buffer(stream, buf, bytes_checked, bytes_read).await?;

    loop {
        bytes_checked += buffer_result.bytes_checked;
        bytes_read += buffer_result.bytes_read;
        complete_messages += buffer_result.complete_messages;

        if bytes_checked == bytes_read {
            break;
        } else {
            buffer_result =
                buffer(stream, &mut buf[bytes_read..], bytes_read, bytes_checked).await?
        }
    }

    Ok(BufferResult {
        complete_messages: complete_messages,
        bytes_checked: bytes_checked,
        bytes_read: bytes_read,
    })
}

/// Check that the buffer contains complete messages.
/// Returns a tuple of how many bytes were checked and how many complete messages are present.
pub fn check(buf: &[u8]) -> (usize, usize) {
    let mut checked = 0;
    let mut complete_messages = 0;

    while checked < buf.len() {
        let c = buf[checked];

        // Hit a null-byte, there is nothing more in the buffer
        if c != 0 {
            checked += 1;
        }

        else {
            break;
        }

        let len = i32::from_be_bytes(buf[checked..checked + 4].try_into().unwrap());

        // Enough data in the buffer for the message to be complete.
        if len as usize <= buf.len() {
            checked += len as usize;
            complete_messages += 1;
            println!("DEBUG: Have complete message {}", c as char);
        }
        // Not enough data left in the buffer to have a complete message.
        else {
            break;
        }
    }

    return (checked, complete_messages);
}

pub fn parse_parameters(buf: &[u8]) -> std::collections::HashMap<String, String> {
    let mut sbuf = String::from("");
    let mut tuple = Vec::new();
    let mut args = std::collections::HashMap::new();

    for c in buf {
        // Strings are null-terminated
        if *c == 0 {
            // We have key
            if tuple.len() < 2 {
                tuple.push(sbuf.clone());
            }

            // We have key and value
            if tuple.len() == 2 {
                args.insert(tuple[0].clone(), tuple[1].clone());
                tuple.clear();
                tuple.push(sbuf.clone());
            }

            sbuf.clear();
        }
        // Normal character
        else {
            sbuf.push(*c as char);
        }
    }

    args
}

pub fn parse_string(buf: &[u8]) -> (usize, String) {
    let mut len = 0;
    while buf[len] != 0 {
        len += 1;
    }

    (len, String::from_utf8_lossy(&buf[0..len]).to_string())
}

pub async fn write_all(stream: &mut tokio::net::TcpStream, buf: &[u8]) -> Result<(), &'static str> {
    match stream.write_all(&buf).await {
        Ok(_n) => Ok(()),
        Err(_err) => Err("ERROR: socket died"),
    }
}

pub async fn read_all(stream: &mut tokio::net::TcpStream, buf: &mut [u8]) -> Result<(), &'static str> {
    match buffer_until_complete(stream, buf).await {
        Ok(_) => Ok(()),
        Err(_err) => Err("ERROR: socket died"),
    }
}