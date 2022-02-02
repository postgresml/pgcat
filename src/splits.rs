
use tokio::net::{TcpStream};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::task;
use tokio::sync::mpsc::{channel, Sender, Receiver};
use bytes::{Bytes, BytesMut, Buf, BufMut};

struct Reader {
    stream: OwnedReadHalf,
    tx: Sender<Bytes>,
    buffer: BytesMut, 
}

impl Reader {
    pub fn new(stream: OwnedReadHalf, tx: Sender<Bytes>) -> Reader {
        Reader {
            stream: stream,
            tx: tx,
            buffer: BytesMut::with_capacity(1024),
        }
    }

    async fn handle(&mut self) {
        loop {
            match self.stream.read_buf(&mut self.buffer).await {
                Ok(n) => {
                    if n < 5 {
                        continue;
                    }

                    let len = i32::from_be_bytes(self.buffer[1..5].try_into().unwrap()) as usize;
                    
                    if self.buffer.len() > len {
                        let bytes = self.buffer.copy_to_bytes(len + 1);
                        self.tx.send(bytes).await;
                    }
                },

                Err(_) => return,
            };
        }
    }
}

struct Writer {
    stream: OwnedWriteHalf,
    rx: Receiver<Bytes>, 
}

impl Writer {
    pub fn new(stream: OwnedWriteHalf, rx: Receiver<Bytes>) -> Writer {
        Writer {
            stream: stream,
            rx: rx,
        }
    }

    async fn handle(&mut self) {
        loop {
            match self.rx.recv().await {
                Some(bytes) => {
                   self.stream.write_all(&bytes).await;
                },

                None => return,
            };
        }
    }
}
