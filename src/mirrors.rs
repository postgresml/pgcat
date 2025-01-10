use std::sync::Arc;

/// A mirrored PostgreSQL client.
/// Packets arrive to us through a channel from the main client and we send them to the server.
use bb8::Pool;
use bytes::{Bytes, BytesMut};
use parking_lot::RwLock;

use crate::config::{get_config, Address, Role, User};
use crate::pool::{ClientServerMap, ServerPool};
use log::{error, info, trace, warn};
use tokio::sync::mpsc::{channel, Receiver, Sender};

pub struct MirroredClient {
    address: Address,
    user: User,
    database: String,
    bytes_rx: Receiver<Bytes>,
    disconnect_rx: Receiver<()>,
    auth_hash: Arc<RwLock<Option<String>>>,
}

impl MirroredClient {
    async fn create_pool(&self) -> Pool<ServerPool> {
        let config = get_config();
        let default = std::time::Duration::from_millis(10_000).as_millis() as u64;
        let (connection_timeout, idle_timeout, _cfg, prepared_statement_cache_size) =
            match config.pools.get(&self.address.pool_name) {
                Some(cfg) => (
                    cfg.connect_timeout.unwrap_or(default),
                    cfg.idle_timeout.unwrap_or(default),
                    cfg.clone(),
                    cfg.prepared_statements_cache_size,
                ),
                None => (default, default, crate::config::Pool::default(), 0),
            };

        let manager = ServerPool::new(
            self.address.clone(),
            self.user.clone(),
            self.database.as_str(),
            ClientServerMap::default(),
            self.auth_hash.clone(),
            None,
            true,
            false,
            prepared_statement_cache_size,
        );

        Pool::builder()
            .max_size(1)
            .connection_timeout(std::time::Duration::from_millis(connection_timeout))
            .idle_timeout(Some(std::time::Duration::from_millis(idle_timeout)))
            .test_on_check_out(false)
            .build(manager)
            .await
            .unwrap()
    }

    pub fn start(mut self) {
        tokio::spawn(async move {
            let pool = self.create_pool().await;
            let address = self.address.clone();
            loop {
                let mut server = match pool.get().await {
                    Ok(server) => server,
                    Err(err) => {
                        error!(
                            "Failed to get connection from pool, Discarding message {:?}, {:?}",
                            err,
                            address.clone()
                        );
                        continue;
                    }
                };

                tokio::select! {
                    // Exit channel events
                    _ = self.disconnect_rx.recv() => {
                        info!("Got mirror exit signal, exiting {:?}", address.clone());
                        break;
                    }

                    // Incoming data from server (we read to clear the socket buffer and discard the data)
                    recv_result = server.recv(None) => {
                        match recv_result {
                            Ok(message) => trace!("Received from mirror: {} {:?}", String::from_utf8_lossy(&message[..]), address.clone()),
                            Err(err) => {
                                server.mark_bad(
                                    format!("Failed to send to mirror, Discarding message {:?}, {:?}", err, address.clone()).as_str()
                                );
                            }
                        }
                    }

                    // Messages to send to the server
                    message = self.bytes_rx.recv() => {
                        match message {
                            Some(bytes) => {
                                match server.send(&BytesMut::from(&bytes[..])).await {
                                    Ok(_) => trace!("Sent to mirror: {} {:?}", String::from_utf8_lossy(&bytes[..]), address.clone()),
                                    Err(err) => {
                                        server.mark_bad(
                                            format!("Failed to receive from mirror {:?} {:?}", err, address.clone()).as_str()
                                        );
                                    }
                                }
                            }
                            None => {
                                info!("Mirror channel closed, exiting {:?}", address.clone());
                                break;
                            },
                        }
                    }
                }
            }
        });
    }
}
pub struct MirroringManager {
    pub byte_senders: Vec<Sender<Bytes>>,
    pub disconnect_senders: Vec<Sender<()>>,
}
impl MirroringManager {
    pub fn from_addresses(
        user: User,
        database: String,
        addresses: Vec<Address>,
        auth_hash: Arc<RwLock<Option<String>>>,
    ) -> MirroringManager {
        let mut byte_senders: Vec<Sender<Bytes>> = vec![];
        let mut exit_senders: Vec<Sender<()>> = vec![];

        addresses.iter().for_each(|mirror| {
            let (bytes_tx, bytes_rx) = channel::<Bytes>(10);
            let (exit_tx, exit_rx) = channel::<()>(1);
            let mut addr = mirror.clone();
            addr.role = Role::Mirror;
            let client = MirroredClient {
                user: user.clone(),
                database: database.to_owned(),
                address: addr,
                bytes_rx,
                disconnect_rx: exit_rx,
                auth_hash: auth_hash.clone(),
            };
            exit_senders.push(exit_tx);
            byte_senders.push(bytes_tx);
            client.start();
        });

        Self {
            byte_senders,
            disconnect_senders: exit_senders,
        }
    }

    pub fn send(&mut self, bytes: &BytesMut) {
        // We want to avoid performing an allocation if we won't be able to send the message
        // There is a possibility of a race here where we check the capacity and then the channel is
        // closed or the capacity is reduced to 0, but mirroring is best effort anyway
        if self
            .byte_senders
            .iter()
            .all(|sender| sender.capacity() == 0 || sender.is_closed())
        {
            return;
        }
        let immutable_bytes = bytes.clone().freeze();
        self.byte_senders.iter_mut().for_each(|sender| {
            match sender.try_send(immutable_bytes.clone()) {
                Ok(_) => {}
                Err(err) => {
                    warn!("Failed to send bytes to a mirror channel {}", err);
                }
            }
        });
    }

    pub fn disconnect(&mut self) {
        self.disconnect_senders
            .iter_mut()
            .for_each(|sender| match sender.try_send(()) {
                Ok(_) => {}
                Err(err) => {
                    warn!(
                        "Failed to send disconnect signal to a mirror channel {}",
                        err
                    );
                }
            });
    }
}
