use std::cmp::{max, min};
use std::time::Duration;

/// Implementation of the PostgreSQL server (database) protocol.
/// Here we are pretending to the a Postgres client.
use bytes::{Bytes, BytesMut};

use tokio::sync::mpsc::{channel, Receiver, Sender};
use tokio::time::sleep;

use crate::config::{Address, Role, User};
use crate::pool::ClientServerMap;
use crate::server::Server;
use crate::stats::get_reporter;
use log::{error, info, trace};

pub enum MirrorOperation {
    Send(Bytes),
    Receive,
}
pub struct MirrorUnit {
    pub address: Address,
    pub user: User,
    pub database: String,
    pub bytes_rx: Receiver<MirrorOperation>,
    pub exit_rx: Receiver<()>,
}

impl MirrorUnit {
    async fn connect(&self, server_id: i32) -> Option<Server> {
        let stats = get_reporter();
        stats.server_register(
            server_id,
            self.address.id,
            self.address.name(),
            self.database.clone(),
            self.address.username.clone(),
        );
        stats.server_login(server_id);

        match Server::startup(
            server_id,
            &self.address.clone(),
            &self.user.clone(),
            self.database.as_str(),
            ClientServerMap::default(),
            get_reporter(),
        )
        .await
        {
            Ok(conn) => {
                stats.server_idle(server_id);
                Some(conn)
            }
            Err(_) => {
                stats.server_disconnecting(server_id);
                None
            }
        }
    }

    pub fn begin(mut self, server_id: i32) {
        tokio::spawn(async move {
            let mut delay = Duration::from_secs(0);
            let min_backoff = Duration::from_millis(100);
            let max_backoff = Duration::from_secs(5);

            loop {
                tokio::select! {
                    _ = self.exit_rx.recv() => {
                        info!("Got mirror exit signal, exiting {:?}", self.address.clone());
                        break;
                    }

                    op = self.bytes_rx.recv() => {
                        let mut server = loop {
                            let server = self.connect(server_id).await;
                            if server.is_some() {
                                let tmp_server: Server = server.unwrap();
                                if !tmp_server.is_bad() {
                                    break tmp_server;
                                }
                            }
                            delay = max(min_backoff, delay);
                            delay = min(max_backoff, delay * 2);
                            sleep(delay).await;
                        };
                        // Server is not None and is good at this point
                        match op {
                            Some(MirrorOperation::Send(bytes)) => {
                                match server.send(&BytesMut::from(&bytes[..])).await {
                                    Ok(_) => trace!("Sent to mirror: {}", String::from_utf8_lossy(&bytes[..])),
                                    Err(err) => error!("Error sending to mirror: {:?}", err)
                                }
                            }
                            Some(MirrorOperation::Receive) => {
                                match server.recv().await {
                                    Ok(_) => (),
                                    Err(err) => error!("Error receiving from mirror: {:?}", err)
                                }
                            }
                            None => {
                                info!("Mirror channel closed, exiting {:?}", self.address.clone());
                                break;
                            }
                        }
                    }
                }
            }
        });
    }
}
pub struct MirroringManager {
    pub byte_senders: Vec<Sender<MirrorOperation>>,
    pub exit_senders: Vec<Sender<()>>,
}
impl MirroringManager {
    pub fn from_addresses(
        user: User,
        database: String,
        addresses: Vec<Address>,
    ) -> MirroringManager {
        let mut byte_senders: Vec<Sender<MirrorOperation>> = vec![];
        let mut exit_senders: Vec<Sender<()>> = vec![];

        addresses.iter().for_each(|mirror| {
            let (bytes_tx, bytes_rx) = channel::<MirrorOperation>(500);
            let (exit_tx, exit_rx) = channel::<()>(1);
            let mut addr = mirror.clone();
            addr.role = Role::Mirror;
            let mirror_unit = MirrorUnit {
                user: user.clone(),
                database: database.to_owned(),
                address: addr,
                bytes_rx,
                exit_rx,
            };
            exit_senders.push(exit_tx.clone());
            byte_senders.push(bytes_tx.clone());
            mirror_unit.begin(rand::random::<i32>());
        });

        Self {
            byte_senders: byte_senders,
            exit_senders: exit_senders,
        }
    }

    pub fn send(self: &mut Self, bytes: &BytesMut) {
        let cpy = bytes.clone().freeze();
        self.byte_senders.iter_mut().for_each(|sender| {
            match sender.try_send(MirrorOperation::Send(cpy.clone())) {
                Ok(_) => {}
                Err(_) => {}
            }
        });
    }

    pub fn receive(self: &mut Self) {
        self.byte_senders.iter_mut().for_each(|sender| {
            match sender.try_send(MirrorOperation::Receive) {
                Ok(_) => {}
                Err(_) => {}
            }
        });
    }

    pub fn exit(self: &mut Self) {
        self.exit_senders
            .iter_mut()
            .for_each(|sender| match sender.try_send(()) {
                Ok(_) => {}
                Err(_) => {}
            });
    }
}
