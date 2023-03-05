/// Implementation of the PostgreSQL server (database) protocol.
/// Here we are pretending to the a Postgres client.
use bytes::{Bytes, BytesMut};

use tokio::sync::mpsc::{channel, Receiver, Sender};

use crate::config::{Address, User};
use crate::pool::ClientServerMap;
use crate::server::Server;
use crate::stats::get_reporter;

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
    pub fn begin(mut self, server_id: i32) {
        tokio::spawn(async move {
            let mut server = Server::startup(
                server_id,
                &self.address.clone(),
                &self.user.clone(),
                self.database.as_str(),
                ClientServerMap::default(),
                get_reporter(),
            )
            .await
            .unwrap();

            loop {
                tokio::select! {
                    _ = self.exit_rx.recv() => {
                        break;
                    }
                    op = self.bytes_rx.recv() => {
                        match op {
                            Some(MirrorOperation::Send(bytes)) => {
                                server.send(&BytesMut::from(&bytes[..])).await.unwrap();
                            }
                            Some(MirrorOperation::Receive) => {
                                server.recv().await.unwrap();
                            }
                            None => {
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
            let mirror_unit = MirrorUnit {
                user: user.clone(),
                database: database.to_owned(),
                address: mirror.clone(),
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
