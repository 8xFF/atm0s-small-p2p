//! PeerAlias allow control a peer-connection from othert task
//! This is done by using control_tx to send control to running task over chanel

use tokio::sync::{mpsc::Sender, oneshot};

use crate::{
    msg::{P2pServiceId, PeerMessage},
    stream::P2pQuicStream,
    ConnectionId, PeerId,
};

use super::PeerConnectionControl;

#[derive(Clone, Debug)]
pub struct PeerConnectionAlias {
    local_id: PeerId,
    to_id: PeerId,
    conn_id: ConnectionId,
    control_tx: Sender<PeerConnectionControl>,
}

impl PeerConnectionAlias {
    pub(super) fn new(local_id: PeerId, to_id: PeerId, conn_id: ConnectionId, control_tx: Sender<PeerConnectionControl>) -> Self {
        Self { local_id, to_id, conn_id, control_tx }
    }

    #[allow(unused)]
    pub(super) fn conn_id(&self) -> ConnectionId {
        self.conn_id
    }

    #[allow(unused)]
    pub(super) fn local_id(&self) -> PeerId {
        self.local_id
    }

    pub(super) fn to_id(&self) -> PeerId {
        self.to_id
    }

    pub(crate) fn try_send(&self, msg: PeerMessage) -> anyhow::Result<()> {
        Ok(self.control_tx.try_send(PeerConnectionControl::Send(msg))?)
    }

    pub(crate) async fn send(&self, msg: PeerMessage) -> anyhow::Result<()> {
        Ok(self.control_tx.send(PeerConnectionControl::Send(msg)).await?)
    }

    pub(crate) async fn open_stream(&self, service: P2pServiceId, source: PeerId, dest: PeerId, meta: Vec<u8>) -> anyhow::Result<P2pQuicStream> {
        let (tx, rx) = oneshot::channel();
        self.control_tx.send(PeerConnectionControl::OpenStream(service, source, dest, meta, tx)).await?;
        rx.await?
    }
}
