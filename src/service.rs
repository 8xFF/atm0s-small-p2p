use tokio::sync::mpsc::{channel, Receiver, Sender};

use crate::{ctx::SharedCtx, msg::P2pServiceId, router::SharedRouterTable, stream::P2pQuicStream, PeerId};

pub mod alias_service;
pub mod pubsub_service;
pub mod visualization_service;

const SERVICE_CHANNEL_SIZE: usize = 10;

#[derive(Debug, PartialEq, Eq)]
pub enum P2pServiceEvent {
    Unicast(PeerId, Vec<u8>),
    Broadcast(PeerId, Vec<u8>),
    Stream(PeerId, Vec<u8>, P2pQuicStream),
}

#[derive(Debug, Clone)]
pub struct P2pServiceRequester {
    service: P2pServiceId,
    ctx: SharedCtx,
}

pub struct P2pService {
    service: P2pServiceId,
    ctx: SharedCtx,
    rx: Receiver<P2pServiceEvent>,
}

impl P2pService {
    pub(super) fn build(service: P2pServiceId, ctx: SharedCtx) -> (Self, Sender<P2pServiceEvent>) {
        let (tx, rx) = channel(SERVICE_CHANNEL_SIZE);
        (Self { service, ctx, rx }, tx)
    }

    pub fn requester(&self) -> P2pServiceRequester {
        P2pServiceRequester {
            service: self.service,
            ctx: self.ctx.clone(),
        }
    }

    pub async fn send_unicast(&self, dest: PeerId, data: Vec<u8>) -> anyhow::Result<()> {
        self.ctx.send_unicast(self.service, dest, data).await
    }

    pub async fn send_broadcast(&self, data: Vec<u8>) {
        self.ctx.send_broadcast(self.service, data).await
    }

    pub async fn try_send_unicast(&self, dest: PeerId, data: Vec<u8>) -> anyhow::Result<()> {
        self.ctx.try_send_unicast(self.service, dest, data)
    }

    pub async fn try_send_broadcast(&self, data: Vec<u8>) {
        self.ctx.try_send_broadcast(self.service, data)
    }

    pub async fn open_stream(&self, dest: PeerId, meta: Vec<u8>) -> anyhow::Result<P2pQuicStream> {
        self.ctx.open_stream(self.service, dest, meta).await
    }

    pub fn router(&self) -> &SharedRouterTable {
        self.ctx.router()
    }

    pub async fn recv(&mut self) -> Option<P2pServiceEvent> {
        self.rx.recv().await
    }
}

impl P2pServiceRequester {
    pub async fn send_unicast(&self, dest: PeerId, data: Vec<u8>) -> anyhow::Result<()> {
        self.ctx.send_unicast(self.service, dest, data).await
    }

    pub async fn send_broadcast(&self, data: Vec<u8>) {
        self.ctx.send_broadcast(self.service, data).await
    }

    pub async fn try_send_unicast(&self, dest: PeerId, data: Vec<u8>) -> anyhow::Result<()> {
        self.ctx.try_send_unicast(self.service, dest, data)
    }

    pub async fn try_send_broadcast(&self, data: Vec<u8>) {
        self.ctx.try_send_broadcast(self.service, data)
    }

    pub async fn open_stream(&self, dest: PeerId, meta: Vec<u8>) -> anyhow::Result<P2pQuicStream> {
        self.ctx.open_stream(self.service, dest, meta).await
    }

    pub fn router(&self) -> &SharedRouterTable {
        self.ctx.router()
    }
}
