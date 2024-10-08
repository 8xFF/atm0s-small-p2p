use std::{collections::HashMap, sync::Arc};

use anyhow::anyhow;
use lru::LruCache;
use parking_lot::RwLock;
use tokio::sync::mpsc::Sender;

use crate::{
    msg::{BroadcastMsgId, P2pServiceId, PeerMessage},
    peer::PeerConnectionAlias,
    router::{RouteAction, SharedRouterTable},
    service::P2pServiceEvent,
    stream::P2pQuicStream,
    utils::ErrorExt,
    ConnectionId, PeerId,
};

#[derive(Debug)]
struct SharedCtxInternal {
    conns: HashMap<ConnectionId, PeerConnectionAlias>,
    received_broadcast_msg: LruCache<BroadcastMsgId, ()>,
    services: [Option<Sender<P2pServiceEvent>>; 256],
}

impl SharedCtxInternal {
    fn set_service(&mut self, service_id: P2pServiceId, tx: Sender<P2pServiceEvent>) {
        assert!(self.services[*service_id as usize].is_none(), "Service ID already used");
        self.services[*service_id as usize] = Some(tx);
    }

    fn get_service(&self, service_id: &P2pServiceId) -> Option<Sender<P2pServiceEvent>> {
        self.services[**service_id as usize].clone()
    }

    fn register_conn(&mut self, conn: ConnectionId, alias: PeerConnectionAlias) {
        self.conns.insert(conn, alias);
    }

    fn unregister_conn(&mut self, conn: &ConnectionId) {
        self.conns.remove(conn);
    }

    fn conn(&self, conn: &ConnectionId) -> Option<PeerConnectionAlias> {
        self.conns.get(conn).cloned()
    }

    fn conns(&self) -> Vec<PeerConnectionAlias> {
        self.conns.values().cloned().collect::<Vec<_>>()
    }

    /// check if we already got the message
    /// if is not, it return true and save to cache list
    /// if already it return false and do nothing
    fn check_broadcast_msg(&mut self, id: BroadcastMsgId) -> bool {
        if !self.received_broadcast_msg.contains(&id) {
            self.received_broadcast_msg.get_or_insert(id, || ());
            true
        } else {
            false
        }
    }
}

#[derive(Debug, Clone)]
pub struct SharedCtx {
    ctx: Arc<RwLock<SharedCtxInternal>>,
    router: SharedRouterTable,
}

impl SharedCtx {
    pub fn new(router: SharedRouterTable) -> Self {
        Self {
            ctx: Arc::new(RwLock::new(SharedCtxInternal {
                conns: Default::default(),
                received_broadcast_msg: LruCache::new(8192.try_into().expect("should ok")),
                services: std::array::from_fn(|_| None),
            })),
            router,
        }
    }

    pub(super) fn set_service(&mut self, service_id: P2pServiceId, tx: Sender<P2pServiceEvent>) {
        self.ctx.write().set_service(service_id, tx);
    }

    pub fn register_conn(&self, conn: ConnectionId, alias: PeerConnectionAlias) {
        self.ctx.write().register_conn(conn, alias);
    }

    pub fn unregister_conn(&self, conn: &ConnectionId) {
        self.ctx.write().unregister_conn(conn);
    }

    pub fn conn(&self, conn: &ConnectionId) -> Option<PeerConnectionAlias> {
        self.ctx.read().conn(conn)
    }

    pub fn conns(&self) -> Vec<PeerConnectionAlias> {
        self.ctx.read().conns()
    }

    pub fn router(&self) -> &SharedRouterTable {
        &self.router
    }

    pub fn get_service(&self, service_id: &P2pServiceId) -> Option<Sender<P2pServiceEvent>> {
        self.ctx.read().get_service(service_id)
    }

    /// check if we already got the message
    /// if is not, it return true and save to cache list
    /// if already it return false and do nothing
    pub fn check_broadcast_msg(&self, id: BroadcastMsgId) -> bool {
        self.ctx.write().check_broadcast_msg(id)
    }

    pub fn try_send_unicast(&self, service_id: P2pServiceId, dest: PeerId, data: Vec<u8>) -> anyhow::Result<()> {
        let next = self.router.action(&dest).ok_or(anyhow!("route not found"))?;
        match next {
            RouteAction::Local => {
                panic!("unsupported send to local node")
            }
            RouteAction::Next(next) => {
                let source = self.router.local_id();
                self.conn(&next).ok_or(anyhow!("peer not found"))?.try_send(PeerMessage::Unicast(source, dest, service_id, data))?;
                Ok(())
            }
        }
    }

    pub async fn send_unicast(&self, service_id: P2pServiceId, dest: PeerId, data: Vec<u8>) -> anyhow::Result<()> {
        let next = self.router.action(&dest).ok_or(anyhow!("route not found"))?;
        match next {
            RouteAction::Local => {
                panic!("unsupported send to local node")
            }
            RouteAction::Next(next) => {
                let source = self.router.local_id();
                self.conn(&next).ok_or(anyhow!("peer not found"))?.send(PeerMessage::Unicast(source, dest, service_id, data)).await?;
                Ok(())
            }
        }
    }

    pub fn try_send_broadcast(&self, service_id: P2pServiceId, data: Vec<u8>) {
        let msg_id = BroadcastMsgId::rand();
        self.check_broadcast_msg(msg_id);
        let source = self.router.local_id();
        let conns = self.conns();
        log::debug!("[ShareCtx] broadcast to {conns:?} connections");
        for conn_alias in conns {
            conn_alias
                .try_send(PeerMessage::Broadcast(source, service_id, msg_id, data.clone()))
                .print_on_err("[ShareCtx] broadcast data over peer alias");
        }
    }

    pub async fn send_broadcast(&self, service_id: P2pServiceId, data: Vec<u8>) {
        let msg_id = BroadcastMsgId::rand();
        self.check_broadcast_msg(msg_id);
        let source = self.router.local_id();
        let conns = self.conns();
        log::debug!("[ShareCtx] broadcast to {conns:?} connections");
        for conn_alias in conns {
            conn_alias
                .send(PeerMessage::Broadcast(source, service_id, msg_id, data.clone()))
                .await
                .print_on_err("[ShareCtx] broadcast data over peer alias");
        }
    }

    pub async fn open_stream(&self, service: P2pServiceId, dest: PeerId, meta: Vec<u8>) -> anyhow::Result<P2pQuicStream> {
        let next = self.router.action(&dest).ok_or(anyhow!("route not found"))?;
        match next {
            RouteAction::Local => {
                panic!("unsupported open_stream to local node")
            }
            RouteAction::Next(next) => {
                let source = self.router.local_id();
                Ok(self.conn(&next).ok_or(anyhow!("peer not found"))?.open_stream(service, source, dest, meta).await?)
            }
        }
    }
}
