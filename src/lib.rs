use std::{
    fmt::{Debug, Display},
    net::SocketAddr,
    ops::Deref,
    str::FromStr,
    sync::Arc,
    time::Duration,
};

use anyhow::anyhow;
use ctx::SharedCtx;
use derive_more::derive::{Deref, Display, From};
use discovery::{PeerDiscovery, PeerDiscoverySync};
use msg::{P2pServiceId, PeerMessage};
use neighbours::NetworkNeighbours;
use peer::PeerConnection;
use quinn::{Endpoint, Incoming, VarInt};
use router::RouterTableSync;
use rustls::pki_types::{CertificateDer, PrivatePkcs8KeyDer};
use serde::{Deserialize, Serialize};
use tokio::{
    select,
    sync::{
        mpsc::{channel, unbounded_channel, Receiver, Sender, UnboundedReceiver, UnboundedSender},
        oneshot,
    },
    time::Interval,
};

use crate::quic::make_server_endpoint;

mod ctx;
mod discovery;
mod msg;
mod neighbours;
mod peer;
mod quic;
mod requester;
mod router;
mod secure;
mod service;
mod stream;
#[cfg(test)]
mod tests;
mod utils;

pub use requester::P2pNetworkRequester;
pub use router::SharedRouterTable;
pub use secure::*;
pub use service::*;
pub use stream::P2pQuicStream;
pub use utils::*;

#[derive(Debug, Display, Clone, Copy, From, Deref, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct PeerId(u64);

#[derive(Debug, Display, From, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct ConnectionId(u64);

impl ConnectionId {
    pub fn rand() -> Self {
        Self(rand::random())
    }
}

#[derive(Debug, Clone, From, Display, Deref, PartialEq, Eq, Serialize, Deserialize)]
pub struct NetworkAddress(SocketAddr);

#[derive(Debug, Clone, From, PartialEq, Eq, Serialize, Deserialize)]
pub struct PeerAddress(PeerId, NetworkAddress);

impl PeerAddress {
    pub fn new(p: PeerId, a: NetworkAddress) -> Self {
        Self(p, a)
    }

    pub fn peer_id(&self) -> PeerId {
        self.0
    }

    pub fn network_address(&self) -> &NetworkAddress {
        &self.1
    }
}

impl Display for PeerAddress {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}@{}", self.peer_id(), self.network_address())
    }
}

impl FromStr for PeerAddress {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let parts: Vec<&str> = s.split('@').collect();
        if parts.len() != 2 {
            return Err("Invalid format, expected 'peer_id@network_address'".to_string());
        }
        let peer_id = parts[0].parse::<u64>().map(PeerId).map_err(|e| e.to_string())?;
        let network_address = parts[1].parse::<SocketAddr>().map(NetworkAddress).map_err(|e| e.to_string())?;
        Ok(Self(peer_id, network_address))
    }
}

#[derive(Debug)]
enum PeerMainData {
    Sync { route: RouterTableSync, advertise: PeerDiscoverySync },
}

#[allow(clippy::enum_variant_names)]
enum InternalEvent {
    PeerConnected(ConnectionId, PeerId, u16),
    PeerConnectError(ConnectionId, Option<PeerId>, anyhow::Error),
    PeerData(ConnectionId, PeerId, PeerMainData),
    PeerDisconnected(ConnectionId, PeerId),
}

enum ControlCmd {
    Connect(PeerAddress, Option<oneshot::Sender<anyhow::Result<()>>>),
}

pub struct P2pNetworkConfig<SECURE> {
    pub peer_id: PeerId,
    pub listen_addr: SocketAddr,
    pub advertise: Option<NetworkAddress>,
    pub priv_key: PrivatePkcs8KeyDer<'static>,
    pub cert: CertificateDer<'static>,
    pub tick_ms: u64,
    pub seeds: Vec<PeerAddress>,
    pub secure: SECURE,
}

#[derive(Debug, PartialEq, Eq)]
pub enum P2pNetworkEvent {
    PeerConnected(ConnectionId, PeerId),
    PeerDisconnected(ConnectionId, PeerId),
    Continue,
}

pub struct P2pNetwork<SECURE> {
    local_id: PeerId,
    endpoint: Endpoint,
    control_tx: UnboundedSender<ControlCmd>,
    control_rx: UnboundedReceiver<ControlCmd>,
    internal_tx: Sender<InternalEvent>,
    internal_rx: Receiver<InternalEvent>,
    neighbours: NetworkNeighbours,
    ticker: Interval,
    router: SharedRouterTable,
    discovery: PeerDiscovery,
    ctx: SharedCtx,
    secure: Arc<SECURE>,
}

impl<SECURE: HandshakeProtocol> P2pNetwork<SECURE> {
    pub async fn new(cfg: P2pNetworkConfig<SECURE>) -> anyhow::Result<Self> {
        log::info!("[P2pNetwork] starting node {}@{}", cfg.peer_id, cfg.listen_addr);
        let endpoint = make_server_endpoint(cfg.listen_addr, cfg.priv_key, cfg.cert)?;
        let (internal_tx, internal_rx) = channel(10);
        let (control_tx, control_rx) = unbounded_channel();
        let mut discovery = PeerDiscovery::new(cfg.seeds);
        let router = SharedRouterTable::new(cfg.peer_id);

        if let Some(addr) = cfg.advertise {
            discovery.enable_local(cfg.peer_id, addr);
        }

        Ok(Self {
            local_id: cfg.peer_id,
            endpoint,
            neighbours: NetworkNeighbours::default(),
            internal_tx,
            internal_rx,
            control_tx,
            control_rx,
            ticker: tokio::time::interval(Duration::from_millis(cfg.tick_ms)),
            ctx: SharedCtx::new(router.clone()),
            router,
            discovery,
            secure: Arc::new(cfg.secure),
        })
    }

    pub fn create_service(&mut self, service_id: P2pServiceId) -> P2pService {
        let (service, tx) = P2pService::build(service_id, self.ctx.clone());
        self.ctx.set_service(service_id, tx);
        service
    }

    pub fn requester(&mut self) -> P2pNetworkRequester {
        P2pNetworkRequester { control_tx: self.control_tx.clone() }
    }

    pub async fn recv(&mut self) -> anyhow::Result<P2pNetworkEvent> {
        select! {
            _ = self.ticker.tick() => {
                self.process_tick(now_ms())
            }
            connecting = self.endpoint.accept() => {
                self.process_incoming(connecting.ok_or(anyhow!("quic crash"))?)
            },
            event = self.internal_rx.recv() => {
                self.process_internal(now_ms(), event.ok_or(anyhow!("internal channel crash"))?)
            },
            event = self.control_rx.recv() => {
                self.process_control(event.ok_or(anyhow!("internal channel crash"))?)
            },

        }
    }

    pub fn shutdown(&mut self) {
        self.endpoint.close(VarInt::from_u32(0), "Shutdown".as_bytes());
    }

    fn process_tick(&mut self, now_ms: u64) -> anyhow::Result<P2pNetworkEvent> {
        self.discovery.clear_timeout(now_ms);
        for conn in self.neighbours.connected_conns() {
            let peer_id = conn.peer_id().expect("connected neighbours should have peer_id");
            let conn_id = conn.conn_id();
            let route: router::RouterTableSync = self.router.create_sync(&peer_id);
            let advertise = self.discovery.create_sync_for(now_ms, &peer_id);
            if let Some(alias) = self.ctx.conn(&conn_id) {
                if let Err(e) = alias.try_send(PeerMessage::Sync { route, advertise }) {
                    log::error!("[P2pNetwork] try send message to peer {peer_id} over conn {conn_id} error {e}");
                }
            }
        }
        for addr in self.discovery.remotes() {
            self.control_tx.send(ControlCmd::Connect(addr.clone(), None))?;
        }
        Ok(P2pNetworkEvent::Continue)
    }

    fn process_incoming(&mut self, incoming: Incoming) -> anyhow::Result<P2pNetworkEvent> {
        let remote = incoming.remote_address();
        log::info!("[P2pNetwork] incoming connect from {remote} => accept");
        let conn = PeerConnection::new_incoming(self.secure.clone(), self.local_id, incoming, self.internal_tx.clone(), self.ctx.clone());
        self.neighbours.insert(conn.conn_id(), conn);
        Ok(P2pNetworkEvent::Continue)
    }

    fn process_internal(&mut self, now_ms: u64, event: InternalEvent) -> anyhow::Result<P2pNetworkEvent> {
        match event {
            InternalEvent::PeerConnected(conn, peer, ttl_ms) => {
                log::info!("[P2pNetwork] connection {conn} connected to {peer}");
                self.router.set_direct(conn, peer, ttl_ms);
                self.neighbours.mark_connected(&conn, peer);
                Ok(P2pNetworkEvent::PeerConnected(conn, peer))
            }
            InternalEvent::PeerData(conn, peer, data) => {
                log::debug!("[P2pNetwork] connection {conn} on data {data:?} from {peer}");
                match data {
                    PeerMainData::Sync { route, advertise } => {
                        self.router.apply_sync(conn, route);
                        self.discovery.apply_sync(now_ms, advertise);
                    }
                }
                Ok(P2pNetworkEvent::Continue)
            }
            InternalEvent::PeerConnectError(conn, peer, err) => {
                log::error!("[P2pNetwork] connection {conn} outgoing: {peer:?} error {err}");
                Ok(P2pNetworkEvent::Continue)
            }
            InternalEvent::PeerDisconnected(conn, peer) => {
                log::info!("[P2pNetwork] connection {conn} disconnected from {peer}");
                self.router.del_direct(&conn);
                self.neighbours.remove(&conn);
                Ok(P2pNetworkEvent::PeerDisconnected(conn, peer))
            }
        }
    }

    fn process_control(&mut self, cmd: ControlCmd) -> anyhow::Result<P2pNetworkEvent> {
        match cmd {
            ControlCmd::Connect(addr, tx) => {
                let res = if self.neighbours.has_peer(&addr.peer_id()) {
                    Ok(())
                } else {
                    log::info!("[P2pNetwork] connecting to {addr}");
                    match self.endpoint.connect(*addr.network_address().deref(), "cluster") {
                        Ok(connecting) => {
                            let conn = PeerConnection::new_connecting(self.secure.clone(), self.local_id, addr.peer_id(), connecting, self.internal_tx.clone(), self.ctx.clone());
                            self.neighbours.insert(conn.conn_id(), conn);
                            Ok(())
                        }
                        Err(err) => Err(err.into()),
                    }
                };

                if let Some(tx) = tx {
                    tx.send(res).print_on_err2("[P2pNetwork] send connect answer");
                }

                Ok(P2pNetworkEvent::Continue)
            }
        }
    }
}
