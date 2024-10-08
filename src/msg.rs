use derive_more::derive::{Deref, Display, From};
use serde::{Deserialize, Serialize};

use super::{discovery::PeerDiscoverySync, router::RouterTableSync, PeerId};

#[derive(Debug, Display, PartialEq, Eq, Hash, Serialize, Deserialize, Clone, Copy)]
pub struct BroadcastMsgId(u64);

#[derive(Debug, Display, PartialEq, Deref, Eq, Hash, Serialize, Deserialize, From, Clone, Copy)]
pub struct P2pServiceId(u16);

impl BroadcastMsgId {
    pub fn rand() -> Self {
        Self(rand::random())
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub enum PeerMessage {
    Sync { route: RouterTableSync, advertise: PeerDiscoverySync },
    Broadcast(PeerId, P2pServiceId, BroadcastMsgId, Vec<u8>),
    Unicast(PeerId, PeerId, P2pServiceId, Vec<u8>),
}

#[derive(Debug, Serialize, Deserialize)]
pub struct StreamConnectReq {
    pub source: PeerId,
    pub dest: PeerId,
    pub service: P2pServiceId,
    pub meta: Vec<u8>,
}

pub type StreamConnectRes = Result<(), String>;
