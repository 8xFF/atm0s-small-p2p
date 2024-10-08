use std::collections::HashMap;

use crate::{peer::PeerConnection, ConnectionId, PeerId};

#[derive(Default)]
pub struct NetworkNeighbours {
    conns: HashMap<ConnectionId, PeerConnection>,
}

impl NetworkNeighbours {
    pub fn insert(&mut self, conn_id: ConnectionId, conn: PeerConnection) {
        self.conns.insert(conn_id, conn);
    }

    pub fn has_peer(&self, peer: &PeerId) -> bool {
        self.conns.values().find(|c| c.peer_id().eq(&Some(*peer))).is_some()
    }

    pub fn mark_connected(&mut self, conn_id: &ConnectionId, peer: PeerId) -> Option<()> {
        self.conns.get_mut(conn_id)?.set_connected(peer);
        Some(())
    }

    pub fn remove(&mut self, conn_id: &ConnectionId) -> Option<()> {
        self.conns.remove(&conn_id)?;
        Some(())
    }

    pub fn connected_conns(&self) -> impl Iterator<Item = &PeerConnection> {
        self.conns.values().into_iter().filter(|c| c.is_connected())
    }
}
