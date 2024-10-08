//! Simple p2p router table
//! The idea behind it is a spread routing, we allow some route loop then it is resolve by 2 method:
//!
//! - Direct rtt always has lower rtt
//! - MAX_HOPS will reject some loop after direct connection disconnected

use std::{collections::BTreeMap, ops::AddAssign, sync::Arc};

use parking_lot::RwLock;
use serde::{Deserialize, Serialize};

use crate::ConnectionId;

use super::PeerId;

const MAX_HOPS: u8 = 6;

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, Clone, Copy)]
pub struct PathMetric {
    relay_hops: u8,
    rtt_ms: u16,
}

impl From<(u8, u16)> for PathMetric {
    fn from(value: (u8, u16)) -> Self {
        Self { relay_hops: value.0, rtt_ms: value.1 }
    }
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct RouterTableSync(Vec<(PeerId, PathMetric)>);

#[derive(Debug, Default)]
struct PeerMemory {
    best: Option<ConnectionId>,
    paths: BTreeMap<ConnectionId, PathMetric>,
}

#[derive(Debug, PartialEq, Eq)]
pub enum RouteAction {
    Local,
    Next(ConnectionId),
}

#[derive(Debug)]
struct RouterTable {
    peer_id: PeerId,
    peers: BTreeMap<PeerId, PeerMemory>,
    directs: BTreeMap<ConnectionId, (PeerId, PathMetric)>,
}

impl RouterTable {
    fn new(peer_id: PeerId) -> Self {
        Self {
            peer_id,
            peers: Default::default(),
            directs: Default::default(),
        }
    }

    fn local_id(&self) -> PeerId {
        self.peer_id
    }

    fn create_sync(&self, dest: &PeerId) -> RouterTableSync {
        RouterTableSync(
            self.peers
                .iter()
                .map(|(addr, history)| (*addr, history.best_metric().expect("should have best")))
                .filter(|(addr, metric)| !dest.eq(addr) && metric.relay_hops <= MAX_HOPS)
                .collect::<Vec<_>>(),
        )
    }

    fn apply_sync(&mut self, conn: ConnectionId, sync: RouterTableSync) {
        let (from_peer, direct_metric) = self.directs.get(&conn).expect("should have direct metric with apply_sync");
        // ensure we have memory for each sync paths
        for (peer, _) in sync.0.iter() {
            self.peers.entry(*peer).or_default();
        }

        let mut new_paths = BTreeMap::<PeerId, PathMetric>::from_iter(sync.0.into_iter());
        // only loop over peer which don't equal source, because it is direct connection
        for (peer, memory) in self.peers.iter_mut().filter(|(p, _)| !from_peer.eq(p)) {
            let previous = memory.paths.contains_key(&conn);
            let current = new_paths.remove(peer);
            match (previous, current) {
                (true, Some(mut new_metric)) => {
                    // has update
                    new_metric += *direct_metric;
                    memory.paths.insert(conn, new_metric);
                    Self::select_best_for(peer, memory);
                }
                (true, None) => {
                    // delete
                    log::info!("[RouterTable] remove path for {peer}");
                    memory.paths.remove(&conn);
                    Self::select_best_for(peer, memory);
                }
                (false, Some(mut new_metric)) => {
                    // new create
                    log::info!("[RouterTable] create path for {peer}");
                    new_metric += *direct_metric;
                    memory.paths.insert(conn, new_metric);
                    Self::select_best_for(peer, memory);
                }
                _ => { //dont changed
                }
            }
        }
        self.peers.retain(|_k, v| v.best().is_some());
    }

    fn set_direct(&mut self, conn: ConnectionId, to: PeerId, ttl_ms: u16) {
        self.directs.insert(conn, (to, (1, ttl_ms).into()));
        let memory = self.peers.entry(to).or_default();
        memory.paths.insert(conn, PathMetric { relay_hops: 0, rtt_ms: ttl_ms });
        Self::select_best_for(&to, memory);
    }

    fn del_direct(&mut self, conn: &ConnectionId) {
        if let Some((to, _)) = self.directs.remove(conn) {
            if let Some(memory) = self.peers.get_mut(&to) {
                memory.paths.remove(&conn);
                Self::select_best_for(&to, memory);
                if memory.best().is_none() {
                    self.peers.remove(&to);
                }
            }
        }
    }

    fn action(&self, dest: &PeerId) -> Option<RouteAction> {
        if self.peer_id.eq(dest) {
            Some(RouteAction::Local)
        } else {
            self.peers.get(dest)?.best().map(RouteAction::Next)
        }
    }

    /// Get next remote
    fn next_remote(&self, next: &PeerId) -> Option<(ConnectionId, PathMetric)> {
        let memory = self.peers.get(next)?;
        let best = memory.best()?;
        let metric = memory.best_metric().expect("should have metric");
        Some((best, metric))
    }

    fn select_best_for(dest: &PeerId, memory: &mut PeerMemory) {
        if let Some((new_best, metric)) = memory.select_best() {
            log::info!(
                "[RouterTable] to {dest} select new path over {new_best} with rtt {} ms over {} hop(s)",
                metric.rtt_ms,
                metric.relay_hops
            );
        }
    }

    fn neighbours(&self) -> Vec<(ConnectionId, PeerId, u16)> {
        self.directs.iter().map(|(k, (peer, v))| (*k, *peer, v.rtt_ms)).collect()
    }
}

impl PathMetric {
    fn score(&self) -> u16 {
        self.rtt_ms + self.relay_hops as u16 * 10
    }
}

impl AddAssign for PathMetric {
    fn add_assign(&mut self, rhs: Self) {
        self.relay_hops += rhs.relay_hops;
        self.rtt_ms += rhs.rtt_ms;
    }
}

impl PeerMemory {
    fn best(&self) -> Option<ConnectionId> {
        self.best
    }

    fn best_metric(&self) -> Option<PathMetric> {
        self.best.map(|p| *self.paths.get(&p).expect("should have metric with best path"))
    }

    fn select_best(&mut self) -> Option<(ConnectionId, PathMetric)> {
        let previous = self.best;
        self.best = None;
        let mut iter = self.paths.iter();
        let (peer, metric) = iter.next()?;
        let mut best_peer = peer;
        let mut best_score = metric.score();

        for (peer, metric) in iter {
            if best_score > metric.score() {
                best_peer = peer;
                best_score = metric.score();
            }
        }

        self.best = Some(*best_peer);
        if self.best != previous {
            let metric = self.best_metric().expect("should have best metric after select success");
            Some((*best_peer, metric))
        } else {
            None
        }
    }
}

#[derive(Debug, Clone)]
pub struct SharedRouterTable {
    table: Arc<RwLock<RouterTable>>,
}

impl SharedRouterTable {
    pub fn new(address: PeerId) -> Self {
        Self {
            table: Arc::new(RwLock::new(RouterTable::new(address))),
        }
    }

    pub fn local_id(&self) -> PeerId {
        self.table.read().local_id()
    }

    pub fn create_sync(&self, dest: &PeerId) -> RouterTableSync {
        self.table.read().create_sync(&dest)
    }

    pub fn apply_sync(&self, conn: ConnectionId, sync: RouterTableSync) {
        self.table.write().apply_sync(conn, sync);
    }

    pub fn set_direct(&self, conn: ConnectionId, to: PeerId, ttl_ms: u16) {
        self.table.write().set_direct(conn, to, ttl_ms);
    }

    pub fn del_direct(&self, conn: &ConnectionId) {
        self.table.write().del_direct(conn);
    }

    pub fn action(&self, dest: &PeerId) -> Option<RouteAction> {
        self.table.read().action(dest)
    }

    pub fn next_remote(&self, dest: &PeerId) -> Option<(ConnectionId, PathMetric)> {
        self.table.read().next_remote(dest)
    }

    pub fn neighbours(&self) -> Vec<(ConnectionId, PeerId, u16)> {
        self.table.read().neighbours()
    }
}

#[cfg(test)]
mod tests {
    use crate::{router::RouterTableSync, ConnectionId, PeerId};

    use super::{RouteAction, RouterTable, MAX_HOPS};

    #[test_log::test]
    fn route_local() {
        let table = RouterTable::new(PeerId(0));
        assert_eq!(table.action(&PeerId(0)), Some(RouteAction::Local));
    }

    #[test_log::test]
    fn create_correct_direct_sync() {
        let mut table = RouterTable::new(PeerId(0));

        let peer1 = PeerId(1);
        let conn1 = ConnectionId(1);
        let peer2 = PeerId(2);
        let conn2 = ConnectionId(2);
        let peer3 = PeerId(3);

        table.set_direct(conn1, peer1, 100);
        table.set_direct(conn2, peer2, 200);

        assert_eq!(table.next_remote(&peer1), Some((conn1, (0, 100).into())));
        assert_eq!(table.next_remote(&peer2), Some((conn2, (0, 200).into())));
        assert_eq!(table.next_remote(&peer3), None);

        assert_eq!(table.create_sync(&peer1), RouterTableSync(vec![(peer2, (0, 200).into())]));
        assert_eq!(table.create_sync(&peer2), RouterTableSync(vec![(peer1, (0, 100).into())]));
    }

    #[test_log::test]
    fn apply_correct_direct_sync() {
        let mut table = RouterTable::new(PeerId(0));

        let peer1 = PeerId(1);
        let conn1 = ConnectionId(1);
        let peer2 = PeerId(2);
        let peer3 = PeerId(3);
        let peer4 = PeerId(4);
        let conn4 = ConnectionId(4);

        table.set_direct(conn1, peer1, 100);
        table.set_direct(conn4, peer4, 400);

        table.apply_sync(conn1, RouterTableSync(vec![(peer2, (0, 200).into())]));

        // now we have NODO => peer1 => peer2
        assert_eq!(table.next_remote(&peer1), Some((conn1, (0, 100).into())));
        assert_eq!(table.next_remote(&peer2), Some((conn1, (1, 300).into())));
        assert_eq!(table.next_remote(&peer3), None);

        // we seems to have loop with peer2 here but it will not effect to routing because we have direct connection, it will always has lower rtt
        assert_eq!(table.create_sync(&peer1), RouterTableSync(vec![(peer2, (1, 300).into()), (peer4, (0, 400).into())]));
        assert_eq!(table.create_sync(&peer4), RouterTableSync(vec![(peer1, (0, 100).into()), (peer2, (1, 300).into())]));
    }

    #[test_log::test]
    fn dont_create_sync_over_max_hops() {
        let mut table = RouterTable::new(PeerId(0));

        let peer1 = PeerId(1);
        let conn1 = ConnectionId(1);
        let peer2 = PeerId(2);
        let peer3 = PeerId(3);
        let conn3 = ConnectionId(3);

        table.set_direct(conn1, peer1, 100);
        table.set_direct(conn3, peer3, 300);

        table.apply_sync(conn1, RouterTableSync(vec![(peer2, (MAX_HOPS, 200).into())]));
        assert_eq!(table.next_remote(&peer2), Some((conn1, (MAX_HOPS + 1, 300).into())));

        // we shouldn't create sync with peer2 because it over MAX_HOPS
        assert_eq!(table.create_sync(&peer3), RouterTableSync(vec![(peer1, (0, 100).into())]));
    }
}
