use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use crate::{NetworkAddress, PeerAddress};

use super::PeerId;

const TIMEOUT_AFTER: u64 = 30_000;

#[derive(Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PeerDiscoverySync(Vec<(PeerId, u64, NetworkAddress)>);

#[derive(Debug, Default)]
pub struct PeerDiscovery {
    seeds: Vec<PeerAddress>,
    local: Option<(PeerId, NetworkAddress)>,
    remotes: BTreeMap<PeerId, (u64, NetworkAddress)>,
}

impl PeerDiscovery {
    pub fn new(seeds: Vec<PeerAddress>) -> Self {
        Self {
            seeds,
            local: None,
            remotes: Default::default(),
        }
    }

    pub fn enable_local(&mut self, peer_id: PeerId, address: NetworkAddress) {
        log::info!("[PeerDiscovery] enable local as {address}");
        self.local = Some((peer_id, address));
    }

    pub fn clear_timeout(&mut self, now_ms: u64) {
        self.remotes.retain(|peer, (last_updated, _addr)| {
            if *last_updated + TIMEOUT_AFTER > now_ms {
                true
            } else {
                log::info!("[PeerDiscovery] remove timeout {peer}");
                false
            }
        });
    }

    pub fn create_sync_for(&self, now_ms: u64, dest: &PeerId) -> PeerDiscoverySync {
        let iter = self.local.iter().map(|(p, addr)| (*p, now_ms, addr.clone()));
        PeerDiscoverySync(
            self.remotes
                .iter()
                .filter(|(k, _)| !dest.eq(k))
                .map(|(k, (v1, v2))| (*k, *v1, v2.clone()))
                .chain(iter)
                .collect::<Vec<_>>(),
        )
    }

    pub fn apply_sync(&mut self, now_ms: u64, sync: PeerDiscoverySync) {
        log::debug!("[PeerDiscovery] apply sync with addrs: {:?}", sync.0);
        for (peer, last_updated, address) in sync.0.into_iter() {
            if last_updated + TIMEOUT_AFTER > now_ms {
                #[allow(clippy::collapsible_else_if)]
                if self.remotes.insert(peer, (last_updated, address)).is_none() {
                    log::info!("[PeerDiscovery] added new peer {peer}");
                }
            }
        }
    }
    pub fn remotes(&self) -> impl Iterator<Item = PeerAddress> + '_ {
        self.remotes.iter().map(|(p, (_, a))| PeerAddress(*p, a.clone())).chain(self.seeds.iter().map(|s| s.clone()))
    }
}

#[cfg(test)]
mod test {
    use crate::{discovery::PeerDiscoverySync, PeerAddress, PeerId};

    use super::{PeerDiscovery, TIMEOUT_AFTER};

    #[test_log::test]
    fn create_local_sync() {
        let mut discovery = PeerDiscovery::default();

        let peer1 = PeerId(1);
        let peer1_addr: PeerAddress = "1@127.0.0.1:9000".parse().expect("should parse peer address");

        let peer2 = PeerId(2);

        assert_eq!(discovery.create_sync_for(0, &peer2), PeerDiscoverySync(vec![]));

        discovery.enable_local(peer1, peer1_addr.network_address().clone());

        assert_eq!(discovery.create_sync_for(100, &peer2), PeerDiscoverySync(vec![(peer1, 100, peer1_addr.network_address().clone())]));
        assert_eq!(discovery.remotes().next(), None);
    }

    #[test_log::test]
    fn apply_sync() {
        let mut discovery = PeerDiscovery::default();

        let peer1 = PeerId(1);
        let peer1_addr: PeerAddress = "1@127.0.0.1:9000".parse().expect("should parse peer address");

        let peer2 = PeerId(2);

        discovery.apply_sync(100, PeerDiscoverySync(vec![(peer1, 90, peer1_addr.network_address().clone())]));

        assert_eq!(discovery.create_sync_for(100, &peer2), PeerDiscoverySync(vec![(peer1, 90, peer1_addr.network_address().clone())]));
        assert_eq!(discovery.create_sync_for(100, &peer1), PeerDiscoverySync(vec![]));
        assert_eq!(discovery.remotes().collect::<Vec<_>>(), vec![peer1_addr]);
    }

    #[test_log::test]
    fn apply_sync_timeout() {
        let mut discovery = PeerDiscovery::default();

        let peer1 = PeerId(1);
        let peer1_addr: PeerAddress = "1@127.0.0.1:9000".parse().expect("should parse peer address");

        let peer2 = PeerId(2);

        discovery.apply_sync(TIMEOUT_AFTER + 100, PeerDiscoverySync(vec![(peer1, 100, peer1_addr.network_address().clone())]));

        assert_eq!(discovery.create_sync_for(100, &peer2), PeerDiscoverySync(vec![]));
        assert_eq!(discovery.create_sync_for(100, &peer1), PeerDiscoverySync(vec![]));
        assert_eq!(discovery.remotes().next(), None);
    }

    #[test_log::test]
    fn clear_timeout() {
        let mut discovery = PeerDiscovery::default();

        let peer1 = PeerId(1);
        let peer1_addr: PeerAddress = "1@127.0.0.1:9000".parse().expect("should parse peer address");

        discovery.apply_sync(100, PeerDiscoverySync(vec![(peer1, 90, peer1_addr.network_address().clone())]));

        assert_eq!(discovery.remotes().collect::<Vec<_>>(), vec![peer1_addr]);

        discovery.clear_timeout(TIMEOUT_AFTER + 90);

        assert_eq!(discovery.remotes().next(), None);
    }
}
