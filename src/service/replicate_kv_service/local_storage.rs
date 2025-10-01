use std::{
    collections::{BTreeMap, VecDeque},
    hash::Hash,
};

use super::messages::{Action, BroadcastEvent, Changed, Event, FetchChangedError, KvEvent, NetEvent, RpcEvent, RpcReq, RpcRes, Slot, SnapshotData, Version};

pub struct LocalStore<N, K, V> {
    slots: BTreeMap<K, Slot<V>>,
    changeds: BTreeMap<Version, Changed<K, V>>,
    max_changeds: usize,
    compose_max_pkts: usize,
    version: Version,
    outs: VecDeque<Event<N, K, V>>,
}

impl<N, K, V> LocalStore<N, K, V>
where
    K: Hash + Ord + Eq + Clone,
    V: Eq + Clone,
{
    pub fn new(max_changeds: usize, compose_max_pkts: usize) -> Self {
        LocalStore {
            slots: BTreeMap::new(),
            changeds: BTreeMap::new(),
            max_changeds,
            compose_max_pkts,
            version: Version(0),
            outs: VecDeque::new(),
        }
    }

    pub fn on_tick(&mut self) {
        self.outs.push_back(Event::NetEvent(NetEvent::Broadcast(BroadcastEvent::Version(self.version))));
    }

    pub fn set(&mut self, key: K, value: V) {
        self.version = self.version + 1;
        let version = self.version;
        let changed = Changed {
            key: key.clone(),
            version,
            action: Action::Set(value.clone()),
        };
        self.changeds.insert(version, changed.clone());
        self.outs.push_back(Event::NetEvent(NetEvent::Broadcast(BroadcastEvent::Changed(changed))));
        self.outs.push_back(Event::KvEvent(KvEvent::Set(None, key.clone(), value.clone())));
        while self.changeds.len() > self.max_changeds {
            self.changeds.pop_first();
        }
        self.slots.insert(key, Slot::new(value, version));
    }

    pub fn del(&mut self, key: K) {
        self.version = self.version + 1;
        let version = self.version;
        let changed = Changed {
            key: key.clone(),
            version,
            action: Action::Del,
        };
        self.changeds.insert(self.version, changed.clone());
        self.outs.push_back(Event::NetEvent(NetEvent::Broadcast(BroadcastEvent::Changed(changed))));
        self.outs.push_back(Event::KvEvent(KvEvent::Del(None, key.clone())));
        while self.changeds.len() > self.max_changeds {
            self.changeds.pop_first();
        }
        self.slots.remove(&key);
    }

    pub fn on_rpc_req(&mut self, from_node: N, req: RpcReq<K>) {
        match req {
            RpcReq::FetchChanged { from, count } => {
                let res = RpcRes::FetchChanged(self.changeds_from_to(from, count));
                self.outs.push_back(Event::NetEvent(NetEvent::Unicast(from_node, RpcEvent::RpcRes(res))));
            }
            RpcReq::FetchSnapshot { from, to, max_version } => {
                let res = RpcRes::FetchSnapshot(self.snapshot(from, to, max_version), self.version);
                self.outs.push_back(Event::NetEvent(NetEvent::Unicast(from_node, RpcEvent::RpcRes(res))));
            }
        }
    }

    fn changeds_from_to(&self, from: Version, count: u64) -> Result<Vec<Changed<K, V>>, FetchChangedError> {
        let to = from + count.min(self.compose_max_pkts as u64);
        let first = self.changeds.first_key_value().ok_or(FetchChangedError::MissingData)?.0;
        let last = self.changeds.last_key_value().ok_or(FetchChangedError::MissingData)?.0;
        if to > *last + 1 || from < *first {
            return Err(FetchChangedError::MissingData);
        }
        Ok(self.changeds.range(from..to).map(|(_, v)| v.clone()).collect())
    }

    fn snapshot(&self, from: Option<K>, to: Option<K>, max_version: Option<Version>) -> Option<SnapshotData<K, V>> {
        let first = self.slots.first_key_value().map(|(k, _)| k.clone());
        let last = self.slots.last_key_value().map(|(k, _)| k.clone());
        if let (Some(first), Some(last)) = (first, last) {
            let from = from.unwrap_or(first);
            let to = to.unwrap_or(last);
            let max_version = max_version.unwrap_or(self.version);
            let mut slots = Vec::new();
            let mut next_key = None;
            let biggest_key = to.clone();
            for (key, slot) in self.slots.range(from..=to) {
                if slot.version > max_version {
                    continue;
                }
                if slots.len() >= self.compose_max_pkts {
                    next_key = Some(key.clone());
                    break;
                }
                slots.push((key.clone(), slot.clone()));
            }
            Some(SnapshotData { slots, next_key, biggest_key })
        } else {
            None
        }
    }

    pub fn pop_out(&mut self) -> Option<Event<N, K, V>> {
        self.outs.pop_front()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn simple_works() {
        let mut store: LocalStore<u16, u16, u16> = LocalStore::new(10, 3);

        assert_eq!(store.snapshot(None, None, None), None);

        store.set(1, 101);

        assert_eq!(
            store.pop_out(),
            Some(Event::NetEvent(NetEvent::Broadcast(BroadcastEvent::Changed(Changed {
                key: 1,
                version: Version(1),
                action: Action::Set(101)
            }))))
        );
        assert_eq!(store.pop_out(), Some(Event::KvEvent(KvEvent::Set(None, 1, 101))));
        assert_eq!(store.pop_out(), None);

        assert_eq!(
            store.snapshot(None, None, None),
            Some(SnapshotData {
                slots: vec![(1, Slot::new(101, Version(1)))],
                next_key: None,
                biggest_key: 1
            })
        );

        store.del(1);

        assert_eq!(
            store.pop_out(),
            Some(Event::NetEvent(NetEvent::Broadcast(BroadcastEvent::Changed(Changed {
                key: 1,
                version: Version(2),
                action: Action::Del
            }))))
        );
        assert_eq!(store.pop_out(), Some(Event::KvEvent(KvEvent::Del(None, 1))));
        assert_eq!(store.pop_out(), None);

        assert_eq!(
            store.changeds_from_to(Version(1), 2),
            Ok(vec![
                Changed {
                    key: 1,
                    version: Version(1),
                    action: Action::Set(101)
                },
                Changed {
                    key: 1,
                    version: Version(2),
                    action: Action::Del
                }
            ])
        );

        assert_eq!(store.snapshot(None, None, None), None);
    }

    #[test]
    fn snapshot_multiple_pkts() {
        let mut store: LocalStore<u16, u16, u16> = LocalStore::new(2, 2);
        for i in 1..=10 {
            store.set(i, i);
        }

        assert_eq!(
            store.snapshot(None, None, None),
            Some(SnapshotData {
                slots: vec![(1, Slot::new(1, Version(1))), (2, Slot::new(2, Version(2)))],
                next_key: Some(3),
                biggest_key: 10
            })
        );

        assert_eq!(
            store.snapshot(Some(3), Some(10), Some(Version(10))),
            Some(SnapshotData {
                slots: vec![(3, Slot::new(3, Version(3))), (4, Slot::new(4, Version(4)))],
                next_key: Some(5),
                biggest_key: 10
            })
        );

        // last pkt
        assert_eq!(
            store.snapshot(Some(9), Some(10), Some(Version(10))),
            Some(SnapshotData {
                slots: vec![(9, Slot::new(9, Version(9))), (10, Slot::new(10, Version(10)))],
                next_key: None,
                biggest_key: 10
            })
        );
    }

    #[test]
    fn auto_clear_changeds() {
        let mut store: LocalStore<u16, u16, u16> = LocalStore::new(2, 2);
        for i in 0..3 {
            store.set(i, i);
        }
        assert_eq!(store.changeds.len(), 2);
        assert_eq!(store.changeds_from_to(Version(1), 3), Err(FetchChangedError::MissingData));
        assert_eq!(
            store.changeds_from_to(Version(2), 2),
            Ok(vec![
                Changed {
                    key: 1,
                    version: Version(2),
                    action: Action::Set(1)
                },
                Changed {
                    key: 2,
                    version: Version(3),
                    action: Action::Set(2)
                }
            ])
        );
    }

    #[test]
    fn tick_broadcasts_version() {
        let mut store: LocalStore<u16, u16, u16> = LocalStore::new(10, 2);
        store.on_tick();
        assert_eq!(store.pop_out(), Some(Event::NetEvent(NetEvent::Broadcast(BroadcastEvent::Version(Version(0))))));
    }
}
