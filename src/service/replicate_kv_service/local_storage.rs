use std::{
    collections::{BTreeMap, HashMap, VecDeque},
    hash::Hash,
};

use super::{Action, BroadcastEvent, Changed, Event, FetchChangedError, KvEvent, NetEvent, RpcEvent, RpcReq, RpcRes, Slot, Version};

const MAX_CHANGE_SINGLE_PKT: u64 = 1024;

pub struct LocalStore<N, K, V> {
    slots: HashMap<K, Slot<V>>,
    changeds: BTreeMap<Version, Changed<K, V>>,
    max_changeds: usize,
    version: Version,
    outs: VecDeque<Event<N, K, V>>,
}

impl<N, K, V> LocalStore<N, K, V>
where
    K: Hash + Ord + Eq + Clone,
    V: Eq + Clone,
{
    pub fn new(max_changeds: usize) -> Self {
        LocalStore {
            slots: HashMap::new(),
            changeds: BTreeMap::new(),
            max_changeds,
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
        self.slots.insert(key, Slot { version, value });
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

    pub fn on_rpc_req(&mut self, from_node: N, req: RpcReq) {
        match req {
            RpcReq::FetchChanged { from, count } => {
                let res = RpcRes::FetchChanged(self.changeds_from_to(from, count));
                self.outs.push_back(Event::NetEvent(NetEvent::Unicast(from_node, RpcEvent::RpcRes(res))));
            }
            RpcReq::FetchSnapshot => {
                let snapshot = self.snapshot();
                let res = RpcRes::FetchSnapshot {
                    slots: snapshot,
                    version: self.version,
                };
                self.outs.push_back(Event::NetEvent(NetEvent::Unicast(from_node, RpcEvent::RpcRes(res))));
            }
        }
    }

    fn changeds_from_to(&self, from: Version, count: u64) -> Result<Vec<Changed<K, V>>, FetchChangedError> {
        let to = from + count.min(MAX_CHANGE_SINGLE_PKT);
        let first = self.changeds.first_key_value().ok_or(FetchChangedError::MissingData)?.0;
        let last = self.changeds.last_key_value().ok_or(FetchChangedError::MissingData)?.0;
        if to > *last + 1 || from < *first {
            return Err(FetchChangedError::MissingData);
        }
        Ok(self.changeds.range(from..to).map(|(_, v)| v.clone()).collect())
    }

    // TODO split to small parts for avoid too much data
    fn snapshot(&self) -> Vec<(K, Slot<V>)> {
        self.slots.iter().map(|(k, v)| (k.clone(), v.clone())).collect::<Vec<_>>()
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
        let mut store: LocalStore<u16, u16, u16> = LocalStore::new(10);

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

        assert_eq!(store.snapshot(), vec![(1, Slot { version: Version(1), value: 101 })]);

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

        assert_eq!(store.snapshot(), vec![]);
    }

    #[test]
    fn auto_clear_changeds() {
        let mut store: LocalStore<u16, u16, u16> = LocalStore::new(2);
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
        let mut store: LocalStore<u16, u16, u16> = LocalStore::new(10);
        store.on_tick();
        assert_eq!(store.pop_out(), Some(Event::NetEvent(NetEvent::Broadcast(BroadcastEvent::Version(Version(0))))));
    }
}
