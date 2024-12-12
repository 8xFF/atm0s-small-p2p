use std::collections::{BTreeMap, HashMap, VecDeque};

use super::{Action, BroadcastEvent, Changed, Key, NetEvent, RpcEvent, RpcReq, RpcRes, Slot, Version};

pub struct LocalStore<N, V> {
    slots: HashMap<Key, Slot<V>>,
    changeds: BTreeMap<Version, Changed<V>>,
    max_changeds: usize,
    version: Version,
    outs: VecDeque<NetEvent<N, V>>,
}

impl<N, V> LocalStore<N, V>
where
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
        self.outs.push_back(NetEvent::Broadcast(BroadcastEvent::Version(self.version)));
    }

    pub fn set(&mut self, key: Key, value: V) {
        self.version = self.version + 1;
        let version = self.version;
        let changed = Changed {
            key,
            version,
            action: Action::Set(value.clone()),
        };
        self.changeds.insert(version, changed.clone());
        self.outs.push_back(NetEvent::Broadcast(BroadcastEvent::Changed(changed)));
        while self.changeds.len() > self.max_changeds {
            self.changeds.pop_first();
        }
        self.slots.insert(key, Slot { version, value });
    }

    pub fn del(&mut self, key: Key) {
        self.version = self.version + 1;
        let version = self.version;
        let changed = Changed { key, version, action: Action::Del };
        self.changeds.insert(self.version, changed.clone());
        self.outs.push_back(NetEvent::Broadcast(BroadcastEvent::Changed(changed)));
        while self.changeds.len() > self.max_changeds {
            self.changeds.pop_first();
        }
        self.slots.remove(&key);
    }

    pub fn on_rpc_req(&mut self, from_node: N, req: RpcReq) {
        match req {
            RpcReq::FetchChanged { from, to } => {
                let changeds = self.changeds_from_to(from, to);
                // TODO split to small parts for avoid too much data
                let res = RpcRes::FetchChanged { changes: changeds, remain: false };
                self.outs.push_back(NetEvent::Unicast(from_node, RpcEvent::RpcRes(res)));
            }
            RpcReq::FetchSnapshot => {
                let snapshot = self.snaphsot();
                let res = RpcRes::FetchSnapshot {
                    slots: snapshot,
                    version: self.version,
                };
                self.outs.push_back(NetEvent::Unicast(from_node, RpcEvent::RpcRes(res)));
            }
        }
    }

    pub fn changeds_from_to(&self, from: Version, to: Option<Version>) -> Vec<Changed<V>> {
        self.changeds.range(from..=to.unwrap_or(self.version)).map(|(_, v)| v.clone()).collect()
    }

    pub fn snaphsot(&self) -> Vec<(Key, Slot<V>)> {
        self.slots.iter().map(|(k, v)| (k.clone(), v.clone())).collect::<Vec<_>>()
    }

    pub fn version(&self) -> Version {
        self.version
    }

    pub fn pop_out(&mut self) -> Option<NetEvent<N, V>> {
        self.outs.pop_front()
    }
}
