//!
//! This module implement replicated local_kv which data is replicated to all nodes.
//! Each key-value is store in local-node and broadcast to all other nodes, which allow other node can access data with local-speed.
//! For simplicity, data is only belong to which node it created from. If a node disconnected, it's data will be deleted all other nodes.
//! Some useful usecase: session map
//!

use std::{
    collections::{HashMap, VecDeque},
    fmt::Debug,
    hash::Hash,
    ops::{Add, Deref, Sub},
};

use local_storage::LocalStore;
use remote_storage::RemoteStore;
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use tokio::{select, time::Interval};

use crate::PeerId;

use super::P2pService;

mod local_storage;
mod remote_storage;

const REMOTE_TIMEOUT_MS: u128 = 10_000;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Slot<V> {
    value: V,
    version: Version,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum Action<V> {
    Set(V),
    Del,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Changed<V> {
    pub(crate) key: Key,
    pub(crate) version: Version,
    pub(crate) action: Action<V>,
}

#[derive(Debug, PartialEq, Eq, Hash, Clone, Copy, Serialize, Deserialize)]
pub struct Key(u64);

#[derive(Debug, PartialEq, Eq, Clone, Copy, Serialize, Deserialize)]
pub struct Version(u64);

impl Add<u64> for Version {
    type Output = Self;

    fn add(self, rhs: u64) -> Self::Output {
        Self(self.0 + rhs)
    }
}

impl Sub<Version> for Version {
    type Output = u64;

    fn sub(self, rhs: Version) -> Self::Output {
        self.0 - rhs.0
    }
}

impl Ord for Version {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.0.cmp(&other.0)
    }
}

impl PartialOrd for Version {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        self.0.partial_cmp(&other.0)
    }
}

impl Deref for Version {
    type Target = u64;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub enum BroadcastEvent<V> {
    Changed(Changed<V>),
    Version(Version),
}

#[derive(Debug, Serialize, Deserialize)]
pub enum RpcReq {
    FetchChanged { from: Version, to: Option<Version> },
    FetchSnapshot,
}

#[derive(Debug, Serialize, Deserialize)]
pub enum RpcRes<V> {
    FetchChanged { changes: Vec<Changed<V>>, remain: bool },
    FetchSnapshot { slots: Vec<(Key, Slot<V>)>, version: Version },
}

#[derive(Debug, Serialize, Deserialize)]
pub enum RpcEvent<V> {
    RpcReq(RpcReq),
    RpcRes(RpcRes<V>),
}

pub enum KvEvent<N, V> {
    Set(N, Key, V),
    Del(N, Key),
}

#[derive(Debug, Serialize, Deserialize)]
pub enum NetEvent<N, V> {
    Broadcast(BroadcastEvent<V>),
    Unicast(N, RpcEvent<V>),
}

pub enum Event<N, V> {
    NetEvent(NetEvent<N, V>),
    KvEvent(KvEvent<N, V>),
}

pub struct ReplicatedKvStore<N, V> {
    remotes: HashMap<N, RemoteStore<N, V>>,
    local: LocalStore<N, V>,
    outs: VecDeque<Event<N, V>>,
}

impl<N, V> ReplicatedKvStore<N, V>
where
    N: Eq + Hash + Clone,
    V: Debug + Eq + Clone,
{
    pub fn new() -> Self {
        ReplicatedKvStore {
            remotes: HashMap::new(),
            local: LocalStore::new(1024),
            outs: VecDeque::new(),
        }
    }

    pub fn on_tick(&mut self) {
        self.local.on_tick();
        while let Some(event) = self.local.pop_out() {
            self.outs.push_back(Event::NetEvent(event));
        }
        self.remotes.retain(|_, remote| remote.last_active().elapsed().as_millis() < REMOTE_TIMEOUT_MS);
    }

    pub fn set(&mut self, key: Key, value: V) {
        self.local.set(key.clone(), value.clone());
        while let Some(event) = self.local.pop_out() {
            self.outs.push_back(Event::NetEvent(event));
        }
    }

    pub fn del(&mut self, key: Key) {
        self.local.del(key.clone());
        while let Some(event) = self.local.pop_out() {
            self.outs.push_back(Event::NetEvent(event));
        }
    }

    pub fn on_remote_event(&mut self, from: N, event: NetEvent<N, V>) {
        if !self.remotes.contains_key(&from) {
            let mut remote = RemoteStore::new(from.clone());
            while let Some(event) = remote.pop_out() {
                self.outs.push_back(Event::NetEvent(event));
            }
            self.remotes.insert(from.clone(), remote);
        }

        match event {
            NetEvent::Broadcast(event) => {
                if let Some(remote) = self.remotes.get_mut(&from) {
                    remote.on_broadcast(event);
                    while let Some(event) = remote.pop_out() {
                        self.outs.push_back(Event::NetEvent(event));
                    }
                }
            }
            NetEvent::Unicast(_from, event) => match event {
                RpcEvent::RpcReq(rpc_req) => {
                    self.local.on_rpc_req(from, rpc_req);
                    while let Some(event) = self.local.pop_out() {
                        self.outs.push_back(Event::NetEvent(event));
                    }
                }
                RpcEvent::RpcRes(rpc_res) => {
                    if let Some(remote) = self.remotes.get_mut(&from) {
                        remote.on_rpc_res(rpc_res);
                        while let Some(event) = remote.pop_out() {
                            self.outs.push_back(Event::NetEvent(event));
                        }
                    }
                }
            },
        }
    }

    pub fn pop(&mut self) -> Option<Event<N, V>> {
        self.outs.pop_front()
    }
}

pub struct ReplicatedKvService<V> {
    service: P2pService,
    tick: Interval,
    store: ReplicatedKvStore<PeerId, V>,
}

impl<V> ReplicatedKvService<V>
where
    V: Debug + Eq + Clone + DeserializeOwned + Serialize,
{
    pub fn new(service: P2pService) -> Self {
        Self {
            service,
            tick: tokio::time::interval(std::time::Duration::from_millis(1000)),
            store: ReplicatedKvStore::new(),
        }
    }

    pub async fn recv(&mut self) -> Option<KvEvent<PeerId, V>> {
        loop {
            if let Some(event) = self.store.pop() {
                match event {
                    Event::NetEvent(net_event) => match net_event {
                        NetEvent::Broadcast(broadcast_event) => {
                            let _ = self.service.try_send_broadcast(bincode::serialize(&broadcast_event).unwrap()).await;
                            continue;
                        }
                        NetEvent::Unicast(to_node, rpc_event) => {
                            let _ = self.service.try_send_unicast(to_node, bincode::serialize(&rpc_event).unwrap()).await;
                            continue;
                        }
                    },
                    Event::KvEvent(kv_event) => {
                        return Some(kv_event);
                    }
                }
            }

            select! {
                _ = self.tick.tick() => {
                    self.store.on_tick();
                }
                event = self.service.recv() => match event? {
                    super::P2pServiceEvent::Unicast(peer_id, vec) => {
                        let event = bincode::deserialize::<NetEvent<PeerId, V>>(&vec).unwrap();
                        self.store.on_remote_event(peer_id, event);
                    }
                    super::P2pServiceEvent::Broadcast(peer_id, vec) => {
                        let event = bincode::deserialize::<NetEvent<PeerId, V>>(&vec).unwrap();
                        self.store.on_remote_event(peer_id, event);
                    }
                    super::P2pServiceEvent::Stream(..) => {}
                }
            }
        }
    }
}
