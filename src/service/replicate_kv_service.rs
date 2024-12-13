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

impl<V> Slot<V> {
    pub fn new(value: V, version: Version) -> Self {
        Self { value, version }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum Action<V> {
    Set(V),
    Del,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Changed<K, V> {
    pub(crate) key: K,
    pub(crate) version: Version,
    pub(crate) action: Action<V>,
}

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

#[derive(Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum BroadcastEvent<K, V> {
    Changed(Changed<K, V>),
    Version(Version),
}

#[derive(Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum RpcReq {
    FetchChanged { from: Version, count: u64 },
    FetchSnapshot,
}

#[derive(Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum FetchChangedError {
    MissingData,
}

#[derive(Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum RpcRes<K, V> {
    FetchChanged(Result<Vec<Changed<K, V>>, FetchChangedError>),
    FetchSnapshot { slots: Vec<(K, Slot<V>)>, version: Version },
}

#[derive(Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum RpcEvent<K, V> {
    RpcReq(RpcReq),
    RpcRes(RpcRes<K, V>),
}

#[derive(Debug, PartialEq, Eq)]
pub enum KvEvent<N, K, V> {
    Set(Option<N>, K, V),
    Del(Option<N>, K),
}

#[derive(Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum NetEvent<N, K, V> {
    Broadcast(BroadcastEvent<K, V>),
    Unicast(N, RpcEvent<K, V>),
}

#[derive(Debug, PartialEq, Eq)]
pub enum Event<N, K, V> {
    NetEvent(NetEvent<N, K, V>),
    KvEvent(KvEvent<N, K, V>),
}

pub struct ReplicatedKvStore<N, K, V> {
    remotes: HashMap<N, RemoteStore<N, K, V>>,
    local: LocalStore<N, K, V>,
    outs: VecDeque<Event<N, K, V>>,
}

impl<N, K, V> ReplicatedKvStore<N, K, V>
where
    N: Eq + Hash + Clone,
    K: Debug + Hash + Ord + Eq + Clone,
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
            self.outs.push_back(event);
        }
        self.remotes.retain(|_, remote| {
            let keep = remote.last_active().elapsed().as_millis() < REMOTE_TIMEOUT_MS;
            if !keep {
                remote.destroy();
                while let Some(event) = remote.pop_out() {
                    self.outs.push_back(event);
                }
            }
            keep
        });
    }

    pub fn set(&mut self, key: K, value: V) {
        self.local.set(key.clone(), value.clone());
        while let Some(event) = self.local.pop_out() {
            self.outs.push_back(event);
        }
    }

    pub fn del(&mut self, key: K) {
        self.local.del(key.clone());
        while let Some(event) = self.local.pop_out() {
            self.outs.push_back(event);
        }
    }

    pub fn on_remote_event(&mut self, from: N, event: NetEvent<N, K, V>) {
        if !self.remotes.contains_key(&from) {
            let mut remote = RemoteStore::new(from.clone());
            while let Some(event) = remote.pop_out() {
                self.outs.push_back(event);
            }
            self.remotes.insert(from.clone(), remote);
        }

        match event {
            NetEvent::Broadcast(event) => {
                if let Some(remote) = self.remotes.get_mut(&from) {
                    remote.on_broadcast(event);
                    while let Some(event) = remote.pop_out() {
                        self.outs.push_back(event);
                    }
                }
            }
            NetEvent::Unicast(_from, event) => match event {
                RpcEvent::RpcReq(rpc_req) => {
                    self.local.on_rpc_req(from, rpc_req);
                    while let Some(event) = self.local.pop_out() {
                        self.outs.push_back(event);
                    }
                }
                RpcEvent::RpcRes(rpc_res) => {
                    if let Some(remote) = self.remotes.get_mut(&from) {
                        remote.on_rpc_res(rpc_res);
                        while let Some(event) = remote.pop_out() {
                            self.outs.push_back(event);
                        }
                    }
                }
            },
        }
    }

    pub fn pop(&mut self) -> Option<Event<N, K, V>> {
        self.outs.pop_front()
    }
}

pub struct ReplicatedKvService<K, V> {
    service: P2pService,
    tick: Interval,
    store: ReplicatedKvStore<PeerId, K, V>,
}

impl<K, V> ReplicatedKvService<K, V>
where
    K: Debug + Hash + Ord + Eq + Clone + DeserializeOwned + Serialize,
    V: Debug + Eq + Clone + DeserializeOwned + Serialize,
{
    pub fn new(service: P2pService) -> Self {
        Self {
            service,
            tick: tokio::time::interval(std::time::Duration::from_millis(1000)),
            store: ReplicatedKvStore::new(),
        }
    }

    pub fn set(&mut self, key: K, value: V) {
        self.store.set(key, value);
    }

    pub fn del(&mut self, key: K) {
        self.store.del(key);
    }

    pub async fn recv(&mut self) -> Option<KvEvent<PeerId, K, V>> {
        loop {
            if let Some(event) = self.store.pop() {
                match event {
                    Event::NetEvent(net_event) => match net_event {
                        NetEvent::Broadcast(broadcast_event) => {
                            let _ = self.service.try_send_broadcast(bincode::serialize(&broadcast_event).expect("should serialize")).await;
                            continue;
                        }
                        NetEvent::Unicast(to_node, rpc_event) => {
                            let _ = self.service.try_send_unicast(to_node, bincode::serialize(&rpc_event).expect("should serialize")).await;
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
                        match bincode::deserialize::<RpcEvent<K, V>>(&vec) {
                            Ok(event) => self.store.on_remote_event(peer_id, NetEvent::Unicast(peer_id, event)),
                            Err(err) => log::error!("[ReplicatedKvService] deserialize error {err}"),
                        }
                    }
                    super::P2pServiceEvent::Broadcast(peer_id, vec) => {
                        match bincode::deserialize::<BroadcastEvent<K, V>>(&vec) {
                            Ok(event) => self.store.on_remote_event(peer_id, NetEvent::Broadcast(event)),
                            Err(err) => log::error!("[ReplicatedKvService] deserialize error {err}"),
                        }
                    }
                    super::P2pServiceEvent::Stream(..) => {}
                }
            }
        }
    }
}
