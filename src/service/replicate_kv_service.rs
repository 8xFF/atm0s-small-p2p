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

mod local_storage;
mod remote_storage;

#[derive(Debug, Clone)]
pub struct Slot<V> {
    value: V,
    version: Version,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Action<V> {
    Set(V),
    Del,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Changed<V> {
    pub(crate) key: Key,
    pub(crate) version: Version,
    pub(crate) action: Action<V>,
}

#[derive(Debug, PartialEq, Eq, Hash, Clone, Copy)]
pub struct Key(u64);

#[derive(Debug, PartialEq, Eq, Clone, Copy)]
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

pub enum BroadcastEvent<V> {
    Changed(Changed<V>),
    Version(Version),
}
pub enum RpcReq {
    FetchChanged { from: Version, to: Option<Version> },
    FetchSnapshot,
}

pub enum RpcRes<V> {
    FetchChanged { changes: Vec<Changed<V>>, remain: bool },
    FetchSnapshot { slots: Vec<(Key, Slot<V>)>, version: Version },
}

pub enum KvEvent<N, V> {
    Set(N, Key, V),
    Del(N, Key),
}

pub enum NetEvent<N, V> {
    Broadcast(BroadcastEvent<V>),
    RpcReq(N, RpcReq),
    RpcRes(N, RpcRes<V>),
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

    pub fn on_joined(&mut self, node: N) {
        let mut remote = RemoteStore::new(node.clone());
        while let Some(event) = remote.pop_out() {
            self.outs.push_back(Event::NetEvent(event));
        }
        self.remotes.insert(node, remote);
    }

    pub fn on_left(&mut self, node: N) {
        self.remotes.remove(&node);
    }

    pub fn on_remote_event(&mut self, from: N, event: NetEvent<N, V>) {
        match event {
            NetEvent::Broadcast(event) => {
                if let Some(remote) = self.remotes.get_mut(&from) {
                    remote.on_broadcast(event);
                    while let Some(event) = remote.pop_out() {
                        self.outs.push_back(Event::NetEvent(event));
                    }
                }
            }
            NetEvent::RpcReq(_, rpc_req) => {
                self.local.on_rpc_req(from, rpc_req);
                while let Some(event) = self.local.pop_out() {
                    self.outs.push_back(Event::NetEvent(event));
                }
            }
            NetEvent::RpcRes(_, rpc_res) => {
                if let Some(remote) = self.remotes.get_mut(&from) {
                    remote.on_rpc_res(rpc_res);
                    while let Some(event) = remote.pop_out() {
                        self.outs.push_back(Event::NetEvent(event));
                    }
                }
            }
        }
    }

    pub fn pop(&mut self) -> Option<Event<N, V>> {
        self.outs.pop_front()
    }
}
