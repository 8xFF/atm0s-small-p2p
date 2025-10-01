use std::ops::{Add, Deref, Sub};

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Slot<V> {
    pub(super) value: V,
    pub(super) version: Version,
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
pub struct Version(pub(super) u64);

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

#[derive(Debug, PartialEq, Eq, Serialize, Deserialize, Clone)]
pub enum BroadcastEvent<K, V> {
    Changed(Changed<K, V>),
    Version(Version),
}

#[derive(Debug, PartialEq, Eq, Serialize, Deserialize, Clone)]
pub enum RpcReq<K> {
    FetchChanged { from: Version, count: u64 },
    FetchSnapshot { from: Option<K>, to: Option<K>, max_version: Option<Version> },
}

#[derive(Debug, PartialEq, Eq, Serialize, Deserialize, Clone)]
pub enum FetchChangedError {
    MissingData,
}

#[derive(Debug, PartialEq, Eq, Serialize, Deserialize, Clone)]
pub struct SnapshotData<K, V> {
    pub(super) slots: Vec<(K, Slot<V>)>,
    pub(super) next_key: Option<K>,
    pub(super) biggest_key: K,
}

#[derive(Debug, PartialEq, Eq, Serialize, Deserialize, Clone)]
pub enum RpcRes<K, V> {
    FetchChanged(Result<Vec<Changed<K, V>>, FetchChangedError>),
    FetchSnapshot(Option<SnapshotData<K, V>>, Version),
}

#[derive(Debug, PartialEq, Eq, Serialize, Deserialize, Clone)]
pub enum RpcEvent<K, V> {
    RpcReq(RpcReq<K>),
    RpcRes(RpcRes<K, V>),
}

#[derive(Debug, PartialEq, Eq)]
pub enum KvEvent<N, K, V> {
    Set(Option<N>, K, V),
    Del(Option<N>, K),
}

#[derive(Debug, PartialEq, Eq)]
pub enum Event<N, K, V> {
    NetEvent(NetEvent<N, K, V>),
    KvEvent(KvEvent<N, K, V>),
}

#[derive(Debug, PartialEq, Eq, Serialize, Deserialize, Clone)]
pub enum NetEvent<N, K, V> {
    Broadcast(BroadcastEvent<K, V>),
    Unicast(N, RpcEvent<K, V>),
}
