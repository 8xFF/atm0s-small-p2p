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
};

use local_storage::LocalStore;
use remote_storage::RemoteStore;
use serde::{de::DeserializeOwned, Serialize};
use tokio::{select, time::Interval};

use crate::PeerId;

use super::P2pService;

mod local_storage;
mod messages;
mod remote_storage;

use messages::{BroadcastEvent, Event, NetEvent, RpcEvent};

pub use messages::KvEvent;

const REMOTE_TIMEOUT_MS: u128 = 10_000;

pub struct ReplicatedKvStore<N, K, V> {
    remotes: HashMap<N, RemoteStore<N, K, V>>,
    local: LocalStore<N, K, V>,
    outs: VecDeque<Event<N, K, V>>,
}

impl<N, K, V> ReplicatedKvStore<N, K, V>
where
    N: Debug + Eq + Hash + Clone,
    K: Debug + Hash + Ord + Eq + Clone,
    V: Debug + Eq + Clone,
{
    pub fn new(max_changeds: usize, max_compose_pkts: usize) -> Self {
        ReplicatedKvStore {
            remotes: HashMap::new(),
            local: LocalStore::new(max_changeds, max_compose_pkts),
            outs: VecDeque::new(),
        }
    }

    pub fn on_tick(&mut self) {
        self.local.on_tick();
        while let Some(event) = self.local.pop_out() {
            self.outs.push_back(event);
        }
        self.remotes.retain(|node, remote| {
            let keep = remote.last_active().elapsed().as_millis() < REMOTE_TIMEOUT_MS;
            if !keep {
                log::info!("[ReplicatedKvService] remove remote {node:?} after timeout");
                remote.destroy();
            } else {
                remote.on_tick();
            }
            while let Some(event) = remote.pop_out() {
                self.outs.push_back(event);
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
            log::info!("[ReplicatedKvService] add remote {from:?}");
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

    fn pop(&mut self) -> Option<Event<N, K, V>> {
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
    pub fn new(service: P2pService, max_changeds: usize, max_compose_pkts: usize) -> Self {
        Self {
            service,
            tick: tokio::time::interval(std::time::Duration::from_millis(1000)),
            store: ReplicatedKvStore::new(max_changeds, max_compose_pkts),
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
