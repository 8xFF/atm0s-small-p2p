//! Simple PubsubService with multi-publishers, multi-subscribers style
//!
//! We trying to implement a pubsub service with only Unicast and Broadcast, without any database.
//! Each time new producer is created or destroyed, it will broadcast to all other nodes, same with new subscriber.
//!
//! For avoiding channel state out-of-sync, we add simple heatbeat, each some seconds each node will broadcast a list of active channel with flag publish and subscribe.

use std::{
    collections::{HashMap, HashSet},
    time::{Duration, Instant},
};

use anyhow::anyhow;
use derive_more::derive::{Display, From};
use publisher::PublisherLocalId;
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use subscriber::SubscriberLocalId;
use thiserror::Error;
use tokio::{
    select,
    sync::{
        mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender},
        oneshot,
    },
    time::Interval,
};

use crate::{ErrorExt, PeerId};

use super::{P2pService, P2pServiceEvent};

mod publisher;
mod subscriber;

pub use publisher::{Publisher, PublisherEvent, PublisherEventOb, PublisherRequester};
pub use subscriber::{Subscriber, SubscriberEvent, SubscriberEventOb, SubscriberRequester};

const HEATBEAT_INTERVAL_MS: u64 = 5_000;
const RPC_TICK_INTERVAL_MS: u64 = 1_000;

#[derive(Debug, Hash, PartialEq, Eq, Clone)]
pub enum PeerSrc {
    Local,
    Remote(PeerId),
}

#[derive(Debug, Display, Clone, Copy, Serialize, Deserialize, Hash, PartialEq, Eq)]
pub struct RpcId(u64);

impl RpcId {
    pub fn rand() -> Self {
        RpcId(rand::random())
    }
}

#[derive(Debug, Serialize, Deserialize)]
struct ChannelHeatbeat {
    channel: PubsubChannelId,
    publish: bool,
    subscribe: bool,
}

#[derive(Debug, Error, PartialEq, Eq)]
pub enum PubsubRpcError {
    #[error("Timeout")]
    Timeout,
    #[error("NoDestination")]
    NoDestination,
}

struct PublishRpcReq {
    started_at: Instant,
    timeout: Duration,
    tx: Option<oneshot::Sender<Result<Vec<u8>, PubsubRpcError>>>,
}

struct FeedbackRpcReq {
    started_at: Instant,
    timeout: Duration,
    tx: Option<oneshot::Sender<Result<Vec<u8>, PubsubRpcError>>>,
}

#[derive(Debug, Serialize, Deserialize)]
enum PubsubMessage {
    PublisherJoined(PubsubChannelId),
    PublisherLeaved(PubsubChannelId),
    SubscriberJoined(PubsubChannelId),
    SubscriberLeaved(PubsubChannelId),
    Heatbeat(Vec<ChannelHeatbeat>),
    GuestPublish(PubsubChannelId, Vec<u8>),
    GuestPublishRpc(PubsubChannelId, Vec<u8>, RpcId, String),
    Publish(PubsubChannelId, Vec<u8>),
    PublishRpc(PubsubChannelId, Vec<u8>, RpcId, String),
    PublishRpcAnswer(Vec<u8>, RpcId),
    GuestFeedback(PubsubChannelId, Vec<u8>),
    GuestFeedbackRpc(PubsubChannelId, Vec<u8>, RpcId, String),
    Feedback(PubsubChannelId, Vec<u8>),
    FeedbackRpc(PubsubChannelId, Vec<u8>, RpcId, String),
    FeedbackRpcAnswer(Vec<u8>, RpcId),
}

enum InternalMsg {
    PublisherCreated(PublisherLocalId, PubsubChannelId, UnboundedSender<PublisherEvent>),
    PublisherDestroyed(PublisherLocalId, PubsubChannelId),
    SubscriberCreated(SubscriberLocalId, PubsubChannelId, UnboundedSender<SubscriberEvent>),
    SubscriberDestroyed(SubscriberLocalId, PubsubChannelId),
    GuestPublish(PubsubChannelId, Vec<u8>),
    GuestPublishRpc(PubsubChannelId, Vec<u8>, String, oneshot::Sender<Result<Vec<u8>, PubsubRpcError>>, Duration),
    Publish(PublisherLocalId, PubsubChannelId, Vec<u8>),
    PublishRpc(PublisherLocalId, PubsubChannelId, Vec<u8>, String, oneshot::Sender<Result<Vec<u8>, PubsubRpcError>>, Duration),
    PublishRpcAnswer(RpcId, PeerSrc, Vec<u8>),
    GuestFeedback(PubsubChannelId, Vec<u8>),
    GuestFeedbackRpc(PubsubChannelId, Vec<u8>, String, oneshot::Sender<Result<Vec<u8>, PubsubRpcError>>, Duration),
    Feedback(SubscriberLocalId, PubsubChannelId, Vec<u8>),
    FeedbackRpc(SubscriberLocalId, PubsubChannelId, Vec<u8>, String, oneshot::Sender<Result<Vec<u8>, PubsubRpcError>>, Duration),
    FeedbackRpcAnswer(RpcId, PeerSrc, Vec<u8>),
}

#[derive(Debug, From, Display, Serialize, Deserialize, Clone, Copy, Hash, PartialEq, Eq)]
pub struct PubsubChannelId(u64);

#[derive(Debug, Clone)]
pub struct PubsubServiceRequester {
    internal_tx: UnboundedSender<InternalMsg>,
}

#[derive(Debug, Default)]
struct PubsubChannelState {
    remote_publishers: HashSet<PeerId>,
    remote_subscribers: HashSet<PeerId>,
    local_publishers: HashMap<PublisherLocalId, UnboundedSender<PublisherEvent>>,
    local_subscribers: HashMap<SubscriberLocalId, UnboundedSender<SubscriberEvent>>,
}

pub struct PubsubService {
    service: P2pService,
    internal_tx: UnboundedSender<InternalMsg>,
    internal_rx: UnboundedReceiver<InternalMsg>,
    channels: HashMap<PubsubChannelId, PubsubChannelState>,
    publish_rpc_reqs: HashMap<RpcId, PublishRpcReq>,
    feedback_rpc_reqs: HashMap<RpcId, FeedbackRpcReq>,
    heatbeat_tick: Interval,
    rpc_tick: Interval,
}

impl PubsubService {
    pub fn new(service: P2pService) -> Self {
        let (internal_tx, internal_rx) = unbounded_channel();
        Self {
            service,
            internal_rx,
            internal_tx,
            channels: HashMap::new(),
            publish_rpc_reqs: HashMap::new(),
            feedback_rpc_reqs: HashMap::new(),
            heatbeat_tick: tokio::time::interval(Duration::from_millis(HEATBEAT_INTERVAL_MS)),
            rpc_tick: tokio::time::interval(Duration::from_millis(RPC_TICK_INTERVAL_MS)),
        }
    }

    pub fn requester(&self) -> PubsubServiceRequester {
        PubsubServiceRequester {
            internal_tx: self.internal_tx.clone(),
        }
    }

    pub async fn run_loop(&mut self) -> anyhow::Result<()> {
        loop {
            select! {
                _ = self.heatbeat_tick.tick() => {
                    self.on_heatbeat_tick().await?;
                },
                _ = self.rpc_tick.tick() => {
                    self.on_rpc_tick().await?;
                },
                e = self.service.recv() => {
                    self.on_service(e.ok_or_else(|| anyhow!("service channel failed"))?).await?;
                },
                e = self.internal_rx.recv() => {
                    self.on_internal(e.ok_or_else(|| anyhow!("internal channel crash"))?).await?;
                },
            }
        }
    }

    async fn on_heatbeat_tick(&mut self) -> anyhow::Result<()> {
        let mut heatbeat = vec![];
        for (channel, state) in self.channels.iter() {
            heatbeat.push(ChannelHeatbeat {
                channel: *channel,
                publish: !state.local_publishers.is_empty(),
                subscribe: !state.local_subscribers.is_empty(),
            });
        }
        self.broadcast(&PubsubMessage::Heatbeat(heatbeat)).await;
        Ok(())
    }

    async fn on_rpc_tick(&mut self) -> anyhow::Result<()> {
        for (_req_id, req) in self.publish_rpc_reqs.iter_mut() {
            if req.started_at.elapsed() >= req.timeout {
                let _ = req.tx.take().expect("should have tx").send(Err(PubsubRpcError::Timeout));
            }
        }

        for (_req_id, req) in self.feedback_rpc_reqs.iter_mut() {
            if req.started_at.elapsed() >= req.timeout {
                let _ = req.tx.take().expect("should have tx").send(Err(PubsubRpcError::Timeout));
            }
        }

        self.publish_rpc_reqs.retain(|_req_id, req| req.tx.is_some());
        self.feedback_rpc_reqs.retain(|_req_id, req| req.tx.is_some());

        Ok(())
    }

    async fn on_service(&mut self, event: P2pServiceEvent) -> anyhow::Result<()> {
        match event {
            P2pServiceEvent::Unicast(from_peer, vec) | P2pServiceEvent::Broadcast(from_peer, vec) => {
                if let Ok(msg) = bincode::deserialize::<PubsubMessage>(&vec) {
                    match msg {
                        PubsubMessage::PublisherJoined(channel) => {
                            if let Some(state) = self.channels.get_mut(&channel) {
                                if state.remote_publishers.insert(from_peer) {
                                    log::info!("[PubsubService] remote peer {from_peer} joined to {channel} as publisher");
                                    // we have new remote publisher then we fire event to local
                                    for (_, sub_tx) in state.local_subscribers.iter() {
                                        let _ = sub_tx.send(SubscriberEvent::PeerJoined(PeerSrc::Remote(from_peer)));
                                    }
                                    // we also send subscribe state it remote, as publisher it only care about wherever this node is a subscriber
                                    if !state.local_subscribers.is_empty() {
                                        self.send_to(from_peer, &PubsubMessage::SubscriberJoined(channel)).await;
                                    }
                                }
                            }
                        }
                        PubsubMessage::PublisherLeaved(channel) => {
                            if let Some(state) = self.channels.get_mut(&channel) {
                                if state.remote_publishers.remove(&from_peer) {
                                    log::info!("[PubsubService] remote peer {from_peer} leaved from {channel} as publisher");
                                    // we have remove remote publisher then we fire event to local
                                    for (_, sub_tx) in state.local_subscribers.iter() {
                                        let _ = sub_tx.send(SubscriberEvent::PeerLeaved(PeerSrc::Remote(from_peer)));
                                    }
                                }
                            }
                        }
                        PubsubMessage::SubscriberJoined(channel) => {
                            if let Some(state) = self.channels.get_mut(&channel) {
                                if state.remote_subscribers.insert(from_peer) {
                                    log::info!("[PubsubService] remote peer {from_peer} joined to {channel} as subscriber");
                                    // we have new remote publisher then we fire event to local
                                    for (_, pub_tx) in state.local_publishers.iter() {
                                        let _ = pub_tx.send(PublisherEvent::PeerJoined(PeerSrc::Remote(from_peer)));
                                    }
                                    // we also send publisher state it remote, as subscriber it only care about wherever this node is a publisher
                                    if !state.local_publishers.is_empty() {
                                        self.send_to(from_peer, &PubsubMessage::PublisherJoined(channel)).await;
                                    }
                                }
                            }
                        }
                        PubsubMessage::SubscriberLeaved(channel) => {
                            if let Some(state) = self.channels.get_mut(&channel) {
                                if state.remote_subscribers.remove(&from_peer) {
                                    log::info!("[PubsubService] remote peer {from_peer} leaved from {channel} as subscriber");
                                    // we have remove remote publisher then we fire event to local
                                    for (_, pub_tx) in state.local_publishers.iter() {
                                        let _ = pub_tx.send(PublisherEvent::PeerLeaved(PeerSrc::Remote(from_peer)));
                                    }
                                }
                            }
                        }
                        PubsubMessage::Heatbeat(channels) => {
                            for heatbeat in channels {
                                if let Some(state) = self.channels.get_mut(&heatbeat.channel) {
                                    if heatbeat.publish && !state.remote_publishers.contains(&from_peer) {
                                        // it we out-of-sync from peer then add it to list then fire event
                                        state.remote_publishers.insert(from_peer);
                                        for (_, sub_tx) in state.local_subscribers.iter() {
                                            let _ = sub_tx.send(SubscriberEvent::PeerJoined(PeerSrc::Remote(from_peer)));
                                        }
                                    }

                                    if heatbeat.subscribe && !state.remote_subscribers.contains(&from_peer) {
                                        // it we out-of-sync from peer then add it to list then fire event
                                        state.remote_subscribers.insert(from_peer);
                                        for (_, pub_tx) in state.local_publishers.iter() {
                                            let _ = pub_tx.send(PublisherEvent::PeerJoined(PeerSrc::Remote(from_peer)));
                                        }
                                    }
                                }
                            }
                        }
                        PubsubMessage::GuestPublish(channel, data) => {
                            if let Some(state) = self.channels.get(&channel) {
                                for (_, sub_tx) in state.local_subscribers.iter() {
                                    let _ = sub_tx.send(SubscriberEvent::GuestPublish(data.clone()));
                                }
                            }
                        }
                        PubsubMessage::GuestPublishRpc(channel, data, rpc_id, method) => {
                            if let Some(state) = self.channels.get(&channel) {
                                for (_, sub_tx) in state.local_subscribers.iter() {
                                    let _ = sub_tx.send(SubscriberEvent::GuestPublishRpc(data.clone(), rpc_id, method.clone(), PeerSrc::Remote(from_peer)));
                                }
                            }
                        }
                        PubsubMessage::Publish(channel, vec) => {
                            if let Some(state) = self.channels.get(&channel) {
                                for (_, sub_tx) in state.local_subscribers.iter() {
                                    let _ = sub_tx.send(SubscriberEvent::Publish(vec.clone()));
                                }
                            }
                        }
                        PubsubMessage::PublishRpc(channel, vec, rpc_id, method) => {
                            if let Some(state) = self.channels.get(&channel) {
                                for (_, sub_tx) in state.local_subscribers.iter() {
                                    let _ = sub_tx.send(SubscriberEvent::PublishRpc(vec.clone(), rpc_id, method.clone(), PeerSrc::Remote(from_peer)));
                                }
                            }
                        }
                        PubsubMessage::PublishRpcAnswer(data, rpc_id) => {
                            if let Some(mut req) = self.publish_rpc_reqs.remove(&rpc_id) {
                                let _ = req.tx.take().expect("should have req_tx").send(Ok(data));
                            } else {
                                log::warn!("[PubsubService] got PublishRpcAnswer with invalid req_id {rpc_id}");
                            }
                        }
                        PubsubMessage::GuestFeedback(channel, vec) => {
                            if let Some(state) = self.channels.get(&channel) {
                                for (_, pub_tx) in state.local_publishers.iter() {
                                    let _ = pub_tx.send(PublisherEvent::GuestFeedback(vec.clone()));
                                }
                            }
                        }
                        PubsubMessage::GuestFeedbackRpc(channel, vec, rpc_id, method) => {
                            if let Some(state) = self.channels.get(&channel) {
                                for (_, pub_tx) in state.local_publishers.iter() {
                                    let _ = pub_tx.send(PublisherEvent::GuestFeedbackRpc(vec.clone(), rpc_id, method.clone(), PeerSrc::Remote(from_peer)));
                                }
                            }
                        }
                        PubsubMessage::Feedback(channel, vec) => {
                            if let Some(state) = self.channels.get(&channel) {
                                for (_, pub_tx) in state.local_publishers.iter() {
                                    let _ = pub_tx.send(PublisherEvent::Feedback(vec.clone()));
                                }
                            }
                        }
                        PubsubMessage::FeedbackRpc(channel, vec, rpc_id, method) => {
                            if let Some(state) = self.channels.get(&channel) {
                                for (_, pub_tx) in state.local_publishers.iter() {
                                    let _ = pub_tx.send(PublisherEvent::FeedbackRpc(vec.clone(), rpc_id, method.clone(), PeerSrc::Remote(from_peer)));
                                }
                            }
                        }
                        PubsubMessage::FeedbackRpcAnswer(data, rpc_id) => {
                            if let Some(mut req) = self.feedback_rpc_reqs.remove(&rpc_id) {
                                let _ = req.tx.take().expect("should have req_tx").send(Ok(data));
                            } else {
                                log::warn!("[PubsubService] got FeedbackRpcAnswer with invalid req_id {rpc_id}");
                            }
                        }
                    }
                }
                Ok(())
            }
            P2pServiceEvent::Stream(..) => Ok(()),
        }
    }

    async fn on_internal(&mut self, control: InternalMsg) -> anyhow::Result<()> {
        match control {
            InternalMsg::PublisherCreated(local_id, channel, tx) => {
                log::info!("[PubsubService] local created pub channel {channel} / {local_id}");
                let state = self.channels.entry(channel).or_default();
                if !state.local_subscribers.is_empty() {
                    // notify that we already have local subscribers
                    let _ = tx.send(PublisherEvent::PeerJoined(PeerSrc::Local));
                }
                if state.local_publishers.is_empty() {
                    // if this is first local_publisher => notify to all local_subscribers
                    for (_, sub_tx) in state.local_subscribers.iter() {
                        let _ = sub_tx.send(SubscriberEvent::PeerJoined(PeerSrc::Local));
                    }
                }
                state.local_publishers.insert(local_id, tx);
                self.broadcast(&PubsubMessage::PublisherJoined(channel)).await;
            }
            InternalMsg::PublisherDestroyed(local_id, channel) => {
                log::info!("[PubsubService] local destroyed pub channel {channel} / {local_id}");

                let state = self.channels.entry(channel).or_default();
                state.local_publishers.remove(&local_id);
                if state.local_publishers.is_empty() {
                    // if this is last local_publisher => notify all subscribers
                    for (_, sub_tx) in state.local_subscribers.iter() {
                        let _ = sub_tx.send(SubscriberEvent::PeerLeaved(PeerSrc::Local));
                    }
                }
                self.broadcast(&PubsubMessage::PublisherLeaved(channel)).await;
            }
            InternalMsg::SubscriberCreated(local_id, channel, tx) => {
                log::info!("[PubsubService] local created sub channel {channel} / {local_id}");
                let state = self.channels.entry(channel).or_default();
                if !state.local_publishers.is_empty() {
                    // notify that we already have local publishers
                    let _ = tx.send(SubscriberEvent::PeerJoined(PeerSrc::Local));
                }
                if state.local_subscribers.is_empty() {
                    // if this is first local_subscriber => notify to all local_publishers
                    for (_, pub_tx) in state.local_publishers.iter() {
                        let _ = pub_tx.send(PublisherEvent::PeerJoined(PeerSrc::Local));
                    }
                }
                state.local_subscribers.insert(local_id, tx);
                self.broadcast(&PubsubMessage::SubscriberJoined(channel)).await;
            }
            InternalMsg::SubscriberDestroyed(local_id, channel) => {
                log::info!("[PubsubService] local destroyed sub channel {channel} / {local_id}");
                let state = self.channels.entry(channel).or_default();
                state.local_subscribers.remove(&local_id);
                if state.local_subscribers.is_empty() {
                    // if this is last local_subscriber => notify all publishers
                    for (_, pub_tx) in state.local_publishers.iter() {
                        let _ = pub_tx.send(PublisherEvent::PeerLeaved(PeerSrc::Local));
                    }
                }
                self.broadcast(&PubsubMessage::SubscriberLeaved(channel)).await;
            }
            InternalMsg::GuestPublish(channel, vec) => {
                if let Some(state) = self.channels.get(&channel) {
                    for (_, sub_tx) in state.local_subscribers.iter() {
                        let _ = sub_tx.send(SubscriberEvent::GuestPublish(vec.clone()));
                    }
                    for sub_peer in state.remote_subscribers.iter() {
                        let _ = self.send_to(*sub_peer, &PubsubMessage::GuestPublish(channel, vec.clone())).await;
                    }
                }
            }
            InternalMsg::GuestPublishRpc(channel, data, method, tx, timeout) => {
                if let Some(state) = self.channels.get(&channel) {
                    let req_id = RpcId::rand();
                    if state.local_subscribers.is_empty() && state.remote_subscribers.is_empty() {
                        let _ = tx.send(Err(PubsubRpcError::NoDestination));
                    } else {
                        for (_, pub_tx) in state.local_subscribers.iter() {
                            let _ = pub_tx.send(SubscriberEvent::GuestPublishRpc(data.clone(), req_id, method.clone(), PeerSrc::Local));
                        }
                        for pub_peer in state.remote_subscribers.iter() {
                            let _ = self.send_to(*pub_peer, &PubsubMessage::GuestPublishRpc(channel, data.clone(), req_id, method.clone())).await;
                        }
                        self.publish_rpc_reqs.insert(
                            req_id,
                            PublishRpcReq {
                                started_at: Instant::now(),
                                timeout,
                                tx: Some(tx),
                            },
                        );
                    }
                } else {
                    let _ = tx.send(Err(PubsubRpcError::NoDestination));
                }
            }
            InternalMsg::Publish(_local_id, channel, vec) => {
                if let Some(state) = self.channels.get(&channel) {
                    for (_, sub_tx) in state.local_subscribers.iter() {
                        let _ = sub_tx.send(SubscriberEvent::Publish(vec.clone()));
                    }
                    for sub_peer in state.remote_subscribers.iter() {
                        let _ = self.send_to(*sub_peer, &PubsubMessage::Publish(channel, vec.clone())).await;
                    }
                }
            }
            InternalMsg::PublishRpc(_local_id, channel, data, method, tx, timeout) => {
                if let Some(state) = self.channels.get(&channel) {
                    let req_id = RpcId::rand();
                    if state.local_subscribers.is_empty() && state.remote_subscribers.is_empty() {
                        let _ = tx.send(Err(PubsubRpcError::NoDestination));
                    } else {
                        for (_, pub_tx) in state.local_subscribers.iter() {
                            let _ = pub_tx.send(SubscriberEvent::PublishRpc(data.clone(), req_id, method.clone(), PeerSrc::Local));
                        }
                        for pub_peer in state.remote_subscribers.iter() {
                            let _ = self.send_to(*pub_peer, &PubsubMessage::PublishRpc(channel, data.clone(), req_id, method.clone())).await;
                        }
                        self.publish_rpc_reqs.insert(
                            req_id,
                            PublishRpcReq {
                                started_at: Instant::now(),
                                timeout,
                                tx: Some(tx),
                            },
                        );
                    }
                } else {
                    let _ = tx.send(Err(PubsubRpcError::NoDestination));
                }
            }
            InternalMsg::GuestFeedback(channel, vec) => {
                if let Some(state) = self.channels.get(&channel) {
                    for (_, pub_tx) in state.local_publishers.iter() {
                        let _ = pub_tx.send(PublisherEvent::GuestFeedback(vec.clone()));
                    }
                    for pub_peer in state.remote_publishers.iter() {
                        let _ = self.send_to(*pub_peer, &PubsubMessage::GuestFeedback(channel, vec.clone())).await;
                    }
                }
            }
            InternalMsg::GuestFeedbackRpc(channel, data, method, tx, timeout) => {
                if let Some(state) = self.channels.get(&channel) {
                    let req_id = RpcId::rand();
                    if state.local_publishers.is_empty() && state.remote_publishers.is_empty() {
                        let _ = tx.send(Err(PubsubRpcError::NoDestination));
                    } else {
                        for (_, pub_tx) in state.local_publishers.iter() {
                            let _ = pub_tx.send(PublisherEvent::GuestFeedbackRpc(data.clone(), req_id, method.clone(), PeerSrc::Local));
                        }
                        for pub_peer in state.remote_publishers.iter() {
                            let _ = self.send_to(*pub_peer, &PubsubMessage::GuestFeedbackRpc(channel, data.clone(), req_id, method.clone())).await;
                        }
                        self.feedback_rpc_reqs.insert(
                            req_id,
                            FeedbackRpcReq {
                                started_at: Instant::now(),
                                timeout,
                                tx: Some(tx),
                            },
                        );
                    }
                } else {
                    let _ = tx.send(Err(PubsubRpcError::NoDestination));
                }
            }
            InternalMsg::Feedback(_local_id, channel, vec) => {
                if let Some(state) = self.channels.get(&channel) {
                    for (_, pub_tx) in state.local_publishers.iter() {
                        let _ = pub_tx.send(PublisherEvent::Feedback(vec.clone()));
                    }
                    for pub_peer in state.remote_publishers.iter() {
                        let _ = self.send_to(*pub_peer, &PubsubMessage::Feedback(channel, vec.clone())).await;
                    }
                }
            }
            InternalMsg::FeedbackRpc(_local_id, channel, data, method, tx, timeout) => {
                if let Some(state) = self.channels.get(&channel) {
                    let req_id = RpcId::rand();
                    if state.local_publishers.is_empty() && state.remote_publishers.is_empty() {
                        let _ = tx.send(Err(PubsubRpcError::NoDestination));
                    } else {
                        for (_, pub_tx) in state.local_publishers.iter() {
                            let _ = pub_tx.send(PublisherEvent::FeedbackRpc(data.clone(), req_id, method.clone(), PeerSrc::Local));
                        }
                        for pub_peer in state.remote_publishers.iter() {
                            let _ = self.send_to(*pub_peer, &PubsubMessage::FeedbackRpc(channel, data.clone(), req_id, method.clone())).await;
                        }
                        self.feedback_rpc_reqs.insert(
                            req_id,
                            FeedbackRpcReq {
                                started_at: Instant::now(),
                                timeout,
                                tx: Some(tx),
                            },
                        );
                    }
                } else {
                    let _ = tx.send(Err(PubsubRpcError::NoDestination));
                }
            }
            InternalMsg::PublishRpcAnswer(rpc_id, peer_src, data) => {
                if let PeerSrc::Remote(peer) = peer_src {
                    let _ = self.send_to(peer, &PubsubMessage::PublishRpcAnswer(data, rpc_id)).await;
                } else {
                    #[allow(clippy::collapsible_if)]
                    if let Some(mut req) = self.publish_rpc_reqs.remove(&rpc_id) {
                        let _ = req.tx.take().expect("should have req_tx").send(Ok(data));
                    } else {
                        log::warn!("[PubsubService] got local PublishRpcAnswer with invalid req_id {rpc_id}");
                    }
                }
            }
            InternalMsg::FeedbackRpcAnswer(rpc_id, peer_src, data) => {
                if let PeerSrc::Remote(peer) = peer_src {
                    let _ = self.send_to(peer, &PubsubMessage::FeedbackRpcAnswer(data, rpc_id)).await;
                } else {
                    #[allow(clippy::collapsible_if)]
                    if let Some(mut req) = self.feedback_rpc_reqs.remove(&rpc_id) {
                        let _ = req.tx.take().expect("should have req_tx").send(Ok(data));
                    } else {
                        log::warn!("[PubsubService] got local FeedbackRpcAnswer with invalid req_id {rpc_id}");
                    }
                }
            }
        }
        Ok(())
    }

    async fn send_to(&self, dest: PeerId, msg: &PubsubMessage) {
        self.service
            .send_unicast(dest, bincode::serialize(msg).expect("should convert to binary"))
            .await
            .print_on_err("[PubsubService] send data");
    }

    async fn broadcast(&self, msg: &PubsubMessage) {
        self.service.send_broadcast(bincode::serialize(msg).expect("should convert to binary")).await;
    }
}

impl PubsubServiceRequester {
    pub async fn publish_as_guest(&self, channel: PubsubChannelId, data: Vec<u8>) -> anyhow::Result<()> {
        self.internal_tx.send(InternalMsg::GuestPublish(channel, data))?;
        Ok(())
    }

    pub async fn publish_as_guest_ob<Ob: Serialize>(&self, channel: PubsubChannelId, ob: Ob) -> anyhow::Result<()> {
        let data = bincode::serialize(&ob).expect("should serialize");
        self.publish_as_guest(channel, data).await
    }

    pub async fn publish_rpc_as_guest(&self, channel: PubsubChannelId, method: &str, data: Vec<u8>, timeout: Duration) -> anyhow::Result<Vec<u8>> {
        let (tx, rx) = oneshot::channel::<Result<Vec<u8>, PubsubRpcError>>();
        self.internal_tx.send(InternalMsg::GuestPublishRpc(channel, data, method.to_owned(), tx, timeout))?;
        let data = rx.await??;
        Ok(data)
    }

    pub async fn publish_rpc_as_guest_ob<REQ: Serialize, RES: DeserializeOwned>(&self, channel: PubsubChannelId, method: &str, req: REQ, timeout: Duration) -> anyhow::Result<RES> {
        let data = bincode::serialize(&req).expect("should serialize");
        let res = self.publish_rpc_as_guest(channel, method, data, timeout).await?;
        Ok(bincode::deserialize(&res)?)
    }

    pub async fn feedback_as_guest(&self, channel: PubsubChannelId, data: Vec<u8>) -> anyhow::Result<()> {
        self.internal_tx.send(InternalMsg::GuestFeedback(channel, data))?;
        Ok(())
    }

    pub async fn feedback_as_guest_ob<Ob: Serialize>(&self, channel: PubsubChannelId, ob: Ob) -> anyhow::Result<()> {
        let data = bincode::serialize(&ob).expect("should serialize");
        self.feedback_as_guest(channel, data).await
    }

    pub async fn feedback_rpc_as_guest(&self, channel: PubsubChannelId, method: &str, data: Vec<u8>, timeout: Duration) -> anyhow::Result<Vec<u8>> {
        let (tx, rx) = oneshot::channel::<Result<Vec<u8>, PubsubRpcError>>();
        self.internal_tx.send(InternalMsg::GuestFeedbackRpc(channel, data, method.to_owned(), tx, timeout))?;
        let data = rx.await??;
        Ok(data)
    }

    pub async fn feedback_rpc_as_guest_ob<REQ: Serialize, RES: DeserializeOwned>(&self, channel: PubsubChannelId, method: &str, req: REQ, timeout: Duration) -> anyhow::Result<RES> {
        let data = bincode::serialize(&req).expect("should serialize");
        let res = self.feedback_rpc_as_guest(channel, method, data, timeout).await?;
        Ok(bincode::deserialize(&res)?)
    }

    pub async fn publisher(&self, channel: PubsubChannelId) -> Publisher {
        Publisher::build(channel, self.internal_tx.clone())
    }

    pub async fn subscriber(&self, channel: PubsubChannelId) -> Subscriber {
        Subscriber::build(channel, self.internal_tx.clone())
    }
}
