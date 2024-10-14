//! Simple PubsubService with multi-publishers, multi-subscribers style
//!
//! We trying to implement a pubsub service with only Unicast and Broadcast, without any database.
//! Each time new producer is created or destroyed, it will broadcast to all other nodes, same with new subscriber.
//!
//! For avoiding channel state out-of-sync, we add simple heatbeat, each some seconds each node will broadcast a list of active channel with flag publish and subscribe.

use std::{
    collections::{HashMap, HashSet},
    time::Duration,
};

use anyhow::anyhow;
use derive_more::derive::{Display, From};
use publisher::PublisherLocalId;
use serde::{Deserialize, Serialize};
use subscriber::SubscriberLocalId;
use tokio::{
    select,
    sync::mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender},
    time::Interval,
};

use crate::{ErrorExt, PeerId};

use super::{P2pService, P2pServiceEvent};

mod publisher;
mod subscriber;

pub use publisher::{Publisher, PublisherEvent};
pub use subscriber::{Subscriber, SubscriberEvent};

const HEATBEAT_INTERVAL_MS: u64 = 5_000;

#[derive(Debug, Hash, PartialEq, Eq)]
pub enum PeerSrc {
    Local,
    Remote(PeerId),
}

#[derive(Debug, Serialize, Deserialize)]
struct ChannelHeatbeat {
    channel: PubsubChannelId,
    publish: bool,
    subscribe: bool,
}

#[derive(Debug, Serialize, Deserialize)]
enum PubsubMessage {
    PublisherJoined(PubsubChannelId),
    PublisherLeaved(PubsubChannelId),
    SubscriberJoined(PubsubChannelId),
    SubscriberLeaved(PubsubChannelId),
    Heatbeat(Vec<ChannelHeatbeat>),
    Data(PubsubChannelId, Vec<u8>),
    Feedback(PubsubChannelId, Vec<u8>),
}

enum InternalMsg {
    PublisherCreated(PublisherLocalId, PubsubChannelId, UnboundedSender<PublisherEvent>),
    PublisherDestroyed(PublisherLocalId, PubsubChannelId),
    SubscriberCreated(SubscriberLocalId, PubsubChannelId, UnboundedSender<SubscriberEvent>),
    SubscriberDestroyed(SubscriberLocalId, PubsubChannelId),
    Publish(PublisherLocalId, PubsubChannelId, Vec<u8>),
    Feedback(SubscriberLocalId, PubsubChannelId, Vec<u8>),
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
    tick: Interval,
}

impl PubsubService {
    pub fn new(service: P2pService) -> Self {
        let (internal_tx, internal_rx) = unbounded_channel();
        Self {
            service,
            internal_rx,
            internal_tx,
            channels: HashMap::new(),
            tick: tokio::time::interval(Duration::from_millis(HEATBEAT_INTERVAL_MS)),
        }
    }

    pub fn requester(&self) -> PubsubServiceRequester {
        PubsubServiceRequester {
            internal_tx: self.internal_tx.clone(),
        }
    }

    pub async fn recv(&mut self) -> anyhow::Result<()> {
        select! {
            _ = self.tick.tick() => {
                self.on_tick().await
            },
            e = self.service.recv() => {
                self.on_service(e.ok_or_else(|| anyhow!("service channel failed"))?).await
            },
            e = self.internal_rx.recv() => {
                self.on_internal(e.ok_or_else(|| anyhow!("internal channel crash"))?).await
            },
        }
    }

    async fn on_tick(&mut self) -> anyhow::Result<()> {
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
                                    // we also send subscribe state it remote, as publisher it only care about whereever this node is a subscriber
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
                                    // we also send publisher state it remote, as subscriber it only care about whereever this node is a publisher
                                    if !state.local_publishers.is_empty() {
                                        self.send_to(from_peer, &&PubsubMessage::PublisherJoined(channel)).await;
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
                        PubsubMessage::Data(channel, vec) => {
                            if let Some(state) = self.channels.get(&channel) {
                                for (_, sub_tx) in state.local_subscribers.iter() {
                                    let _ = sub_tx.send(SubscriberEvent::Data(vec.clone()));
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
                    // if this is last local_publisher => notify all subscibers
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
                    // if this is first local_subsciber => notify to all local_publishers
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
            InternalMsg::Publish(_local_id, channel, vec) => {
                if let Some(state) = self.channels.get(&channel) {
                    for (_, sub_tx) in state.local_subscribers.iter() {
                        let _ = sub_tx.send(SubscriberEvent::Data(vec.clone()));
                    }
                    for sub_peer in state.remote_subscribers.iter() {
                        let _ = self.send_to(*sub_peer, &PubsubMessage::Data(channel, vec.clone())).await;
                    }
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
    pub async fn publisher(&self, channel: PubsubChannelId) -> Publisher {
        Publisher::build(channel, self.internal_tx.clone())
    }

    pub async fn subscriber(&self, channel: PubsubChannelId) -> Subscriber {
        Subscriber::build(channel, self.internal_tx.clone())
    }
}
