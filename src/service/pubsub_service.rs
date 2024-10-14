use std::collections::HashMap;

use derive_more::derive::{Display, From};
use publisher::PublisherLocalId;
use serde::{Deserialize, Serialize};
use subscriber::SubscriberLocalId;
use tokio::sync::mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender};

use crate::PeerId;

use super::P2pService;

mod publisher;
mod subscriber;

pub use publisher::{Publisher, PublisherEvent};
pub use subscriber::{Subscriber, SubscriberEvent};

#[derive(Debug, Hash, PartialEq, Eq)]
pub enum PeerSrc {
    Local,
    Remote(PeerId),
}

#[derive(Debug, Serialize, Deserialize)]
enum PubsubMessage {
    PublisherJoined(PubsubChannelId),
    PublisherLeaved(PubsubChannelId),
    SubscriberJoined(PubsubChannelId),
    SubscriberLeaved(PubsubChannelId),
    PublisherHeatbeat(PubsubChannelId),
    SubscriberHeatbeat(PubsubChannelId),
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
    // remote_publishers: HashSet<PeerId>,
    // remote_subscribers: HashSet<PeerId>,
    local_publishers: HashMap<PublisherLocalId, UnboundedSender<PublisherEvent>>,
    local_subscribers: HashMap<SubscriberLocalId, UnboundedSender<SubscriberEvent>>,
}

pub struct PubsubService {
    service: P2pService,
    internal_tx: UnboundedSender<InternalMsg>,
    internal_rx: UnboundedReceiver<InternalMsg>,
    channels: HashMap<PubsubChannelId, PubsubChannelState>,
}

impl PubsubService {
    pub fn new(service: P2pService) -> Self {
        let (internal_tx, internal_rx) = unbounded_channel();
        Self {
            service,
            internal_rx,
            internal_tx,
            channels: HashMap::new(),
        }
    }

    pub fn requester(&self) -> PubsubServiceRequester {
        PubsubServiceRequester {
            internal_tx: self.internal_tx.clone(),
        }
    }

    pub async fn recv(&mut self) -> anyhow::Result<()> {
        let control = self.internal_rx.recv().await.expect("internal channel crash");
        match control {
            InternalMsg::PublisherCreated(local_id, channel, tx) => {
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
            }
            InternalMsg::PublisherDestroyed(local_id, channel) => {
                let state = self.channels.entry(channel).or_default();
                state.local_publishers.remove(&local_id);
                if state.local_publishers.is_empty() {
                    // if this is last local_publisher => notify all subscibers
                    for (_, sub_tx) in state.local_subscribers.iter() {
                        let _ = sub_tx.send(SubscriberEvent::PeerLeaved(PeerSrc::Local));
                    }
                }
            }
            InternalMsg::SubscriberCreated(local_id, channel, tx) => {
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
            }
            InternalMsg::SubscriberDestroyed(local_id, channel) => {
                let state = self.channels.entry(channel).or_default();
                state.local_subscribers.remove(&local_id);
                if state.local_subscribers.is_empty() {
                    // if this is last local_subscriber => notify all publishers
                    for (_, pub_tx) in state.local_publishers.iter() {
                        let _ = pub_tx.send(PublisherEvent::PeerLeaved(PeerSrc::Local));
                    }
                }
            }
            InternalMsg::Publish(_local_id, channel, vec) => {
                if let Some(state) = self.channels.get(&channel) {
                    for (_, sub_tx) in state.local_subscribers.iter() {
                        let _ = sub_tx.send(SubscriberEvent::Data(vec.clone()));
                    }
                }
            }
            InternalMsg::Feedback(_local_id, channel, vec) => {
                if let Some(state) = self.channels.get(&channel) {
                    for (_, pub_tx) in state.local_publishers.iter() {
                        let _ = pub_tx.send(PublisherEvent::Feedback(vec.clone()));
                    }
                }
            }
        }
        Ok(())
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
