use std::{
    task::{Context, Poll},
    time::Duration,
};

use anyhow::anyhow;
use derive_more::derive::Display;
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use tokio::sync::{
    mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender},
    oneshot,
};

use super::{InternalMsg, PeerSrc, PubsubChannelId, PubsubRpcError, RpcId};

#[derive(Debug, Display, Hash, PartialEq, Eq, Clone, Copy, Serialize, Deserialize)]
pub struct PublisherLocalId(u64);
impl PublisherLocalId {
    pub fn rand() -> Self {
        Self(rand::random())
    }
}

#[derive(Debug, PartialEq, Eq)]
pub enum PublisherEvent {
    PeerJoined(PeerSrc),
    PeerLeaved(PeerSrc),
    Feedback(Vec<u8>),
    FeedbackRpc(Vec<u8>, RpcId, String, PeerSrc),
    GuestFeedback(Vec<u8>),
    GuestFeedbackRpc(Vec<u8>, RpcId, String, PeerSrc),
}

#[derive(Debug, PartialEq, Eq)]
pub enum PublisherEventOb<Fb> {
    PeerJoined(PeerSrc),
    PeerLeaved(PeerSrc),
    Feedback(Fb),
    FeedbackDeseializeErr(Vec<u8>),
    FeedbackRpc(Fb, RpcId, String, PeerSrc),
    FeedbackRpcDeseializeErr(Vec<u8>, RpcId, String, PeerSrc),
    GuestFeedback(Fb),
    GuestFeedbackDeseializeErr(Vec<u8>),
    GuestFeedbackRpc(Fb, RpcId, String, PeerSrc),
    GuestFeedbackRpcDeseializeErr(Vec<u8>, RpcId, String, PeerSrc),
}

pub struct Publisher {
    local_id: PublisherLocalId,
    channel_id: PubsubChannelId,
    control_tx: UnboundedSender<InternalMsg>,
    requester: PublisherRequester,
    pub_rx: UnboundedReceiver<PublisherEvent>,
}

impl Publisher {
    pub(super) fn build(channel_id: PubsubChannelId, control_tx: UnboundedSender<InternalMsg>) -> Self {
        let (pub_tx, pub_rx) = unbounded_channel();
        let local_id = PublisherLocalId::rand();
        log::info!("[Publisher {channel_id}/{local_id}] created");
        let _ = control_tx.send(InternalMsg::PublisherCreated(local_id, channel_id, pub_tx));

        Self {
            local_id,
            channel_id,
            control_tx: control_tx.clone(),
            requester: PublisherRequester { local_id, channel_id, control_tx },
            pub_rx,
        }
    }

    pub fn requester(&self) -> &PublisherRequester {
        &self.requester
    }

    pub fn poll_recv(&mut self, cx: &mut Context<'_>) -> Poll<Option<PublisherEvent>> {
        self.pub_rx.poll_recv(cx)
    }

    pub async fn recv(&mut self) -> anyhow::Result<PublisherEvent> {
        self.pub_rx.recv().await.ok_or_else(|| anyhow!("internal channel error"))
    }

    pub async fn recv_ob<Fb: DeserializeOwned>(&mut self) -> anyhow::Result<PublisherEventOb<Fb>> {
        let event = match self.recv().await? {
            PublisherEvent::PeerJoined(peer_src) => PublisherEventOb::PeerJoined(peer_src),
            PublisherEvent::PeerLeaved(peer_src) => PublisherEventOb::PeerLeaved(peer_src),
            PublisherEvent::Feedback(data) => {
                if let Ok(ob) = bincode::deserialize(&data) {
                    PublisherEventOb::Feedback(ob)
                } else {
                    PublisherEventOb::FeedbackDeseializeErr(data)
                }
            }
            PublisherEvent::FeedbackRpc(data, rpc_id, method, peer_src) => {
                if let Ok(ob) = bincode::deserialize(&data) {
                    PublisherEventOb::FeedbackRpc(ob, rpc_id, method, peer_src)
                } else {
                    PublisherEventOb::FeedbackRpcDeseializeErr(data, rpc_id, method, peer_src)
                }
            }
            PublisherEvent::GuestFeedback(data) => {
                if let Ok(ob) = bincode::deserialize(&data) {
                    PublisherEventOb::GuestFeedback(ob)
                } else {
                    PublisherEventOb::GuestFeedbackDeseializeErr(data)
                }
            }
            PublisherEvent::GuestFeedbackRpc(data, rpc_id, method, peer_src) => {
                if let Ok(ob) = bincode::deserialize(&data) {
                    PublisherEventOb::GuestFeedbackRpc(ob, rpc_id, method, peer_src)
                } else {
                    PublisherEventOb::GuestFeedbackRpcDeseializeErr(data, rpc_id, method, peer_src)
                }
            }
        };
        Ok(event)
    }
}

impl Drop for Publisher {
    fn drop(&mut self) {
        log::info!("[Publisher {}/{}] destroy", self.channel_id, self.local_id);
        let _ = self.control_tx.send(InternalMsg::PublisherDestroyed(self.local_id, self.channel_id));
    }
}

#[derive(Debug, Clone)]
pub struct PublisherRequester {
    local_id: PublisherLocalId,
    channel_id: PubsubChannelId,
    control_tx: UnboundedSender<InternalMsg>,
}

impl PublisherRequester {
    pub async fn publish(&self, data: Vec<u8>) -> anyhow::Result<()> {
        self.control_tx.send(InternalMsg::Publish(self.local_id, self.channel_id, data))?;
        Ok(())
    }

    pub async fn publish_ob<Ob: Serialize>(&self, ob: &Ob) -> anyhow::Result<()> {
        let data = bincode::serialize(ob).expect("should serialize");
        self.publish(data).await
    }

    pub async fn publish_rpc(&self, method: &str, data: Vec<u8>, timeout: Duration) -> anyhow::Result<Vec<u8>> {
        let (tx, rx) = oneshot::channel::<Result<Vec<u8>, PubsubRpcError>>();
        self.control_tx.send(InternalMsg::PublishRpc(self.local_id, self.channel_id, data, method.to_owned(), tx, timeout))?;
        let data = rx.await??;
        Ok(data)
    }

    pub async fn publish_rpc_ob<REQ: Serialize, RES: DeserializeOwned>(&self, method: &str, req: &REQ, timeout: Duration) -> anyhow::Result<RES> {
        let data = bincode::serialize(req).expect("should convert to buffer");
        let res = self.publish_rpc(method, data, timeout).await?;
        Ok(bincode::deserialize(&res)?)
    }

    pub async fn answer_feedback_rpc(&self, rpc: RpcId, source: PeerSrc, data: Vec<u8>) -> anyhow::Result<()> {
        self.control_tx.send(InternalMsg::FeedbackRpcAnswer(rpc, source, data))?;
        Ok(())
    }

    pub async fn answer_feedback_rpc_ob<RES: Serialize>(&self, rpc: RpcId, source: PeerSrc, res: &RES) -> anyhow::Result<()> {
        self.control_tx.send(InternalMsg::FeedbackRpcAnswer(rpc, source, bincode::serialize(res).expect("should serialize")))?;
        Ok(())
    }
}
