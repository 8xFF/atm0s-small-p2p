use std::time::Duration;

use anyhow::anyhow;
use derive_more::derive::Display;
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use tokio::sync::{
    mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender},
    oneshot,
};

use super::{InternalMsg, PeerSrc, PubsubChannelId, PubsubRpcError, RpcId};

#[derive(Debug, Display, Hash, PartialEq, Eq, Clone, Copy, Serialize, Deserialize)]
pub struct SubscriberLocalId(u64);
impl SubscriberLocalId {
    pub fn rand() -> Self {
        Self(rand::random())
    }
}

#[derive(Debug, PartialEq, Eq)]
pub enum SubscriberEvent {
    PeerJoined(PeerSrc),
    PeerLeaved(PeerSrc),
    GuestPublish(Vec<u8>),
    GuestPublishRpc(Vec<u8>, RpcId, String, PeerSrc),
    Publish(Vec<u8>),
    PublishRpc(Vec<u8>, RpcId, String, PeerSrc),
}

#[derive(Debug, PartialEq, Eq)]
pub enum SubscriberEventOb<Fb> {
    PeerJoined(PeerSrc),
    PeerLeaved(PeerSrc),
    Publish(Fb),
    PublishDeseializeErr(Vec<u8>),
    PublishRpc(Fb, RpcId, String, PeerSrc),
    PublishRpcDeseializeErr(Vec<u8>, RpcId, String, PeerSrc),
    GuestPublish(Fb),
    GuestPublishDeseializeErr(Vec<u8>),
    GuestPublishRpc(Fb, RpcId, String, PeerSrc),
    GuestPublishRpcDeseializeErr(Vec<u8>, RpcId, String, PeerSrc),
}

pub struct Subscriber {
    local_id: SubscriberLocalId,
    channel_id: PubsubChannelId,
    control_tx: UnboundedSender<InternalMsg>,
    requester: SubscriberRequester,
    sub_rx: UnboundedReceiver<SubscriberEvent>,
}

impl Subscriber {
    pub(super) fn build(channel_id: PubsubChannelId, control_tx: UnboundedSender<InternalMsg>) -> Self {
        let (sub_tx, sub_rx) = unbounded_channel();
        let local_id = SubscriberLocalId::rand();
        log::info!("[Subscriber {channel_id}/{local_id}] created");
        let _ = control_tx.send(InternalMsg::SubscriberCreated(local_id, channel_id, sub_tx));

        Self {
            local_id,
            channel_id,
            control_tx: control_tx.clone(),
            requester: SubscriberRequester { local_id, channel_id, control_tx },
            sub_rx,
        }
    }

    pub fn requester(&self) -> &SubscriberRequester {
        &self.requester
    }

    pub async fn recv(&mut self) -> anyhow::Result<SubscriberEvent> {
        self.sub_rx.recv().await.ok_or_else(|| anyhow!("internal channel error"))
    }

    pub async fn recv_ob<Fb: DeserializeOwned>(&mut self) -> anyhow::Result<SubscriberEventOb<Fb>> {
        let event = match self.recv().await? {
            SubscriberEvent::PeerJoined(peer_src) => SubscriberEventOb::PeerJoined(peer_src),
            SubscriberEvent::PeerLeaved(peer_src) => SubscriberEventOb::PeerLeaved(peer_src),
            SubscriberEvent::Publish(data) => {
                if let Ok(ob) = bincode::deserialize(&data) {
                    SubscriberEventOb::Publish(ob)
                } else {
                    SubscriberEventOb::PublishDeseializeErr(data)
                }
            }
            SubscriberEvent::PublishRpc(data, rpc_id, method, peer_src) => {
                if let Ok(ob) = bincode::deserialize(&data) {
                    SubscriberEventOb::PublishRpc(ob, rpc_id, method, peer_src)
                } else {
                    SubscriberEventOb::PublishRpcDeseializeErr(data, rpc_id, method, peer_src)
                }
            }
            SubscriberEvent::GuestPublish(data) => {
                if let Ok(ob) = bincode::deserialize(&data) {
                    SubscriberEventOb::GuestPublish(ob)
                } else {
                    SubscriberEventOb::GuestPublishDeseializeErr(data)
                }
            }
            SubscriberEvent::GuestPublishRpc(data, rpc_id, method, peer_src) => {
                if let Ok(ob) = bincode::deserialize(&data) {
                    SubscriberEventOb::GuestPublishRpc(ob, rpc_id, method, peer_src)
                } else {
                    SubscriberEventOb::GuestPublishRpcDeseializeErr(data, rpc_id, method, peer_src)
                }
            }
        };
        Ok(event)
    }
}

impl Drop for Subscriber {
    fn drop(&mut self) {
        log::info!("[Subscriber {}/{}] destroy", self.channel_id, self.local_id);
        let _ = self.control_tx.send(InternalMsg::SubscriberDestroyed(self.local_id, self.channel_id));
    }
}

#[derive(Debug, Clone)]
pub struct SubscriberRequester {
    local_id: SubscriberLocalId,
    channel_id: PubsubChannelId,
    control_tx: UnboundedSender<InternalMsg>,
}

impl SubscriberRequester {
    pub async fn feedback(&self, data: Vec<u8>) -> anyhow::Result<()> {
        self.control_tx.send(InternalMsg::Feedback(self.local_id, self.channel_id, data))?;
        Ok(())
    }

    pub async fn feedback_ob<Ob: Serialize>(&self, ob: &Ob) -> anyhow::Result<()> {
        let data = bincode::serialize(ob).expect("should serialize");
        self.feedback(data).await
    }

    pub async fn feedback_rpc(&self, method: &str, data: Vec<u8>, timeout: Duration) -> anyhow::Result<Vec<u8>> {
        let (tx, rx) = oneshot::channel::<Result<Vec<u8>, PubsubRpcError>>();
        self.control_tx.send(InternalMsg::FeedbackRpc(self.local_id, self.channel_id, data, method.to_owned(), tx, timeout))?;
        let data = rx.await??;
        Ok(data)
    }

    pub async fn feedback_rpc_ob<REQ: Serialize, RES: DeserializeOwned>(&self, method: &str, req: &REQ, timeout: Duration) -> anyhow::Result<RES> {
        let data = bincode::serialize(req).expect("should convert to buffer");
        let res = self.feedback_rpc(method, data, timeout).await?;
        Ok(bincode::deserialize(&res)?)
    }

    pub async fn answer_publish_rpc(&self, rpc: RpcId, source: PeerSrc, data: Vec<u8>) -> anyhow::Result<()> {
        self.control_tx.send(InternalMsg::PublishRpcAnswer(rpc, source, data))?;
        Ok(())
    }

    pub async fn answer_publish_rpc_ob<RES: Serialize>(&self, rpc: RpcId, source: PeerSrc, res: &RES) -> anyhow::Result<()> {
        self.control_tx.send(InternalMsg::PublishRpcAnswer(rpc, source, bincode::serialize(res).expect("should serialize")))?;
        Ok(())
    }
}
