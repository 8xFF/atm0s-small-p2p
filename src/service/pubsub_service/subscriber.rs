use std::time::Duration;

use anyhow::anyhow;
use derive_more::derive::Display;
use serde::{Deserialize, Serialize};
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
    Publish(Vec<u8>),
    PublishRpc(Vec<u8>, RpcId, String, PeerSrc),
}

pub struct Subscriber {
    local_id: SubscriberLocalId,
    channel_id: PubsubChannelId,
    control_tx: UnboundedSender<InternalMsg>,
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
            sub_rx,
            control_tx,
        }
    }

    pub async fn feedback(&self, data: Vec<u8>) -> anyhow::Result<()> {
        self.control_tx.send(InternalMsg::Feedback(self.local_id, self.channel_id, data))?;
        Ok(())
    }

    pub async fn feedback_rpc(&self, method: &str, data: Vec<u8>, timeout: Duration) -> anyhow::Result<Vec<u8>> {
        let (tx, rx) = oneshot::channel::<Result<Vec<u8>, PubsubRpcError>>();
        self.control_tx.send(InternalMsg::FeedbackRpc(self.local_id, self.channel_id, data, method.to_owned(), tx, timeout))?;
        let data = rx.await??;
        Ok(data)
    }

    pub async fn answer_publish_rpc(&self, rpc: RpcId, source: PeerSrc, data: Vec<u8>) -> anyhow::Result<()> {
        self.control_tx.send(InternalMsg::PublishRpcAnswer(rpc, source, data))?;
        Ok(())
    }

    pub async fn recv(&mut self) -> anyhow::Result<SubscriberEvent> {
        self.sub_rx.recv().await.ok_or_else(|| anyhow!("internal channel error"))
    }
}

impl Drop for Subscriber {
    fn drop(&mut self) {
        log::info!("[Subscriber {}/{}] destroy", self.channel_id, self.local_id);
        let _ = self.control_tx.send(InternalMsg::SubscriberDestroyed(self.local_id, self.channel_id));
    }
}
