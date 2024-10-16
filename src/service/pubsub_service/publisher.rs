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
}

pub struct Publisher {
    local_id: PublisherLocalId,
    channel_id: PubsubChannelId,
    control_tx: UnboundedSender<InternalMsg>,
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
            pub_rx,
            control_tx,
        }
    }

    pub async fn publish(&self, data: Vec<u8>) -> anyhow::Result<()> {
        self.control_tx.send(InternalMsg::Publish(self.local_id, self.channel_id, data))?;
        Ok(())
    }

    pub async fn publish_rpc(&self, method: &str, data: Vec<u8>, timeout: Duration) -> anyhow::Result<Vec<u8>> {
        let (tx, rx) = oneshot::channel::<Result<Vec<u8>, PubsubRpcError>>();
        self.control_tx.send(InternalMsg::PublishRpc(self.local_id, self.channel_id, data, method.to_owned(), tx, timeout))?;
        let data = rx.await??;
        Ok(data)
    }

    pub async fn answer_feedback_rpc(&self, rpc: RpcId, source: PeerSrc, data: Vec<u8>) -> anyhow::Result<()> {
        self.control_tx.send(InternalMsg::FeedbackRpcAnswer(rpc, source, data))?;
        Ok(())
    }

    pub async fn recv(&mut self) -> anyhow::Result<PublisherEvent> {
        self.pub_rx.recv().await.ok_or_else(|| anyhow!("internal channel error"))
    }
}

impl Drop for Publisher {
    fn drop(&mut self) {
        log::info!("[Publisher {}/{}] destroy", self.channel_id, self.local_id);
        let _ = self.control_tx.send(InternalMsg::PublisherDestroyed(self.local_id, self.channel_id));
    }
}
