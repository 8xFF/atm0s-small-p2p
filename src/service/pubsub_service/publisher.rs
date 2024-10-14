use anyhow::anyhow;
use derive_more::derive::Display;
use tokio::sync::mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender};

use super::{InternalMsg, PeerSrc, PubsubChannelId};

#[derive(Debug, Display, Hash, PartialEq, Eq, Clone, Copy)]
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

    pub async fn send(&self, data: Vec<u8>) -> anyhow::Result<()> {
        self.control_tx.send(InternalMsg::Publish(self.local_id, self.channel_id, data))?;
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
