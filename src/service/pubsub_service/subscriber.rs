use anyhow::anyhow;
use derive_more::derive::Display;
use tokio::sync::mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender};

use super::{InternalMsg, PeerSrc, PubsubChannelId};

#[derive(Debug, Display, Hash, PartialEq, Eq, Clone, Copy)]
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
    Data(Vec<u8>),
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
