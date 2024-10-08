use std::net::SocketAddr;

use anyhow::anyhow;
use peer_internal::PeerConnectionInternal;
use quinn::{Connecting, Connection, Incoming, RecvStream, SendStream};
use serde::{Deserialize, Serialize};
use tokio::sync::{
    mpsc::{channel, Sender},
    oneshot,
};

use crate::{
    ctx::SharedCtx,
    msg::P2pServiceId,
    stream::{wait_object, write_object, P2pQuicStream},
    ConnectionId, PeerId,
};

use super::{msg::PeerMessage, InternalEvent};

mod peer_alias;
mod peer_internal;

pub use peer_alias::PeerConnectionAlias;

enum PeerConnectionControl {
    Send(PeerMessage),
    OpenStream(P2pServiceId, PeerId, PeerId, Vec<u8>, oneshot::Sender<anyhow::Result<P2pQuicStream>>),
}

pub struct PeerConnection {
    conn_id: ConnectionId,
    peer_id: Option<PeerId>,
}

impl PeerConnection {
    pub fn new_incoming(local_id: PeerId, incoming: Incoming, internal_tx: Sender<InternalEvent>, ctx: SharedCtx) -> Self {
        let remote = incoming.remote_address();
        let conn_id = ConnectionId::rand();

        tokio::spawn(async move {
            log::info!("[PeerConnection] wait incoming from {remote}");
            match incoming.await {
                Ok(connection) => {
                    log::info!("[PeerConnection] got connection from {remote}");
                    match connection.accept_bi().await {
                        Ok((send, recv)) => {
                            if let Err(e) = run_connection(ctx, remote, conn_id, local_id, PeerConnectionDirection::Incoming, connection, send, recv, internal_tx).await {
                                log::error!("[PeerConnection] connection from {remote} error {e}");
                            }
                        }
                        Err(err) => internal_tx.send(InternalEvent::PeerConnectError(conn_id, None, err.into())).await.expect("should send to main"),
                    }
                }
                Err(err) => internal_tx.send(InternalEvent::PeerConnectError(conn_id, None, err.into())).await.expect("should send to main"),
            }
        });
        Self { conn_id, peer_id: None }
    }

    pub fn new_connecting(local_id: PeerId, to_peer: PeerId, connecting: Connecting, internal_tx: Sender<InternalEvent>, ctx: SharedCtx) -> Self {
        let remote = connecting.remote_address();
        let conn_id = ConnectionId::rand();

        tokio::spawn(async move {
            match connecting.await {
                Ok(connection) => {
                    log::info!("[PeerConnection] connected to {remote}");
                    match connection.open_bi().await {
                        Ok((send, recv)) => {
                            if let Err(e) = run_connection(ctx, remote, conn_id, local_id, PeerConnectionDirection::Outgoing(to_peer), connection, send, recv, internal_tx).await {
                                log::error!("[PeerConnection] connection from {remote} error {e}");
                            }
                        }
                        Err(err) => internal_tx
                            .send(InternalEvent::PeerConnectError(conn_id, Some(to_peer), err.into()))
                            .await
                            .expect("should send to main"),
                    }
                }
                Err(err) => internal_tx
                    .send(InternalEvent::PeerConnectError(conn_id, Some(to_peer), err.into()))
                    .await
                    .expect("should send to main"),
            }
        });
        Self { conn_id, peer_id: None }
    }

    pub fn conn_id(&self) -> ConnectionId {
        self.conn_id
    }

    pub fn peer_id(&self) -> Option<PeerId> {
        self.peer_id
    }

    pub fn set_connected(&mut self, peer_id: PeerId) {
        self.peer_id = Some(peer_id);
    }

    pub fn is_connected(&self) -> bool {
        self.peer_id.is_some()
    }
}

enum PeerConnectionDirection {
    Incoming,
    Outgoing(PeerId),
}

#[derive(Debug, Serialize, Deserialize)]
struct ConnectReq {
    from: PeerId,
    to: PeerId,
}

#[derive(Debug, Serialize, Deserialize)]
struct ConnectRes {
    success: bool,
}

async fn run_connection(
    ctx: SharedCtx,
    remote: SocketAddr,
    conn_id: ConnectionId,
    local_id: PeerId,
    direction: PeerConnectionDirection,
    connection: Connection,
    mut send: SendStream,
    mut recv: RecvStream,
    internal_tx: Sender<InternalEvent>,
) -> anyhow::Result<()> {
    let to_id = if let PeerConnectionDirection::Outgoing(dest) = direction {
        write_object::<_, _, 500>(&mut send, &ConnectReq { from: local_id, to: dest }).await?;
        let res: ConnectRes = wait_object::<_, _, 500>(&mut recv).await?;
        if !res.success {
            return Err(anyhow!("destination rejected"));
        }
        dest
    } else {
        let req: ConnectReq = wait_object::<_, _, 500>(&mut recv).await?;
        if req.to != local_id {
            write_object::<_, _, 500>(&mut send, &ConnectRes { success: false }).await?;
            return Err(anyhow!("destination wrong"));
        } else {
            write_object::<_, _, 500>(&mut send, &ConnectRes { success: true }).await?;
            req.from
        }
    };

    let rtt_ms = connection.rtt().as_millis().min(u16::MAX as u128) as u16;
    let (control_tx, control_rx) = channel(10);
    let alias = PeerConnectionAlias::new(local_id, to_id, conn_id, control_tx);
    let mut internal = PeerConnectionInternal::new(ctx.clone(), conn_id, to_id, connection.clone(), send, recv, internal_tx.clone(), control_rx);
    log::info!("[PeerConnection] started {remote}, rtt: {rtt_ms}");
    ctx.register_conn(conn_id, alias);
    internal_tx.send(InternalEvent::PeerConnected(conn_id, to_id, rtt_ms)).await.expect("should send to main");
    log::info!("[PeerConnection] run loop for {remote}");
    loop {
        if let Err(e) = internal.recv_complex().await {
            log::error!("[PeerConnection] {remote} error {e}");
            break;
        }
    }
    internal_tx.send(InternalEvent::PeerDisconnected(conn_id, to_id)).await.expect("should send to main");
    log::info!("[PeerConnection] end loop for {remote}");
    ctx.unregister_conn(&conn_id);
    Ok(())
}
