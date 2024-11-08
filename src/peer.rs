use std::{net::SocketAddr, sync::Arc, time::Duration};

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
    now_ms,
    secure::HandshakeProtocol,
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
    pub fn new_incoming<SECURE: HandshakeProtocol>(secure: Arc<SECURE>, local_id: PeerId, incoming: Incoming, internal_tx: Sender<InternalEvent>, ctx: SharedCtx) -> Self {
        let remote = incoming.remote_address();
        let conn_id = ConnectionId::rand();

        tokio::spawn(async move {
            log::info!("[PeerConnection {conn_id}] wait incoming from {remote}");
            match incoming.await {
                Ok(connection) => {
                    log::info!("[PeerConnection {conn_id}] got connection from {remote}");
                    match connection.accept_bi().await {
                        Ok((send, recv)) => {
                            if let Err(e) = run_connection(secure, ctx, remote, conn_id, local_id, PeerConnectionDirection::Incoming, &connection, send, recv, internal_tx).await {
                                log::error!("[PeerConnection {conn_id}] connection from {remote} error {e}");
                                let _ = tokio::time::timeout(Duration::from_secs(2), connection.closed()).await;
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

    pub fn new_connecting<SECURE: HandshakeProtocol>(secure: Arc<SECURE>, local_id: PeerId, to_peer: PeerId, connecting: Connecting, internal_tx: Sender<InternalEvent>, ctx: SharedCtx) -> Self {
        let remote = connecting.remote_address();
        let conn_id = ConnectionId::rand();

        tokio::spawn(async move {
            match connecting.await {
                Ok(connection) => {
                    log::info!("[PeerConnection {conn_id}] connected to {remote}");
                    match connection.open_bi().await {
                        Ok((send, recv)) => {
                            if let Err(e) = run_connection(secure, ctx, remote, conn_id, local_id, PeerConnectionDirection::Outgoing(to_peer), &connection, send, recv, internal_tx).await {
                                log::error!("[PeerConnection {conn_id}] connection to {remote} error {e}");
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
    auth: Vec<u8>,
}

#[derive(Debug, Serialize, Deserialize)]
struct ConnectRes {
    result: Result<Vec<u8>, String>,
}

#[allow(clippy::too_many_arguments)]
async fn run_connection<SECURE: HandshakeProtocol>(
    secure: Arc<SECURE>,
    ctx: SharedCtx,
    remote: SocketAddr,
    conn_id: ConnectionId,
    local_id: PeerId,
    direction: PeerConnectionDirection,
    connection: &Connection,
    mut send: SendStream,
    mut recv: RecvStream,
    internal_tx: Sender<InternalEvent>,
) -> anyhow::Result<()> {
    let to_id = if let PeerConnectionDirection::Outgoing(dest) = direction {
        let auth = secure.create_request(local_id, dest, now_ms());
        write_object::<_, _, 500>(&mut send, &ConnectReq { from: local_id, to: dest, auth }).await?;
        let res: ConnectRes = wait_object::<_, _, 500>(&mut recv).await?;
        log::info!("{res:?}");
        match res.result {
            Ok(auth) => {
                if let Err(e) = secure.verify_response(auth, dest, local_id, now_ms()) {
                    return Err(anyhow!("destination auth failure: {e}"));
                }
                dest
            }
            Err(err) => {
                return Err(anyhow!("destination rejected: {err}"));
            }
        }
    } else {
        let req: ConnectReq = wait_object::<_, _, 500>(&mut recv).await?;
        if let Err(e) = secure.verify_request(req.auth, req.from, req.to, now_ms()) {
            write_object::<_, _, 500>(&mut send, &ConnectRes { result: Err(e.clone()) }).await?;
            return Err(anyhow!("destination auth failure: {e}"));
        } else if req.to != local_id {
            write_object::<_, _, 500>(
                &mut send,
                &ConnectRes {
                    result: Err("destination not match".to_owned()),
                },
            )
            .await?;
            return Err(anyhow!("destination wrong"));
        } else {
            let auth = secure.create_response(req.to, req.from, now_ms());
            write_object::<_, _, 500>(&mut send, &ConnectRes { result: Ok(auth) }).await?;
            req.from
        }
    };

    let rtt_ms = connection.rtt().as_millis().min(u16::MAX as u128) as u16;
    let (control_tx, control_rx) = channel(10);
    let alias = PeerConnectionAlias::new(local_id, to_id, conn_id, control_tx);
    let mut internal = PeerConnectionInternal::new(ctx.clone(), conn_id, to_id, connection.clone(), send, recv, internal_tx.clone(), control_rx);
    log::info!("[PeerConnection {conn_id}] started {remote}, rtt: {rtt_ms}");
    ctx.register_conn(conn_id, alias);
    internal_tx.send(InternalEvent::PeerConnected(conn_id, to_id, rtt_ms)).await.expect("should send to main");
    log::info!("[PeerConnection {conn_id}] run loop for {remote}");
    if let Err(e) = internal.run_loop().await {
        log::error!("[PeerConnection {conn_id}] {remote} error {e}");
    }
    internal_tx.send(InternalEvent::PeerDisconnected(conn_id, to_id)).await.expect("should send to main");
    log::info!("[PeerConnection {conn_id}] end loop for {remote}");
    ctx.unregister_conn(&conn_id);
    Ok(())
}
