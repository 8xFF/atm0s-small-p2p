//! Task runner for a single connection
//! This must ensure not blocking by other actor.
//! We have some strict rules
//!
//! - Only use async with current connection stream
//! - For other communication should use try_send for avoiding blocking

use std::{
    net::SocketAddr,
    time::{Duration, Instant},
};

use anyhow::anyhow;
use futures::{SinkExt, StreamExt};
use quinn::{Connection, RecvStream, SendStream};
use serde::{Deserialize, Serialize};
use tokio::{
    io::copy_bidirectional,
    select,
    sync::mpsc::{Receiver, Sender},
    time::Interval,
};
use tokio_util::codec::Framed;

use crate::{
    ctx::SharedCtx,
    msg::{P2pServiceId, PeerMessage, StreamConnectReq, StreamConnectRes},
    router::RouteAction,
    stream::{wait_object, write_object, BincodeCodec, P2pQuicStream},
    utils::ErrorExt,
    ConnectionId, MainEvent, P2pServiceEvent, PeerId, PeerMainData,
};

use super::PeerConnectionControl;

const OPEN_BI_TIMEOUT: Duration = Duration::from_secs(2);
const MAX_CONTROL_STREAM_PKT: usize = 60000;

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
pub struct PeerConnectionMetric {
    pub uptime: u64,
    pub rtt: u16,
    pub sent_pkt: u64,
    pub lost_pkt: u64,
    pub lost_bytes: u64,
    pub send_bytes: u64,
    pub recv_bytes: u64,
    pub current_mtu: u16,
}

pub struct PeerConnectionInternal {
    conn_id: ConnectionId,
    to_id: PeerId,
    ctx: SharedCtx,
    remote: SocketAddr,
    connection: Connection,
    framed: Framed<P2pQuicStream, BincodeCodec<PeerMessage>>,
    main_tx: Sender<MainEvent>,
    control_rx: Receiver<PeerConnectionControl>,
    ticker: Interval,
    started: Instant,
}

impl PeerConnectionInternal {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        ctx: SharedCtx,
        conn_id: ConnectionId,
        to_id: PeerId,
        connection: Connection,
        main_send: SendStream,
        main_recv: RecvStream,
        main_tx: Sender<MainEvent>,
        control_rx: Receiver<PeerConnectionControl>,
    ) -> Self {
        let stream = P2pQuicStream::new(main_recv, main_send);

        Self {
            conn_id,
            to_id,
            ctx,
            remote: connection.remote_address(),
            connection,
            framed: Framed::new(stream, BincodeCodec::default()),
            main_tx,
            control_rx,
            ticker: tokio::time::interval(Duration::from_secs(1)),
            started: Instant::now(),
        }
    }

    pub async fn run_loop(&mut self) -> anyhow::Result<()> {
        loop {
            select! {
                _ = self.ticker.tick() => {
                    let rtt_ms = self.connection.rtt().as_millis().min(u16::MAX as u128) as u16;
                    let connection_stats = self.connection.stats();
                    self.ctx.router().set_direct(self.conn_id, self.to_id, rtt_ms);
                    let metrics = PeerConnectionMetric {
                        uptime: self.started.elapsed().as_secs(),
                        sent_pkt: connection_stats.path.sent_packets,
                        lost_pkt: connection_stats.path.lost_packets,
                        lost_bytes: connection_stats.path.lost_bytes,
                        rtt: rtt_ms,
                        send_bytes: connection_stats.udp_tx.bytes,
                        recv_bytes: connection_stats.udp_rx.bytes,
                        current_mtu: connection_stats.path.current_mtu,
                    };
                    let _ = self.main_tx.try_send(MainEvent::PeerStats(self.conn_id, self.to_id, metrics));
                },
                open = self.connection.accept_bi() => {
                    let (send, recv) = open?;
                    self.on_accept_bi(send, recv).await?;
                },
                frame = self.framed.next() => {
                    let msg = frame.ok_or(anyhow!("peer main stream ended"))??;
                    self.on_msg(msg).await?;
                },
                control = self.control_rx.recv() => {
                    let control = control.ok_or(anyhow!("peer control channel ended"))?;
                    self.on_control(control).await?;
                }
            }
        }
    }

    async fn on_accept_bi(&mut self, send: SendStream, recv: RecvStream) -> anyhow::Result<()> {
        log::info!("[PeerConnectionInternal {}] on new bi", self.remote);
        let stream = P2pQuicStream::new(recv, send);
        tokio::spawn(accept_bi(self.to_id, stream, self.ctx.clone()));
        Ok(())
    }

    async fn on_control(&mut self, control: PeerConnectionControl) -> anyhow::Result<()> {
        match control {
            PeerConnectionControl::Send(item) => Ok(self.framed.send(item).await?),
            PeerConnectionControl::OpenStream(service, source, dest, meta, tx) => {
                let remote = self.remote;
                let connection = self.connection.clone();
                tokio::spawn(async move {
                    log::info!("[PeerConnectionInternal {remote}] open_bi for service {service}");
                    let res = open_bi(connection, source, dest, service, meta).await;
                    if let Err(e) = &res {
                        log::error!("[PeerConnectionInternal {remote}] open_bi for service {service} error {e}");
                    } else {
                        log::info!("[PeerConnectionInternal {remote}] open_bi for service {service} success");
                    }
                    tx.send(res).map_err(|_| "internal channel error").print_on_err("[PeerConnectionInternal] answer open_bi");
                });
                Ok(())
            }
        }
    }

    async fn on_msg(&mut self, msg: PeerMessage) -> anyhow::Result<()> {
        match msg {
            PeerMessage::Sync { route, advertise } => {
                if let Err(_e) = self.main_tx.try_send(MainEvent::PeerData(self.conn_id, self.to_id, PeerMainData::Sync { route, advertise })) {
                    log::warn!("[PeerConnectionInternal {}] queue main loop full", self.remote);
                }
            }
            PeerMessage::Broadcast(source, service_id, msg_id, data) => {
                if self.ctx.check_broadcast_msg(msg_id) {
                    for conn in self.ctx.conns().into_iter().filter(|p| !self.to_id.eq(&p.to_id())) {
                        conn.try_send(PeerMessage::Broadcast(source, service_id, msg_id, data.clone()))
                            .print_on_err("[PeerConnectionInternal] broadcast data over peer alias");
                    }

                    if let Some(service) = self.ctx.get_service(&service_id) {
                        log::debug!("[PeerConnectionInternal {}] broadcast msg {msg_id} to service {service_id}", self.remote);
                        service.try_send(P2pServiceEvent::Broadcast(source, data)).print_on_err("[PeerConnectionInternal] send service msg");
                    } else {
                        log::warn!("[PeerConnectionInternal {}] broadcast msg to unknown service {service_id}", self.remote);
                    }
                } else {
                    log::debug!("[PeerConnectionInternal {}] broadcast msg {msg_id} already deliveried", self.remote);
                }
            }
            PeerMessage::Unicast(source, dest, service_id, data) => match self.ctx.router().action(&dest) {
                Some(RouteAction::Local) => {
                    if let Some(service) = self.ctx.get_service(&service_id) {
                        service.try_send(P2pServiceEvent::Unicast(source, data)).print_on_err("[PeerConnectionInternal] send service msg");
                    } else {
                        log::warn!("[PeerConnectionInternal {}] service {service_id} not found", self.remote);
                    }
                }
                Some(RouteAction::Next(next)) => {
                    if let Some(conn) = self.ctx.conn(&next) {
                        conn.try_send(PeerMessage::Unicast(source, dest, service_id, data))
                            .print_on_err("[PeerConnectionInternal] send data over peer alias");
                    } else {
                        log::warn!("[PeerConnectionInternal {}] peer {next} not found", self.remote);
                    }
                }
                None => {
                    log::warn!("[PeerConnectionInternal {}] path to {dest} not found", self.remote);
                }
            },
        }
        Ok(())
    }
}

async fn open_bi(connection: Connection, source: PeerId, dest: PeerId, service: P2pServiceId, meta: Vec<u8>) -> anyhow::Result<P2pQuicStream> {
    let (send, recv) = tokio::time::timeout(OPEN_BI_TIMEOUT, connection.open_bi()).await??;
    let mut stream = P2pQuicStream::new(recv, send);
    write_object::<_, _, MAX_CONTROL_STREAM_PKT>(&mut stream, &StreamConnectReq { source, dest, service, meta }).await?;
    let res = wait_object::<_, StreamConnectRes, MAX_CONTROL_STREAM_PKT>(&mut stream).await?;
    res.map(|_| stream).map_err(|e| anyhow!("{e}"))
}

async fn accept_bi(to_peer: PeerId, mut stream: P2pQuicStream, ctx: SharedCtx) -> anyhow::Result<()> {
    let req = wait_object::<_, StreamConnectReq, MAX_CONTROL_STREAM_PKT>(&mut stream).await?;
    let StreamConnectReq { dest, source, service, meta } = req;
    match ctx.router().action(&dest) {
        Some(RouteAction::Local) => {
            if let Some(service_tx) = ctx.get_service(&service) {
                log::info!("[PeerConnectionInternal {to_peer}] stream service {service} source {source} to dest {dest} => process local");
                write_object::<_, _, MAX_CONTROL_STREAM_PKT>(&mut stream, &Ok::<_, String>(())).await?;
                service_tx
                    .send(P2pServiceEvent::Stream(source, meta, stream))
                    .await
                    .print_on_err("[PeerConnectionInternal] send accepted stream to service");
                Ok(())
            } else {
                log::warn!("[PeerConnectionInternal {to_peer}] stream service {service} source {source} to dest {dest} => service not found");
                write_object::<_, _, MAX_CONTROL_STREAM_PKT>(&mut stream, &Err::<(), _>("service not found".to_string())).await?;
                Err(anyhow!("service not found"))
            }
        }
        Some(RouteAction::Next(next)) => {
            if let Some(alias) = ctx.conn(&next) {
                log::info!("[PeerConnectionInternal {to_peer}] stream service {service} source {source} to dest {dest} => forward to {next}");
                match alias.open_stream(service, source, dest, meta).await {
                    Ok(mut next_stream) => {
                        write_object::<_, _, MAX_CONTROL_STREAM_PKT>(&mut stream, &Ok::<_, String>(())).await?;
                        log::info!("[PeerConnectionInternal {to_peer}] stream service {service} source {source} to dest {dest} => start copy_bidirectional");
                        match copy_bidirectional(&mut next_stream, &mut stream).await {
                            Ok(stats) => {
                                log::info!("[PeerConnectionInternal {to_peer}] stream service {service} source {source} to dest {dest} done {stats:?}");
                            }
                            Err(err) => {
                                log::error!("[PeerConnectionInternal {to_peer}] stream service {service} source {source} to dest {dest} err {err}");
                            }
                        }
                        Ok(())
                    }
                    Err(err) => {
                        log::error!("[PeerConnectionInternal {to_peer}] stream service {service} source {source} to dest {dest} => open bi error {err}");
                        write_object::<_, _, MAX_CONTROL_STREAM_PKT>(&mut stream, &Err::<(), _>(err.to_string())).await?;
                        Err(err)
                    }
                }
            } else {
                log::warn!("[PeerConnectionInternal {to_peer}] new stream with service {service} source {source} to dest {dest} => but connection for next {next} not found");
                write_object::<_, _, MAX_CONTROL_STREAM_PKT>(&mut stream, &Err::<(), _>("route not found".to_string())).await?;
                Err(anyhow!("route not found"))
            }
        }
        None => {
            log::warn!("[PeerConnectionInternal {to_peer}] new stream with service {service} source {source} to dest {dest} => but route path not found");
            write_object::<_, _, MAX_CONTROL_STREAM_PKT>(&mut stream, &Err::<(), _>("route not found".to_string())).await?;
            Err(anyhow!("route not found"))
        }
    }
}
