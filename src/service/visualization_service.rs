use std::{
    collections::{HashMap, VecDeque},
    time::Duration,
};

use serde::{Deserialize, Serialize};
use tokio::{select, time::Interval};

use crate::{now_ms, ErrorExt, PeerAddress};

use super::{P2pService, P2pServiceEvent};

#[derive(Debug, PartialEq, Eq)]
pub enum VisualizationServiceEvent {
    PeerJoined(PeerAddress, Vec<(PeerAddress, u16)>),
    PeerUpdated(PeerAddress, Vec<(PeerAddress, u16)>),
    PeerLeaved(PeerAddress),
}

#[derive(Debug, Serialize, Deserialize)]
enum Message {
    Scan,
    Info(Vec<(PeerAddress, u16)>),
}

pub struct VisualizationService {
    service: P2pService,
    neighbours: HashMap<PeerAddress, u64>,
    ticker: Interval,
    collect_interval: Option<Duration>,
    collect_me: bool,
    outs: VecDeque<VisualizationServiceEvent>,
}

impl VisualizationService {
    pub fn new(collect_interval: Option<Duration>, collect_me: bool, service: P2pService) -> Self {
        let ticker = tokio::time::interval(collect_interval.unwrap_or(Duration::from_secs(100)));

        Self {
            ticker,
            collect_interval,
            collect_me,
            neighbours: HashMap::new(),
            outs: if collect_me {
                VecDeque::from([VisualizationServiceEvent::PeerJoined(service.router().local_address(), vec![])])
            } else {
                VecDeque::new()
            },
            service,
        }
    }

    pub async fn recv(&mut self) -> anyhow::Result<VisualizationServiceEvent> {
        loop {
            if let Some(out) = self.outs.pop_front() {
                return Ok(out);
            }

            select! {
                _ = self.ticker.tick() => {
                    if let Some(interval) = self.collect_interval {
                        if self.collect_me {
                            // for update local node
                            self.outs.push_back(VisualizationServiceEvent::PeerUpdated(self.service.router().local_address(), self.service.router().neighbours()));
                        }

                        let requester = self.service.requester();
                        tokio::spawn(async move {
                            requester.send_broadcast(bincode::serialize(&Message::Scan).expect("should convert to buf")).await;
                        });

                        let now = now_ms();
                        let mut timeout_peers = vec![];
                        for (peer, last_updated) in self.neighbours.iter() {
                            if now >= *last_updated + interval.as_millis() as u64 * 2 {
                                timeout_peers.push(*peer);
                                self.outs.push_back(VisualizationServiceEvent::PeerLeaved(*peer));
                            }
                        }

                        for peer in timeout_peers {
                            self.neighbours.remove(&peer);
                        }
                    }
                }
                event = self.service.recv() => match event.expect("should work") {
                    P2pServiceEvent::Unicast(from, data) | P2pServiceEvent::Broadcast(from, data) => {
                        if let Ok(msg) = bincode::deserialize::<Message>(&data) {
                            match msg {
                                Message::Scan => {
                                    let requester = self.service.requester();
                                    let neighbours: Vec<(PeerAddress, u16)> = requester.router().neighbours();
                                    tokio::spawn(async move {
                                        requester
                                            .send_unicast(from, bincode::serialize(&Message::Info(neighbours)).expect("should convert to buf"))
                                            .await
                                            .print_on_err("send neighbour info to visualization collector");
                                    });
                                }
                                Message::Info(neighbours) => {
                                    if self.neighbours.insert(from, now_ms()).is_none() {
                                        self.outs.push_back(VisualizationServiceEvent::PeerJoined(from, neighbours));
                                    } else {
                                        self.outs.push_back(VisualizationServiceEvent::PeerUpdated(from, neighbours));
                                    }
                                }
                            }
                        }
                    }
                    P2pServiceEvent::Stream(..) => {}
                }
            }
        }
    }
}
