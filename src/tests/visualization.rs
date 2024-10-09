use std::time::Duration;

use test_log::test;

use crate::visualization_service::{VisualizationService, VisualizationServiceEvent};

use super::create_node;

#[test(tokio::test)]
async fn discovery_new_node() {
    let (mut node1, addr1) = create_node(true, 1, vec![]).await;
    let mut service1 = VisualizationService::new(None, false, node1.create_service(0.into()));
    tokio::spawn(async move { while let Ok(_) = node1.recv().await {} });
    tokio::spawn(async move { while let Ok(_) = service1.recv().await {} });

    let (mut node2, addr2) = create_node(false, 2, vec![addr1.clone()]).await;
    let mut service2 = VisualizationService::new(Some(Duration::from_secs(1)), false, node2.create_service(0.into()));
    tokio::spawn(async move { while let Ok(_) = node2.recv().await {} });

    tokio::time::sleep(Duration::from_secs(1)).await;

    let mut events = vec![
        tokio::time::timeout(Duration::from_secs(3), service2.recv()).await.expect("").expect(""),
        tokio::time::timeout(Duration::from_secs(3), service2.recv()).await.expect("").expect(""),
    ];

    for event in events.iter_mut() {
        match event {
            VisualizationServiceEvent::PeerJoined(_, neighbours) | VisualizationServiceEvent::PeerUpdated(_, neighbours) => {
                for (conn, _, rtt) in neighbours.iter_mut() {
                    *conn = 0.into();
                    *rtt = 0;
                }
            }
            VisualizationServiceEvent::PeerLeaved(_) => {}
        }
    }

    assert_eq!(
        events,
        vec![
            VisualizationServiceEvent::PeerJoined(addr1.peer_id(), vec![(0.into(), addr2.peer_id(), 0)]),
            VisualizationServiceEvent::PeerUpdated(addr1.peer_id(), vec![(0.into(), addr2.peer_id(), 0)]),
        ]
    );
}
