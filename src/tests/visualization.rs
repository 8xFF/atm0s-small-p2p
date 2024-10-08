use std::time::Duration;

use test_log::test;

use crate::visualization_service::{VisualizationService, VisualizationServiceEvent};

use super::create_random_node;

#[test(tokio::test)]
async fn discovery_new_node() {
    let (mut node1, addr1) = create_random_node(true).await;
    let mut service1 = VisualizationService::new(None, false, node1.create_service(0.into()));
    tokio::spawn(async move { while let Ok(_) = node1.recv().await {} });
    tokio::spawn(async move { while let Ok(_) = service1.recv().await {} });

    let (mut node2, addr2) = create_random_node(false).await;
    let mut service2 = VisualizationService::new(Some(Duration::from_secs(1)), false, node2.create_service(0.into()));
    let node2_requester = node2.requester();
    tokio::spawn(async move { while let Ok(_) = node2.recv().await {} });

    node2_requester.connect(addr1).await.expect("should connect success");
    tokio::time::sleep(Duration::from_secs(1)).await;

    let mut events = vec![
        tokio::time::timeout(Duration::from_secs(3), service2.recv()).await.unwrap().unwrap(),
        tokio::time::timeout(Duration::from_secs(3), service2.recv()).await.unwrap().unwrap(),
    ];

    for event in events.iter_mut() {
        match event {
            VisualizationServiceEvent::PeerJoined(_, neighbours) | VisualizationServiceEvent::PeerUpdated(_, neighbours) => {
                for (_, rtt) in neighbours.iter_mut() {
                    *rtt = 0;
                }
            }
            VisualizationServiceEvent::PeerLeaved(_) => {}
        }
    }

    assert_eq!(
        events,
        vec![
            VisualizationServiceEvent::PeerJoined(addr1, vec![(addr2, 0)]),
            VisualizationServiceEvent::PeerUpdated(addr1, vec![(addr2, 0)]),
        ]
    );
}
