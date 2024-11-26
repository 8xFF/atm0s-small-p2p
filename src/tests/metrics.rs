use std::time::Duration;

use crate::{
    metrics_service::{MetricsService, MetricsServiceEvent},
    PeerId,
};
use test_log::test;

use super::create_node;

#[test(tokio::test)]
async fn metric_collect() {
    let (mut node1, addr1) = create_node(true, 1, vec![]).await;
    let mut service1 = MetricsService::new(None, node1.create_service(0.into()), true);
    tokio::spawn(async move { while node1.recv().await.is_ok() {} });

    let (mut node2, _) = create_node(true, 2, vec![addr1.clone()]).await;
    let mut service2 = MetricsService::new(None, node2.create_service(0.into()), false);
    tokio::spawn(async move { while node2.recv().await.is_ok() {} });
    tokio::spawn(async move { while service2.recv().await.is_ok() {} });

    tokio::time::sleep(Duration::from_secs(1)).await;

    let events = vec![
        tokio::time::timeout(Duration::from_secs(3), service1.recv()).await.expect("").expect(""),
        tokio::time::timeout(Duration::from_secs(3), service1.recv()).await.expect("").expect(""),
        tokio::time::timeout(Duration::from_secs(3), service1.recv()).await.expect("").expect(""),
        tokio::time::timeout(Duration::from_secs(3), service1.recv()).await.expect("").expect(""),
    ];

    let event_from_peers: Vec<(PeerId, usize)> = events
        .iter()
        .map(|e| match e {
            MetricsServiceEvent::OnPeerConnectionMetric(peer, metrics) => (*peer, metrics.len()),
        })
        .collect();

    println!("{:?}", events);

    assert_eq!(event_from_peers, vec![(PeerId(1), 1), (PeerId(1), 1), (PeerId(2), 1), (PeerId(2), 1)]);
}
