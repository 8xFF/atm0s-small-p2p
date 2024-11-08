use std::{collections::HashSet, sync::Arc, time::Duration};

use super::create_node;
use parking_lot::Mutex;
use test_log::test;

#[test(tokio::test)]
async fn discovery_remain_node() {
    let (mut node1, addr1) = create_node(true, 1, vec![]).await;
    log::info!("created node1 {addr1}");
    tokio::spawn(async move { while node1.recv().await.is_ok() {} });

    let (mut node2, addr2) = create_node(false, 2, vec![addr1]).await;
    log::info!("created node2 {addr2}");
    tokio::spawn(async move { while node2.recv().await.is_ok() {} });

    let (mut node3, addr3) = create_node(false, 3, vec![addr2]).await;
    log::info!("created node3 {addr3}");
    let node3_neighbours = Arc::new(Mutex::new(HashSet::new()));
    let node3_neighbours_c = node3_neighbours.clone();
    tokio::spawn(async move {
        while let Ok(event) = node3.recv().await {
            match event {
                crate::P2pNetworkEvent::PeerConnected(_conn, peer) => {
                    node3_neighbours_c.lock().insert(peer);
                }
                crate::P2pNetworkEvent::PeerDisconnected(_conn, peer) => {
                    node3_neighbours_c.lock().remove(&peer);
                }
                crate::P2pNetworkEvent::Continue => {}
            }
        }
    });

    tokio::time::sleep(Duration::from_secs(1)).await;

    // after some cycle node3 should have node1 as neighbour
    assert_eq!(node3_neighbours.lock().len(), 2);
}
