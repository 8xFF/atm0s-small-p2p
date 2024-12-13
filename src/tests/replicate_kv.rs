use std::time::Duration;

use test_log::test;
use tokio::time::timeout;

use crate::replicate_kv_service::{KvEvent, ReplicatedKvService};

use super::create_node;

const WAIT: Duration = Duration::from_secs(3);

#[test(tokio::test)]
async fn single_node() {
    let (mut node1, _addr1) = create_node(true, 1, vec![]).await;
    let mut kv1: ReplicatedKvService<u16, u16> = ReplicatedKvService::new(node1.create_service(0.into()));
    tokio::spawn(async move { while node1.recv().await.is_ok() {} });

    kv1.set(1, 1);

    assert_eq!(timeout(WAIT, kv1.recv()).await, Ok(Some(KvEvent::Set(None, 1, 1))));
}

#[test(tokio::test)]
async fn full_sync() {
    let (mut node1, addr1) = create_node(true, 1, vec![]).await;
    let (mut node2, addr2) = create_node(true, 2, vec![]).await;

    let node1_requester = node1.requester();

    let mut kv1: ReplicatedKvService<u16, u16> = ReplicatedKvService::new(node1.create_service(0.into()));
    let mut kv2: ReplicatedKvService<u16, u16> = ReplicatedKvService::new(node2.create_service(0.into()));

    tokio::spawn(async move {
        kv1.set(1, 1);
        kv1.set(2, 2);
        kv1.set(3, 3);
        while let Some(_event) = kv1.recv().await {}
    });

    tokio::spawn(async move { while node1.recv().await.is_ok() {} });
    tokio::spawn(async move { while node2.recv().await.is_ok() {} });

    tokio::time::sleep(Duration::from_millis(1000)).await;
    node1_requester.connect(addr2).await.expect("should connect success");

    assert_eq!(timeout(WAIT, kv2.recv()).await, Ok(Some(KvEvent::Set(Some(addr1.peer_id()), 1, 1))));
    assert_eq!(timeout(WAIT, kv2.recv()).await, Ok(Some(KvEvent::Set(Some(addr1.peer_id()), 2, 2))));
    assert_eq!(timeout(WAIT, kv2.recv()).await, Ok(Some(KvEvent::Set(Some(addr1.peer_id()), 3, 3))));
}

#[test(tokio::test)]
async fn continuos_sync() {
    let (mut node1, addr1) = create_node(true, 1, vec![]).await;
    let (mut node2, _addr2) = create_node(true, 2, vec![addr1.clone()]).await;

    let mut kv1: ReplicatedKvService<u16, u16> = ReplicatedKvService::new(node1.create_service(0.into()));
    let mut kv2: ReplicatedKvService<u16, u16> = ReplicatedKvService::new(node2.create_service(0.into()));

    tokio::spawn(async move { while node1.recv().await.is_ok() {} });
    tokio::spawn(async move { while node2.recv().await.is_ok() {} });

    tokio::time::sleep(Duration::from_millis(1000)).await;

    tokio::spawn(async move {
        kv1.set(1, 1);
        kv1.set(2, 2);
        kv1.set(3, 3);
        while let Some(_event) = kv1.recv().await {}
    });

    assert_eq!(timeout(WAIT, kv2.recv()).await, Ok(Some(KvEvent::Set(Some(addr1.peer_id()), 1, 1))));
    assert_eq!(timeout(WAIT, kv2.recv()).await, Ok(Some(KvEvent::Set(Some(addr1.peer_id()), 2, 2))));
    assert_eq!(timeout(WAIT, kv2.recv()).await, Ok(Some(KvEvent::Set(Some(addr1.peer_id()), 3, 3))));
}
