use std::time::Duration;

use test_log::test;

use crate::alias_service::{AliasFoundLocation, AliasId, AliasService};

use super::create_node;

#[test(tokio::test)]
async fn alias_guard() {
    let (mut node1, _addr1) = create_node(true, 1, vec![]).await;
    let mut service1 = AliasService::new(node1.create_service(0.into()));
    let service1_requester = service1.requester();
    tokio::spawn(async move { while let Ok(_) = node1.recv().await {} });
    tokio::spawn(async move { service1.run_loop().await });

    // we register alias before connect
    let alias_id: AliasId = 1000.into();
    let alia_guard = service1_requester.register(alias_id);

    tokio::time::sleep(Duration::from_secs(1)).await;
    assert_eq!(service1_requester.find(alias_id).await, Some(AliasFoundLocation::Local));
    drop(alia_guard);

    tokio::time::sleep(Duration::from_secs(1)).await;
    assert_eq!(service1_requester.find(alias_id).await, None);
}

#[test(tokio::test)]
async fn alias_multi_guards() {
    let (mut node1, _addr1) = create_node(true, 1, vec![]).await;
    let mut service1 = AliasService::new(node1.create_service(0.into()));
    let service1_requester = service1.requester();
    tokio::spawn(async move { while let Ok(_) = node1.recv().await {} });
    tokio::spawn(async move { service1.run_loop().await });

    // we register alias before connect
    let alias_id: AliasId = 1000.into();
    let alia_guard1 = service1_requester.register(alias_id);
    let alia_guard2 = service1_requester.register(alias_id);

    tokio::time::sleep(Duration::from_secs(1)).await;
    assert_eq!(service1_requester.find(alias_id).await, Some(AliasFoundLocation::Local));
    drop(alia_guard1);

    tokio::time::sleep(Duration::from_secs(1)).await;
    assert_eq!(service1_requester.find(alias_id).await, Some(AliasFoundLocation::Local));
    drop(alia_guard2);

    tokio::time::sleep(Duration::from_secs(1)).await;
    assert_eq!(service1_requester.find(alias_id).await, None);
}

#[test(tokio::test)]
async fn alias_scan() {
    let (mut node1, addr1) = create_node(true, 1, vec![]).await;
    let mut service1 = AliasService::new(node1.create_service(0.into()));
    let service1_requester = service1.requester();
    tokio::spawn(async move { while let Ok(_) = node1.recv().await {} });
    tokio::spawn(async move { service1.run_loop().await });

    let (mut node2, _addr2) = create_node(false, 2, vec![addr1.clone()]).await;
    let mut service2 = AliasService::new(node2.create_service(0.into()));
    let service2_requester = service2.requester();
    tokio::spawn(async move { while let Ok(_) = node2.recv().await {} });
    tokio::spawn(async move { service2.run_loop().await });

    // we register alias before connect
    let alias_id: AliasId = 1000.into();
    let _alia_guard = service1_requester.register(alias_id);

    tokio::time::sleep(Duration::from_secs(1)).await;
    assert_eq!(service2_requester.find(alias_id).await, Some(AliasFoundLocation::Scan(addr1.peer_id())));
}

#[test(tokio::test)]
async fn alias_hint() {
    let (mut node1, addr1) = create_node(true, 1, vec![]).await;
    let mut service1 = AliasService::new(node1.create_service(0.into()));
    let service1_requester = service1.requester();
    tokio::spawn(async move { while let Ok(_) = node1.recv().await {} });
    tokio::spawn(async move { service1.run_loop().await });

    let (mut node2, _addr2) = create_node(false, 2, vec![addr1.clone()]).await;
    let mut service2 = AliasService::new(node2.create_service(0.into()));
    let service2_requester = service2.requester();
    tokio::spawn(async move { while let Ok(_) = node2.recv().await {} });
    tokio::spawn(async move { service2.run_loop().await });

    tokio::time::sleep(Duration::from_secs(1)).await;

    // we register alias after connect
    let alias_id: AliasId = 1000.into();
    let _alia_guard = service1_requester.register(alias_id);

    tokio::time::sleep(Duration::from_secs(1)).await;

    assert_eq!(service2_requester.find(alias_id).await, Some(AliasFoundLocation::Hint(addr1.peer_id())));
}

#[test(tokio::test)]
async fn alias_timeout() {
    let (mut node1, addr1) = create_node(true, 1, vec![]).await;
    let mut service1 = AliasService::new(node1.create_service(0.into()));
    tokio::spawn(async move { while let Ok(_) = node1.recv().await {} });
    tokio::spawn(async move { service1.run_loop().await });

    let (mut node2, _addr2) = create_node(false, 2, vec![addr1.clone()]).await;
    let mut service2 = AliasService::new(node2.create_service(0.into()));
    let service2_requester = service2.requester();
    tokio::spawn(async move { while let Ok(_) = node2.recv().await {} });
    tokio::spawn(async move { service2.run_loop().await });

    tokio::time::sleep(Duration::from_secs(1)).await;

    let alias_id: AliasId = 1000.into();
    assert_eq!(service2_requester.find(alias_id).await, None);
}
