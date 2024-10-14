use std::time::Duration;

use test_log::test;
use tokio::time::timeout;

use crate::pubsub_service::{PeerSrc, PublisherEvent, PubsubChannelId, PubsubService, SubscriberEvent};

use super::create_node;

#[test(tokio::test)]
async fn pubsub_local_single_pair_pub_first() {
    let (mut node1, _addr1) = create_node(true, 1, vec![]).await;
    let mut service1 = PubsubService::new(node1.create_service(0.into()));
    let service1_requester = service1.requester();
    tokio::spawn(async move { while let Ok(_) = node1.recv().await {} });
    tokio::spawn(async move { while let Ok(_) = service1.recv().await {} });

    // we create publisher first
    let channel_id: PubsubChannelId = 1000.into();
    let mut publisher = service1_requester.publisher(channel_id).await;
    let mut subscriber = service1_requester.subscriber(channel_id).await;

    let ttl = Duration::from_secs(1);

    assert_eq!(
        timeout(ttl, subscriber.recv()).await.expect("should not timeout").expect("should recv"),
        SubscriberEvent::PeerJoined(PeerSrc::Local)
    );
    assert_eq!(
        timeout(ttl, publisher.recv()).await.expect("should not timeout").expect("should recv"),
        PublisherEvent::PeerJoined(PeerSrc::Local)
    );

    publisher.send(vec![1, 2, 3]).await.expect("should ok");
    assert_eq!(
        timeout(ttl, subscriber.recv()).await.expect("should not timeout").expect("should recv"),
        SubscriberEvent::Data(vec![1, 2, 3])
    );

    subscriber.feedback(vec![2, 3, 4]).await.expect("should ok");
    assert_eq!(
        timeout(ttl, publisher.recv()).await.expect("should not timeout").expect("should recv"),
        PublisherEvent::Feedback(vec![2, 3, 4])
    );
}

#[test(tokio::test)]
async fn pubsub_local_single_pair_sub_first() {
    let (mut node1, _addr1) = create_node(true, 1, vec![]).await;
    let mut service1 = PubsubService::new(node1.create_service(0.into()));
    let service1_requester = service1.requester();
    tokio::spawn(async move { while let Ok(_) = node1.recv().await {} });
    tokio::spawn(async move { while let Ok(_) = service1.recv().await {} });

    // we create publisher first
    let channel_id: PubsubChannelId = 1000.into();
    let mut subscriber = service1_requester.subscriber(channel_id).await;
    let mut publisher = service1_requester.publisher(channel_id).await;

    let ttl = Duration::from_secs(1);

    assert_eq!(
        timeout(ttl, subscriber.recv()).await.expect("should not timeout").expect("should recv"),
        SubscriberEvent::PeerJoined(PeerSrc::Local)
    );
    assert_eq!(
        timeout(ttl, publisher.recv()).await.expect("should not timeout").expect("should recv"),
        PublisherEvent::PeerJoined(PeerSrc::Local)
    );

    publisher.send(vec![1, 2, 3]).await.expect("should ok");
    assert_eq!(
        timeout(ttl, subscriber.recv()).await.expect("should not timeout").expect("should recv"),
        SubscriberEvent::Data(vec![1, 2, 3])
    );

    subscriber.feedback(vec![2, 3, 4]).await.expect("should ok");
    assert_eq!(
        timeout(ttl, publisher.recv()).await.expect("should not timeout").expect("should recv"),
        PublisherEvent::Feedback(vec![2, 3, 4])
    );
}

#[test(tokio::test)]
async fn pubsub_local_multi_subs() {
    let (mut node1, _addr1) = create_node(true, 1, vec![]).await;
    let mut service1 = PubsubService::new(node1.create_service(0.into()));
    let service1_requester = service1.requester();
    tokio::spawn(async move { while let Ok(_) = node1.recv().await {} });
    tokio::spawn(async move { while let Ok(_) = service1.recv().await {} });

    // we create publisher first
    let channel_id: PubsubChannelId = 1000.into();
    let mut subscriber1 = service1_requester.subscriber(channel_id).await;
    let mut subscriber2 = service1_requester.subscriber(channel_id).await;
    let mut publisher = service1_requester.publisher(channel_id).await;

    let ttl = Duration::from_secs(1);

    assert_eq!(
        timeout(ttl, subscriber1.recv()).await.expect("should not timeout").expect("should recv"),
        SubscriberEvent::PeerJoined(PeerSrc::Local)
    );
    assert_eq!(
        timeout(ttl, subscriber2.recv()).await.expect("should not timeout").expect("should recv"),
        SubscriberEvent::PeerJoined(PeerSrc::Local)
    );
    assert_eq!(
        timeout(ttl, publisher.recv()).await.expect("should not timeout").expect("should recv"),
        PublisherEvent::PeerJoined(PeerSrc::Local)
    );

    publisher.send(vec![1, 2, 3]).await.expect("should ok");
    assert_eq!(
        timeout(ttl, subscriber1.recv()).await.expect("should not timeout").expect("should recv"),
        SubscriberEvent::Data(vec![1, 2, 3])
    );
    assert_eq!(
        timeout(ttl, subscriber2.recv()).await.expect("should not timeout").expect("should recv"),
        SubscriberEvent::Data(vec![1, 2, 3])
    );

    subscriber1.feedback(vec![2, 3, 4]).await.expect("should ok");
    assert_eq!(
        timeout(ttl, publisher.recv()).await.expect("should not timeout").expect("should recv"),
        PublisherEvent::Feedback(vec![2, 3, 4])
    );

    subscriber2.feedback(vec![3, 4, 5]).await.expect("should ok");
    assert_eq!(
        timeout(ttl, publisher.recv()).await.expect("should not timeout").expect("should recv"),
        PublisherEvent::Feedback(vec![3, 4, 5])
    );
}

#[test(tokio::test)]
async fn pubsub_local_multi_pubs() {
    let (mut node1, _addr1) = create_node(true, 1, vec![]).await;
    let mut service1 = PubsubService::new(node1.create_service(0.into()));
    let service1_requester = service1.requester();
    tokio::spawn(async move { while let Ok(_) = node1.recv().await {} });
    tokio::spawn(async move { while let Ok(_) = service1.recv().await {} });

    // we create publisher first
    let channel_id: PubsubChannelId = 1000.into();
    let mut publisher1 = service1_requester.publisher(channel_id).await;
    let mut publisher2 = service1_requester.publisher(channel_id).await;
    let mut subscriber = service1_requester.subscriber(channel_id).await;

    let ttl = Duration::from_secs(1);

    assert_eq!(
        timeout(ttl, subscriber.recv()).await.expect("should not timeout").expect("should recv"),
        SubscriberEvent::PeerJoined(PeerSrc::Local)
    );
    assert_eq!(
        timeout(ttl, publisher1.recv()).await.expect("should not timeout").expect("should recv"),
        PublisherEvent::PeerJoined(PeerSrc::Local)
    );
    assert_eq!(
        timeout(ttl, publisher2.recv()).await.expect("should not timeout").expect("should recv"),
        PublisherEvent::PeerJoined(PeerSrc::Local)
    );

    publisher1.send(vec![1, 2, 3]).await.expect("should ok");
    assert_eq!(
        timeout(ttl, subscriber.recv()).await.expect("should not timeout").expect("should recv"),
        SubscriberEvent::Data(vec![1, 2, 3])
    );

    publisher2.send(vec![1, 2, 4]).await.expect("should ok");
    assert_eq!(
        timeout(ttl, subscriber.recv()).await.expect("should not timeout").expect("should recv"),
        SubscriberEvent::Data(vec![1, 2, 4])
    );

    subscriber.feedback(vec![2, 3, 4]).await.expect("should ok");
    assert_eq!(
        timeout(ttl, publisher1.recv()).await.expect("should not timeout").expect("should recv"),
        PublisherEvent::Feedback(vec![2, 3, 4])
    );
    assert_eq!(
        timeout(ttl, publisher2.recv()).await.expect("should not timeout").expect("should recv"),
        PublisherEvent::Feedback(vec![2, 3, 4])
    );
}

#[test(tokio::test)]
async fn pubsub_remote_single_pair_pub_first() {
    let (mut node1, addr1) = create_node(true, 1, vec![]).await;
    let mut service1 = PubsubService::new(node1.create_service(0.into()));
    let service1_requester = service1.requester();
    tokio::spawn(async move { while let Ok(_) = node1.recv().await {} });
    tokio::spawn(async move { while let Ok(_) = service1.recv().await {} });

    let (mut node2, addr2) = create_node(false, 2, vec![addr1.clone()]).await;
    let mut service2 = PubsubService::new(node2.create_service(0.into()));
    let service2_requester = service2.requester();
    tokio::spawn(async move { while let Ok(_) = node2.recv().await {} });
    tokio::spawn(async move { while let Ok(_) = service2.recv().await {} });

    tokio::time::sleep(Duration::from_secs(1)).await;
    let ttl = Duration::from_secs(1);

    // we create publisher first
    let channel_id: PubsubChannelId = 1000.into();
    let mut publisher = service1_requester.publisher(channel_id).await;
    let mut subscriber = service2_requester.subscriber(channel_id).await;

    assert_eq!(
        timeout(ttl, subscriber.recv()).await.expect("should not timeout").expect("should recv"),
        SubscriberEvent::PeerJoined(PeerSrc::Remote(addr1.peer_id()))
    );
    assert_eq!(
        timeout(ttl, publisher.recv()).await.expect("should not timeout").expect("should recv"),
        PublisherEvent::PeerJoined(PeerSrc::Remote(addr2.peer_id()))
    );

    publisher.send(vec![1, 2, 3]).await.expect("should ok");
    assert_eq!(
        timeout(ttl, subscriber.recv()).await.expect("should not timeout").expect("should recv"),
        SubscriberEvent::Data(vec![1, 2, 3])
    );

    subscriber.feedback(vec![2, 3, 4]).await.expect("should ok");
    assert_eq!(
        timeout(ttl, publisher.recv()).await.expect("should not timeout").expect("should recv"),
        PublisherEvent::Feedback(vec![2, 3, 4])
    );
}

#[test(tokio::test)]
async fn pubsub_remote_single_pair_sub_first() {
    let (mut node1, addr1) = create_node(true, 1, vec![]).await;
    let mut service1 = PubsubService::new(node1.create_service(0.into()));
    let service1_requester = service1.requester();
    tokio::spawn(async move { while let Ok(_) = node1.recv().await {} });
    tokio::spawn(async move { while let Ok(_) = service1.recv().await {} });

    let (mut node2, addr2) = create_node(false, 2, vec![addr1.clone()]).await;
    let mut service2 = PubsubService::new(node2.create_service(0.into()));
    let service2_requester = service2.requester();
    tokio::spawn(async move { while let Ok(_) = node2.recv().await {} });
    tokio::spawn(async move { while let Ok(_) = service2.recv().await {} });

    let ttl = Duration::from_secs(1);
    tokio::time::sleep(Duration::from_secs(1)).await;

    // we create publisher first
    let channel_id: PubsubChannelId = 1000.into();
    let mut subscriber = service1_requester.subscriber(channel_id).await;
    let mut publisher = service2_requester.publisher(channel_id).await;

    assert_eq!(
        timeout(ttl, subscriber.recv()).await.expect("should not timeout").expect("should recv"),
        SubscriberEvent::PeerJoined(PeerSrc::Remote(addr2.peer_id()))
    );
    assert_eq!(
        timeout(ttl, publisher.recv()).await.expect("should not timeout").expect("should recv"),
        PublisherEvent::PeerJoined(PeerSrc::Remote(addr1.peer_id()))
    );

    publisher.send(vec![1, 2, 3]).await.expect("should ok");
    assert_eq!(
        timeout(ttl, subscriber.recv()).await.expect("should not timeout").expect("should recv"),
        SubscriberEvent::Data(vec![1, 2, 3])
    );

    subscriber.feedback(vec![2, 3, 4]).await.expect("should ok");
    assert_eq!(
        timeout(ttl, publisher.recv()).await.expect("should not timeout").expect("should recv"),
        PublisherEvent::Feedback(vec![2, 3, 4])
    );
}

#[test(tokio::test)]
async fn pubsub_remote_multi_subs() {
    let (mut node1, addr1) = create_node(true, 1, vec![]).await;
    let mut service1 = PubsubService::new(node1.create_service(0.into()));
    let service1_requester = service1.requester();
    tokio::spawn(async move { while let Ok(_) = node1.recv().await {} });
    tokio::spawn(async move { while let Ok(_) = service1.recv().await {} });

    let (mut node2, addr2) = create_node(false, 2, vec![addr1.clone()]).await;
    let mut service2 = PubsubService::new(node2.create_service(0.into()));
    let service2_requester = service2.requester();
    tokio::spawn(async move { while let Ok(_) = node2.recv().await {} });
    tokio::spawn(async move { while let Ok(_) = service2.recv().await {} });

    let ttl = Duration::from_secs(1);
    tokio::time::sleep(Duration::from_secs(1)).await;

    // we create publisher first
    let channel_id: PubsubChannelId = 1000.into();
    let mut subscriber1 = service1_requester.subscriber(channel_id).await;
    let mut subscriber2 = service2_requester.subscriber(channel_id).await;
    let mut publisher = service1_requester.publisher(channel_id).await;

    assert_eq!(
        timeout(ttl, subscriber1.recv()).await.expect("should not timeout").expect("should recv"),
        SubscriberEvent::PeerJoined(PeerSrc::Local)
    );
    assert_eq!(
        timeout(ttl, subscriber2.recv()).await.expect("should not timeout").expect("should recv"),
        SubscriberEvent::PeerJoined(PeerSrc::Remote(addr1.peer_id()))
    );
    assert_eq!(
        timeout(ttl, publisher.recv()).await.expect("should not timeout").expect("should recv"),
        PublisherEvent::PeerJoined(PeerSrc::Local)
    );
    assert_eq!(
        timeout(ttl, publisher.recv()).await.expect("should not timeout").expect("should recv"),
        PublisherEvent::PeerJoined(PeerSrc::Remote(addr2.peer_id()))
    );

    publisher.send(vec![1, 2, 3]).await.expect("should ok");
    assert_eq!(
        timeout(ttl, subscriber1.recv()).await.expect("should not timeout").expect("should recv"),
        SubscriberEvent::Data(vec![1, 2, 3])
    );
    assert_eq!(
        timeout(ttl, subscriber2.recv()).await.expect("should not timeout").expect("should recv"),
        SubscriberEvent::Data(vec![1, 2, 3])
    );

    subscriber1.feedback(vec![2, 3, 4]).await.expect("should ok");
    assert_eq!(
        timeout(ttl, publisher.recv()).await.expect("should not timeout").expect("should recv"),
        PublisherEvent::Feedback(vec![2, 3, 4])
    );

    subscriber2.feedback(vec![3, 4, 5]).await.expect("should ok");
    assert_eq!(
        timeout(ttl, publisher.recv()).await.expect("should not timeout").expect("should recv"),
        PublisherEvent::Feedback(vec![3, 4, 5])
    );
}

#[test(tokio::test)]
async fn pubsub_remote_multi_pubs() {
    let (mut node1, addr1) = create_node(true, 1, vec![]).await;
    let mut service1 = PubsubService::new(node1.create_service(0.into()));
    let service1_requester = service1.requester();
    tokio::spawn(async move { while let Ok(_) = node1.recv().await {} });
    tokio::spawn(async move { while let Ok(_) = service1.recv().await {} });

    let (mut node2, addr2) = create_node(false, 2, vec![addr1.clone()]).await;
    let mut service2 = PubsubService::new(node2.create_service(0.into()));
    let service2_requester = service2.requester();
    tokio::spawn(async move { while let Ok(_) = node2.recv().await {} });
    tokio::spawn(async move { while let Ok(_) = service2.recv().await {} });

    let ttl = Duration::from_secs(1);
    tokio::time::sleep(Duration::from_secs(1)).await;

    // we create publisher first
    let channel_id: PubsubChannelId = 1000.into();
    let mut publisher1 = service1_requester.publisher(channel_id).await;
    let mut publisher2 = service2_requester.publisher(channel_id).await;
    let mut subscriber = service1_requester.subscriber(channel_id).await;

    assert_eq!(
        timeout(ttl, subscriber.recv()).await.expect("should not timeout").expect("should recv"),
        SubscriberEvent::PeerJoined(PeerSrc::Local)
    );
    assert_eq!(
        timeout(ttl, subscriber.recv()).await.expect("should not timeout").expect("should recv"),
        SubscriberEvent::PeerJoined(PeerSrc::Remote(addr2.peer_id()))
    );
    assert_eq!(
        timeout(ttl, publisher1.recv()).await.expect("should not timeout").expect("should recv"),
        PublisherEvent::PeerJoined(PeerSrc::Local)
    );
    assert_eq!(
        timeout(ttl, publisher2.recv()).await.expect("should not timeout").expect("should recv"),
        PublisherEvent::PeerJoined(PeerSrc::Remote(addr1.peer_id()))
    );

    publisher1.send(vec![1, 2, 3]).await.expect("should ok");
    assert_eq!(
        timeout(ttl, subscriber.recv()).await.expect("should not timeout").expect("should recv"),
        SubscriberEvent::Data(vec![1, 2, 3])
    );

    publisher2.send(vec![1, 2, 4]).await.expect("should ok");
    assert_eq!(
        timeout(ttl, subscriber.recv()).await.expect("should not timeout").expect("should recv"),
        SubscriberEvent::Data(vec![1, 2, 4])
    );

    subscriber.feedback(vec![2, 3, 4]).await.expect("should ok");
    assert_eq!(
        timeout(ttl, publisher1.recv()).await.expect("should not timeout").expect("should recv"),
        PublisherEvent::Feedback(vec![2, 3, 4])
    );
    assert_eq!(
        timeout(ttl, publisher2.recv()).await.expect("should not timeout").expect("should recv"),
        PublisherEvent::Feedback(vec![2, 3, 4])
    );
}
