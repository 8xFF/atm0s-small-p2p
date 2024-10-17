use std::{
    net::UdpSocket,
    time::{Duration, Instant},
};

use rustls::pki_types::{CertificateDer, PrivatePkcs8KeyDer};

use atm0s_small_p2p::{P2pNetwork, P2pNetworkConfig, PeerAddress, PeerId, SharedKeyHandshake};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

pub const DEFAULT_CLUSTER_CERT: &[u8] = include_bytes!("../certs/dev.cluster.cert");
pub const DEFAULT_CLUSTER_KEY: &[u8] = include_bytes!("../certs/dev.cluster.key");
pub const DEFAULT_SECURE_KEY: &str = "atm0s";

async fn create_node(advertise: bool, peer_id: u64, seeds: Vec<PeerAddress>) -> (P2pNetwork<SharedKeyHandshake>, PeerAddress) {
    let _ = rustls::crypto::ring::default_provider().install_default();

    let priv_key: PrivatePkcs8KeyDer<'_> = PrivatePkcs8KeyDer::from(DEFAULT_CLUSTER_KEY.to_vec());
    let cert = CertificateDer::from(DEFAULT_CLUSTER_CERT.to_vec());

    let addr = {
        let socket = UdpSocket::bind("127.0.0.1:0").expect("should bind");
        socket.local_addr().expect("should get local")
    };
    let peer_id = PeerId::from(peer_id);
    (
        P2pNetwork::new(P2pNetworkConfig {
            peer_id,
            listen_addr: addr,
            advertise: advertise.then(|| addr.into()),
            priv_key,
            cert,
            tick_ms: 100,
            seeds,
            secure: DEFAULT_SECURE_KEY.into(),
        })
        .await
        .expect("should create network"),
        (peer_id, addr.into()).into(),
    )
}

#[tokio::main]
async fn main() {
    let (mut node1, addr1) = create_node(false, 1, vec![]).await;
    let (mut node2, addr2) = create_node(false, 2, vec![addr1.clone()]).await;

    let service1 = node1.create_service(0.into());
    let mut service2 = node2.create_service(0.into());

    tokio::spawn(async move { while let Ok(_) = node1.recv().await {} });
    tokio::spawn(async move { while let Ok(_) = node2.recv().await {} });

    tokio::time::sleep(Duration::from_secs(2)).await;

    tokio::spawn(async move {
        let mut stream = service1.open_stream(addr2.peer_id(), vec![]).await.expect("should open stream");
        let data = [0; 65000];
        loop {
            let _ = stream.write_all(&data).await;
        }
    });

    while let Some(event) = service2.recv().await {
        match event {
            atm0s_small_p2p::P2pServiceEvent::Unicast(..) => {}
            atm0s_small_p2p::P2pServiceEvent::Broadcast(..) => {}
            atm0s_small_p2p::P2pServiceEvent::Stream(.., mut p2p_quic_stream) => {
                tokio::spawn(async move {
                    let mut buf = [0; 65000];
                    let mut recv_count = 0;
                    let mut recv_at = Instant::now();
                    while let Ok(size) = p2p_quic_stream.read(&mut buf).await {
                        recv_count += size;
                        if recv_at.elapsed() > Duration::from_secs(1) {
                            println!("Speed {} kbps", recv_count * 8 / recv_at.elapsed().as_millis() as usize);
                            recv_at = Instant::now();
                            recv_count = 0;
                        }
                    }
                });
            }
        }
    }
}
