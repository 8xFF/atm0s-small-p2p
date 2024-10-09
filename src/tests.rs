use std::net::UdpSocket;

use rustls::pki_types::{CertificateDer, PrivatePkcs8KeyDer};

use crate::{P2pNetwork, P2pNetworkConfig, PeerAddress, PeerId};

mod alias;
mod cross_nodes;
mod discovery;
mod visualization;

pub const DEFAULT_CLUSTER_CERT: &[u8] = include_bytes!("../certs/dev.cluster.cert");
pub const DEFAULT_CLUSTER_KEY: &[u8] = include_bytes!("../certs/dev.cluster.key");

async fn create_node(advertise: bool, peer_id: u64, seeds: Vec<PeerAddress>) -> (P2pNetwork, PeerAddress) {
    let _ = rustls::crypto::ring::default_provider().install_default();

    let key: PrivatePkcs8KeyDer<'_> = PrivatePkcs8KeyDer::from(DEFAULT_CLUSTER_KEY.to_vec());
    let cert = CertificateDer::from(DEFAULT_CLUSTER_CERT.to_vec());

    let addr = {
        let socket = UdpSocket::bind("127.0.0.1:0").unwrap();
        socket.local_addr().unwrap()
    };
    let peer_id = PeerId::from(peer_id);
    (
        P2pNetwork::new(P2pNetworkConfig {
            peer_id,
            listen_addr: addr,
            advertise: advertise.then(|| addr.into()),
            priv_key: key,
            cert: cert,
            tick_ms: 100,
            seeds,
        })
        .await
        .unwrap(),
        (peer_id, addr.into()).into(),
    )
}
