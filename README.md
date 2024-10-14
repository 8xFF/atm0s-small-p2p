# P2P Network with atm0s Routing

## Description

A lightweight peer-to-peer (P2P) network utilizing the atm0s routing mechanism, built entirely with asynchronous programming and QUIC (using the quinn library). This project aims to provide a robust and efficient framework for P2P communication.

## Features

- **Asynchronous Communication**: Built with async programming for high performance and scalability.
- **QUIC Protocol**: Utilizes the QUIC protocol for secure and fast data transmission.
- **atm0s Routing**: Implements the atm0s routing mechanism for efficient peer discovery and message routing.

## Architecture

The architecture of the P2P network is designed to facilitate efficient communication between peers. Key components include:

- **Peer Discovery**: The `PeerDiscovery` module manages the discovery of peers in the network, allowing nodes to find and connect to each other.
- **Routing**: The `RouterTable` manages the routing of messages between peers, ensuring that data is sent through the most efficient paths.
- **Secure Communication**: The `SharedKeyHandshake` protocol ensures that connections between peers are secure, using cryptographic techniques to verify identities and protect data.

## Getting Started

### Usage

To get started with the P2P network, you need to set up a node. Here’s a basic example of how to create a node:

```rust
let _ = rustls::crypto::ring::default_provider().install_default();

let priv_key: PrivatePkcs8KeyDer<'_> = PrivatePkcs8KeyDer::from(DEFAULT_CLUSTER_KEY.to_vec());
let cert = CertificateDer::from(DEFAULT_CLUSTER_CERT.to_vec());

let peer_id = PeerId::from("127.0.0.1:10000".parse().unwrap());
let network = P2pNetwork::new(P2pNetworkConfig {
    peer_id,
    listen_addr: addr,
    advertise: advertise.then(|| addr.into()),
    priv_key,
    cert,
    tick_ms: 100,
    seeds,
    secure: DEFAULT_SECURE_KEY.into(),
}).await;
```

### Create a service

```rust
let service = network.create_service(1.into());
```

We can handle event from service

```rust
while let Some(event) = service.recv() {
    match event {
        P2pServiceEvent::Unicast(from_peer, data) => {
            // handle data from other node here
        },
        P2pServiceEvent::Broadcast(from_peer, data) => {
            // handle broadcast data from other node here
        },
        P2pServiceEvent::Stream(from_peer, meta, stream) => {
            // stream is AsyncRead + AsyncWrite, we can tunnel it to other by bicopydirection ...
        },
    }
}
```

To send messages, you can use the `send_unicast` method for direct communication or `send_broadcast` for broadcasting messages to all connected peers:

```rust
service.send_unicast(dest_peer_id, data).await.expect("should send ok");
```

## Testing

The project includes a suite of tests to ensure the functionality of the P2P network. You can run the tests using:

```bash
cargo test
```