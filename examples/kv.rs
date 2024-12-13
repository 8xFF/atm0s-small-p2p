use std::{collections::BTreeMap, net::SocketAddr, str::FromStr};

use atm0s_small_p2p::{
    replicate_kv_service::{KvEvent, ReplicatedKvService},
    P2pNetwork, P2pNetworkConfig, PeerAddress, PeerId, SharedKeyHandshake,
};
use clap::Parser;
use poem::{
    get, handler,
    listener::TcpListener,
    middleware::Tracing,
    put,
    web::{Data, Json, Path},
    EndpointExt, Route, Server,
};
use rustls::pki_types::{CertificateDer, PrivatePkcs8KeyDer};
use serde::{Deserialize, Serialize};
use tokio::{
    select,
    sync::{mpsc, oneshot},
};
use tracing_subscriber::{fmt, layer::SubscriberExt, util::SubscriberInitExt, EnvFilter};

pub const DEFAULT_CLUSTER_CERT: &[u8] = include_bytes!("../certs/dev.cluster.cert");
pub const DEFAULT_CLUSTER_KEY: &[u8] = include_bytes!("../certs/dev.cluster.key");

/// A Replicated KV store
#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    /// HTTP listen for api
    #[arg(env, long, default_value = "0.0.0.0:3000")]
    http_listen: SocketAddr,

    /// UDP/TCP port for serving QUIC/TCP connection for SDN network
    #[arg(env, long, default_value_t = 0)]
    sdn_peer_id: u64,

    /// UDP/TCP port for serving QUIC/TCP connection for SDN network
    #[arg(env, long, default_value = "0.0.0.0:11111")]
    sdn_listener: SocketAddr,

    /// Seeds
    #[arg(env, long, value_delimiter = ',')]
    sdn_seeds: Vec<String>,

    /// Allow it broadcast address to other peers
    /// This allows other peer can active connect to this node
    /// This option is useful with high performance relay node
    #[arg(env, long)]
    sdn_advertise_address: Option<SocketAddr>,

    /// Sdn secure code
    #[arg(env, long, default_value = "insecure")]
    sdn_secure_code: String,
}

enum Control {
    Get(u64, oneshot::Sender<Option<ValueInfo>>),
    Put(u64, u64, oneshot::Sender<()>),
    Del(u64, oneshot::Sender<()>),
}

#[derive(Debug, Clone, Serialize)]
struct ValueInfo {
    value: u64,
    node: Option<PeerId>,
}

#[handler]
async fn get_key(Path(key): Path<u64>, Data(control): Data<&mpsc::Sender<Control>>) -> Json<Option<ValueInfo>> {
    let (tx, rx) = oneshot::channel();
    control.send(Control::Get(key, tx)).await.expect("should send to main loop");
    Json(rx.await.expect("should get value"))
}

#[derive(Debug, Deserialize)]
struct PutParams {
    key: u64,
    value: u64,
}

#[handler]
async fn put_key(Path(params): Path<PutParams>, Data(control): Data<&mpsc::Sender<Control>>) -> String {
    let (tx, rx) = oneshot::channel();
    control.send(Control::Put(params.key, params.value, tx)).await.expect("should send to main loop");
    rx.await.expect("should get value");
    "Ok".to_string()
}

#[handler]
async fn del_key(Path(key): Path<u64>, Data(control): Data<&mpsc::Sender<Control>>) -> String {
    let (tx, rx) = oneshot::channel();
    control.send(Control::Del(key, tx)).await.expect("should send to main loop");
    rx.await.expect("should get value");
    "Ok".to_string()
}

#[tokio::main]
async fn main() {
    rustls::crypto::ring::default_provider().install_default().expect("should install ring as default");

    if std::env::var_os("RUST_LOG").is_none() {
        std::env::set_var("RUST_LOG", "info");
    }
    if std::env::var_os("RUST_BACKTRACE").is_none() {
        std::env::set_var("RUST_BACKTRACE", "1");
    }
    let args: Args = Args::parse();
    tracing_subscriber::registry().with(fmt::layer()).with(EnvFilter::from_default_env()).init();

    let priv_key = PrivatePkcs8KeyDer::from(DEFAULT_CLUSTER_KEY.to_vec());
    let cert = CertificateDer::from(DEFAULT_CLUSTER_CERT.to_vec());

    let mut p2p = P2pNetwork::new(P2pNetworkConfig {
        peer_id: args.sdn_peer_id.into(),
        listen_addr: args.sdn_listener,
        advertise: args.sdn_advertise_address.map(|a| a.into()),
        priv_key,
        cert,
        tick_ms: 100,
        seeds: args.sdn_seeds.into_iter().map(|s| PeerAddress::from_str(s.as_str()).expect("should parse address")).collect::<Vec<_>>(),
        secure: SharedKeyHandshake::from(args.sdn_secure_code.as_str()),
    })
    .await
    .expect("should create network");

    let mut kv_service: ReplicatedKvService<u64, u64> = ReplicatedKvService::new(p2p.create_service(0.into()));

    let (control_tx, mut control_rx) = mpsc::channel::<Control>(10);
    tokio::spawn(async move {
        let app = Route::new()
            .at("/kv/:key/:value", put(put_key))
            .at("/kv/:key", get(get_key).delete(del_key))
            .data(control_tx)
            .with(Tracing);
        Server::new(TcpListener::bind(args.http_listen)).run(app).await.expect("bind http error");
    });

    let mut kv_local = BTreeMap::new();

    loop {
        select! {
            ctr = control_rx.recv() => {
                match ctr.expect("should recv control") {
                    Control::Get(key, tx) => {
                        let value = kv_local.get(&key);
                        let _ = tx.send(value.cloned());
                    }
                    Control::Put(key, value, tx) => {
                        kv_service.set(key, value);
                        let _ = tx.send(());
                    }
                    Control::Del(key, tx) => {
                        kv_service.del(key);
                        let _ = tx.send(());
                    }
                }
            },
            event = kv_service.recv() => match event.expect("should recv event") {
                KvEvent::Set(node, key, value) => {
                    kv_local.insert(key, ValueInfo { value, node } );
                },
                KvEvent::Del(_, key) => {
                    kv_local.remove(&key);
                },
            },
            _ = p2p.recv() => {}
        }
    }
}
