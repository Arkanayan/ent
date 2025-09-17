use std::{
    collections::HashMap,
    fmt::format,
    net::{SocketAddr, ToSocketAddrs},
};

use bitvec::bitbox;
use log::info;
use tokio::net::UdpSocket;
use types::{Message, MessageType, Query};

mod types;

const BOOTSTRAP_NODE: &str = "router.utorrent.com";

pub async fn start_dht_node() -> anyhow::Result<()> {
    info!("Starting DHT Node server");

    // let addr: SocketAddr = format!("{}:6881", BOOTSTRAP_NODE).parse().unwrap();
    // let addr = "router.utorrent.com:6881"
    // let addr = "dht.libtorrent.org:25401"
    // let addr = "router.bittorrent.com:8991"
    let addr = "172.26.55.82:6881"
        .to_socket_addrs()
        .expect("Unable to resolve address")
        .next()
        .ok_or_else(|| anyhow::anyhow!("Cannot find destination"))?;

    info!("Resolved DHT Bootstrap Server: {:?}", addr);

    let sock = UdpSocket::bind("0.0.0.0:8080").await?;
    // let sock = UdpSocket::bind("172.26.55.82:8080").await?;

    sock.connect(addr).await?;

    info!("Connected to bootstrap server");

    let mut ping_data = HashMap::new();
    ping_data.insert("id".to_owned(), "dk38dn5932ndk38dn38d".to_owned());

    let ping = Message {
        transaction_id: "3g".to_string(),
        client_version: None,
        message: MessageType::Query(Query {
            method_name: "ping".to_owned(),
            args: ping_data,
        }),
    };

    let b: Vec<u8> = serde_bencode::to_bytes(&ping).unwrap();

    info!("Sending ping to {}", addr);

    sock.send(&b).await?;

    let mut buf: Vec<u8> = Vec::new();

    loop {
        let n = sock.recv(&mut buf).await?;
        info!("Received {} bytes", n);
    }

    let decoded: serde_bencode::value::Value = serde_bencode::from_bytes(&buf)?;

    info!("Received: {:?}", decoded);
    Ok(())
}
