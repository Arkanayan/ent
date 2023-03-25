use anyhow::Result;
use log::info;
use reqwest;
use serde::de;
use serde::Deserialize;
use serde_bytes::ByteBuf;
use std::net::Ipv4Addr;

use crate::torrent::Torrent;

#[derive(Deserialize, Debug)]
pub struct TrackerData {
    pub complete: Option<i64>,
    pub incomplete: Option<i64>,
    pub interval: Option<u64>,
    #[serde(deserialize_with = "deserialize_peers")]
    pub peers: Vec<Peer>,
}

#[derive(Debug)]
pub struct Peer {
    pub ip: Ipv4Addr,
    pub port: u16,
}

fn deserialize_peers<'de, D>(deserializer: D) -> Result<Vec<Peer>, D::Error>
where
    D: de::Deserializer<'de>,
{
    let mut peers = vec![];
    let v: ByteBuf = Deserialize::deserialize(deserializer)?;
    let num_peers = v.len() / 6;
    if num_peers == 0 {
        return Ok(peers);
    }

    for i in (0..num_peers).step_by(6) {
        let ip: &[u8; 4] = &v[i..i + 4].try_into().unwrap();
        let port: &[u8; 2] = &v[i..i + 2].try_into().unwrap();
        // let ip = Ipv4Addr::from(ip.clone());
        // let port = (port[1] as u16) << 8 | port[0] as u16;
        // let port = u16::from_be_bytes(port.clone());
        let ip = Ipv4Addr::new(ip[0], ip[1], ip[2], ip[3]);
        let port = u16::from_be_bytes([port[0], port[1]]);
        peers.push(Peer { ip, port });
    }
    Ok(peers)
}

pub async fn get_peer_details_from_tracker(torrent: &Torrent, peer_id: &str) -> Result<TrackerData> {
    let tracker_url = torrent.announce.as_deref().unwrap();
    let info_hash = torrent.info_hash();
    let escaped_hash = url_encode_bytes(&info_hash);
    let peer_id = "aaaaaaaaaaaaaaaaaaaa";

    let client = reqwest::Client::new();
    let req = client
        .get(tracker_url.to_owned() + "?info_hash=" + escaped_hash.as_str())
        .query(&[
            ("peer_id", peer_id),
            ("port", "6881"),
            ("downloaded", "0"),
            ("uploaded", "0"),
            ("event", "started"),
            ("compact", "1"),
            ("left", &torrent.info.length.unwrap().to_string()),
        ])
        .build()
        .unwrap();
    let resp = client.execute(req).await;
    let body = resp.unwrap().bytes().await.unwrap();
    let t= serde_bencode::from_bytes(&body)?;
    Ok(t)
}

pub fn url_encode_bytes(content: &[u8]) -> String {
    let mut out: String = String::new();

    for byte in content.iter() {
        match *byte as char {
            '0'..='9' | 'a'..='z' | 'A'..='Z' | '.' | '-' | '_' | '~' => out.push(*byte as char),
            _ => out.push_str(format!("%{:02X}", byte).as_str()),
        };
    }

    out
}
