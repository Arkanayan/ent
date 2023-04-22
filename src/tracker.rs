use anyhow::Result;

use tracing::info;
use reqwest;
use serde::de;
use serde::Deserialize;

use std::fmt;
use std::net::IpAddr;
use std::net::Ipv4Addr;
use std::net::SocketAddr;
use bytes::Buf;
use std::str;

use crate::metainfo::{MetaInfo, PeerID};

#[derive(Deserialize, Debug)]
pub struct TrackerData {
    pub complete: Option<i64>,
    pub incomplete: Option<i64>,
    pub interval: Option<u64>,
    #[serde(deserialize_with = "deserialize_peers")]
    pub peers: Vec<SocketAddr>,
}

#[derive(Debug)]
pub struct Peer {
    pub ip: Ipv4Addr,
    pub port: u16,
}

// fn deserialize_peers<'de, D>(deserializer: D) -> Result<Vec<Peer>, D::Error>
// where
//     D: de::Deserializer<'de>,
// {
//     let mut peers = vec![];
//     let v: ByteBuf = Deserialize::deserialize(deserializer)?;
//     let num_peers = v.len() / 6;
//     if num_peers == 0 {
//         return Ok(peers);
//     }

//     for i in (0..num_peers).step_by(6) {
//         let ip: &[u8; 4] = &v[i..i + 4].try_into().unwrap();
//         let port: &[u8; 2] = &v[i..i + 2].try_into().unwrap();
//         // let ip = Ipv4Addr::from(ip.clone());
//         // let port = (port[1] as u16) << 8 | port[0] as u16;
//         // let port = u16::from_be_bytes(port.clone());
//         let ip = Ipv4Addr::new(ip[0], ip[1], ip[2], ip[3]);
//         let port = u16::from_be_bytes([port[0], port[1]]);
//         peers.push(Peer { ip, port });
//     }
//     Ok(peers)
// }

fn deserialize_peers<'de, D>(
    deserializer: D,
) -> Result<Vec<SocketAddr>, D::Error>
where
    D: de::Deserializer<'de>,
{
    struct Visitor;

    impl<'de> de::Visitor<'de> for Visitor {
        type Value = Vec<SocketAddr>;

        fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
            formatter.write_str("a string or list of dicts representing peers")
        }

        // TODO: we can possibly simplify this by deserializing into an untagged
        // enum where one of the enums has a `serde(with = "serde_bytes")`
        // attribute for the compact list

        /// Deserializes a compact string of peers.
        ///
        /// Each entry is 6 bytes long, where the first 4 bytes are the IPv4
        /// address of the peer, and the last 2 bytes are the port of the peer.
        /// Both are in network byte order.
        fn visit_bytes<E>(self, mut b: &[u8]) -> Result<Self::Value, E>
        where
            E: de::Error,
        {
            // in compact representation each peer must be 6 bytes
            // long
            const ENTRY_LEN: usize = 6;
            let buf_len = b.len();

            if buf_len % ENTRY_LEN != 0 {
                return Err("peers compact string must be a multiple of 6")
                .map_err(E::custom);
            }

            let buf_len = b.len();
            let mut peers = Vec::with_capacity(buf_len / ENTRY_LEN);

            for _ in (0..buf_len).step_by(ENTRY_LEN) {
                let addr = Ipv4Addr::from(b.get_u32());
                let port = b.get_u16();
                // let myport = u16::from_be_bytes(b.clone().try_into().unwrap());
                // info!("Their port: {}, my port: {}", port, myport );
                let peer = SocketAddr::new(IpAddr::V4(addr), port);
                peers.push(peer);
            }

            Ok(peers)
        }

        /// Deserializes a list of dicts containing the peer information.
        fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
        where
            A: de::SeqAccess<'de>,
        {
            #[derive(Debug, Deserialize)]
            struct RawPeer {
                ip: String,
                port: u16,
            }

            let mut peers = Vec::with_capacity(seq.size_hint().unwrap_or(0));
            while let Some(RawPeer { ip, port }) = seq.next_element()? {
                let ip = if let Ok(ip) = ip.parse() {
                    ip
                } else {
                    continue;
                };
                peers.push(SocketAddr::new(ip, port));
            }

            Ok(peers)
        }
    }

    deserializer.deserialize_any(Visitor)
}

pub async fn get_peer_details_from_tracker(torrent: &MetaInfo, peer_id: &PeerID) -> Result<TrackerData> {
    let tracker_url = torrent.announce.as_deref().unwrap();
    let info_hash = torrent.info_hash();
    let escaped_hash = url_encode_bytes(&info_hash);
    let peer_id = str::from_utf8(peer_id)?;
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
    let t= serde_bencode::from_bytes(&body);
    match t {
        Ok(t) => Ok(t),
        Err(e) => { info!("hi"); Err(e.into())},
    }
    // Ok(t)
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
