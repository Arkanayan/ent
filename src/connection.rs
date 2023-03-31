use anyhow::{anyhow, Result};
use futures::{stream::SplitSink, SinkExt, StreamExt};
use log::info;
use tokio::net::TcpStream;
use tokio_util::codec::Framed;

use crate::messages::{HandShake, HandShakeCodec};
use crate::metainfo::{MetaInfo, PeerID};
use crate::tracker::TrackerData;

pub async fn start_connection(
    peer_id: PeerID,
    torrent: MetaInfo,
    tracker_data: TrackerData,
) -> Result<()> {
    // let ip = tracker_data.peers[1].ip.clone();
    // let port = tracker_data.peers[1].port.clone();

    // info!("Connecting to ({},{})", ip, port);
    // let addresses = tracker_data.peers.iter().map(|p| (p.ip.clone(), p.port.clone()).into()).collect::<Vec<_>>();
    // info!("Peers found: {:?}", addresses);
    // let socket = TcpStream::connect(addresses.as_slice()).await?;
    // // let socket = TcpStream::connect((ip, port)).await?;
    // info!("TCP Socket Connected");
    if tracker_data.peers.len() == 0 {
        return Err(anyhow::anyhow!("No peers found"));
    }

    let peer = tracker_data.peers[0];
    info!("Connecting to {}", peer);

    let socket = TcpStream::connect(peer).await?;
    info!("TCP Socket Connected");

    let mut socket = Framed::new(socket, HandShakeCodec);

    let info_hash = torrent.info_hash();
    let handshake = HandShake::new(peer_id, info_hash);

    socket.send(handshake).await?;
    info!("Handshake sent");

    if let Some(received) = socket.next().await {
        info!("Packet received from peer. Should be handshake");

        let handshake = received?;
        info!("Handshake received");
		log::trace!("Handshake: {:?}, Peer Id: {}", handshake, String::from_utf8_lossy(&handshake.peer_id));

		if handshake.info_hash != info_hash {
			return Err(anyhow!("Invalid InfoHash"))
		}
	
    }
    Ok(())
}
