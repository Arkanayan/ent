use anyhow::Result;
use log::info;
use tokio::net::{TcpStream, ToSocketAddrs, TcpListener};


use crate::metainfo::{MetaInfo, PeerID};
use crate::tracker::TrackerData;

pub async fn start_connection(peer_id: PeerID, torrent: MetaInfo, tracker_data: TrackerData) -> Result<()> {
	// let ip = tracker_data.peers[1].ip.clone();
	// let port = tracker_data.peers[1].port.clone();

	// info!("Connecting to ({},{})", ip, port);
	// let addresses = tracker_data.peers.iter().map(|p| (p.ip.clone(), p.port.clone()).into()).collect::<Vec<_>>();
	// info!("Peers found: {:?}", addresses);
	// let socket = TcpStream::connect(addresses.as_slice()).await?;
	// // let socket = TcpStream::connect((ip, port)).await?;
	// info!("TCP Socket Connected");
	info!("Connecting to {:?}", tracker_data.peers);
	let socket = TcpStream::connect(tracker_data.peers.as_slice()).await?;
	info!("TCP Socket Connected");


	Ok(())
}