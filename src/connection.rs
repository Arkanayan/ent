use anyhow::Result;
use log::info;
use tokio::net::{TcpStream, ToSocketAddrs};


use crate::torrent::Torrent;
use crate::tracker::TrackerData;

pub async fn start_connection(peer_id: String, torrent: Torrent, tracker_data: TrackerData) -> Result<()> {
	let ip = tracker_data.peers[1].ip.clone();
	let port = tracker_data.peers[1].port.clone();

	info!("Connecting to ({},{})", ip, port);
	let addresses = tracker_data.peers.iter().map(|p| (p.ip.clone(), p.port.clone()).into()).collect::<Vec<_>>();
	let socket = TcpStream::connect(addresses.as_slice()).await?;
	// let socket = TcpStream::connect((ip, port)).await?;
	info!("TCP Socket Connected");

	Ok(())
}