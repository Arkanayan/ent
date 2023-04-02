use std::{collections::HashSet, net::SocketAddr, sync::Arc};

use tracing::info;
use tokio::net::TcpStream;
use tokio_util::codec::{Framed, FramedParts};
use anyhow::{Result, anyhow};
use futures::{SinkExt, StreamExt};

use crate::{
    metainfo::{PeerID, MetaInfo},
    torrent::{BlockInfo, TorrentInfo, BlockData, TorrentContext}, messages::{HandShake, HandShakeCodec, MessageCodec, BitField, Message}, tracker::TrackerData,
};

#[derive(Debug, Default)]
pub enum PeerState {
    Connected,
    HandShakeDone,
    Choke,
    Unchoke,
    Interested,
    NotInterested,
    #[default]
    Disconnected,
}

#[derive(Debug)]
pub struct PeerSession {
    pub state: PeerState,
    pub addr: SocketAddr,
    pub peer_id: Option<PeerID>,
    pub in_download_blocks: HashSet<BlockInfo>,
    pub ctx: Arc<TorrentContext>,
	pub have: BitField
}

impl PeerSession {
    pub fn new(torrent_context: Arc<TorrentContext>, addr: SocketAddr) -> Self {
		let bf = BitField::repeat(false, torrent_context.pieces.len());
        PeerSession {
            state: PeerState::Disconnected,
            addr: addr,
            peer_id: None,
            in_download_blocks: HashSet::new(),
            ctx: torrent_context,
			have: bf
        }
    }

    pub async fn start_connection(
		&mut self,
    ) -> Result<()> {
        // let ip = tracker_data.peers[1].ip.clone();
        // let port = tracker_data.peers[1].port.clone();

        // info!("Connecting to ({},{})", ip, port);
        // let addresses = tracker_data.peers.iter().map(|p| (p.ip.clone(), p.port.clone()).into()).collect::<Vec<_>>();
        // info!("Peers found: {:?}", addresses);
        // let socket = TcpStream::connect(addresses.as_slice()).await?;
        // // let socket = TcpStream::connect((ip, port)).await?;
        // info!("TCP Socket Connected");
        info!("Connecting to {}", self.addr);

        let socket = TcpStream::connect(self.addr).await?;
        info!("TCP Socket Connected");
		self.state = PeerState::Connected;

        let mut socket = Framed::new(socket, HandShakeCodec);

        let info_hash = self.ctx.info_hash;
        let handshake = HandShake::new(self.ctx.peer_id, info_hash);

        socket.send(handshake).await?;
        info!("Handshake sent");

        if let Some(received) = socket.next().await {
            info!("Packet received from peer. Should be handshake");

            let handshake = received?;
            info!("Handshake received");
            log::trace!(
                "Handshake: {:?}, Peer Id: {}",
                handshake,
                String::from_utf8_lossy(&handshake.peer_id)
            );

            if handshake.info_hash != info_hash {
                return Err(anyhow!("Invalid InfoHash"));
            }

			self.state = PeerState::HandShakeDone;
        }

        let parts = socket.into_parts();

        let mut socket = FramedParts::new(parts.io, MessageCodec);
        socket.read_buf = parts.read_buf;
        socket.write_buf = parts.write_buf;
        let mut socket = Framed::from_parts(socket);

		socket.send(Message::Interested).await?;
		info!("Interested sent");

        while let Some(received) = socket.next().await {
            let received = received?;
            // info!("Received: {:?}", received);

			match received {
				Message::Piece(BlockData{piece_index, offset, data}) => {
					info!("Piece block received. Data length: {}", data.len());
				},
				Message::Have(piece_index) => {
					info!("Have received. Marking. Piece: {}", piece_index);
					self.have.set(piece_index, true);

					info!("Asking for piece: {}", piece_index);

					socket.send(Message::Request(BlockInfo { piece_index: piece_index, start: 0, length: 2u32.pow(14)})).await?;
				},
				_ => {}
				
			}
        }

        Ok(())
    }

	pub async fn run(&mut self) -> Result<()> {
		self.start_connection().await?;
		Ok(())
	}
}
