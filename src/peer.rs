use std::{
    collections::HashSet,
    hash::Hash,
    iter::Extend,
    net::SocketAddr,
    ops::Index,
    sync::Arc,
    time::{Duration, Instant},
};

use anyhow::{anyhow, Result, Ok};
use bitvec::macros::internal::funty::Integral;
use futures::{executor::block_on, stream::SplitSink, SinkExt, StreamExt};
use tokio::{net::TcpStream, sync::RwLock, time};
use tokio_util::codec::{Framed, FramedParts};
use tracing::{debug, info, trace};

use crate::{
    messages::{BitField, HandShake, HandShakeCodec, Message, MessageCodec},
    metainfo::{MetaInfo, PeerID},
    torrent::{
        BlockData, BlockInfo, BlockStatus, PieceDownload, TorrentContext, TorrentInfo,
    },
    tracker::TrackerData,
};
pub const MAX_BLOCK_SIZE: u32 = 1 << 14;

#[derive(Debug, Default, PartialEq)]
pub enum ConnectionState {
    #[default]
    Disconnected,
    Connecting,
    Handshaking,
    AvailabilityExchange,
    Connected,
}

#[derive(Debug)]
pub struct PeerSession {
    pub log_target: String,
    pub state: SessionState,
    pub outgoing_requests: HashSet<BlockInfo>,
    pub torrent: Arc<TorrentContext>,
    pub peer: PeerInfo,
}

#[derive(Debug)]
pub struct PeerInfo {
    pub addr: SocketAddr,
    pub id: Option<PeerID>,
    pub pieces: BitField,
    pub piece_count: usize,
}

#[derive(Debug)]
pub struct SessionState {
    pub connection: ConnectionState,
    // Is the peer interested in us
    pub is_peer_interested: bool,
    // Are we choking the peer
    pub is_peer_choked: bool,
    // Are we interested in the peer
    pub is_interested: bool,
    // Are we choked by the peer
    pub is_choked: bool,
    // How many bytes downloaded from this peer 
    pub total_downloaded_bytes: u64
}

impl Default for SessionState {
    fn default() -> Self {
        Self {
            connection: Default::default(),
            is_peer_interested: false,
            is_peer_choked: true,
            is_interested: false,
            is_choked: true,
            total_downloaded_bytes: 0
        }
    }
}

impl PeerSession {
    pub fn new(torrent_context: Arc<TorrentContext>, addr: SocketAddr) -> Self {
        let piece_count = torrent_context.storage.piece_count;
        PeerSession {
            peer: PeerInfo {
                addr: addr,
                id: None,
                pieces: BitField::with_capacity(piece_count),
                piece_count: piece_count,
            },
            outgoing_requests: HashSet::new(),
            torrent: torrent_context,
            state: Default::default(),
            log_target: format!("{}", addr),
        }
    }

    pub async fn start_connection(&mut self) -> Result<()> {
        // let ip = tracker_data.peers[1].ip.clone();
        // let port = tracker_data.peers[1].port.clone();

        // info!("Connecting to ({},{})", ip, port);
        // let addresses = tracker_data.peers.iter().map(|p| (p.ip.clone(), p.port.clone()).into()).collect::<Vec<_>>();
        // info!("Peers found: {:?}", addresses);
        // let socket = TcpStream::connect(addresses.as_slice()).await?;
        // // let socket = TcpStream::connect((ip, port)).await?;
        // info!("TCP Socket Connected");
        info!("Connecting to {}", self.peer.addr);

        let socket = TcpStream::connect(self.peer.addr).await?;
        info!(peer = self.log_target, "TCP Socket Connected");

        let socket = Framed::new(socket, HandShakeCodec);
        self.state.connection = ConnectionState::Handshaking;

        self.handle_connection(socket).await
    }

    pub async fn handle_connection(
        &mut self,
        mut socket: Framed<TcpStream, HandShakeCodec>,
    ) -> Result<()> {
        let info_hash = self.torrent.info_hash;
        let handshake = HandShake::new(self.torrent.client_id, info_hash);

        socket.send(handshake).await?;
        info!("Handshake sent");

        if let Some(peer_handshake) = socket.next().await {
            info!("Packet received from peer. Should be handshake");

            let handshake = peer_handshake?;
            info!(target: "connection", addr = self.peer.addr.to_string(), "Handshake received");
            log::trace!(
                target: &self.log_target,
                "Handshake: {:?}, Peer Id: {}",
                handshake,
                String::from_utf8_lossy(&handshake.peer_id)
            );

            if handshake.info_hash != info_hash {
                return Err(anyhow!("Invalid InfoHash"));
            }

            self.peer.id = Some(handshake.peer_id);
            self.state.connection = ConnectionState::AvailabilityExchange;
        }

        let old_parts = socket.into_parts();
        let mut socket = FramedParts::new(old_parts.io, MessageCodec);
        socket.read_buf = old_parts.read_buf;
        socket.write_buf = old_parts.write_buf;
        let socket = Framed::from_parts(socket);

        // if let Err(e) = self.run(socke).await {
        //     return Err(anyhow!("Peer Disconnected"));
        // }
        self.run(socket).await
    }

    pub async fn run(&mut self, mut socket: Framed<TcpStream, MessageCodec>) -> Result<()> {
        let (mut sink, mut stream) = socket.split();

        // tis the time to send bitfield
        {
            let piece_picker_guard = self.torrent.piece_picker.read().await;
            let own_pieces = piece_picker_guard.own_pieces();
            if own_pieces.any() {
                info!("Sending piece availability");
                sink.send(Message::BitField(own_pieces.clone())).await?;
                info!("Sent piece availability");
            }
        }

        let mut tick_timer = time::interval(Duration::from_secs(1));

        loop {
            tokio::select! {
                now = tick_timer.tick() => {
                    info!("Time Ticking");
                    self.handle_tick(&mut sink, now.into_std()).await?;
                }
                Some(msg) = stream.next() => {
                    let msg = msg?;

                    if self.state.connection == ConnectionState::AvailabilityExchange {
                        if let Message::BitField(bitfield) = msg {
                            info!("bitfield message got. {}", bitfield.count_ones());
                            self.handle_bitfield_msg(&mut sink, bitfield).await?;
                        } else {
                            // info!("Other message. {:?}", msg);
                            self.handle_msg(&mut sink, msg).await?;
                        }

                        self.state.connection = ConnectionState::Connected;
                        info!("Session state: {:?}", self.state.connection);
                    } else {
                        // info!("Other message. {:?}", msg);
                        self.handle_msg(&mut sink, msg).await?;
                    }
                }
            }
        }
        // while let Some(received) = stream.next().await {
        //     let received = received?;
        //     // info!("Received: {:?}", received);

        //     match received {
        //         Message::Piece(BlockData {
        //             piece_index,
        //             offset,
        //             data,
        //         }) => {
        //             info!("Piece block received. Data length: {}", data.len());
        //         }
        //         Message::Have(piece_index) => {
        //             info!("Have received. Marking. Piece: {}", piece_index);
        //             // self.handle_have(piece_index);
        //             info!("Asking for piece: {}", piece_index);

        //             sink.send(Message::Request(BlockInfo {
        //                 piece_index: piece_index,
        //                 start: 0,
        //                 length: 2u32.pow(14),
        //             }))
        //             .await?;
        //         }
        //         Message::Unchoke => {
        //             info!("Unchoke received");
        //             // socket.send(Message::Request(BlockInfo { piece_index: 1, start: 0, length: 2u32.pow(14)})).await?;
        //         }
        //         _ => {}
        //     }
        // }
        Ok(())
    }

    async fn handle_msg(
        &mut self,
        sink: &mut SplitSink<Framed<TcpStream, MessageCodec>, Message>,
        message: Message,
    ) -> Result<()> {
        match message {
            Message::KeepAlive => {
                info!("KeepAlive received from peer");
            }
            Message::Choke => {
                info!("Peer choking us");
                if !self.state.is_choked {
                    self.state.is_choked = true;
                    self.free_pending_blocks().await;
                }
            }
            Message::Unchoke => {
                if self.state.is_choked {
                    info!("Peer unchoked us");
                    self.state.is_choked = false;

                    if self.state.is_interested {
                        self.make_requests(sink).await?
                    }
                }
            }
            Message::Interested => {
                tracing::info!("Peer is interested in us. Nothing to do here!");
                self.state.is_peer_interested = true;
            }
            Message::NotInterested => {
                tracing::info!("Peer is not interested in us. Nothing to do here!");
                self.state.is_peer_interested = false;
            }
            Message::Have(piece_index) => {
                tracing::info!("Peer has piece {}", piece_index);
                if let Some(mut peer_piece) = self.peer.pieces.get_mut(piece_index) {
                    *peer_piece = true;
                }
                let is_interested = self
                    .torrent
                    .piece_picker
                    .write()
                    .await
                    .register_peer_piece(piece_index);

                self.update_interest(sink, is_interested).await?;
            }
            Message::BitField(_) => {
                return Err(anyhow!("Bitfield message is not allowed at this stage"))
            }
            Message::Request(piece_block) => {
                tracing::info!("Peer requested piece {}", piece_block.piece_index);
            }
            Message::Piece(block_data) => {
                let block_info = BlockInfo { piece_index: block_data.piece_index, start: block_data.offset, length: block_data.data.len() as u32};
                self.state.total_downloaded_bytes += block_data.data.len() as u64;

                let torrent_size = self.torrent.torrent.metainfo.info.length.unwrap();
                let percent_downloaded = (self.state.total_downloaded_bytes as f64 / torrent_size as f64) * 100 as f64;
                tracing::info!(
                    "Peer send us piece={} block={} downloaded={}/{} {:.3}%",
                    block_data.piece_index,
                    block_info.index_in_piece(),
                    self.state.total_downloaded_bytes,
                    torrent_size,
                    percent_downloaded
                );

                self.outgoing_requests.remove(&block_info);
                // Notify everyone we got block
                let mut piece_complete = false;
                if let Some(piece_download) = self.torrent.downloads.read().await.get(&block_data.piece_index) {
                    piece_download.write().await.received_block(&block_info);

                    if piece_download.read().await
                        .blocks
                        .iter()
                        .all(|b| *b == BlockStatus::Received)
                    {
                        piece_complete = true;
                    }
                }
                if piece_complete {
                    tracing::info!("Completed piece {}", block_data.piece_index);
                    self.torrent
                        .piece_picker
                        .write()
                        .await
                        .received_piece(block_data.piece_index);
                }
            }
            Message::Cancel(piece_block) => {
                tracing::info!("Peer sent cancel of piece {}", piece_block.piece_index);
            }
            Message::Port(listen_port) => info!("Port message received: {}", listen_port),
        }

        Ok(())
    }

    async fn free_pending_blocks(&mut self) {
        let download_guard = self.torrent.downloads.write().await;

        for block_info in self.outgoing_requests.drain() {
            if let Some(download) = download_guard.get(&block_info.piece_index()) {
                debug!("Freeing block: {:?}", block_info);
                download.write().await.free_block(&block_info);
            }
        }
    }

    async fn handle_bitfield_msg(
        &mut self,
        sink: &mut SplitSink<Framed<TcpStream, MessageCodec>, Message>,
        mut bitfield: BitField,
    ) -> Result<()> {
        bitfield.resize(self.torrent.storage.piece_count, false);

        let interested = self
            .torrent
            .piece_picker
            .write()
            .await
            .register_peer_pieces(&bitfield);
        self.peer.pieces = bitfield;
        self.peer.piece_count = self.peer.pieces.count_ones();

        if self.peer.piece_count == self.torrent.storage.piece_count {
            info!("This peer is a seed");
        } else {
            info!(
                "Peer has {}/{} pieces. Interested? {}",
                self.peer.piece_count, self.torrent.storage.piece_count, interested
            );
        }

        self.update_interest(sink, interested).await;

        Ok(())
    }

    async fn update_interest(
        &mut self,
        sink: &mut SplitSink<Framed<TcpStream, MessageCodec>, Message>,
        is_interested: bool,
    ) -> Result<()> {
        if !self.state.is_interested && is_interested {
            sink.send(Message::Interested).await?;
            info!("Became interested in peer");
            self.state.is_interested = true;
        } else if self.state.is_interested && !is_interested {
            info!("No longer interested in peer");
            self.state.is_interested = false;
        }

        Ok(())
    }

    async fn handle_tick(
        &mut self,
        sink: &mut SplitSink<Framed<TcpStream, MessageCodec>, Message>,
        instant: Instant,
    ) -> Result<()> {
        let mut free_count = 0;
        if !self.state.is_choked {
            if self.state.is_interested && self.outgoing_requests.len() < 8 {
                self.make_requests(sink).await?;
            }
            // } else {
            //     let piece_picker = self.torrent.piece_picker.read().await;
            //     free_count = piece_picker.free_count;
            // }
            // if free_count > 0 {
            //     self.update_interest(sink, true).await?;
            // }
        }
        Ok(())
    }

    async fn make_requests(
        &mut self,
        sink: &mut SplitSink<Framed<TcpStream, MessageCodec>, Message>,
    ) -> Result<()> {
        info!("Making requests");

        if self.state.is_choked {
            debug!("Can't make requests while choked");
            return Ok(());
        }

        if !self.state.is_interested {
            debug!("Can't make requests while not interested");
            return Ok(());
        }

        const REQUEST_QUEUE_SIZE: usize = 8;

        let mut requests_left = REQUEST_QUEUE_SIZE.saturating_sub(self.outgoing_requests.len());

        info!("Can make {} requests", requests_left);

        if requests_left == 0 {
            return Ok(());
        }

        let mut request_queue = Vec::with_capacity(requests_left);

        // let mut in_download_piece_map = self.torrent.downloads.write().await;
        trace!("Total in torrent download {}", self.torrent.downloads.read().await.len());
        // First try to complete alredy downloading pieces
        for block in &self.outgoing_requests {
            trace!("Block in peer outgoing requests {:?}", block);
            if let Some(piece) = self.torrent.downloads.write().await.get_mut(&block.piece_index()) {
                if requests_left <= 0 {
                    break;
                }

                let new_blocks = piece.write().await.pick_blocks(requests_left);
                if new_blocks.len() > 0 {
                    requests_left -= new_blocks.len();
                    request_queue.extend(new_blocks.into_iter());
                }
            }
        }

        // If the queue is still not filled; grab new piece
        for _ in 0..requests_left {
            if let Some(piece_index) = self
                .torrent
                .piece_picker
                .write()
                .await
                .pick_piece(&self.peer.pieces)
            {
                // if let Some(piece_download_guard) =
                //     self.torrent.downloads.write().await.get_mut(&piece_index)
                // {
                //     let mut piece_download = piece_download_guard.write().await;
                //     let picked_blocks = piece_download.pick_blocks(requests_left);
                //     let num_picked_blocks = picked_blocks.len();
                //     if num_picked_blocks > 0 {
                //         request_queue.extend(picked_blocks.into_iter());
                //     }
                //     requests_left -= num_picked_blocks;
                // } else {
                    let piece_info = &self.torrent.torrent.pieces[piece_index];
                    let mut piece_download = PieceDownload::new(piece_index, piece_info.length);
                    let new_blocks = piece_download.pick_blocks(requests_left);

                    if new_blocks.len() > 0 {
                        requests_left -= new_blocks.len();
                        request_queue.extend(new_blocks.into_iter());
                    }
                    self.torrent
                        .downloads
                        .write()
                        .await
                        .insert(piece_index, RwLock::new(piece_download));
                // }
            }
        }

        // if requests_left > 0 {
        //     for piece_download in self.torrent.downloads.write().await.values_mut() {
        //         let mut piece_download = piece_download.write().await;
        //         if let Some(have) = self.peer.pieces.get(piece_download.index) {
        //             if *have {
        //                 let new_blocks = piece_download.pick_blocks(requests_left);

        //                 if new_blocks.len() > 0 {
        //                     requests_left -= new_blocks.len();
        //                     request_queue.extend(new_blocks.into_iter());
        //                 }
        //             }
        //         }
        //     }
        // }

        // Make some requests now
        self.outgoing_requests
            .extend(request_queue.clone().into_iter());
        let mut it = futures::stream::iter(
            request_queue
                .into_iter()
                .map(|b| Message::Request(b))
                .map(Ok),
        );
        trace!("Sending requests through sink. Total outgoing requests now: {}", self.outgoing_requests.len());
        sink.send_all(&mut it).await?;

        Ok(())
    }
}
