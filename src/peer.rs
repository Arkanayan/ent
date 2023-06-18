use std::{
    collections::HashSet,
    iter::Extend,
    net::SocketAddr,
    sync::Arc,
    time::{Duration, Instant},
};

use anyhow::{anyhow, Ok, Result};
use futures::{stream::SplitSink, SinkExt, StreamExt};
use tokio::{
    net::TcpStream,
    sync::{
        mpsc::{UnboundedReceiver, UnboundedSender},
        RwLock,
    },
    time,
};
use tokio_util::codec::{Framed, FramedParts};
use tracing::{debug, info, trace};

use crate::{
    disk::{self, CommandSender},
    download::{PieceDownload, BLOCK_LEN},
    messages::{BitField, HandShake, HandShakeCodec, Message, MessageCodec, MessageId},
    metainfo::PeerID,
    stat::ThruputCounters,
    torrent::{BlockInfo, TorrentContext},
    units::PieceIndex,
};

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
pub enum Command {
    PieceCompletion(PieceIndex),
    Shutdown,
}

pub(crate) type Sender = UnboundedSender<Command>;
type Receiver = UnboundedReceiver<Command>;

#[derive(Debug)]
pub struct PeerSession {
    pub repr: String,
    pub outgoing_requests: HashSet<BlockInfo>,
    pub torrent: Arc<TorrentContext>,
    pub peer: PeerInfo,
    cmd_rx: Receiver,
    disk_tx: CommandSender,
    pub ctx: SessionContext,
}

#[derive(Debug)]
pub struct SessionContext {
    pub counters: ThruputCounters,
    pub state: SessionState,
    pub desired_queue_size: usize,
    pub in_slow_start: bool,
    pub in_end_game: bool,
    pub last_sent_time: Option<Instant>
}

impl Default for SessionContext {
    fn default() -> Self {
        Self {
            counters: Default::default(),
            state: Default::default(),
            desired_queue_size: 4,
            in_slow_start: true,
            in_end_game: false,
            last_sent_time: None
        }
    }
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
}

impl Default for SessionState {
    fn default() -> Self {
        Self {
            connection: Default::default(),
            is_peer_interested: false,
            is_peer_choked: true,
            is_interested: false,
            is_choked: true,
        }
    }
}

impl PeerSession {
    const TIMEOUT: Duration = Duration::from_secs(120);

    pub fn new(
        torrent_context: Arc<TorrentContext>,
        addr: SocketAddr,
        cmd_rx: Receiver,
        disk_tx: CommandSender,
    ) -> Self {
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
            repr: format!("{}", addr),
            cmd_rx: cmd_rx,
            disk_tx: disk_tx,
            ctx: Default::default(),
        }
    }

    pub async fn start_connection(&mut self) -> Result<()> {
        info!("Connecting to {}", self.peer.addr);

        let socket = TcpStream::connect(self.peer.addr).await?;
        info!(peer = self.repr, "TCP Socket Connected");

        let socket = Framed::new(socket, HandShakeCodec);
        self.ctx.state.connection = ConnectionState::Handshaking;

        self.handle_connection(socket).await
    }

    pub async fn handle_connection(
        &mut self,
        mut socket: Framed<TcpStream, HandShakeCodec>,
    ) -> Result<()> {
        let info_hash = self.torrent.info_hash;
        let handshake = HandShake::new(self.torrent.client_id, info_hash);

        self.ctx.counters.protocol.up.add(handshake.len());

        socket.send(handshake).await?;
        info!(target: "outgoing_message", peer = self.repr, "Handshake sent");
        self.ctx.last_sent_time = Some(Instant::now());

        if let Some(peer_handshake) = socket.next().await {
            info!(target: "incoming_message", "Packet received from peer. Should be handshake");

            let handshake = peer_handshake?;
            self.ctx.counters.protocol.down.add(handshake.len());
            info!(target: "connection", peer = self.repr, "Handshake received");
            log::trace!(
                target: &self.repr,
                "Handshake: {:?}, Peer Id: {}",
                handshake,
                String::from_utf8_lossy(&handshake.peer_id)
            );

            if handshake.info_hash != info_hash {
                return Err(anyhow!("Invalid InfoHash"));
            }

            self.peer.id = Some(handshake.peer_id);
            self.ctx.state.connection = ConnectionState::AvailabilityExchange;
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

    pub async fn run(&mut self, socket: Framed<TcpStream, MessageCodec>) -> Result<()> {
        let (mut sink, mut stream) = socket.split();

        // tis the time to send bitfield
        {
            let piece_picker_guard = self.torrent.piece_picker.read().await;
            let own_pieces = piece_picker_guard.own_pieces();
            if own_pieces.any() {
                info!("Sending piece availability");
                self.ctx
                    .counters
                    .protocol
                    .up
                    .add(MessageId::BitField.header_len() + own_pieces.len() as u64);
                sink.send(Message::BitField(own_pieces.clone())).await?;
                info!("Sent piece availability");
                self.ctx.last_sent_time = Some(Instant::now());
            }
        }

        let mut tick_timer = time::interval(Duration::from_secs(1));

        loop {
            tokio::select! {
                now = tick_timer.tick() => {
                    trace!("Time Ticking");
                    self.handle_tick(&mut sink, now.into_std()).await?;
                }
                Some(msg) = stream.next() => {
                    let msg = msg?;

                    if self.ctx.state.connection == ConnectionState::AvailabilityExchange {
                        if let Message::BitField(bitfield) = msg {
                            info!("bitfield message got. {}", bitfield.count_ones());
                            self.handle_bitfield_msg(&mut sink, bitfield).await?;
                        } else {
                            // info!("Other message. {:?}", msg);
                            self.handle_msg(&mut sink, msg).await?;
                        }

                        self.ctx.state.connection = ConnectionState::Connected;
                        info!("Session state: {:?}", self.ctx.state.connection);
                    } else {
                        // info!("Other message. {:?}", msg);
                        self.handle_msg(&mut sink, msg).await?;
                    }
                }
                Some(cmd) = self.cmd_rx.recv() => {
                    match cmd {
                        Command::PieceCompletion(index) => self.handle_piece_completion(index).await,
                        Command::Shutdown => {
                            self.handle_shutdown(&mut sink).await?;
                            return Ok(());
                        },
                    }
                }
            }
        }
    }

    async fn handle_msg(
        &mut self,
        sink: &mut SplitSink<Framed<TcpStream, MessageCodec>, Message>,
        message: Message,
    ) -> Result<()> {
        self.ctx.counters.protocol.down.add(message.protocol_len());

        match message {
            Message::KeepAlive => {
                info!(target: "incoming_message", peer = self.repr, "KeepAlive received");
            }
            Message::Choke => {
                info!("Peer choking us");
                if !self.ctx.state.is_choked {
                    self.ctx.state.is_choked = true;
                    self.free_pending_blocks().await;
                }
            }
            Message::Unchoke => {
                if self.ctx.state.is_choked {
                    info!("Peer unchoked us");
                    self.ctx.state.is_choked = false;

                    if self.ctx.state.is_interested {
                        self.make_requests(sink).await?
                    }
                }
            }
            Message::Interested => {
                tracing::info!("Peer is interested in us. Nothing to do here!");
                self.ctx.state.is_peer_interested = true;
            }
            Message::NotInterested => {
                tracing::info!("Peer is not interested in us. Nothing to do here!");
                self.ctx.state.is_peer_interested = false;
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
                let block_info = BlockInfo {
                    piece_index: block_data.piece_index,
                    offset: block_data.offset,
                    length: block_data.data.len() as u32,
                };
                self.ctx
                    .counters
                    .payload
                    .down
                    .add(block_data.data.len() as u64);

                // let torrent_size = self.torrent.torrent.metainfo.info.length.unwrap();
                // let percent_downloaded =
                //     (self.state.total_downloaded_bytes as f64 / torrent_size as f64) * 100 as f64;
                // tracing::info!(
                //     "Peer send us piece={} block={} downloaded={}/{} {:.3}%",
                //     block_data.piece_index,
                //     block_info.index_in_piece(),
                //     self.state.total_downloaded_bytes,
                //     torrent_size,
                //     percent_downloaded
                // );

                self.outgoing_requests.remove(&block_info);

                self.disk_tx
                    .send(disk::Command::WriteBlock(
                        block_info.clone(),
                        block_data.data.into(),
                    ))
                    .ok();

                // Notify everyone we got block
                // let mut piece_complete = false;
                if let Some(piece_download) = self
                    .torrent
                    .downloads
                    .read()
                    .await
                    .get(&block_data.piece_index)
                {
                    piece_download.write().await.received_block(&block_info);

                    // if piece_download
                    //     .read()
                    //     .await
                    //     .blocks
                    //     .iter()
                    //     .all(|b| *b == BlockStatus::Received)
                    // {
                    //     piece_complete = true;
                    // }
                }
                // if piece_complete {
                //     tracing::info!("Completed piece {}", block_data.piece_index);
                //     self.torrent
                //         .piece_picker
                //         .write()
                //         .await
                //         .received_piece(block_data.piece_index);
                // }

                if self.ctx.in_slow_start {
                    self.ctx.desired_queue_size += 1;
                }

                self.update_desired_queue_size();

                self.make_requests(sink).await?;
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
        self.ctx
            .counters
            .payload
            .down
            .add(MessageId::BitField.header_len() + bitfield.len() as u64);

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

        self.update_interest(sink, interested).await?;

        Ok(())
    }

    async fn update_interest(
        &mut self,
        sink: &mut SplitSink<Framed<TcpStream, MessageCodec>, Message>,
        is_interested: bool,
    ) -> Result<()> {
        if !self.ctx.state.is_interested && is_interested {
            self.ctx
                .counters
                .protocol
                .up
                .add(MessageId::Interested.header_len());
            sink.send(Message::Interested).await?;
            info!("Became interested in peer");
            self.ctx.state.is_interested = true;
            self.ctx.last_sent_time = Some(Instant::now());
        } else if self.ctx.state.is_interested && !is_interested {
            info!("No longer interested in peer");
            self.ctx.state.is_interested = false;
        }

        Ok(())
    }

    async fn handle_tick(
        &mut self,
        sink: &mut SplitSink<Framed<TcpStream, MessageCodec>, Message>,
        _instant: Instant,
    ) -> Result<()> {
        trace!(target: "peer_connection_tick", peer = self.repr,
         request_in_flight = self.outgoing_requests.len(), queue_size = self.ctx.desired_queue_size);

        if !self.ctx.state.is_choked {
            if self.ctx.state.is_interested
                && self.outgoing_requests.len() < self.ctx.desired_queue_size
            {
                self.make_requests(sink).await?;
            }
        }

        if self.ctx.in_slow_start
            && !self.ctx.state.is_choked
            && self.ctx.counters.payload.down.round() > 0
            && self.ctx.counters.payload.down.round() + 10000 < self.ctx.counters.payload.down.avg()
        {
            self.ctx.in_slow_start = false;
        }
        self.ctx.counters.reset();

        self.send_keepalive(sink).await?;

        self.update_desired_queue_size();

        Ok(())
    }

    async fn make_requests(
        &mut self,
        sink: &mut SplitSink<Framed<TcpStream, MessageCodec>, Message>,
    ) -> Result<()> {
        trace!("Making requests");

        if self.ctx.state.is_choked {
            debug!("Can't make requests while choked");
            return Ok(());
        }

        if !self.ctx.state.is_interested {
            debug!("Can't make requests while not interested");
            return Ok(());
        }

        let desired_queue_size = self.ctx.desired_queue_size;

        let mut num_requests = desired_queue_size.saturating_sub(self.outgoing_requests.len());

        trace!("Can make {} requests", num_requests);

        if num_requests == 0 {
            return Ok(());
        }

        let mut request_queue = Vec::with_capacity(num_requests);

        trace!(
            "Total in torrent download {}",
            self.torrent.downloads.read().await.len()
        );
        // First try to complete alredy downloading pieces
        for block in &self.outgoing_requests {
            if let Some(piece) = self
                .torrent
                .downloads
                .write()
                .await
                .get_mut(&block.piece_index())
            {
                if num_requests <= 0 {
                    break;
                }

                let new_blocks = piece.write().await.pick_blocks(num_requests);
                if new_blocks.len() > 0 {
                    num_requests -= new_blocks.len();
                    request_queue.extend(new_blocks.into_iter());
                }
            }
        }

        // If the queue is still not filled; grab new piece
        while num_requests > 0 {
            if let Some(piece_index) = self
                .torrent
                .piece_picker
                .write()
                .await
                .pick_piece(&self.peer.pieces)
            {
                let piece_info = &self.torrent.torrent.pieces[piece_index];
                let mut piece_download = PieceDownload::new(piece_index, piece_info.length);
                let new_blocks = piece_download.pick_blocks(num_requests);

                if new_blocks.len() > 0 {
                    num_requests -= new_blocks.len();
                    request_queue.extend(new_blocks.into_iter());
                }
                self.torrent
                    .downloads
                    .write()
                    .await
                    .insert(piece_index, RwLock::new(piece_download));
            } else {
                break;
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

        // let requests = request_queue.into_iter().map(|b| Message::Request(b)).collect::<Vec<_>>();

        let mut requests = Vec::with_capacity(request_queue.len());
        let mut protocol_bytes = 0u64;

        for req in request_queue {
            let msg = Message::Request(req);

            protocol_bytes += msg.protocol_len();

            requests.push(Ok(msg));
        }

        self.ctx.counters.protocol.up.add(protocol_bytes);

        trace!(
            "Sending requests through sink. Total outgoing requests now: {}",
            self.outgoing_requests.len()
        );
        let mut it = futures::stream::iter(requests.into_iter());
        sink.send_all(&mut it).await?;
        self.ctx.last_sent_time = Some(Instant::now());
        Ok(())
    }

    async fn handle_piece_completion(&mut self, index: PieceIndex) {
        self.outgoing_requests.retain(|b| b.piece_index() != index)
    }

    async fn handle_shutdown(
        &mut self,
        sink: &mut SplitSink<Framed<TcpStream, MessageCodec>, Message>,
    ) -> Result<()> {
        // Send cancel to all pending messages
        let mut protocol_bytes = 0u64;

        for block_info in self.outgoing_requests.drain() {
            let msg = Message::Cancel(block_info);
            protocol_bytes += msg.protocol_len();
            sink.send(msg).await.ok();
        }
        self.ctx.last_sent_time = Some(Instant::now());

        self.ctx.counters.protocol.up.add(protocol_bytes);

        Ok(())
    }

    fn update_desired_queue_size(&mut self) {
        let prev_queue_size = self.ctx.desired_queue_size;

        let download_rate = self.ctx.counters.payload.down.avg();

        // the length of the request queue given in the number of seconds it
        // should take for the other end to send all the pieces. i.e. the
        // actual number of requests depends on the download rate and this
        // number.
        let queue_time = 3;

        let max_queue_size = 500;
        let min_queue_size = 2;

        if !self.ctx.in_slow_start {
            self.ctx.desired_queue_size = ((queue_time * download_rate / BLOCK_LEN as u64)
                as usize)
                .clamp(min_queue_size, max_queue_size);
        }

        if prev_queue_size != self.ctx.desired_queue_size {
            info!(target: "Request queue size changed", peer = self.repr, previous = prev_queue_size, new = self.ctx.desired_queue_size);
        }
    }

    async fn send_keepalive(&mut self, sink: &mut SplitSink<Framed<TcpStream, MessageCodec>, Message>) -> Result<()> {
        
        if !matches!(self.ctx.state.connection, ConnectionState::Connected) {
            return Ok(());
        }

        if let Some(last) = self.ctx.last_sent_time {
            let duration = Instant::now().saturating_duration_since(last);

            if duration < Self::TIMEOUT / 2 {
                return Ok(());
            }

            trace!(target: "outgoing_message", peer = self.repr, r#type = "keepalive");
            sink.send(Message::KeepAlive).await?;
        }
        Ok(())
    }
}