use core::num;
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
use tracing::{debug, info, trace, warn};

use crate::{
    avg::SlidingAvg,
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
    pub download_queue: HashSet<BlockInfo>,
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
    pub last_outgoing_request_time: Option<Instant>,
    /// the average time between incoming pieces. Or if there is no
    /// outstanding request, the time since the piece was requested. It
    /// is essentially an estimate of the time it will take to completely
    /// receive payload message after it has been requested
    pub avg_request_time: SlidingAvg<20>,
    pub snubbed: bool,
}

impl Default for SessionContext {
    fn default() -> Self {
        Self {
            counters: Default::default(),
            state: Default::default(),
            desired_queue_size: 4,
            in_slow_start: true,
            in_end_game: false,
            last_outgoing_request_time: None,
            avg_request_time: Default::default(),
            snubbed: false,
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
    const REQUEST_TIMEOUT: Duration = Duration::from_secs(10);

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
            download_queue: HashSet::new(),
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
        self.ctx.state.connection = ConnectionState::Connecting;

        self.handle_connection(socket).await
    }

    pub async fn handle_connection(
        &mut self,
        mut socket: Framed<TcpStream, HandShakeCodec>,
    ) -> Result<()> {
        let info_hash = self.torrent.info_hash;
        let handshake = HandShake::new(self.torrent.client_id, info_hash);

        self.ctx.counters.protocol.up.add(handshake.len());

        self.ctx.state.connection = ConnectionState::Handshaking;
        socket.send(handshake).await?;
        info!(target: "outgoing_message", peer = self.repr, "Handshake sent");
        self.ctx.last_outgoing_request_time = Some(Instant::now());

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

        if let Err(e) = self.run(socket).await {
            return Err(e.context("Peer disconnected with error"));
        }

        self.ctx.state.connection = ConnectionState::Disconnected;

        self.torrent
            .cmd_tx
            .send(crate::torrent::Command::PeerDisconnected {
                addr: self.peer.addr,
            })?;

        Ok(())
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
                self.ctx.last_outgoing_request_time = Some(Instant::now());
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
                        self.torrent.cmd_tx.send(crate::torrent::Command::PeerConnected { addr: self.peer.addr, id: self.peer.id.unwrap() })?;
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

                let make_requests = self
                    .handle_incoming_piece(block_info, block_data.data.into())
                    .await?;

                if make_requests {
                    self.make_requests(sink).await?;
                }
            }
            Message::Cancel(piece_block) => {
                tracing::info!("Peer sent cancel of piece {}", piece_block.piece_index);
            }
            Message::Port(listen_port) => info!("Port message received: {}", listen_port),
        }

        Ok(())
    }

    async fn handle_incoming_piece(
        &mut self,
        block_info: BlockInfo,
        data: Vec<u8>,
    ) -> Result<bool> {
        let present = self.download_queue.remove(&block_info);
        if !present {
            info!(target: "incoming_piece", peer = self.repr, block = ?block_info, "Invalid block received. Block was not requested");
            return Ok(false);
        }

        self.update_download_stats(block_info.length as u64);

        let mut already_downloaded = false;
        if let Some(piece_download) = self
            .torrent
            .downloads
            .read()
            .await
            .get(&block_info.piece_index())
        {
            let pd = piece_download.read().await;
            already_downloaded = pd.is_downloaded(&block_info);
        }

        if already_downloaded {
            info!(target: "incoming_piece", peer = self.repr, block = ?block_info, "Block already downloaded");
            return Ok(true);
        }

        if let Some(last_request_time) = self.ctx.last_outgoing_request_time {
           let now = Instant::now();

           if now.saturating_duration_since(last_request_time) < self.request_timeout() && self.ctx.snubbed {
                info!(target: "peer_incoming_piece", peer = self.repr, "Peer unsnubbed");
                self.ctx.snubbed = false;
           }
        }

        self.disk_tx
            .send(disk::Command::WriteBlock(block_info.clone(), data.into()))
            .ok();

        // Notify everyone we got block
        // let mut piece_complete = false;
        if let Some(piece_download) = self
            .torrent
            .downloads
            .write()
            .await
            .get(&block_info.piece_index)
        {
            piece_download.write().await.received_block(&block_info);
        }

        Ok(true)
    }

    fn update_download_stats(&mut self, block_len: u64) {
        self.ctx.counters.payload.down.add(block_len);

        let now = Instant::now();
        if let Some(lrt) = self.ctx.last_outgoing_request_time {
            self.ctx
                .avg_request_time
                .add_sample(now.duration_since(lrt));
            trace!(
                "Request_Time {} +-{} ms",
                self.ctx.avg_request_time.mean().as_millis(),
                self.ctx.avg_request_time.avg_deviation().as_millis()
            );
        }

        // we completed an incoming block, and there are still outstanding reqeusts.
        // The next block we expect to receive now has another timeout period until
        // we time out. So, reset the timer.
        if !self.download_queue.is_empty() {
            self.ctx.last_outgoing_request_time = Some(now);
        }

        if self.ctx.in_slow_start {
            self.ctx.desired_queue_size += 1;
        }

        self.update_desired_queue_size();
    }

    async fn free_pending_blocks(&mut self) {
        //TODO: Update piece picker about the free pieces
        let download_guard = self.torrent.downloads.write().await;

        for block_info in self.download_queue.drain() {
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
        } else if self.ctx.state.is_interested && !is_interested {
            info!("No longer interested in peer");
            self.ctx.state.is_interested = false;
        }

        Ok(())
    }

    async fn handle_tick(
        &mut self,
        sink: &mut SplitSink<Framed<TcpStream, MessageCodec>, Message>,
        now: Instant,
    ) -> Result<()> {
        trace!(target: "peer_connection_tick", peer = self.repr,
         request_in_flight = self.download_queue.len(), queue_size = self.ctx.desired_queue_size);

        if !self.ctx.state.is_choked {
            if self.ctx.state.is_interested
                && self.download_queue.len() < self.ctx.desired_queue_size
            {
                self.make_requests(sink).await?;
            }
        }

        if let Some(last_requested) = self.ctx.last_outgoing_request_time {
            let since_last_request = now.saturating_duration_since(last_requested);
            let request_timeout = self.request_timeout();

            if !self.download_queue.is_empty() && since_last_request > request_timeout {
                warn!(target: "peer_request_timeout", peer = self.repr, "Timeout after {} ms, cancelling {} request(s)",
                    since_last_request.as_millis(),
                    self.download_queue.len()
                );
                self.snub_peer(sink).await?;
            }
        }

        if self.ctx.in_slow_start
            && !self.ctx.state.is_choked
            && self.ctx.counters.payload.down.round() > 0
            && self.ctx.counters.payload.down.round() + 10000 < self.ctx.counters.payload.down.avg()
        {
            trace!(target: "update_slow_start", peer = self.repr, "Slow start disabled");
            self.ctx.in_slow_start = false;
        }
        self.ctx.counters.reset();

        self.send_keepalive(sink).await?;

        self.update_desired_queue_size();

        Ok(())
    }

    async fn snub_peer(
        &mut self,
        sink: &mut SplitSink<Framed<TcpStream, MessageCodec>, Message>,
    ) -> Result<()> {
        self.free_pending_blocks().await;

        self.ctx.desired_queue_size = 1;
        self.ctx.snubbed = true;
        self.ctx.in_slow_start = false;

        self.make_requests(sink).await
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

        let mut num_requests = desired_queue_size.saturating_sub(self.download_queue.len());

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
        for block in &self.download_queue {
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
        self.download_queue
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
            self.download_queue.len()
        );
        let num_requests = requests.len();
        let mut it = futures::stream::iter(requests.into_iter());
        sink.send_all(&mut it).await?;

        if num_requests > 0 {
            self.ctx.last_outgoing_request_time = Some(Instant::now());
        }
        Ok(())
    }

    fn request_timeout(&self) -> Duration {
        let deviation = self.ctx.avg_request_time.avg_deviation();
        let avg = self.ctx.avg_request_time.mean();
        let num_samples = self.ctx.avg_request_time.num_samples();

        let mut timeout;
        if num_samples < 2 {
            if num_samples == 0 {
                return Self::REQUEST_TIMEOUT;
            }
            timeout = avg + avg / 5;
        } else {
            timeout = avg + deviation * 4;
        }

        return timeout.max(Duration::from_secs(2));
    }

    async fn handle_piece_completion(&mut self, index: PieceIndex) {
        self.download_queue.retain(|b| b.piece_index() != index)
    }

    async fn handle_shutdown(
        &mut self,
        sink: &mut SplitSink<Framed<TcpStream, MessageCodec>, Message>,
    ) -> Result<()> {
        // Send cancel to all pending messages
        let mut protocol_bytes = 0u64;

        for block_info in self.download_queue.drain() {
            let msg = Message::Cancel(block_info);
            protocol_bytes += msg.protocol_len();
            sink.send(msg).await.ok();
        }
        self.ctx.last_outgoing_request_time = Some(Instant::now());

        self.ctx.counters.protocol.up.add(protocol_bytes);

        Ok(())
    }

    fn update_desired_queue_size(&mut self) {

        if self.ctx.snubbed {
            self.ctx.desired_queue_size = 1;
            return;
        }

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

    async fn send_keepalive(
        &mut self,
        sink: &mut SplitSink<Framed<TcpStream, MessageCodec>, Message>,
    ) -> Result<()> {
        if !matches!(self.ctx.state.connection, ConnectionState::Connected) {
            return Ok(());
        }

        if let Some(last) = self.ctx.last_outgoing_request_time {
            let duration = Instant::now().saturating_duration_since(last);

            if duration < Self::TIMEOUT / 2 {
                return Ok(());
            }

            trace!(target: "outgoing_message", peer = self.repr, r#type = "keepalive");
            sink.send(Message::KeepAlive).await?;
        }
        Ok(())
    }

    // async fn pick_blocks(&mut self) -> Result<()> {

    // }
}
