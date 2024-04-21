use core::num;
use std::{
    collections::VecDeque,
    iter::Extend,
    net::SocketAddr,
    sync::{atomic::Ordering, Arc},
    time::{Duration, Instant},
};

use anyhow::{anyhow, bail, Ok, Result};
use futures::{executor::block_on, stream::SplitSink, SinkExt, StreamExt};
use sha1::digest::{block_buffer::Block, typenum::private::ShiftDiff};
use tokio::{
    net::TcpStream,
    sync::{
        mpsc::{UnboundedReceiver, UnboundedSender},
        RwLock,
    },
    time,
};
use tokio_util::codec::{Framed, FramedParts};
use tracing::{Instrument, debug, info, span, trace, warn, Level, Span};

use crate::{
    avg::SlidingAvg,
    disk::{self, CommandSender, Piece},
    download::{PieceDownload, BLOCK_LEN},
    messages::{BitField, HandShake, HandShakeCodec, Message, MessageCodec, MessageId},
    metainfo::PeerID,
    piece_picker::{PieceBlock, PiecePicker},
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
    CancelRequest(PieceBlock),
    Shutdown,
}

pub(crate) type Sender = UnboundedSender<Command>;
type Receiver = UnboundedReceiver<Command>;

#[derive(Clone)]
pub struct PendingBlock {
    pub block: PieceBlock,
    pub busy: bool,
    pub not_wanted: bool,
    pub timed_out: bool,
}

impl PendingBlock {
    fn new(block: PieceBlock) -> Self {
        PendingBlock {
            block: block,
            busy: false,
            not_wanted: false,
            timed_out: false,
        }
    }
}

impl PartialEq for PendingBlock {
    fn eq(&self, other: &Self) -> bool {
        self.block == other.block
            && self.not_wanted == other.not_wanted
            && self.timed_out == other.timed_out
    }
}

pub struct PeerSession {
    pub repr: String,
    pub download_queue: VecDeque<PendingBlock>,
    pub request_queue: VecDeque<PendingBlock>,
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
    pub last_receive_time: Instant,
    pub connect_time: Instant,
    pub last_unchoked: Instant,// TODO implement disconnection when peer has not responded; In libtorrent, if the number of connections aren't too large, it doesn't bother to disconnect
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
            last_receive_time: Instant::now(),
            last_unchoked: Instant::now(),
            connect_time: Instant::now(),
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
    const PEER_CONNECT_TIMEOUT: Duration = Duration::from_secs(15);
    const HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(10);
    const REQUEST_TIMEOUT: Duration = Duration::from_secs(10);

    pub fn new(
        torrent_context: Arc<TorrentContext>,
        addr: SocketAddr,
        cmd_rx: Receiver,
        disk_tx: CommandSender,
    ) -> Self {
        let piece_count = torrent_context.storage.piece_count;
        let mut pieces = BitField::with_capacity(piece_count);
        pieces.resize(piece_count, false);
        PeerSession {
            peer: PeerInfo {
                addr: addr,
                id: None,
                pieces: pieces,
                piece_count: piece_count,
            },
            download_queue: VecDeque::new(),
            request_queue: VecDeque::new(),
            torrent: torrent_context,
            repr: format!("{}", addr),
            cmd_rx: cmd_rx,
            disk_tx: disk_tx,
            ctx: Default::default(),
        }
    }

    pub async fn start_connection(&mut self) -> Result<()> {
        info!("Connecting to {}", self.peer.addr);

        let socket = tokio::time::timeout(Self::PEER_CONNECT_TIMEOUT, TcpStream::connect(self.peer.addr)).await??;
        info!(peer = self.repr, "TCP Socket Connected");

        let socket = Framed::new(socket, HandShakeCodec);
        self.ctx.state.connection = ConnectionState::Connecting;
        self.ctx.connect_time = Instant::now();
        self.ctx.last_receive_time = self.ctx.connect_time;

        self.handle_connection(socket).await
    }

    pub async fn handle_connection(
        &mut self,
        mut socket: Framed<TcpStream, HandShakeCodec>,
    ) -> Result<()> {
        let span = span!(tracing::Level::INFO, "Peer Handle connection", peer = %self.peer.addr);

        let info_hash = self.torrent.info_hash;
        let handshake = HandShake::new(self.torrent.client_id, info_hash);

        self.ctx.counters.protocol.up.add(handshake.len());

        self.ctx.state.connection = ConnectionState::Handshaking;
        socket.send(handshake).await?;
        info!("Handshake sent");
        self.ctx.last_outgoing_request_time = Some(Instant::now());

        match tokio::time::timeout(Self::HANDSHAKE_TIMEOUT, socket.next()).await {
         Result::Ok(Some(peer_handshake)) => {
            info!("Packet received from peer. Should be handshake");

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
        },
        Result::Ok(None) => {
            info!("Handshake hasn't received. Disconnecting");
            bail!("Handshake not received")
        },
        Err(e) => {
            info!("Handshake timeout. Disconnecting");
            return Err(e.into());
       }
    }

        let socket = socket.map_codec(|_| MessageCodec);

        if let Err(e) = self.run(socket).instrument(span).await {
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

        use Command::*;
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
                        PieceCompletion(index) => self.handle_piece_completion(index).await,
                        Shutdown => {
                            self.handle_shutdown(&mut sink).await?;
                            return Ok(());
                        },
                        CancelRequest(pb) => {
                            self.cancel_request(pb, &mut sink).await?;
                        }
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
                    self.ctx.last_unchoked = Instant::now();

                    if self.ctx.state.is_interested {
                        self.pick_blocks().await;
                        self.send_block_requests(sink).await?;
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
                    if self.pick_blocks().await {
                        self.send_block_requests(sink).await;
                    }
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
        self.update_download_stats(block_info.length as u64);

        if block_info.length == 0 {
            // incoming reject request by bitcomet
            return Ok(true);
        }

        // if we are seed; ignore it

        let block_finished = PieceBlock {
            piece_index: block_info.piece_index,
            block_index: (block_info.offset / BLOCK_LEN) as usize,
        };

        info!("Incoming Block: {:?} peer: {:?}", block_finished, self.peer.addr);

        let pb = self
            .download_queue
            .iter()
            .enumerate()
            .find(|(_, b)| b.block == block_finished);

        if pb.is_none() {
            info!(target: "peer_connection::handle_piece", peer = ?self.peer.addr, piece_id = block_finished.piece_index, block_id = block_finished.block_index, "Unwanted block received");
            return Ok(false);
        }

        let (idx, pb) = pb.unwrap();

        let mut picker = self.torrent.piece_picker.write().await;

        let now = Instant::now();

        // the block we got is already finished, ignore it
        if picker.is_downloaded(&block_finished) {
            trace!(target: "peer_connection::handle_piece", peer = ?self.peer.addr, piece_id = block_finished.piece_index, block_id = block_finished.block_index, "Block already finished");

            self.download_queue.remove(idx);
            return Ok(true);
        }

        if let Some(last_requested) = self.ctx.last_outgoing_request_time {
            // we received a request within the timeout, make sure this peer is not snubbed anymore
            if now.saturating_duration_since(last_requested) < self.request_timeout()
                && self.ctx.snubbed
            {
                self.ctx.snubbed = false;
            }
        }

        self.download_queue.remove(idx);

        self.disk_tx
            .send(disk::Command::WriteBlock(block_finished, data.into()))
            .ok();

        let multi = picker.num_peers(&block_finished) > 1;

        // picker.mark_as_writing(&block_finished, self.peer.addr);
        trace!("Marking piece/block {}/{} as finished", block_finished.piece_index, block_finished.block_index);
        picker.mark_as_finished(&block_finished, Some(self.peer.addr));

        // if we requested this block from other peers, cancel it now
        if multi {
            self.torrent
                .cmd_tx
                .send(crate::torrent::Command::CancelRequest(block_finished))?;
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

    async fn free_pending_blocks(&mut self) {}

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
                if self.pick_blocks().await {
                    self.send_block_requests(sink).await?;
                }
            }
        }

        if let Some(last_requested) = self.ctx.last_outgoing_request_time {
            let since_last_request = now.saturating_duration_since(last_requested);
            let request_timeout = self.request_timeout();

            if !self.download_queue.is_empty() && since_last_request > request_timeout {
                warn!(target: "peer_request_timeout", peer = self.repr, "Timeout after {} ms, cancelling {} request(s) and snubbing peer",
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
        self.ctx.snubbed = true;
        self.ctx.in_slow_start = false;

        self.ctx.desired_queue_size = 1;

        let mut picker = self.torrent.piece_picker.write().await;

        // wipe unsent requests
        for b in self.request_queue.drain(..) {
            picker.abort_download(&b.block, &self.peer.addr);
        }
        drop(picker);

        // time out the last request eligible block in the queue
        if let Some((idx, pd)) = self
            .download_queue
            .iter()
            .enumerate()
            .rev()
            .find(|(_, pd)| !pd.timed_out && !pd.not_wanted)
        {
            let r = pd.block;

            let picker = self.torrent.piece_picker.read().await;

            // only cancel a request if it blocks the piece from being completed
            // (i.e. no free blocks to request from it)
            let dp = picker
                .piece_downloads
                .get(&r.piece_index)
                .expect("Downloading piece should be present");

            let free_blocks = picker.blocks_in_piece(r.piece_index) as i32
                - dp.finished as i32
                - dp.writing as i32
                - dp.requested as i32;

            // if there are still blocks available for other peers to pick, we're still not holding up the completion of the piece
            // and there's no need to cancel the requests.
            // http://blog.libtorrent.org/2011/11/block-request-time-outs/
            if free_blocks > 0 {
                return Ok(());
            }

            drop(picker);
            // request a new block before removing the previous one
            // in order to prevent in from picking the same block again,
            // stalling the same piece indefinitely.
            self.ctx.desired_queue_size = 2;

            self.pick_blocks().await;

            self.ctx.desired_queue_size = 1;

            self.torrent
                .piece_picker
                .write()
                .await
                .abort_download(&r, &self.peer.addr);
        }

        self.send_block_requests(sink).await?;

        Ok(())
    }

    fn request_timeout(&self) -> Duration {
        let deviation = self.ctx.avg_request_time.avg_deviation();
        let avg = self.ctx.avg_request_time.mean();
        let num_samples = self.ctx.avg_request_time.num_samples();

        let timeout;
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
        self.download_queue.retain(|b| b.block.piece_index != index)
    }

    async fn handle_shutdown(
        &mut self,
        sink: &mut SplitSink<Framed<TcpStream, MessageCodec>, Message>,
    ) -> Result<()> {
        // Send cancel to all pending messages
        let mut protocol_bytes = 0u64;

        for block in self.download_queue.drain(..) {
            let block_offset = block.block.block_index as u32 * BLOCK_LEN;
            let block_size = self
                .torrent
                .piece_picker
                .read()
                .await
                .piece_size(block.block.piece_index)
                .saturating_sub(block_offset)
                .min(BLOCK_LEN);
            let block_request = BlockInfo {
                piece_index: block.block.piece_index,
                offset: block_offset,
                length: block_size,
            };
            let msg = Message::Cancel(block_request);
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

    async fn pick_blocks(&mut self) -> bool {
        let desired_queue_size = self.ctx.desired_queue_size;

        let num_requests =
            desired_queue_size.saturating_sub(self.download_queue.len() + self.request_queue.len());

        if num_requests == 0 {
            return false;
        }

        let torrent = Arc::clone(&self.torrent);
        let mut picker = torrent.piece_picker.write().await;
        let num_peers = self.torrent.num_connected_peers.load(Ordering::Relaxed);

        let mut interesting_pieces = Vec::with_capacity(num_requests);

        picker.pick_pieces(
            &self.peer.pieces,
            &mut interesting_pieces,
            num_requests,
            &self.peer.addr,
            num_peers,
        );

        info!(target: "peer_connection:pick_blocks", num_blocks_picked = interesting_pieces.len());

        let strict_end_game_mode = true; // TODO - Move it into a config

        // if the number of pieces we have + the number of pieces we're requesting from is less than
        // the number of pieces in the torrect, there are still some unrequested pieces and we're not strictly speaking in end-game mode yet
        // also, if we already have at least one oustanding request, we shouldn't pick any busy pieces either
        // in time critical mode, it's OK to request busy blocks
        let dont_pick_busy_blocks = (strict_end_game_mode
            && (picker.piece_downloads.len() as u32) < picker.num_left())
            || self.download_queue.len() + self.request_queue.len() > 0;

        // this is filled with an interesting piece that some other peer is currently downloading
        let mut busy_block = None;

        let mut num_blocks_picked = interesting_pieces.len() as i32;

        for pb in &interesting_pieces {
            if num_blocks_picked <= 0 {
                break;
            }

            let num_block_requests = picker.num_peers(&pb);

            if num_block_requests > 0 {
                // have we picked enough pieces?
                if num_blocks_picked <= 0 {
                    break;
                }

                // this block is busy. This means all the following blocks
                // in the interesting_pieces are busy as well, we might as well just exit the loop
                if dont_pick_busy_blocks {
                    break;
                }

                busy_block = Some(pb);
            }

            // don't request pieces we already have in our request queue
            // This happens when pieces time out or the peer sends us pieces
            // we didn't request. Those aren't marked in the piece picker, but we still
            // keep track of them in the donwload queue
            // if self.download_queue.contains(pb) || self.request_queue.contains(pb) {
            //     continue;
            // }
            info!("Adding block request: {pb:?} ");
            if !self.add_request(&pb, &mut picker, false).await {
                continue;
            }
            assert_eq!(picker.num_peers(pb), 1);
            // assert_eq!(picker.is_requested(pb), true);
            num_blocks_picked -= 1;
        }

        // we have picked as many blocks as we should
        // we're done!
        if num_blocks_picked <= 0 {
            // since we could pick as many blocks as we requested without having
            // to resort to picking busy ones, we 're not in end-game mode
            // self.set_endgame(false);
            return true;
        }

        // we did not pickas many pieces as we wanted, because there aren't enough. This means we're in end-game mode
        // as long as we have at least one request outstanding, we shouldn't pick another piece
        // if we are attempting to download 'allowed' pieces and can't find any, that doesn't count as end-game
        if !self.ctx.state.is_choked {
            // self.set_endgame(true);
        }

        if busy_block.is_none() || self.download_queue.len() + self.request_queue.len() > 0 {
            return true;
        }

        info!("Adding busy block: {busy_block:?} ");
        self.add_request(
            busy_block.expect("busy block should not be None"),
            &mut picker,
            true,
        )
        .await;

        return true;
    }

    /// adds a block to the request queue
    /// returns true if successful otherwise false
    async fn add_request(&mut self, pb: &PieceBlock, picker: &mut PiecePicker, busy: bool) -> bool {
        if self.ctx.state.connection == ConnectionState::Disconnected {
            return false;
        }

        if busy {
            // this block is busy (i.e. it has been requested from another peer already).
            // Only allow one busy request in the pipeline at the time this rule

            if self.download_queue.iter().any(|pb| pb.busy)
                || self.request_queue.iter().any(|pb| pb.busy)
            {
                info!(target: "piece_picker", "not_picking: busy block already exists");
                return false;
            }
        }

        if !picker.mark_as_downloading(&pb, Some(self.peer.addr)) {
            info!(target: "piece_picker", "not_picking: {}, {} falied to mark_as_downloading, peer = {:?}", pb.piece_index, pb.block_index, self.peer.addr);
            return false;
        }

        let mut pending_block = PendingBlock::new(pb.to_owned());
        pending_block.busy = busy;

        self.request_queue.push_back(pending_block);

        true
    }

    async fn send_block_requests(
        &mut self,
        sink: &mut SplitSink<Framed<TcpStream, MessageCodec>, Message>,
    ) -> Result<()> {
        info!("Send block requests");

        if self.download_queue.len() >= self.ctx.desired_queue_size {
            return Ok(());
        }

        let is_empty_download_queue: bool = self.download_queue.is_empty();

        let mut picker = self.torrent.piece_picker.write().await;

        let mut messages = vec![];

        info!("Request queue size: {}", self.request_queue.len());

        while !self.request_queue.is_empty()
            && self.download_queue.len() < self.ctx.desired_queue_size
        {
            let block = self
                .request_queue
                .pop_front()
                .expect("Request queue should not be empty");

            // this can happen if a block times out, is req-requested and then arrives "unexpectedly"
            if picker.is_downloaded(&block.block) {
                picker.abort_download(&block.block, &self.peer.addr);
                continue;
            }

            let block_offset = block.block.block_index as u32 * BLOCK_LEN;
            let block_size = picker
                .piece_size(block.block.piece_index)
                .saturating_sub(block_offset)
                .min(BLOCK_LEN);
            let block_request = BlockInfo {
                piece_index: block.block.piece_index,
                offset: block_offset,
                length: block_size,
            };

            self.download_queue.push_back(block);

            let message = Message::Request(block_request);
            trace!("Block request message {:?}", message);
            messages.push(message);
        }
        
        info!("Sending block requests: {}", messages.len());
        let mut stream = futures::stream::iter(messages.into_iter().map(Ok));
        sink.send_all(&mut stream).await?;

        if !self.download_queue.is_empty() && is_empty_download_queue {
            // This means we just added a request to this connection that
            // previously did not have a request. That's when we start the request timeout.
            self.ctx.last_outgoing_request_time = Some(Instant::now());
        }

        Ok(())
    }

    async fn cancel_request(
        &mut self,
        pb: PieceBlock,
        sink: &mut SplitSink<Framed<TcpStream, MessageCodec>, Message>,
    ) -> Result<()> {
        info!(target: "peer_connection::cancel_request", block = ?pb, peer = ?self.peer.addr, "Cancelling block request");

        let mut picker = self.torrent.piece_picker.write().await;

        if !picker.is_requested(&pb) {
            return Ok(());
        }

        if let Some((i, b)) = self
            .download_queue
            .iter_mut()
            .enumerate()
            .find(|(_, p)| p.block == pb)
        {
            let block_offset = b.block.block_index as u32 * BLOCK_LEN;
            let block_size = picker
                .piece_size(b.block.piece_index)
                .saturating_sub(block_offset)
                .min(BLOCK_LEN);
            let block_request = BlockInfo {
                piece_index: b.block.piece_index,
                offset: block_offset,
                length: block_size,
            };

            picker.abort_download(&b.block, &self.peer.addr);

            let msg = Message::Cancel(block_request);
            sink.send(msg).await?;
        } else if let Some((i, b)) = self
            .request_queue
            .iter()
            .enumerate()
            .find(|(_, p)| p.block == pb)
        {
            picker.abort_download(&b.block, &self.peer.addr);
            self.request_queue.remove(i);
        }

        Ok(())
    }
}
