use std::{
    collections::HashMap,
    io::Cursor,
    net::SocketAddr,
    sync::{atomic::AtomicUsize, Arc},
    time::{Duration, Instant},
};

use anyhow::Result;
use bytes::{Buf, Bytes};
use tokio::{
    select,
    sync::{
        mpsc::{self, unbounded_channel},
        RwLock,
    },
    task::JoinHandle,
    time::interval,
};
use tracing::{debug, info, trace};

use crate::{
    disk::{self, DiskEntry, DiskStorage},
    download::{PieceDownload, BLOCK_LEN},
    metainfo::{MetaInfo, PeerID},
    peer::{self, PeerSession},
    piece_picker::PiecePicker,
    storage::StorageInfo,
    tracker::{Peer, TrackerData},
    units::PieceIndex,
};

#[derive(Debug)]
pub struct TorrentInfo {
    pub info_hash: [u8; 20],
    pub pieces: Vec<PieceInfo>,
    pub peers: Vec<SocketAddr>,
    pub metainfo: MetaInfo,
}

#[derive(Debug)]
pub struct TorrentContext {
    pub storage: StorageInfo,
    pub info_hash: [u8; 20],
    pub client_id: PeerID,
    pub piece_picker: RwLock<PiecePicker>,
    pub downloads: RwLock<HashMap<PieceIndex, RwLock<PieceDownload>>>,
    pub torrent: Arc<TorrentInfo>,
    /// Number of peers connected at the moment
    pub num_connected_peers: AtomicUsize,
    /// Used for sending messages to the torrent by the peers
    pub cmd_tx: CmdSender,
}

#[derive(Debug, Clone)]
pub struct PieceInfo {
    pub index: usize,
    pub sha1: [u8; 20],
    pub length: u32,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct BlockInfo {
    pub piece_index: usize,
    pub offset: u32,
    pub length: u32,
}

impl BlockInfo {
    pub fn piece_index(&self) -> PieceIndex {
        self.piece_index
    }

    pub fn index_in_piece(&self) -> usize {
        debug_assert!(self.length <= BLOCK_LEN);
        debug_assert!(self.length > 0);
        (self.offset / BLOCK_LEN) as usize
    }
}

#[derive(Debug, PartialEq)]
pub struct BlockData {
    pub piece_index: usize,
    pub offset: u32,
    pub data: Bytes,
}

impl TorrentInfo {
    pub fn new(meta_info: &MetaInfo, tracker_data: &TrackerData) -> Self {
        let num_pieces = (meta_info.info.pieces.len() / 20) as usize;
        let piece_length = meta_info.info.piece_length;
        // Considering single file mode
        let length = meta_info.info.length.unwrap();
        debug_assert_eq!(
            (length as f32 / meta_info.info.piece_length as f32).ceil() as usize,
            num_pieces
        );

        let mut pieces = Vec::with_capacity(num_pieces);

        let mut temp_sha1 = Cursor::new(&meta_info.info.pieces);

        for index in 0..num_pieces {
            let mut piece_sha1 = [0u8; 20];
            temp_sha1.copy_to_slice(&mut piece_sha1);

            let p_len = if index == (num_pieces - 1) {
                (length - ((num_pieces - 1) as u32 * piece_length) as i64) as u32
            } else {
                piece_length
            };

            pieces.push(PieceInfo {
                index: index,
                sha1: piece_sha1,
                length: p_len,
            });
        }

        Self {
            info_hash: meta_info.info_hash(),
            pieces: pieces,
            peers: tracker_data.peers.clone(),
            metainfo: meta_info.clone(),
        }
    }
}

pub type PeerNotificationSender = mpsc::UnboundedSender<PeerNotification>;
pub type PeerNotificationReceiver = mpsc::UnboundedReceiver<PeerNotification>;

pub enum PeerNotification {
    Connected { addr: SocketAddr },
    Disconnected { addr: SocketAddr },
}

pub type CmdSender = mpsc::UnboundedSender<Command>;
pub type CmdReceiver = mpsc::UnboundedReceiver<Command>;

#[derive(Debug)]
pub enum Command {
    PeerConnected { addr: SocketAddr, id: PeerID },
    PeerDisconnected { addr: SocketAddr },
}

pub struct PeerSessionEntry {
    pub handle: JoinHandle<Result<()>>,
    pub cmd_tx: peer::Sender,
}

pub struct Torrent {
    pub client_id: PeerID,
    pub torrent: Arc<TorrentContext>,
    pub available_peers: Vec<SocketAddr>,
    pub peer_sessions: HashMap<SocketAddr, PeerSessionEntry>,
    pub start_time: Option<Instant>,
    pub run_duration: Duration,
    pub disk: Option<DiskEntry>,
    /// Num Pieces we have
    pieces_count: usize,
    cmd_rx: CmdReceiver,
}

impl Torrent {
    pub fn new(torrent: TorrentInfo, client_id: PeerID) -> Self {
        let peers = torrent.peers.clone();
        let num_pieces = torrent.pieces.len();
        let piece_len = torrent.metainfo.info.piece_length;
        let torrent_len = torrent.metainfo.torrent_len();
        let last_piece_len = torrent_len - piece_len as u64 * (num_pieces - 1) as u64;
        let last_piece_len = last_piece_len as u32;

        let storage_info = StorageInfo {
            piece_count: num_pieces,
            piece_len: torrent.metainfo.info.piece_length,
            last_piece_len: last_piece_len,
            torrent_len: torrent_len,
        };

        let (tx, rx) = unbounded_channel();
        let torrent_context = TorrentContext {
            client_id,
            piece_picker: RwLock::new(PiecePicker::new(torrent.pieces.len())),
            downloads: Default::default(),
            info_hash: torrent.info_hash,
            torrent: Arc::new(torrent),
            storage: storage_info,
            num_connected_peers: AtomicUsize::new(0),
            cmd_tx: tx,
        };

        Self {
            client_id,
            torrent: Arc::new(torrent_context),
            available_peers: peers,
            peer_sessions: HashMap::new(),
            start_time: None,
            run_duration: Default::default(),
            disk: None,
            pieces_count: 0,
            cmd_rx: rx,
        }
    }

    const MAX_PEERS_COUNT: usize = 8;

    pub async fn start(&mut self) -> Result<()> {
        info!("Starting torrent");

        self.start_time = Some(Instant::now());

        let (storage, cmd_tx, alert_rx) = DiskStorage::new(
            self.torrent.torrent.metainfo.info.name.to_owned(),
            self.torrent.torrent.metainfo.info.pieces.clone().into_vec(),
            self.torrent.storage.clone(),
        );

        let handle = tokio::spawn(async move { storage.start().await });

        let disk_entry = DiskEntry {
            alert_rx: alert_rx,
            cmd_tx: cmd_tx,
            handle: handle,
        };

        self.disk = Some(disk_entry);

        if let Err(e) = self.run().await {
            return Err(e.into());
        }

        Ok(())
    }

    async fn run(&mut self) -> Result<()> {
        let mut ticker = interval(Duration::from_secs(1));

        let mut last_tick_time = None;

        debug_assert!(
            self.disk.is_some(),
            "Disk thread should be started before calling run"
        );

        use disk::Alert::*;
        loop {
            select! {
                tick_time = ticker.tick() => {
                    self.tick(&mut last_tick_time, tick_time.into_std()).await?;
                }
                Some(cmd) = self.disk.as_mut().unwrap().alert_rx.recv() => {
                    match cmd {
                        BlockWriteError(_, _) => todo!(),
                        PieceCompletion(index) => {
                            self.handle_piece_completion(index).await?;
                        },
                        PieceHashMismatch(index) => {
                            self.handle_piece_hash_mismatch(index).await;
                        },
                        TorrentCompletion => {
                            // Shutdown all operations
                            self.handle_torrent_completion().await?;
                            info!("Torrent Completed");
                            return Ok(());
                        }
                    }
                }
                Some(cmd) = self.cmd_rx.recv() => {
                    match cmd {
                        Command::PeerConnected { addr, id } => {
                            info!(target: "peer_events", addr = %addr, "Peer Connected");
                            self.torrent.num_connected_peers.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                        }
                        Command::PeerDisconnected { addr } => {
                            info!(target: "peer_events", addr = %addr, "Peer Disconnected");
                            self.torrent.num_connected_peers.fetch_sub(1, std::sync::atomic::Ordering::Relaxed);
                        }
                    }
                }
            }
        }
    }

    async fn tick(&mut self, last_tick_time: &mut Option<Instant>, now: Instant) -> Result<()> {
        let tick_interval = last_tick_time
            .or(self.start_time)
            .map(|t| now.saturating_duration_since(t))
            .unwrap_or_default();
        self.run_duration += tick_interval;
        *last_tick_time = Some(now);

        self.connect_to_peers().await;

        Ok(())
    }

    async fn handle_torrent_completion(&mut self) -> Result<()> {
        for sess in self.peer_sessions.values() {
            sess.cmd_tx.send(peer::Command::Shutdown).ok();
        }

        for (addr, sess) in self.peer_sessions.drain() {
            trace!("Peer {} shutdown", addr);
            sess.handle.await.ok();
        }

        if let Some(disk) = self.disk.take() {
            disk.cmd_tx.send(disk::Command::Shutdown).ok();
            disk.handle.await.ok();
        }

        Ok(())
    }

    async fn handle_piece_hash_mismatch(&mut self, index: PieceIndex) {
        debug!("Piece Hash Mismatch, Index: {}", index);
        //TODO: Put the participating peers in parole

        self.torrent.downloads.write().await.remove(&index);

        self.torrent
            .piece_picker
            .write()
            .await
            .register_failed_piece(index);
    }

    async fn handle_piece_completion(&mut self, index: PieceIndex) -> Result<()> {
        info!("Completed piece: {}", index);

        self.pieces_count += 1;

        let percent_complete =
            (self.pieces_count as f32 / self.torrent.storage.piece_count as f32) as f32 * 100f32;
        info!(
            "We have {}/{} {:.2}%",
            self.pieces_count, self.torrent.storage.piece_count, percent_complete
        );

        self.torrent.downloads.write().await.remove(&index);

        self.torrent
            .piece_picker
            .write()
            .await
            .received_piece(index);

        // Inform all peers
        for p in self.peer_sessions.values_mut() {
            p.cmd_tx.send(peer::Command::PieceCompletion(index)).ok();
        }
        Ok(())
    }

    /// Connect to peers, if any available
    async fn connect_to_peers(&mut self) {
        let can_connect_to = Self::MAX_PEERS_COUNT
            .saturating_sub(self.peer_sessions.len())
            .min(self.available_peers.len());

        // Connect to peers
        info!("Found peers: {}", self.available_peers.len());
        for addr in self.available_peers.drain(..can_connect_to) {
            let (tx, rx) = unbounded_channel();
            let disk_tx = self.disk.as_ref().map(|d| d.cmd_tx.clone()).unwrap();
            let mut peer_session =
                PeerSession::new(Arc::clone(&self.torrent), addr.clone(), rx, disk_tx);
            let handle = tokio::spawn(async move { peer_session.start_connection().await });
            self.torrent
                .num_connected_peers
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            self.peer_sessions
                .insert(addr, PeerSessionEntry { handle, cmd_tx: tx });
        }
    }
}
