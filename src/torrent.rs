use std::{
    collections::{HashMap, HashSet},
    default,
    io::Cursor,
    net::SocketAddr,
    sync::Arc,
};

use anyhow::Result;
use bytes::{Buf, Bytes};
use futures::channel::mpsc::unbounded;
use tokio::{
    sync::{
        mpsc::{self, unbounded_channel},
        RwLock,
    },
    task::JoinHandle,
};
use tracing::{info, trace};

use crate::{
    download::{PieceDownload, BLOCK_LEN},
    metainfo::{self, MetaInfo, PeerID},
    peer::{self, PeerSession},
    piece_picker::{Piece, PiecePicker},
    storage::StorageInfo,
    tracker::TrackerData,
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
    pub start: u32,
    pub length: u32,
}

impl BlockInfo {
    pub fn piece_index(&self) -> PieceIndex {
        self.piece_index
    }

    pub fn index_in_piece(&self) -> usize {
        debug_assert!(self.length <= BLOCK_LEN);
        debug_assert!(self.length > 0);
        (self.start / BLOCK_LEN) as usize
    }
}

#[derive(Debug)]
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

pub type Sender = mpsc::UnboundedSender<Command>;
pub type Receiver = mpsc::UnboundedReceiver<Command>;

pub enum Command {
    PeerDisconnected { addr: SocketAddr },
    PieceCompletion { piece_index: u32 },
}

pub struct PeerSessionEntry {
    pub handle: JoinHandle<Result<()>>,
    pub cmd_tx: Sender,
}

pub struct Torrent {
    pub client_id: PeerID,
    pub ctx: Arc<TorrentContext>,
    pub available_peers: Vec<SocketAddr>,
    pub peer_sessions: HashMap<SocketAddr, PeerSessionEntry>,
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

        let torrent_context = TorrentContext {
            client_id,
            piece_picker: RwLock::new(PiecePicker::new(torrent.pieces.len())),
            downloads: Default::default(),
            info_hash: torrent.info_hash,
            torrent: Arc::new(torrent),
            storage: storage_info,
        };

        Self {
            client_id,
            ctx: Arc::new(torrent_context),
            available_peers: peers,
            peer_sessions: HashMap::new(),
        }
    }

    const TOTAL_PEERS: usize = 2;

    pub async fn run(&mut self) -> Result<()> {
        todo!("Implement select! here")
    }

    pub async fn tick(&mut self) -> Result<()> {
        todo!()
    }

    /// Connect to peers, if any available
    pub async fn connect_to_peers(&mut self) -> Result<()> {
        if self.peer_sessions.len() >= Self::TOTAL_PEERS {
            return Ok(());
        }

        if self.available_peers.len() == 0 {
            return Ok(());
        }

        let can_connect_to = self.peer_sessions.len().saturating_sub(Self::TOTAL_PEERS);

        // Connect to peers
        for addr in self
            .available_peers
            .drain(..self.available_peers.len().min(Self::TOTAL_PEERS))
        {
            let (tx, rx) = unbounded_channel();
            let mut peer_session = PeerSession::new(self.ctx.clone(), addr.clone(), rx);
            let handle = tokio::spawn(async move { peer_session.start_connection().await });
            self.peer_sessions
                .insert(addr, PeerSessionEntry { handle, cmd_tx: tx });
        }

        Ok(())
    }
}
