use std::{collections::{HashSet, HashMap}, io::Cursor, net::SocketAddr, sync::Arc};

use bytes::{Buf, Bytes};
use tokio::{sync::{RwLock, mpsc}, task::JoinHandle};
use anyhow::Result;
use tracing::info;

use crate::{
    metainfo::{self, MetaInfo, PeerID},
    tracker::TrackerData, peer::{self, PeerSession},
};

#[derive(Debug)]
pub struct TorrentInfo {
    pub info_hash: [u8; 20],
    pub pieces: Vec<PieceInfo>,
    pub peers: Vec<SocketAddr>,
    pub blocks: Vec<BlockInfo>,
    pub metainfo: MetaInfo,
}

#[derive(Debug)]
pub struct TorrentContext {
    pub peer_id: PeerID,
    pub info_hash: [u8; 20],
    pub pieces: Vec<PieceDownload>,
    pub in_transit_blocks: RwLock<HashSet<BlockDownload>>,
    pub available_blocks: RwLock<HashSet<BlockDownload>>
}

#[derive(Debug)]
pub struct PieceDownload {
    pub index: usize,
    pub len: u32,
    pub blocks: Vec<BlockDownload>
}

#[derive(Debug)]
pub struct BlockDownload {
    pub piece_index: usize,
    pub index: usize,
    pub state: DownloadState
}

#[derive(Debug)]
pub enum DownloadState {
    Free,
    Requested,
    Downloaded 
}

#[derive(Debug, Clone)]
pub struct PieceInfo {
    pub index: usize,
    pub sha1: [u8; 20],
    pub length: u32,
}

#[derive(Debug, Clone)]
pub struct BlockInfo {
    pub piece_index: usize,
    pub start: u32,
    pub length: u32,
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
        // debug_assert_eq!((length / meta_info.info.piece_length)f32).ceil() as usize, num_pieces);

        let mut pieces = Vec::with_capacity(num_pieces);
        let mut blocks = Vec::new();

        let mut temp_sha1 = Cursor::new(&meta_info.info.pieces);

        let mut tmp_length = length;
        let block_size = 2u64.pow(14);

        for index in 0..num_pieces {
            let mut piece_sha1 = [0u8; 20];
            temp_sha1.copy_to_slice(&mut piece_sha1);

            tmp_length = tmp_length - piece_length;

            let p_len = if tmp_length < piece_length {
                tmp_length
            } else {
                piece_length
            };

            pieces.push(PieceInfo {
                index: index,
                sha1: piece_sha1,
                length: p_len as u32,
            });

            let num_blocks = ((p_len / block_size as i64) as f64).ceil() as usize;

            let mut tmp_block_len = p_len;

            for block_index in 0..num_blocks {
                tmp_block_len = tmp_block_len - block_size as i64;

                let current_block_size = if tmp_block_len < block_size as i64 {
                    tmp_block_len
                } else {
                    block_size as i64
                };

                let offset = block_size * block_index as u64;

                blocks.push(BlockInfo {
                    piece_index: index,
                    start: offset as u32,
                    length: current_block_size as u32,
                });
            }
        }
        Self {
            info_hash: meta_info.info_hash(),
            pieces: pieces,
            peers: tracker_data.peers.clone(),
            blocks: blocks,
            metainfo: meta_info.clone(),
        }
    }
}

type Sender = mpsc::UnboundedSender<Command>;
type Receiver = mpsc::UnboundedSender<Command>;

pub enum Command {
    PeerDisconnected { addr: SocketAddr}
}

pub struct Torrent {
    pub client_id: PeerID,
    pub torrent: TorrentInfo,
    pub join_handles: HashMap<SocketAddr, JoinHandle<Result<()>>>,
}

impl Torrent {
    pub fn new(torrent: TorrentInfo, client_id: PeerID) -> Self {
        Self {
            client_id,
            torrent: torrent,
            join_handles: HashMap::new()
        }
    }

    pub async fn run(&mut self) -> Result<()> {
        const TOTAL_PEERS: usize = 30;
        const MAX_BLOCK_SIZE: u32 = 1 << 14;

        let mut pieces = Vec::with_capacity(self.torrent.pieces.len());

        for p in &self.torrent.pieces {
            let piece_index = p.index;

            let num_blocks = ((p.length as i32 / MAX_BLOCK_SIZE as i32) as f32).ceil() as usize;

            let mut blocks = Vec::with_capacity(num_blocks);

            for i in 0..num_blocks {
                blocks.push(BlockDownload{ piece_index: piece_index, index: i, state: DownloadState::Free});
            }
            pieces.push(PieceDownload{index: piece_index, len: p.length, blocks: blocks});
        }

        let torrent_context = TorrentContext {
            peer_id: self.client_id,
            pieces: pieces,
            available_blocks: RwLock::new(HashSet::new()),
            in_transit_blocks: RwLock::new(HashSet::new()),
            info_hash: self.torrent.info_hash.clone(),
        };

        let ctx = Arc::new(torrent_context);
        let peers_to_connect = self.torrent.peers.drain(0..self.torrent.peers.len().min(TOTAL_PEERS)).collect::<Vec<_>>();

        for peer in peers_to_connect {
            let mut peer_session = PeerSession::new(ctx.clone(), peer.clone());
            let handle = tokio::spawn(async move { peer_session.run().await });
            self.join_handles.insert(peer.clone(), handle);
        }

        for (addr, handle) in self.join_handles.drain() {
            handle.await?;
        }

        Ok(())
    }
}
