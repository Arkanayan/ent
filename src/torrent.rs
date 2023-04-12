use std::{collections::{HashSet, HashMap}, io::Cursor, net::SocketAddr, sync::Arc, default};

use bytes::{Buf, Bytes};
use tokio::{sync::{RwLock, mpsc}, task::JoinHandle};
use anyhow::Result;
use tracing::info;

use crate::{
    metainfo::{self, MetaInfo, PeerID},
    tracker::TrackerData, peer::{self, PeerSession, MAX_BLOCK_SIZE}, piece_picker::{PiecePicker, Piece},
    storage::StorageInfo, units::PieceIndex
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
    pub torrent: Arc<TorrentInfo>
}

#[derive(Debug)]
pub struct PieceDownload {
    pub index: usize,
    pub len: u32,
    pub blocks: Vec<BlockStatus>
}

impl PieceDownload {
    pub fn new(piece_index: PieceIndex, len: u32) -> Self {
        let num_blocks = ((len as f64 / MAX_BLOCK_SIZE as f64) as f64).ceil() as usize;
        let mut blocks = Vec::with_capacity(num_blocks);
        blocks.fill_with(Default::default);
        Self {
            index: piece_index,
            len: len,
            blocks: blocks
        }
    }

    pub fn free_block(&mut self, block_info: &BlockInfo) {
        let block_index = block_in(self.len, block_info.start);

        self.blocks[block_index] = BlockStatus::Free;
    }

    pub fn get_next_block(&self, current_block: &BlockInfo) -> Option<(usize, BlockInfo)> {

        if current_block.piece_index != self.index {
            return None;
        }

        let block_index = block_in(self.len, current_block.start);
        if block_index > self.blocks.len() { return None };
        let block_start = block_index as u32 * MAX_BLOCK_SIZE;
        let length = (self.len - block_start).min(MAX_BLOCK_SIZE);

        return Some((block_index, BlockInfo {piece_index: self.index, start: block_start, length: length }))
    }

    pub fn pick_blocks(&mut self, count: usize) -> Vec<BlockInfo> {

        let mut picked = 0;
        let mut picked_blocks = Vec::with_capacity(count);

        for (i, block) in self.blocks.iter_mut().enumerate() {

            if picked == count {
                break;
            }
            
            if *block == BlockStatus::Free {
                let block_info = BlockInfo {
                        piece_index: self.index,
                        start: i as u32 * MAX_BLOCK_SIZE,
                        length: block_len(self.len, i)
                };

                *block = BlockStatus::Requested;

                picked_blocks.push(block_info);
                picked += 1;
                
            }
        }

        if picked > 0 {
            tracing::trace!("Picked {} blocks from piece {}", picked, self.index);
        } else {
            tracing::trace!("Cannot pick any blocks from piece {}", self.index);
        }

        picked_blocks
    }
}

pub fn block_in(piece_length: u32, block_offset: u32) -> usize {
    (((piece_length - block_offset) as f32) / peer::MAX_BLOCK_SIZE as f32).ceil() as usize
}

fn block_len(piece_len: u32, block_index: usize) -> u32 {
    let block_index = block_index as u32;
    let block_offset = block_index * MAX_BLOCK_SIZE;
    assert!(piece_len > block_offset);
    std::cmp::min(piece_len - block_offset, MAX_BLOCK_SIZE)
}


#[derive(Debug, Default, PartialEq)]
pub enum BlockStatus {
    #[default]
    Free,
    Requested,
    Received 
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
        // let mut blocks = Vec::new();

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

            // let num_blocks = ((p_len / block_size as i64) as f64).ceil() as usize;

            // let mut tmp_block_len = p_len;

            // for block_index in 0..num_blocks {
            //     tmp_block_len = tmp_block_len - block_size as i64;

            //     let current_block_size = if tmp_block_len < block_size as i64 {
            //         tmp_block_len
            //     } else {
            //         block_size as i64
            //     };

            //     let offset = block_size * block_index as u64;

            //     blocks.push(BlockInfo {
            //         piece_index: index,
            //         start: offset as u32,
            //         length: current_block_size as u32,
            //     });
            }
        // }
        Self {
            info_hash: meta_info.info_hash(),
            pieces: pieces,
            peers: tracker_data.peers.clone(),
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
    pub ctx: Arc<TorrentContext>,
    pub peers: Vec<SocketAddr>,
    pub join_handles: HashMap<SocketAddr, JoinHandle<Result<()>>>,
}

impl Torrent {
    pub fn new(torrent: TorrentInfo, client_id: PeerID) -> Self {
        let peers = torrent.peers.clone();

        let torrent_context = TorrentContext {
            client_id,
            piece_picker: RwLock::new(PiecePicker::new(torrent.pieces.len())),
            downloads: Default::default(),
            info_hash: torrent.info_hash,
            storage: StorageInfo::new(torrent.pieces.len()),
            torrent: Arc::new(torrent)
        };
        Self {
            client_id,
            ctx: Arc::new(torrent_context),
            peers: peers,
            join_handles: HashMap::new()
        }
    }

    pub async fn run(&mut self) -> Result<()> {

        // let mut pieces = Vec::with_capacity(self.torrent.pieces.len());
        // let num_pieces = pieces.len();
        // for p in &self.torrent.pieces {
        //     let piece_index = p.index;

        //     let num_blocks = ((p.length as i32 / MAX_BLOCK_SIZE as i32) as f32).ceil() as usize;

        //     let mut blocks = Vec::with_capacity(num_blocks);

        //     for i in 0..num_blocks {
        //         blocks.push(BlockDownload{ piece_index: piece_index, index: i, state: DownloadState::Free});
        //     }
        //     pieces.push(PieceDownload{index: piece_index, len: p.length, blocks: blocks});
        // }


        const TOTAL_PEERS: usize = 2;

        let peers_to_connect = self.peers.drain(0..self.peers.len().min(TOTAL_PEERS)).collect::<Vec<_>>();

        for peer in peers_to_connect {
            let mut peer_session = PeerSession::new(self.ctx.clone(), peer.clone());
            let handle = tokio::spawn(async move { peer_session.start_connection().await });
            self.join_handles.insert(peer.clone(), handle);
        }

        for (addr, handle) in self.join_handles.drain() {
            handle.await?;
        }

        Ok(())
    }
}
