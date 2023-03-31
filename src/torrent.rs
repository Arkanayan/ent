use std::net::SocketAddr;

use bytes::Bytes;



#[derive(Debug)]
pub struct TorrentInfo {
    pub info_hash: [u8; 20],
    pub pieces: Vec<PieceInfo>,
    pub peers: Vec<SocketAddr>
}

#[derive(Debug)]
pub struct PieceInfo {
    pub index: usize,
    pub sha1: [u8; 20],
    pub length: u32
}

#[derive(Debug)]
pub struct BlockInfo {
    pub piece_index: usize,
    pub start: u32,
    pub length: u32
}

#[derive(Debug)]
pub struct BlockData {
    pub block_index: usize,
    pub data: Bytes
}