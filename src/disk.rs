use std::collections::{HashMap, BTreeMap};

use crate::{storage::StorageInfo, units::PieceIndex};


pub struct DiskStorage {
    pub num_pieces: usize,
    pub filename: String,
    pub info: StorageInfo,
    pub pieces: HashMap<PieceIndex, Piece>

}

pub struct Piece {
    pub hash: [u8; 20],
    pub blocks: BTreeMap<u32, Bytes>
}