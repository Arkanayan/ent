use crate::download::BLOCK_LEN;

pub type PieceIndex = usize;

pub fn block_count(piece_len: u32) -> usize {
    ((piece_len as f64 / BLOCK_LEN as f64) as f64).ceil() as usize
}
