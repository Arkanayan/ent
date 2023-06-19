use tracing::trace;

use crate::{torrent::BlockInfo, units::PieceIndex};


pub const BLOCK_LEN: u32 = 1 << 14;

#[derive(Debug)]
pub struct PieceDownload {
    pub index: usize,
    pub len: u32,
    pub blocks: Vec<BlockStatus>
}

impl PieceDownload {
    pub fn new(piece_index: PieceIndex, len: u32) -> Self {
        let num_blocks = ((len as f64 / BLOCK_LEN as f64) as f64).ceil() as usize;
        let blocks = vec![BlockStatus::Free; num_blocks];
        Self {
            index: piece_index,
            len: len,
            blocks: blocks
        }
    }

    pub fn free_block(&mut self, block_info: &BlockInfo) {
        let block_index = block_info.index_in_piece();

        self.blocks[block_index] = BlockStatus::Free;
    }

    pub fn get_next_block(&self, current_block: &BlockInfo) -> Option<(usize, BlockInfo)> {

        if current_block.piece_index != self.index {
            return None;
        }

        let block_index = current_block.index_in_piece();
        if block_index > self.blocks.len() { return None };
        let block_start = block_index as u32 * BLOCK_LEN;
        let length = (self.len - block_start).min(BLOCK_LEN);

        return Some((block_index, BlockInfo {piece_index: self.index, offset: block_start, length: length }))
    }

    pub fn received_block(&mut self, block_info: &BlockInfo) -> BlockStatus {
        let index = block_info.index_in_piece();

        let block = &mut self.blocks[index];
        let prev_status = *block;
        *block = BlockStatus::Received;

        return prev_status;
    }

    pub fn pick_blocks(&mut self, count: usize) -> Vec<BlockInfo> {
        trace!("Trying to pick {} blocks of total {}", count, self.blocks.len());
        let mut picked = 0;
        let mut picked_blocks = Vec::with_capacity(count);

        for (i, block) in self.blocks.iter_mut().enumerate() {

            if picked == count {
                break;
            }
            
            if *block == BlockStatus::Free {
                trace!("Found free block {}", i);
                let block_info = BlockInfo {
                        piece_index: self.index,
                        offset: i as u32 * BLOCK_LEN,
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

    pub fn is_downloaded(&self, block: &BlockInfo) -> bool {
        matches!(self.blocks[block.index_in_piece()], BlockStatus::Received)

    }
}


fn block_len(piece_len: u32, block_index: usize) -> u32 {
    let block_index = block_index as u32;
    let block_offset = block_index * BLOCK_LEN;
    assert!(piece_len > block_offset);
    std::cmp::min(piece_len - block_offset, BLOCK_LEN)
}


#[derive(Debug, Default, PartialEq, Clone, Copy)]
pub enum BlockStatus {
    #[default]
    Free,
    Requested,
    Received 
}