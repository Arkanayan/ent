use crate::{download::BLOCK_LEN, piece_picker::PieceBlock};


#[derive(Debug, Clone)]
pub struct StorageInfo {
	pub piece_count: usize,
	pub piece_len: u32,
	pub last_piece_len: u32,
	pub torrent_len: u64,
}

impl StorageInfo {

	pub fn get_offset_in_piece(&self, pb: &PieceBlock) -> u32 {
		let piece_len = self.piece_len(&pb);

		(piece_len / BLOCK_LEN) * pb.block_index as u32
	}

	pub fn piece_len(&self, pb: &PieceBlock)	-> u32 {

		assert!(pb.piece_index < self.piece_count);

		if pb.piece_index < self.piece_count-1 { 
			self.piece_len
		} else {
			self.last_piece_len
		}
	}
}
