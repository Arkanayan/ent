use std::{collections::HashMap, sync::Weak};

use crate::{messages::BitField, units::PieceIndex, torrent::PieceInfo};

#[derive(Debug)]
pub struct PiecePicker {
    pub own_pieces: BitField,

    pub pieces: Vec<Piece>,

    // Number of pieces haven't received yet (maybe picked)
    pub missing_count: usize,
    // Number of pieces that can be picked
    pub free_count: usize,
}

#[derive(Debug, Default)]
pub struct Piece {
    /// How many peers have this piece?
    pub frequency: usize,
    /// Whether we have already picked this piece and is downloading.
    /// We set this when this piece is picked
    pub is_pending: bool,
}

impl PiecePicker {
    pub fn new(num_pieces: usize) -> Self {
        Self {
            own_pieces: BitField::repeat(false, num_pieces),
            pieces: (0..num_pieces).map(|_| Piece::default()).collect(),
            missing_count: num_pieces,
            free_count: 0,
        }
    }

    pub fn own_pieces(&self) -> &BitField {
        &self.own_pieces
    }

    /// Returns interested based on the whether we have the piece or not
    pub fn register_peer_pieces(&mut self, bitfield: &BitField) -> bool {
        let mut interested = false;

        if !bitfield.any() {
            return false;
        }

        for index in bitfield.iter_ones() {
            let have_piece = self.own_pieces[index];

            if !have_piece {
                interested = true;
            }
            self.pieces[index].frequency += 1;
        }

        return interested;
    }

	pub fn register_peer_piece(&mut self, piece: &PieceIndex) -> bool {
		let mut interested = false;

		self.
	}

    pub fn pick_piece(&mut self, peer_pieces: &BitField) -> Option<PieceIndex> {
        for next_piece_index in (peer_pieces.clone() & !self.own_pieces.clone()).iter_ones() {
			let piece = &mut self.pieces[next_piece_index];
			if piece.is_pending {
				continue;
			} else {
				piece.is_pending = true;
				self.free_count 
				return Some(next_piece_index);
			}
		}

		return None;
    }
}
