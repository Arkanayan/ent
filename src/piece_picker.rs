use tracing::info;

use crate::{messages::BitField, units::PieceIndex, metainfo::Info};

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
                self.free_count += 1;
            }
            self.pieces[index].frequency += 1;
        }

        return interested;
    }

	pub fn register_peer_piece(&mut self, index: PieceIndex) -> bool {
		let mut interested = false;

		let peer_piece = &mut self.pieces[index];
        peer_piece.frequency += 1;

        if !self.own_pieces[index] {
            self.free_count += 1;
            interested = true;
        }
       interested 
	}

    pub fn pick_piece(&mut self, peer_pieces: &BitField) -> Option<PieceIndex> {
        for piece_index in  self.own_pieces.iter_zeros() {
            if let Some(h ) = peer_pieces.get(piece_index) {
                if !*h {
                    continue;
                }
            }
			let piece = &mut self.pieces[piece_index];
			if piece.is_pending {
				continue;
			} else {
				piece.is_pending = true;
				self.free_count -= 1;
				return Some(piece_index);
			}
		}
		return None;
    }

    /// Registers that we have received the piece
    /// 
    /// # Panics
    /// 
    /// Panics if the piece already there
    pub fn received_piece(&mut self, index: PieceIndex) {
        tracing::trace!("Registering received piece: {}", index);

        let mut have_piece = self.own_pieces.get_mut(index).expect("Invalid piece index");
        assert!(!*have_piece);

        *have_piece = true;
        self.missing_count -= 1;

        let piece = &mut self.pieces[index];

        if !piece.is_pending {
            self.free_count -= 1;
            piece.is_pending = false;
        }
    }

    pub fn register_failed_piece(&mut self, index: PieceIndex) {
        self.own_pieces.set(index, false);
        self.pieces[index].is_pending = false;
        self.free_count -= 1;
    }
}
