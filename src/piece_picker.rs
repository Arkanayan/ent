use std::{cmp::Ordering, collections::HashMap, default, hash::Hash, net::SocketAddr, num};

use bitvec::{macros::internal::funty::Fundamental, order};
use sha1::digest::typenum::bit;
use tracing::info;

use crate::{download::BLOCK_LEN, messages::BitField, metainfo::Info, torrent, units::PieceIndex};

pub const MAX_BLOCK_LEN: u32 = 1 << 14;

pub struct PieceBlock {
    pub piece_index: PieceIndex,
    pub block_index: usize
}

pub struct PiecePicker {
    pub own_pieces: BitField,
    pub piece_size: u32,
    pub total_size: u64,
    pub pieces: Vec<Piece>,

    /// the number of pieces we have
    pub num_have: u32,
    /// Number of pieces haven't received yet (maybe picked)
    pub missing_count: u32,

    pub piece_downloads: HashMap<PieceIndex, DownloadingPiece>,
    pub blocks_in_last_piece: u16,
}

#[derive(Default)]
pub enum State {
    /// the piece is open to be picked
    #[default]
    Open,
    /// the piece is partially downloaded or requested
    Downloading,
    /// partial pieces where all the blocks in the piece have been requested
    Full,
    /// all the blocks of the piece have been received
    Finished,
}

pub struct Piece {
    pub index: PieceIndex,
    pub size: u32,
    /// How many peers have this piece?
    pub peer_count: usize,
    pub download_state: State,
}

impl Piece {
    fn have(&self) -> bool {
        matches!(self.download_state, State::Finished)
    }

    fn set_have(&mut self) {
        self.download_state = State::Finished;
    }
}

pub struct DownloadingPiece {
    pub index: PieceIndex,
    /// the number of blocks in finished state
    pub finished: usize,
    /// the number of blocks in requested state
    pub requested: usize,
    pub blocks: Vec<BlockInfo>,
}

#[derive(Default)]
pub struct BlockInfo {
    /// the peer this block was requested or downloaded from
    pub peer: Option<SocketAddr>,
    /// the number of peers that has this block in their download queues
    pub num_peers: usize,
    /// the state of this block
    pub state: BlockStatus,
}

#[derive(Debug, Default, PartialEq, Clone, Copy)]
pub enum BlockStatus {
    #[default]
    Free,
    Requested,
    Received, //TODO Add writing, finished variants instead of Received
}

impl PiecePicker {
    pub fn new(total_size: u64, piece_size: u32) -> Self {
        let num_pieces = (total_size as f64 / piece_size as f64).ceil() as u32;
        let mut pieces = Vec::with_capacity(num_pieces as usize);
        let last_piece_size = match total_size % piece_size as u64 {
            0 => piece_size,
            s @ _ => s as u32,
        };

        for i in 0..num_pieces {
            let mut piece = Piece {
                index: i as usize,
                download_state: State::Open,
                peer_count: 0,
                size: if i == num_pieces - 1 {
                    piece_size
                } else {
                    last_piece_size
                },
            };
            pieces.push(piece);
        }

        Self {
            own_pieces: BitField::with_capacity(num_pieces as usize),
            num_have: 0,
            missing_count: num_pieces,
            piece_size: piece_size,
            total_size: total_size,
            pieces: pieces,
            piece_downloads: HashMap::new(),
            blocks_in_last_piece: ((last_piece_size / MAX_BLOCK_LEN) as f32).ceil() as u16,
        }
    }

    pub fn own_pieces(&self) -> &BitField {
        &self.own_pieces
    }

    /// Returns interested based on the whether we have the piece or not
    pub fn register_peer_pieces(&mut self, bitfield: &BitField) -> bool {
        assert!(self.pieces.len() <= bitfield.len());

        let mut interested = false;

        if !bitfield.any() {
            return false;
        }

        for index in bitfield.iter_ones() {
            let have_piece = self.pieces[index].have();

            if !have_piece {
                interested = true;
            }
            self.pieces[index].peer_count += 1;
        }

        return interested;
    }

    pub fn register_peer_piece(&mut self, index: PieceIndex) -> bool {
        let mut interested = false;

        let peer_piece = &mut self.pieces[index];
        peer_piece.peer_count += 1;

        if !peer_piece.have() {
            interested = true;
        }
        interested
    }

    pub fn pick_piece(&mut self, peer_pieces: &BitField) -> Option<PieceIndex> {
        // for piece_index in  self.own_pieces.iter_zeros() {
        //     if let Some(h ) = peer_pieces.get(piece_index) {
        //         if !*h {
        //             continue;
        //         }
        //     }
        // 	let piece = &mut self.pieces[piece_index];
        // 	if piece.is_pending {
        // 		continue;
        // 	} else {
        // 		piece.is_pending = true;
        // 		self.free_count -= 1;
        // 		return Some(piece_index);
        // 	}
        // }
        return None;
    }

    /// Registers that we have received the piece
    ///
    /// # Panics
    ///
    /// Panics if the piece already there
    pub fn received_piece(&mut self, index: PieceIndex) {
        tracing::trace!("Registering received piece: {}", index);

        assert!(self.pieces.len() <= index);

        let _ = self
            .piece_downloads
            .remove(&index)
            .expect("Piece was not being downloaded");

        let mut have_piece = &self.pieces[index];

        let piece = &mut self.pieces[index];

        piece.set_have();
    }

    pub fn register_failed_piece(&mut self, index: PieceIndex) {
        // self.own_pieces.set(index, false);
        // self.pieces[index].is_pending = false;
        // self.free_count -= 1;
    }

    pub fn pick_pieces(
        &self,
        pieces: &BitField,
        interesting_blocks: &mut Vec<PieceBlock>,
        num_blocks: usize,
        peer: SocketAddr,
        num_peers: usize,
    ) {
        let num_partials = self.piece_downloads.len();

        let prioritize_partials =
            num_partials > num_peers * 3 / 2 || num_partials * self.blocks_per_piece() > 2048;

        // Using rarest first approach

        if prioritize_partials {
            // Populates and filters acceptable pieces (i.e. remote peer has && we don't have)
            let mut ordered_partials: Vec<_> = self
                .piece_downloads
                .values()
                .filter(|pd| self.is_piece_free(pd, pieces))
                .collect();

            // Sort by rarest
            ordered_partials.sort_by(|a, b| self.compare_rarest_first(a, b));

            let mut num_blocks = num_blocks;
            for partial_piece in ordered_partials {

                num_blocks = self.pick_blocks(partial_piece, pieces, num_blocks, &peer, interesting_blocks);

                if num_blocks <= 0 {
                    return;
                }

            }
        }
    }

    fn pick_blocks(
        &self,
        dp: &DownloadingPiece,
        pieces: &BitField,
        num_blocks: usize,
        peer: &SocketAddr,
        interesting_blocks: &mut Vec<PieceBlock>
    ) -> usize {
        let num_blocks_in_piece = self.blocks_in_piece(dp.index);
        let mut num_blocks = num_blocks;

        let (exclusive, exclusive_active, contiguous_blocks, first_block) =
            self.requested_from(dp, num_blocks_in_piece, peer);


        let blocks = &dp.blocks;

        for (index, block) in blocks.iter().enumerate() {
            if block.state != BlockStatus::Free {
                continue;
            }

            interesting_blocks.push(PieceBlock { piece_index: dp.index, block_index: index });
            num_blocks -= 1;

            if num_blocks <= 0 {
                return 0;
            }
        }

        if num_blocks <= 0 {
            return 0;
        }

        return num_blocks;
    }

    /// the first bool is true if this is the only peer that has requested and downloaded blocks from this piece
    /// the second bool is true if this is the only active peer that is requesting and downloading blocks from this piece. Active means having a connection.
    fn requested_from(
        &self,
        dp: &DownloadingPiece,
        num_blocks_in_piece: u32,
        peer: &SocketAddr,
    ) -> (bool, bool, u32, usize) {
        let mut exclusive = true;
        let mut exclusive_active = true;
        let mut contiguous_blocks = 0;
        let mut max_contiguous = 0;
        let mut first_block = 0;

        for (index, block) in dp.blocks.iter().enumerate() {
            if block.state == BlockStatus::Free {
                contiguous_blocks += 1;
                continue;
            }

            if contiguous_blocks > max_contiguous {
                max_contiguous = contiguous_blocks;
                first_block = index - contiguous_blocks;
            }

            contiguous_blocks = 0;

            if Some(peer) != block.peer.as_ref() {
                exclusive = false;

                if block.state == BlockStatus::Requested && block.peer.is_some() {
                    exclusive_active = false;
                }
            }

            if contiguous_blocks > max_contiguous {
                max_contiguous = contiguous_blocks;
                first_block = num_blocks_in_piece as usize - contiguous_blocks;
            }
        }

        (
            exclusive,
            exclusive_active,
            max_contiguous as u32,
            first_block,
        )
    }

    /// Whether the piece is free to be picked and the remote peer has it
    fn is_piece_free(&self, piece: &DownloadingPiece, bitmask: &BitField) -> bool {
        !self.own_pieces[piece.index] && bitmask[piece.index]
    }

    /// lower availability comes first. This is a less than comparison, it returns true if lhs has lower availability than rhs
    fn compare_rarest_first(&self, lhs: &DownloadingPiece, rhs: &DownloadingPiece) -> Ordering {
        let lhs_availability = self.pieces[lhs.index].peer_count;
        let rhs_availability = self.pieces[rhs.index].peer_count;
        if lhs_availability != rhs_availability {
            return lhs_availability.cmp(&rhs_availability);
        }

        // if availability is the same, prefer the piece that's closest to being complete
        let lhs_blocks = lhs.finished + lhs.requested; // TODO: + lhs.writing
        let rhs_blocks = rhs.finished + rhs.requested; // TODO: + rhs.writing

        lhs_blocks.cmp(&rhs_blocks)
    }

    fn blocks_per_piece(&self) -> usize {
        (self.piece_size + (BLOCK_LEN - 1) / BLOCK_LEN) as usize
    }

    fn blocks_in_piece(&self, index: PieceIndex) -> u32 {
        if index == self.pieces.len() - 1 {
            self.blocks_in_last_piece as u32
        } else {
            self.blocks_per_piece() as u32
        }
    }
}
