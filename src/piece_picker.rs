use std::{cmp::Ordering, collections::HashMap, default, hash::Hash, net::SocketAddr, num};

use bitvec::{macros::internal::funty::Fundamental, order, vec};
use sha1::digest::typenum::bit;
use tracing::{debug, field::debug, info, trace};

use crate::{download::BLOCK_LEN, messages::BitField, metainfo::Info, torrent, units::PieceIndex};

pub const MAX_BLOCK_LEN: u32 = 1 << 14;

#[derive(PartialEq, Clone, Copy, Debug)]
pub struct PieceBlock {
    pub piece_index: PieceIndex,
    pub block_index: usize,
}

impl From<(usize, usize)> for PieceBlock {
    fn from((piece, block): (usize, usize)) -> Self {
        Self {
            piece_index: piece,
            block_index: block,
        }
    }
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

#[derive(Default, PartialEq, Debug)]
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

    fn is_downloading(&self) -> bool {
        self.download_state != State::Open
    }
}

pub struct DownloadingPiece {
    /// Piece index
    pub index: PieceIndex,
    /// the number of blocks in finished state
    pub finished: usize,
    /// the number of blocks in requested state
    pub requested: usize,
    /// the number of blocks in writing state
    pub writing: usize,
    pub blocks: Vec<BlockInfo>,
}

impl DownloadingPiece {
    fn new(index: PieceIndex, num_blocks: usize) -> Self {
        Self {
            blocks: vec![Default::default(); num_blocks],
            finished: 0,
            requested: 0,
            writing: 0,
            index,
        }
    }
    fn get_piece_state(&self) -> State {
        let num_blocks_in_piece = self.blocks.len();

        if self.requested + self.writing + self.finished == 0 {
            State::Open
        } else if self.requested + self.writing + self.finished < num_blocks_in_piece {
            State::Downloading
        } else if self.requested > 0 {
            assert!(self.requested + self.writing + self.finished == num_blocks_in_piece);
            State::Full
        } else {
            assert!(self.finished == num_blocks_in_piece);
            State::Finished
        }
    }
}

#[derive(Default, Clone, Debug)]
pub struct BlockInfo {
    /// the peer this block was requested or downloaded from
    pub peer: Option<SocketAddr>,
    /// the number of peers that has this block in their download or request queues
    pub num_peers: usize,
    /// the state of this block
    pub state: BlockStatus,
}

#[derive(Debug, Default, PartialEq, Clone, Copy)]
pub enum BlockStatus {
    #[default]
    Free,
    Requested,
    Writing,
    Received,
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

        let mut own_pieces = BitField::with_capacity(num_pieces as usize);
        own_pieces.resize(num_pieces as usize, false);

        Self {
            own_pieces: own_pieces,
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

    /// increases the peer count for the given piece
    /// (is used when a BITFIELD message is received);
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

    /// increases the peer count for the given piece
    /// (is used when a HAVE message is received)
    pub fn register_peer_piece(&mut self, index: PieceIndex) -> bool {
        let mut interested = false;

        let peer_piece = &mut self.pieces[index];
        peer_piece.peer_count += 1;

        if !peer_piece.have() {
            interested = true;
        }
        interested
    }

    /// Registers that we have received the piece
    ///
    /// # Panics
    ///
    /// Panics if the piece already there
    pub fn received_piece(&mut self, index: PieceIndex) {
        tracing::trace!("Registering received piece: {}", index);

        assert!(index < self.pieces.len());

        let piece = &mut self.pieces[index];

        piece.set_have();

        self.own_pieces.set(index, true);
        self.piece_downloads.remove(&index);
    }

    pub fn register_failed_piece(&mut self, index: PieceIndex) {
        self.own_pieces.set(index, false);
        self.pieces[index].download_state = State::Open;
        self.num_have -= 1;
        self.missing_count += 1;
    }

    /// pieces describes which pieces the peer we're requesting from has.
    /// interesting_blocks is an out parameter, and will be filled with (up to)
    /// num_blocks of interesting blocks that the peer has.
    /// prefer_contiguous_blocks can be set if this peer should download whole
    /// pieces rather than trying to download blocks from the same piece as other
    /// peers. the peer argument is the torrent_peer of the peer we're
    /// picking pieces from. This is used when downloading whole pieces, to only
    /// pick from the same piece the same peer is downloading from.

    /// options are:
    /// * rarest_first
    ///     pick the rarest pieces first
    /// * reverse
    ///     reverse the piece picking. Pick the most common
    ///     pieces first or the last pieces (if picking sequential)
    /// * sequential
    ///     download pieces in-order
    /// * on_parole
    ///     the peer is on parole, only pick whole pieces which
    ///     has only been downloaded and requested from the same
    ///     peer
    /// * prioritize_partials
    ///     pick blocks from downloading pieces first

    // only one of rarest_first or sequential can be set

    // the return value is a combination of picker_flags_t,
    // indicating which path thought the picker we took to arrive at the
    // returned block picks.
    pub fn pick_pieces(
        &self,
        pieces: &BitField,
        interesting_blocks: &mut Vec<PieceBlock>,
        num_blocks: usize,
        peer: &SocketAddr,
        num_peers: usize,
    ) {
        let num_partials = self.piece_downloads.len();
        let mut num_blocks = num_blocks;

		// prevent the number of partial pieces to grow indefinitely
		// make this scale by the number of peers we have. For large
		// scale clients, we would have more peers, and allow a higher
		// threshold for the number of partials
		// the second condition is to make sure we cap the number of partial
		// _bytes_. The larger the pieces are, the fewer partial pieces we want.
		// 2048 corresponds to 32 MiB
		// TODO: 2 make the 2048 limit configurable
        let prioritize_partials =
            num_partials > num_peers * 3 / 2 || num_partials * self.blocks_per_piece() > 2048;

        // Using rarest first approach
        info!("prioritize_partials = {prioritize_partials}");
        if prioritize_partials {
            // Populates and filters acceptable pieces (i.e. remote peer has && we don't have)
            let mut ordered_partials: Vec<_> = self
                .piece_downloads
                .values()
                .filter(|pd| self.is_piece_free(pd.index, pieces))
                .collect();

            // Sort by rarest
            ordered_partials.sort_by(|a, b| self.compare_rarest_first(a, b));

            for partial_piece in ordered_partials {
                num_blocks = self.add_blocks_downloading(
                    partial_piece,
                    pieces,
                    num_blocks,
                    peer,
                    interesting_blocks,
                );

                if num_blocks == 0 {
                    return;
                }
            }
        } else {
            // Rarest first
            let mut ordered_pieces = self.pieces.iter().collect::<Vec<_>>();
            ordered_pieces.sort_by(|a, b| a.peer_count.cmp(&b.peer_count));

            for piece in ordered_pieces {
                if !self.is_piece_free(piece.index, pieces) {
                    continue;
                }
                if piece.download_state != State::Open && piece.download_state != State::Downloading {
                    continue;
                }
                if piece.download_state == State::Downloading {
                    let dp = self
                        .piece_downloads
                        .get(&piece.index)
                        .expect("Downloading piece should be present");
                    num_blocks = self.add_blocks_downloading(
                        dp,
                        pieces,
                        num_blocks,
                        peer,
                        interesting_blocks,
                    );
                } else {
                    let mut payload_blocks = self.blocks_in_piece(piece.index) as usize;

                    if payload_blocks > num_blocks {
                        payload_blocks = num_blocks;
                    }

                    for i in 0..payload_blocks {
                        interesting_blocks.push(PieceBlock {
                            piece_index: piece.index,
                            block_index: i,
                        });
                    }
                    num_blocks -= payload_blocks;
                }

                if num_blocks == 0 {
                    return;
                }
            }
        }
    }

    fn add_blocks_downloading(
        &self,
        dp: &DownloadingPiece,
        pieces: &BitField,
        num_blocks: usize,
        peer: &SocketAddr,
        interesting_blocks: &mut Vec<PieceBlock>,
    ) -> usize {
        let num_blocks_in_piece = self.blocks_in_piece(dp.index);
        let mut num_blocks = num_blocks;
        let (exclusive, exclusive_active, contiguous_blocks, first_block) =
            self.requested_from(dp, num_blocks_in_piece, peer);

        if let Some(dp) = self.piece_downloads.get(&dp.index) {
            let blocks = &dp.blocks;

            for (index, block) in blocks.iter().enumerate() {
                info!("P/b: {}/{}, status: {:?}", dp.index, index, block.state);
                if block.state != BlockStatus::Free {
                    continue;
                }

                interesting_blocks.push(PieceBlock {
                    piece_index: dp.index,
                    block_index: index,
                });
                num_blocks -= 1;

                if num_blocks == 0 {
                    return 0;
                }
            }
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
    fn is_piece_free(&self, index: PieceIndex, bitmask: &BitField) -> bool {
        !self.own_pieces[index] && bitmask[index]
    }

    /// lower availability comes first. This is a less than comparison, it returns true if lhs has lower availability than rhs
    fn compare_rarest_first(&self, lhs: &DownloadingPiece, rhs: &DownloadingPiece) -> Ordering {
        let lhs_availability = self.pieces[lhs.index].peer_count;
        let rhs_availability = self.pieces[rhs.index].peer_count;
        if lhs_availability != rhs_availability {
            return lhs_availability.cmp(&rhs_availability);
        }

        // if availability is the same, prefer the piece that's closest to being complete
        let lhs_blocks = lhs.finished + lhs.writing + lhs.requested;
        let rhs_blocks = rhs.finished + rhs.writing + rhs.requested;

        lhs_blocks.cmp(&rhs_blocks)
    }

    fn blocks_per_piece(&self) -> usize {
        ((self.piece_size + (BLOCK_LEN - 1)) / BLOCK_LEN) as usize
    }

    pub fn blocks_in_piece(&self, index: PieceIndex) -> u32 {
        if index == self.pieces.len() - 1 {
            self.blocks_in_last_piece as u32
        } else {
            self.blocks_per_piece() as u32
        }
    }

    /// the number of pieces we want and don't have
    pub fn num_left(&self) -> u32 {
        self.pieces.len() as u32 - self.num_have
    }

    /// the number of peers this block has been requested from
    pub fn num_peers(&self, block: &PieceBlock) -> usize {
        if !self.pieces[block.piece_index].is_downloading() {
            return 0;
        }

        if let Some(b) = self.piece_downloads.get(&block.piece_index) {
            b.blocks[block.block_index].num_peers
        } else {
            0
        }
    }

    /// returns false if the block could not be marked as downloading
    pub fn mark_as_downloading(&mut self, block: &PieceBlock, peer: Option<SocketAddr>) -> bool {
        let piece_index = block.piece_index;
        if self.pieces[block.piece_index].download_state == State::Open {
            self.pieces[block.piece_index].download_state = State::Downloading;

            let dp = DownloadingPiece::new(
                block.piece_index,
                self.blocks_in_piece(block.piece_index) as usize,
            );
            assert!(self.piece_downloads.contains_key(&block.piece_index) == false);

            let dp = self.piece_downloads.entry(block.piece_index).or_insert(dp);

            let block = dp
                .blocks
                .get_mut(block.block_index)
                .expect("Block should be present");

            block.state = BlockStatus::Requested;
            block.peer = peer;
            block.num_peers = 1;

            dp.requested += 1;

            self.pieces[piece_index].download_state = dbg!(dp.get_piece_state());
        } else {
            let dp = self
                .piece_downloads
                .get_mut(&block.piece_index)
                .expect("Downloading Piece should be present");

            let block = &mut dp.blocks[block.block_index];

            if block.state == BlockStatus::Received || block.state == BlockStatus::Writing {
                info!("Block: {:?} received or writing", block);
                return false;
            }

            assert!(
                block.state == BlockStatus::Free
                    || (block.state == BlockStatus::Requested && block.num_peers > 0)
            );

            block.peer = peer;
            block.num_peers += 1;

            if block.state != BlockStatus::Requested {
                block.state = BlockStatus::Requested;

                dp.requested += 1;

            }
            self.pieces[piece_index].download_state = dbg!(dp.get_piece_state());
        }

        true
    }

    pub fn is_downloaded(&self, block: &PieceBlock) -> bool {
        let piece = &self.pieces[block.piece_index];
        if piece.download_state == State::Finished {
            return true;
        }

        if let Some(dp) = self.piece_downloads.get(&block.piece_index) {
            let b = &dp.blocks[block.block_index];
            return b.state == BlockStatus::Writing || b.state == BlockStatus::Received;
        }

        return false;
    }

    /// this is called when a request is rejected or when
    /// a peer disconnects. The piece might be in any state
    pub fn abort_download(&mut self, block: &PieceBlock, peer: &SocketAddr) {
        let piece = &mut self.pieces[block.piece_index];
        if piece.download_state == State::Open {
            return;
        }

        let dp = self
            .piece_downloads
            .get_mut(&block.piece_index)
            .expect("Piece should be in downloads");

        let b = &mut dp.blocks[block.block_index];

        if b.state != BlockStatus::Requested {
            return;
        }

        if b.num_peers > 0 {
            b.num_peers -= 1;
        }

        if Some(peer) == b.peer.as_ref() {
            b.peer = None;
        }

        // if there are other peers, leave the block requested
        if b.num_peers > 0 {
            return;
        }

        b.peer = None;

        // clear this block as being downloaded
        b.state = BlockStatus::Free;
        dp.requested -= 1;

        piece.download_state = dp.get_piece_state();

        // if there are no other blocks in this piece
        // thats' being downloaded, remove it from the list
        if dp.requested + dp.finished + dp.writing == 0 {
            self.piece_downloads.remove(&block.piece_index);
            return;
        }
    }

    pub fn piece_size(&self, index: PieceIndex) -> u32 {
        self.pieces[index].size
    }

    pub fn mark_as_writing(&mut self, block: &PieceBlock, peer: SocketAddr) -> bool {
        let piece = &self.pieces[block.piece_index];

        if piece.download_state != State::Downloading {
            if piece.download_state == State::Finished {
                return false;
            }
        }

        let dp = self.piece_downloads.get_mut(&block.piece_index).unwrap();

        let b = &mut dp.blocks[block.block_index];

        b.peer = Some(peer);

        if b.state == BlockStatus::Requested {
            dp.requested -= 1;
        }

        if b.state == BlockStatus::Writing || b.state == BlockStatus::Received {
            return false;
        }

        dp.writing += 1;
        b.state = BlockStatus::Writing;

        // all other requests for this block should have been cancelled now
        b.num_peers = 0;

        self.pieces[block.piece_index].download_state = dp.get_piece_state();

        true
    }

    pub fn mark_as_finished(&mut self, block: &PieceBlock, peer: Option<SocketAddr>) {
        if self.pieces[block.piece_index].download_state == State::Finished {
            return;
        }

        //TODO: Handle the case where downloading piece is not found; that means we got the current
        // block from somewhere (maybe it was available locally), Create a downloading piece
        let dp = &mut self
            .piece_downloads
            .get_mut(&block.piece_index)
            .expect("Downloading Piece should be present");
        dp.finished += 1;
        dp.requested -= 1;

        let binfo = &mut dp.blocks[block.block_index];

        binfo.peer = peer;

        binfo.state = BlockStatus::Received;

        self.pieces[block.piece_index].download_state = dp.get_piece_state();
    }

    pub fn is_requested(&self, pb: &PieceBlock) -> bool {
        let state = &self.pieces[pb.piece_index].download_state;
        if *state == State::Open {
            return false;
        }

        if let Some(dp) = self.piece_downloads.get(&pb.piece_index) {
            return dp.blocks[pb.block_index].state == BlockStatus::Requested;
        }
        false
    }

    pub fn is_piece_finished(&self, piece: PieceIndex) -> bool {
        todo!()
    }
}

#[cfg(test)]
mod tests {
    use std::{
        collections::HashSet,
        net::{Ipv4Addr, SocketAddrV4},
    };

    use crate::peer;

    use super::*;

    const blocks_per_piece: u32 = 4;
    const default_piece_size: u32 = blocks_per_piece * MAX_BLOCK_LEN;
    const peer0: SocketAddr = SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::new(127, 0, 0, 1), 8080));

    #[test]
    fn should_pick_all_pieces_one_by_one() {
        let num_pieces = 10usize;

        let mut p = PiecePicker::new(
            num_pieces as u64 * default_piece_size as u64,
            default_piece_size,
        );

        let available_pieces = BitField::repeat(true, num_pieces as usize);

        p.register_peer_pieces(&available_pieces);

        let mut picked = HashSet::with_capacity(num_pieces as usize);

        loop {
            let mut picked_blocks = Vec::new();
            p.pick_pieces(
                &available_pieces,
                &mut picked_blocks,
                blocks_per_piece as usize,
                &peer0,
                1,
            );

            if picked_blocks.is_empty() {
                break;
            }

            for b in picked_blocks.drain(..) {
                p.mark_as_downloading(&b, None);
                p.mark_as_finished(&b, None);
                picked.insert(b.piece_index);
            }
        }
        assert_eq!(picked.len(), num_pieces);
    }

    #[test]
    fn should_mark_piece_as_received() {
        let num_pieces = 10usize;

        let mut p = PiecePicker::new(
            num_pieces as u64 * default_piece_size as u64,
            default_piece_size,
        );

        let available_pieces = BitField::repeat(true, num_pieces as usize);

        p.register_peer_pieces(&available_pieces);

        // mark  pieces as received
        let owned_pieces = [2, 5, 7];
        for index in owned_pieces.iter() {
            p.received_piece(*index);
            assert!(p.own_pieces[*index]);
        }
    }

    #[test]
    fn should_abort_download() {
        let num_pieces = 10usize;

        let mut p = PiecePicker::new(
            num_pieces as u64 * default_piece_size as u64,
            default_piece_size,
        );

        let mut available_pieces = BitField::repeat(false, num_pieces as usize);
        available_pieces.set(0, true);
        available_pieces.set(1, true);
        available_pieces.set(2, true);

        let mut picked_pieces = Vec::new();

        let pb0 = (0, 0).into();
        let pb1 = (0, 1).into();

        p.pick_pieces(
            &available_pieces,
            &mut picked_pieces,
            self::blocks_per_piece as usize,
            &peer0,
            1,
        );
        assert!(p.is_requested(&pb0) == false);
        assert!(picked_pieces.contains(&pb0));

        p.abort_download(&pb0, &peer0);

        picked_pieces.clear();

        p.pick_pieces(
            &available_pieces,
            &mut picked_pieces,
            self::blocks_per_piece as usize,
            &peer0,
            1,
        );
        assert!(p.is_requested(&pb0) == false);
        assert!(picked_pieces.contains(&pb0));

        picked_pieces.clear();

        p.mark_as_downloading(&pb0, Some(peer0.clone()));
        p.pick_pieces(
            &available_pieces,
            &mut picked_pieces,
            self::blocks_per_piece as usize,
            &peer0,
            1,
        );
        assert!(p.is_requested(&pb0) == true);
        assert!(!picked_pieces.contains(&pb0));

        picked_pieces.clear();

        p.abort_download(&pb0, &peer0);
        p.pick_pieces(
            &available_pieces,
            &mut picked_pieces,
            self::blocks_per_piece as usize,
            &peer0,
            1,
        );
        assert!(p.is_requested(&pb0) == false);
        assert!(picked_pieces.contains(&pb0));

        picked_pieces.clear();

        p.mark_as_downloading(&pb0, Some(peer0.clone()));
        p.mark_as_downloading(&pb1, Some(peer0.clone()));
        p.abort_download(&pb0, &peer0);
        p.pick_pieces(
            &available_pieces,
            &mut picked_pieces,
            self::blocks_per_piece as usize,
            &peer0,
            1,
        );
        assert!(p.is_requested(&pb0) == false);
        assert!(picked_pieces.contains(&pb0));

        picked_pieces.clear();

        p.mark_as_downloading(&pb0, Some(peer0.clone()));
        p.pick_pieces(
            &available_pieces,
            &mut picked_pieces,
            self::blocks_per_piece as usize,
            &peer0,
            1,
        );
        assert!(picked_pieces.contains(&(1, 0).into()) || picked_pieces.contains(&(2, 0).into()));

        picked_pieces.clear();

        p.abort_download(&pb0, &peer0);
        p.pick_pieces(
            &available_pieces,
            &mut picked_pieces,
            self::blocks_per_piece as usize,
            &peer0,
            1,
        );
        assert!(p.is_requested(&pb0) == false);
        assert!(picked_pieces.contains(&pb0));

        picked_pieces.clear();

        p.mark_as_downloading(&pb0, Some(peer0.clone()));
        p.mark_as_finished(&pb0, Some(peer0.clone()));
        p.abort_download(&pb0, &peer0);
        p.pick_pieces(
            &available_pieces,
            &mut picked_pieces,
            self::blocks_per_piece as usize,
            &peer0,
            1,
        );
        assert!(p.is_requested(&pb0) == false);
        assert!(!picked_pieces.contains(&pb0));
    }
}
