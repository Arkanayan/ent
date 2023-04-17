
#[derive(Debug, Clone)]
pub struct StorageInfo {
	pub piece_count: usize,
	pub piece_len: u32,
	pub last_piece_len: u32,
	pub torrent_len: u64,
}
