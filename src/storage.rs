
#[derive(Debug)]
pub struct StorageInfo {
	pub piece_count: usize
}

impl StorageInfo {
    pub fn new(piece_count: usize) -> Self {
		Self { piece_count }
	}
}