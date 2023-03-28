use crate::metainfo::{PeerID};

#[derive(Debug)]
pub struct HandShake {
    pub protocol_identifier: [u8; 19],
    pub reserved: [u8; 8],
    pub info_hash: [u8; 20],
    pub peer_id: PeerID
}
