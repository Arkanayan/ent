use std::collections::{HashMap, BTreeMap};
use std::io;

use tokio::sync::mpsc::{UnboundedSender, UnboundedReceiver, WeakUnboundedSender};

use crate::{storage::StorageInfo, units::PieceIndex, torrent::BlockInfo};


#[derive(Debug)]
pub struct Piece {
    pub hash: [u8; 20],
    pub len: u32,
    pub blocks: BTreeMap<u32, Vec<u8>>
}

#[derive(Debug)]
pub struct DiskStorage {
    pub filename: String,
    pub hashes: Vec<u8>,
    pub info: StorageInfo,
    pub pieces: HashMap<PieceIndex, Piece>
}

pub type CommandSender = UnboundedSender<Command>;
pub type CommandReceiver = UnboundedReceiver<Command>;

#[derive(Debug)]
pub enum Command {
    WriteBlock(BlockInfo, Vec<u8>),
    Shutdown
    
}

pub type AlertSender = UnboundedSender<Alert>;
pub type AlertReceiver = UnboundedReceiver<Alert>;

#[derive(Debug)]
pub enum Alert {
    BlockWriteError(BlockInfo, BlockWriteError),
    PieceCompletion(PieceIndex),
}

#[derive(Debug)]
pub enum BlockWriteError {
    Unknown,
    Err(io::ErrorKind)
}
