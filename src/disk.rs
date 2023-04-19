use std::collections::{BTreeMap, HashMap};
use std::io;

use anyhow::Result;
use tokio::sync::mpsc::{
    unbounded_channel, UnboundedReceiver, UnboundedSender, WeakUnboundedSender,
};
use tokio::task::JoinHandle;

use crate::{storage::StorageInfo, torrent::BlockInfo, units::PieceIndex};

#[derive(Debug)]
pub struct Piece {
    pub len: u32,
    pub blocks: BTreeMap<u32, Vec<u8>>,
}

#[derive(Debug)]
pub struct DiskEntry {
    pub cmd_tx: CommandSender,
    pub alert_rx: AlertReceiver,
    pub handle: JoinHandle<Result<()>>,
}

pub struct DiskComm {
    pub alert_rx: AlertReceiver,
    pub cmd_tx: CommandSender
}

#[derive(Debug)]
pub struct DiskStorage {
    filename: String,
    hashes: Vec<u8>,
    info: StorageInfo,
    pieces: HashMap<PieceIndex, Piece>,
    cmd_rx: CommandReceiver,
    alert_tx: AlertSender,
}

impl DiskStorage {
    pub fn new(
        filename: String,
        hashes: Vec<u8>,
        info: StorageInfo,
    ) -> (Self, CommandSender, AlertReceiver) {
        let (tx, rx) = unbounded_channel();
        let (cmd_tx, cmd_rx) = unbounded_channel();
        (
            Self {
                filename,
                hashes,
                info,
                pieces: HashMap::new(),
                cmd_rx,
                alert_tx: tx,
            },
            cmd_tx,
            rx,
        )
    }
}

pub type CommandSender = UnboundedSender<Command>;
pub type CommandReceiver = UnboundedReceiver<Command>;

#[derive(Debug)]
pub enum Command {
    WriteBlock(BlockInfo, Vec<u8>),
    Shutdown,
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
    Err(io::ErrorKind),
}
