use std::collections::{BTreeMap, HashMap};
use std::io;

use anyhow::Result;

use sha1::{Digest, Sha1};


use tokio::sync::mpsc::{
    unbounded_channel, UnboundedReceiver, UnboundedSender,
};
use tokio::task::{JoinHandle, spawn_blocking};
use tokio::{select, fs};
use tokio::io::{AsyncSeek, AsyncSeekExt, AsyncWrite, AsyncWriteExt};
use tracing::{debug, trace, info};

use crate::units::block_count;
use crate::{storage::StorageInfo, torrent::BlockInfo, units::PieceIndex};

#[derive(Debug)]
pub struct Piece {
    pub len: u32,
    pub blocks: BTreeMap<u32, Vec<u8>>,
    pub expected_hash: [u8; 20],
}

impl Piece {
    pub fn is_complete(&self) -> bool {
        self.blocks.len() == block_count(self.len)
    }

    pub fn verify_hash(&mut self) -> bool {
        let mut hasher = Sha1::new();
        for b in self.blocks.values() {
            hasher.update(b);
        }
        let hash = hasher.finalize();

        if self.expected_hash.eq(&hash.as_slice()) {
            return true;
        }
        false
    }
}

#[derive(Debug)]
pub struct DiskEntry {
    pub cmd_tx: CommandSender,
    pub alert_rx: AlertReceiver,
    pub handle: JoinHandle<Result<()>>,
}

pub struct DiskComm {
    pub alert_rx: AlertReceiver,
    pub cmd_tx: CommandSender,
}

#[derive(Debug)]
pub struct DiskStorage {
    filename: String,
    hashes: Vec<u8>,
    info: StorageInfo,
    pieces: HashMap<PieceIndex, Piece>,
    cmd_rx: CommandReceiver,
    alert_tx: AlertSender,
    missing_count: usize
}

impl DiskStorage {
    pub fn new(
        filename: String,
        hashes: Vec<u8>,
        info: StorageInfo,
    ) -> (Self, CommandSender, AlertReceiver) {
        let (tx, rx) = unbounded_channel();
        let (cmd_tx, cmd_rx) = unbounded_channel();
        let missing_pieces_count = info.piece_count;
        (
            Self {
                filename,
                hashes,
                info,
                pieces: HashMap::new(),
                cmd_rx,
                alert_tx: tx,
                missing_count: missing_pieces_count
            },
            cmd_tx,
            rx,
        )
    }

    pub async fn start(mut self) -> Result<()> {
        trace!("Starting Disk Thread");
        let f = fs::OpenOptions::new().create(true).read(true).write(true).open(&self.filename).await?;
        let mut buf = tokio::io::BufWriter::new(f);

        loop {
            select! {
               Some(cmd) = self.cmd_rx.recv() => {
                    match cmd {
                        Command::WriteBlock(block_info, bytes) => {
                            info!("Received block. Piece: {}, block offset: {}, size: {}", block_info.piece_index, block_info.offset, bytes.len());
                            self.write_block(&mut buf, block_info, bytes).await;
                        },
                        Command::Shutdown => {
                            info!("Shutting down disk storage");
                            buf.flush().await.ok();
                            return Ok(());
                        }
                    }
                }
            }
        }
    }

    async fn write_block<F: AsyncSeek + AsyncWrite + Unpin>(&mut self, file: &mut F,block_info: BlockInfo, data: Vec<u8>) {
        let piece_index = block_info.piece_index();
        if let Some(piece) = self.pieces.get_mut(&block_info.piece_index) {
            if piece
                .blocks
                .contains_key(&block_info.offset)
            {
                debug!(
                    "Duplicate block download: Piece: {} Block in piece: {}",
                    block_info.piece_index,
                    block_info.index_in_piece()
                );
                return;
            }
            piece
                .blocks
                .insert(block_info.offset, data);
        } else {
            let piece_len = if block_info.piece_index() == (self.info.piece_count - 1) {
                self.info.last_piece_len
            } else {
                self.info.piece_len
            };

            let mut piece_hash = [0u8; 20];
            piece_hash.copy_from_slice(&self.hashes[piece_index*20..piece_index*20+20]);

            let mut piece = Piece {
                len: piece_len,
                blocks: BTreeMap::new(),
                expected_hash: piece_hash,
            };
            piece.blocks.insert(block_info.offset, data);
            self.pieces.insert(piece_index, piece);
        }

        let piece = self.pieces.get(&piece_index).expect("Piece not found");

        if piece.is_complete() {
            let mut piece = self.pieces.remove(&piece_index).expect("Piece not found");
            let alert_tx = self.alert_tx.clone();

            let (verified, piece) = spawn_blocking(move || {
                (piece.verify_hash(), piece)
            }).await.expect("Hash calculation error");

            if verified {
                self.missing_count -= 1;
                let file_start = piece_index * piece.len as usize;
                file.seek(io::SeekFrom::Start(file_start as u64)).await.ok();
                for b in piece.blocks.into_values() {
                    file.write_all(b.as_slice()).await.ok();
                }
                alert_tx.send(Alert::PieceCompletion(piece_index)).ok();
            } else {
                alert_tx.send(Alert::PieceHashMismatch(piece_index)).ok();
            }

            if self.missing_count == 0 {
                alert_tx.send(Alert::TorrentCompletion).ok();
            }

        }
        
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
    PieceHashMismatch(PieceIndex),
    BlockWriteError(BlockInfo, BlockWriteError),
    PieceCompletion(PieceIndex),
    TorrentCompletion
}

#[derive(Debug)]
pub enum BlockWriteError {
    Unknown,
    Err(io::ErrorKind),
}
