use std::collections::{BTreeMap, HashMap};
use std::io;
use std::path::{Path, PathBuf};

use anyhow::Result;

use sha1::{Digest, Sha1};

use tokio::io::{AsyncSeek, AsyncSeekExt, AsyncWrite, AsyncWriteExt};
use tokio::sync::mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender};
use tokio::task::{spawn_blocking, JoinHandle};
use tokio::{fs, select, fs::File, io::BufWriter};
use tracing::{debug, info, trace};

use crate::metainfo;
use crate::piece_picker::PieceBlock;
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
    /// filename in case of single file torrent
    /// parent directory name in case of multi file
    name: String,
    hashes: Vec<u8>,
    info: StorageInfo,
    pieces: HashMap<PieceIndex, Piece>,
    cmd_rx: CommandReceiver,
    alert_tx: AlertSender,
    missing_count: usize,
    files: Option<Vec<metainfo::File>>,
    buffers: HashMap<String, BufWriter<File>>
}

impl DiskStorage {
    pub fn new(
        filename: String,
        hashes: Vec<u8>,
        info: StorageInfo,
        files: Option<Vec<metainfo::File>>,
    ) -> (Self, CommandSender, AlertReceiver) {
        let (tx, rx) = unbounded_channel();
        let (cmd_tx, cmd_rx) = unbounded_channel();
        let missing_pieces_count = info.piece_count;
        (
            Self {
                name: filename,
                hashes,
                info,
                pieces: HashMap::new(),
                cmd_rx,
                alert_tx: tx,
                missing_count: missing_pieces_count,
                files: files,
                buffers: HashMap::new()
            },
            cmd_tx,
            rx,
        )
    }

    fn is_multifile(&self) -> bool {
        self.files.is_some()
    }

    pub async fn start(mut self) -> Result<()> {
        trace!("Starting Disk Thread");

        loop {
            select! {
               Some(cmd) = self.cmd_rx.recv() => {
                    match cmd {
                        Command::WriteBlock(pb, bytes) => {
                            trace!("Received block. Piece: {}, Block: {}, size: {}", pb.piece_index, pb.block_index, bytes.len());
                            self.write_block(pb, bytes).await;
                        },
                        Command::Shutdown => {
                            info!("Shutting down disk storage");
                            self.flush_buffers().await?;
                            return Ok(());
                        }
                    }
                }
            }
        }
    }

    async fn write_block(
        &mut self,
        pb: PieceBlock,
        data: Vec<u8>,
    ) {
        let piece_index = pb.piece_index;
        let block_offset = self.info.get_offset_in_piece(&pb);
        if let Some(piece) = self.pieces.get_mut(&pb.piece_index) {
            if piece.blocks.contains_key(&block_offset) {
                debug!(
                    "Duplicate block download: Piece: {} Block in piece: {}",
                    pb.piece_index,
                    pb.block_index
                );
                return;
            }
            piece.blocks.insert(block_offset, data);
        } else {
            let piece_len = self.info.piece_len(&pb);

            let mut piece_hash = [0u8; 20];
            piece_hash.copy_from_slice(&self.hashes[piece_index * 20..piece_index * 20 + 20]);

            let mut piece = Piece {
                len: piece_len,
                blocks: BTreeMap::new(),
                expected_hash: piece_hash,
            };
            piece.blocks.insert(block_offset, data);
            self.pieces.insert(piece_index, piece);
        }

        let piece = self.pieces.get(&piece_index).expect("Piece not found");

        if piece.is_complete() {
            let mut piece = self.pieces.remove(&piece_index).expect("Piece not found");
            let alert_tx = self.alert_tx.clone();

            let (verified, piece) = spawn_blocking(move || (piece.verify_hash(), piece))
                .await
                .expect("Hash calculation error");

            if verified {
                self.missing_count -= 1;
                let offset = piece_index as u64 * piece.len as u64;

                let (filename, relative_offset) = self.get_filename(offset);
                let buffer = self.get_file_buffer(&filename).await;
                buffer.seek(io::SeekFrom::Start(relative_offset as u64)).await.ok();

                for b in piece.blocks.into_values() {
                    buffer.write_all(b.as_slice()).await.ok();
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

    fn get_filename(&self, offset: u64) -> (PathBuf, u64) {
        
        if !self.is_multifile() {
            return (PathBuf::from(self.name.as_str()), offset);
        }

        let files = self.files.as_ref().unwrap();

        let mut idx = 0;

        for (i, f) in files.iter().enumerate() {

            if offset < f.offset {
                idx = i;
            } else {
                break;
            }
        }
        let mut p = PathBuf::from(self.name.as_str());
        p.extend(files[idx].path.iter());
        (p, offset - files[idx].offset)
    }

    async fn get_file_buffer(&mut self, file_path: &Path) -> &mut BufWriter<File> {

        if self.buffers.contains_key(file_path.to_str().unwrap()) {
            // return &mut self.buffers[file_path.to_str().unwrap()];
            return self.buffers.get_mut(file_path.to_str().unwrap()).unwrap();
        }

        if let Some(parent) = file_path.parent() {
            tokio::fs::create_dir_all(parent).await.ok();
        }

        let f = fs::OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .open(file_path)
            .await.unwrap();

        let buf = tokio::io::BufWriter::new(f);

        self.buffers.insert(file_path.to_str().unwrap().to_owned(), buf);

        self.buffers.get_mut(file_path.to_str().unwrap()).unwrap()
    }

    async fn flush_buffers(&mut self) -> io::Result<()> {

        for (_, mut b) in self.buffers.drain() {
            b.flush().await?;
        }
        Ok(()) 
    }
}

pub type CommandSender = UnboundedSender<Command>;
pub type CommandReceiver = UnboundedReceiver<Command>;

#[derive(Debug)]
pub enum Command {
    WriteBlock(PieceBlock, Vec<u8>),
    Shutdown,
}

pub type AlertSender = UnboundedSender<Alert>;
pub type AlertReceiver = UnboundedReceiver<Alert>;

#[derive(Debug)]
pub enum Alert {
    PieceHashMismatch(PieceIndex),
    BlockWriteError(BlockInfo, BlockWriteError),
    PieceCompletion(PieceIndex),
    TorrentCompletion,
}

#[derive(Debug)]
pub enum BlockWriteError {
    Unknown,
    Err(io::ErrorKind),
}
