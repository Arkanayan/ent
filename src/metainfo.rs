use anyhow::Result;
use serde::{Deserialize, Serialize};
use serde_bytes::ByteBuf;
use sha1::{Digest, Sha1};
use std::fs;
use std::io::Read;
use std::path::Path;


pub type PeerID = [u8; 20];

#[derive(Debug, Deserialize, Clone)]
pub struct Node(String, i64);

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct File {
    pub path: Vec<String>,
    pub length: u64,
    #[serde(default)]
    pub md5sum: Option<String>,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct Info {
    pub name: String,
    pub pieces: ByteBuf,
    #[serde(rename = "piece length")]
    pub piece_length: u32,
    #[serde(default)]
    pub md5sum: Option<String>,
    #[serde(default)]
    pub length: Option<i64>,
    #[serde(default)]
    pub files: Option<Vec<File>>,
    #[serde(default)]
    pub private: Option<u8>,
    #[serde(default)]
    pub path: Option<Vec<String>>,
    #[serde(default)]
    #[serde(rename = "root hash")]
    pub root_hash: Option<String>,
}

#[derive(Debug, Deserialize, Clone)]
pub struct MetaInfo {
    pub info: Info,
    #[serde(default)]
    pub announce: Option<String>,
    #[serde(default)]
    pub nodes: Option<Vec<Node>>,
    #[serde(default)]
    pub encoding: Option<String>,
    #[serde(default)]
    pub httpseeds: Option<Vec<String>>,
    #[serde(default)]
    #[serde(rename = "announce-list")]
    pub announce_list: Option<Vec<Vec<String>>>,
    #[serde(default)]
    #[serde(rename = "creation date")]
    pub creation_date: Option<i64>,
    #[serde(rename = "comment")]
    pub comment: Option<String>,
    #[serde(default)]
    #[serde(rename = "created by")]
    pub created_by: Option<String>,
}

impl MetaInfo {
    pub fn num_pieces(&self) -> usize {
        self.info.pieces.len() / 20
    }

    pub fn get_piece_sha1(&self, piece_index: usize) -> &[u8] {
        let piece_start = piece_index * 20;
        &self.info.pieces[piece_start..piece_start + 20]
    }

    pub fn info_hash(&self) -> [u8; 20] {
        let mut hasher = Sha1::new();
        hasher.update(serde_bencode::to_bytes(&self.info).unwrap());
        let result = hasher.finalize();
        result.into()
    }

    pub fn torrent_len(&self) -> u64 {
        if let Some(files) = self.info.files {
            return files.iter().map(|f| f.length).sum();
        }

        self.info.length.unwrap() as u64
    }

}

pub fn read_torrent_file<T: AsRef<Path>>(path: T) -> Result<MetaInfo> {
    let mut bytes = Vec::new();
    let mut f = fs::File::open(path.as_ref())?;
    let _ = f.read_to_end(&mut bytes)?;

    let deserialized: MetaInfo = serde_bencode::from_bytes(bytes.as_slice())?;
    Ok(deserialized)
}
