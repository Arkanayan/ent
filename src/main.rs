

use anyhow::Result;

use once_cell::sync::{Lazy};

use rand::{thread_rng, distributions::Alphanumeric, Rng, seq::SliceRandom};
use torrent::{TorrentInfo, Torrent};
use tracing::info;

mod metainfo;
mod tracker;
mod connection;
mod protocol;
mod messages;
mod torrent;
mod peer;
mod piece_picker;
mod storage;
mod units;
mod download;
mod disk;
mod stat;
mod avg;


static PEER_ID: Lazy<metainfo::PeerID> = Lazy::new(|| {
    thread_rng().sample_iter(&Alphanumeric).take(20).collect::<Vec<_>>().try_into().unwrap()
});

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();

    // info!("PEER_ID: {}", PEER_ID);
    let meta_info = metainfo::read_torrent_file("debian-12.5.torrent")?;
    // let meta_info = metainfo::read_torrent_file("debian.torrent")?;
    // info!("{}", t.info.pieces.len());
    let tracker_data = tracker::get_peer_details_from_tracker(&meta_info, &PEER_ID).await?;
    info!(target: "main", peers = ?tracker_data.peers);

    let torrent_info = TorrentInfo::new(&meta_info, &tracker_data);

    let _peer = if let Some(p) = torrent_info.peers.choose(&mut rand::thread_rng()) {
        p
    } else {
        return Err(anyhow::anyhow!("No peers found"));
    };

    let mut torrent = Torrent::new(torrent_info, *PEER_ID);
    torrent.start().await?;
    // let mut peer_session = PeerSession::new(torrent_info.clone(), peer.clone());
    // peer_session.start_connection(*PEER_ID).await?;

    Ok(())
}
