use anyhow::Result;
use std::{fmt::Debug, str};

use once_cell::sync::Lazy;

use rand::{distributions::Alphanumeric, seq::SliceRandom, thread_rng, Rng};
use torrent::{Torrent, TorrentInfo};
use tracing::info;

mod avg;
mod connection;
mod dht;
mod disk;
mod download;
mod messages;
mod metainfo;
mod peer;
mod piece_picker;
mod protocol;
mod stat;
mod storage;
mod torrent;
mod tracker;
mod units;

static PEER_ID: Lazy<metainfo::PeerID> = Lazy::new(|| {
    thread_rng()
        .sample_iter(&Alphanumeric)
        .take(20)
        .collect::<Vec<_>>()
        .try_into()
        .unwrap()
});

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();

    info!("PEER_ID: {}", str::from_utf8(PEER_ID.as_ref()).unwrap());
    // let meta_info = metainfo::read_torrent_file("ubuntu-22.10-server.torrent")?;
    // let meta_info = metainfo::read_torrent_file("ubuntu-24.04-server.torrent")?;
    // let meta_info = metainfo::read_torrent_file("ubuntu-24.04-desktop.torrent")?;
    // let meta_info = metainfo::read_torrent_file("debian-12.5.torrent")?;
    let meta_info = metainfo::read_torrent_file("debian.torrent")?;
    // let meta_info = metainfo::read_torrent_file("multi-file.torrent")?;
    // let meta_info = metainfo::read_torrent_file("slackware-14.2-install-d1.torrent")?;
    // let meta_info = metainfo::read_torrent_file("test-academic.torrent")?;
    // let meta_info = metainfo::read_torrent_file("slackware-14.2-install-d3.torrent")?;
    // let meta_info = metainfo::read_torrent_file("nord-osmosis.torrent")?;
    // let meta_info = metainfo::read_torrent_file("billy-multi-file.torrent")?;
    // let meta_info = metainfo::read_torrent_file("imp.torrent")?;
    // info!("{}", t.info.pieces.len());
    info!("Getting peer details from tracker");
    let tracker_data = tracker::get_peer_details_from_tracker(&meta_info, &PEER_ID).await?;
    println!("{:?}", tracker_data);
    info!(target: "main", peers = ?tracker_data.peers);

    let torrent_info = TorrentInfo::new(&meta_info, &tracker_data);

    let _peer = if let Some(p) = torrent_info.peers.choose(&mut rand::thread_rng()) {
        p
    } else {
        return Err(anyhow::anyhow!("No peers found"));
    };

    let mut torrent = Torrent::new(torrent_info, *PEER_ID);

    // futures::try_join!(torrent.start(), dht::start_dht_node())?;
    // torrent.start().await?;
    // let mut peer_session = PeerSession::new(torrent_info.clone(), peer.clone());
    // peer_session.start_connection(*PEER_ID).await?;

    dht::start_dht_node().await?;

    Ok(())
}
