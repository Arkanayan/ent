use anyhow::Result;
use log::info;
use once_cell::sync::{OnceCell, Lazy};
use rand::{thread_rng, distributions::Alphanumeric, Rng};

mod metainfo;
mod tracker;
mod connection;
mod protocol;
mod messages;


static PEER_ID: Lazy<metainfo::PeerID> = Lazy::new(|| {
    thread_rng().sample_iter(&Alphanumeric).take(20).collect::<Vec<_>>().try_into().unwrap()
});

#[tokio::main]
async fn main() -> Result<()> {
    env_logger::init();

    // info!("PEER_ID: {}", PEER_ID);
    let t = metainfo::read_torrent_file("ubuntu-22-10.torrent")?;
    // let t = metainfo::read_torrent_file("debian.torrent")?;
    // info!("{}", t.info.pieces.len());
    let tracker_data = tracker::get_peer_details_from_tracker(&t, &PEER_ID).await?;
    connection::start_connection(PEER_ID.clone(), t, tracker_data).await?;
    Ok(())
}
