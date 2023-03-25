use anyhow::Result;
use log::info;
use once_cell::sync::{OnceCell, Lazy};
use rand::{thread_rng, distributions::Alphanumeric, Rng};

mod torrent;
mod tracker;
mod connection;
mod protocol;

static PEER_ID: Lazy<String> = Lazy::new(|| {
    thread_rng().sample_iter(&Alphanumeric).take(20).map(char::from).collect()
});

#[tokio::main]
async fn main() -> Result<()> {
    env_logger::init();

    info!("PEER_ID: {}", &*PEER_ID);
    // let t = torrent::read_torrent_file("ubuntu-22-10.torrent")?;
    let t = torrent::read_torrent_file("debian.torrent")?;
    // info!("{}", t.info.pieces.len());
    let tracker_data = tracker::get_peer_details_from_tracker(&t, &*PEER_ID.as_str()).await?;
    connection::start_connection((&*PEER_ID).to_string(), t, tracker_data).await?;
    Ok(())
}
