use anyhow::{anyhow, Result};
use futures::{stream::SplitSink, SinkExt, StreamExt};
use log::info;
use tokio::net::TcpStream;
use tokio_util::codec::{Framed, FramedParts};

use crate::messages::{HandShake, HandShakeCodec, MessageCodec};
use crate::metainfo::{MetaInfo, PeerID};
use crate::tracker::TrackerData;

