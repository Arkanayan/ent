use anyhow::anyhow;
use bytes::{Buf, BufMut};
use log::info;
use std::io::{prelude::*, Cursor};
use tokio::io::AsyncReadExt;
use tokio_util::codec::{Decoder, Encoder};

use crate::metainfo::PeerID;

const PROTOCOL_IDENTIFIER: &str = "BitTorrent protocol";
#[derive(Debug)]
pub struct HandShake {
    pub protocol_identifier: [u8; 19],
    pub reserved: [u8; 8],
    pub info_hash: [u8; 20],
    pub peer_id: PeerID,
}

impl HandShake {
    pub fn new(peer_id: PeerID, info_hash: [u8; 20]) -> Self {
        HandShake {
            protocol_identifier: PROTOCOL_IDENTIFIER.as_bytes().try_into().unwrap(),
            reserved: [0u8; 8],
            info_hash,
            peer_id,
        }
    }

    pub fn len(&self) -> usize {
        19 + 8 + 20 + 20
    }
}

pub struct HandShakeCodec;

impl Encoder<HandShake> for HandShakeCodec {
    type Error = anyhow::Error;

    fn encode(
        &mut self,
        hand_shake: HandShake,
        dst: &mut bytes::BytesMut,
    ) -> Result<(), Self::Error> {
        let protocol_identifier_bytes = PROTOCOL_IDENTIFIER.as_bytes();

        debug_assert_eq!(protocol_identifier_bytes.len(), 19);
        dst.put_u8(protocol_identifier_bytes.len() as u8);
        dst.extend_from_slice(PROTOCOL_IDENTIFIER.as_bytes());
        dst.extend_from_slice(&hand_shake.reserved);
        dst.extend_from_slice(&hand_shake.info_hash);
        dst.extend_from_slice(&hand_shake.peer_id);

        info!("Length of dst buffer: {}", dst.len());
        Ok(())
    }
}

impl Decoder for HandShakeCodec {
    type Item = HandShake;

    type Error = anyhow::Error;

    fn decode(&mut self, src: &mut bytes::BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        if src.is_empty() {
            return Ok(None);
        }

        let mut cursor = Cursor::new(&src[..]);
        let proto_len = cursor.get_u8() as usize;

        if proto_len != PROTOCOL_IDENTIFIER.as_bytes().len() {
            return Err(anyhow!(
                "Protocol identifier string length must be 19. Found: {}",
                proto_len
            ));
        }

        let payload_len = proto_len + 8 + 20 + 20;

        if cursor.remaining() > payload_len {
            src.advance(1);
        } else {
            return Ok(None);
        }

        let mut proto_identifier = [0; 19];
        src.copy_to_slice(&mut proto_identifier);

        let mut reserved = [0; 8];
        src.copy_to_slice(&mut reserved);

        let mut info_hash = [0; 20];
        src.copy_to_slice(&mut info_hash);

        let mut peer_id = [0; 20];
        src.copy_to_slice(&mut peer_id);

        Ok(Some(HandShake {
            protocol_identifier: proto_identifier,
            reserved,
            info_hash,
            peer_id,
        }))
    }
}

#[repr(u8)]
#[derive(Debug)]
pub enum MessageId {
    Choke = 0,
    Unchoke = 1,
    Interested = 2,
    NotInterested = 3,
    Have = 4,
    BitField = 5,
    Request = 6,
    Piece = 7,
    Cancel = 8,
    Port = 9
}

#[derive(Debug)]
pub enum Mesasge {

}