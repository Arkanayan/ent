use anyhow::anyhow;
use bitvec::prelude::{BitVec, Msb0};
use bytes::{Buf, BufMut, Bytes};
use std::io;
use std::io::{prelude::*, Cursor};
use tokio::io::AsyncReadExt;
use tokio_util::codec::{Decoder, Encoder};

use crate::metainfo::PeerID;
use crate::torrent::{BlockData, BlockInfo};

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
    Port = 9,
}

impl TryFrom<u8> for MessageId {
    type Error = anyhow::Error;

    fn try_from(message_id: u8) -> Result<Self, Self::Error> {
        match message_id {
            0 => Ok(Self::Choke),
            1 => Ok(Self::Unchoke),
            2 => Ok(Self::Interested),
            3 => Ok(Self::NotInterested),
            4 => Ok(Self::Have),
            5 => Ok(Self::BitField),
            6 => Ok(Self::Request),
            7 => Ok(Self::Piece),
            8 => Ok(Self::Cancel),
            9 => Ok(Self::Port),
            i => Err(anyhow::anyhow!("Invalid Message Id {}", i)),
        }
    }
}

#[derive(Debug)]
pub enum Message {
    KeepAlive,
    Choke,
    Unchoke,
    Interested,
    NotInterested,
    Have(usize),
    BitField(BitField),
    Request(BlockInfo),
    Piece(BlockData),
    Cancel(BlockInfo),
    Port(u16),
}

pub type BitField = BitVec<u8, Msb0>;

#[derive(Debug)]
pub struct MessageCodec;

impl Encoder<Message> for MessageCodec {
    type Error = anyhow::Error;

    fn encode(&mut self, message: Message, dst: &mut bytes::BytesMut) -> Result<(), Self::Error> {
        use Message::*;

        match message {
            KeepAlive => {
                let msg_len = 0;
                dst.reserve(4);
                dst.put_u32(msg_len);
            }
            Choke => {
                let msg_len = 1;
                dst.reserve(4 + 4);
                dst.put_u32(msg_len);
                dst.put_u8(MessageId::Choke as u8);
            }
            Unchoke => {
                let msg_len = 1;
                dst.reserve(4 + 4);
                dst.put_u32(msg_len);
                dst.put_u8(MessageId::Unchoke as u8);
            }
            Interested => {
                let msg_len = 1;
                dst.reserve(4 + 4);
                dst.put_u32(msg_len);
                dst.put_u8(MessageId::Interested as u8);
            }
            NotInterested => {
                let msg_len = 1;
                dst.reserve(4 + 4);
                dst.put_u32(msg_len);
                dst.put_u8(MessageId::NotInterested as u8);
            }
            Have(index) => {
                let msg_len = 1 + 4;
                dst.reserve(4 * 3);
                dst.put_u32(msg_len as u32);
                dst.put_u8(MessageId::Have as u8);
                dst.put_u32(index as u32);
            }
            BitField(bv) => {
                let msg_len = 1 + bv.len() / 8;
                dst.reserve(4 + 4);
                dst.put_u32(msg_len as u32);
                dst.put_u8(MessageId::BitField as u8);
                dst.extend_from_slice(bv.as_raw_slice());
            }
            Request(BlockInfo {
                piece_index,
                offset: start,
                length,
            }) => {
                let msg_len = 1 + 4 + 4 + 4;
                dst.reserve(4 + msg_len * 4);
                dst.put_u32(msg_len as u32);
                dst.put_u8(MessageId::Request as u8);
                dst.put_u32(piece_index as u32);
                dst.put_u32(start);
                dst.put_u32(length);
            }
            Piece(BlockData {
                piece_index,
                offset,
                mut data,
            }) => {
                let msg_len = 1 + 4 + 4 + data.len();
                dst.reserve(4 + msg_len * 4);
                dst.put_u32(msg_len as u32);
                dst.put_u8(MessageId::Piece as u8);
                dst.put_u32(piece_index as u32);
                dst.put_u32(offset);
                data.copy_to_slice(dst);
            }
            Cancel(BlockInfo {
                piece_index,
                offset: start,
                length,
            }) => {
                let msg_len = 1 + 4 + 4 + 4;
                dst.reserve(4 + msg_len * 4);
                dst.put_u32(msg_len as u32);
                dst.put_u8(MessageId::Cancel as u8);
                dst.put_u32(piece_index as u32);
                dst.put_u32(start);
                dst.put_u32(length);
            }
            Port(port) => {
                let msg_len = 1 + 2;
                dst.reserve(1 + msg_len * 4);
                dst.put_u32(msg_len as u32);
                dst.put_u8(MessageId::Port as u8);
                dst.put_u16(port);
            }
        }
        Ok(())
    }
}

impl Decoder for MessageCodec {
    type Item = Message;

    type Error = anyhow::Error;

    fn decode(&mut self, src: &mut bytes::BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        if src.is_empty() {
            return Ok(None);
        }

        // The frame doesn't even contain the length
        if src.len() < 4 {
            return Ok(None);
        }

        let mut cursor = Cursor::new(&src);
        let msg_len = cursor.get_u32() as usize;

        cursor.set_position(0);

        // Whole frame hasn't received yet
        if src.remaining() < 4 + msg_len {
            return Ok(None);
        }

        src.advance(4);

        // This is keepalive
        if msg_len == 0 {
            return Ok(Some(Message::KeepAlive));
        }

        let msg_id = MessageId::try_from(src.get_u8())?;

        let msg = match msg_id {
            MessageId::Choke => Message::Choke,
            MessageId::Unchoke => Message::Unchoke,
            MessageId::Interested => Message::Interested,
            MessageId::NotInterested => Message::NotInterested,
            MessageId::Have => {
                let piece_index = src.get_u32();
                let piece_index = piece_index
                    .try_into()
                    .map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e))?;
                Message::Have(piece_index)
            }
            MessageId::BitField => {
                let mut bitfield = vec![0; msg_len - 1];
                src.copy_to_slice(&mut bitfield);
                Message::BitField(BitField::from_vec(bitfield))
            }
            MessageId::Request => {
                let piece_index = src.get_u32();
                let begin = src.get_u32();
                let length = src.get_u32();

                let piece_index = piece_index
                    .try_into()
                    .map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e))?;
                Message::Request(BlockInfo {
                    piece_index: piece_index,
                    offset: begin,
                    length: length,
                })
            }
            MessageId::Piece => {
                let piece_index = src.get_u32();
                let offset = src.get_u32();
                let mut data = vec![0; msg_len - 1 - 4 - 4];
                src.copy_to_slice(&mut data);
                
                let piece_index = piece_index
                    .try_into()
                    .map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e))?;
                Message::Piece(BlockData {
                    piece_index: piece_index,
                    offset: offset,
                    data: data.into(),
                })
            }
            MessageId::Cancel => {
                let piece_index = src.get_u32();
                let begin = src.get_u32();
                let length = src.get_u32();

                let piece_index = piece_index
                    .try_into()
                    .map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e))?;
                Message::Cancel(BlockInfo {
                    piece_index: piece_index,
                    offset: begin,
                    length: length,
                })
            }
            MessageId::Port => {
                let port = src.get_u16();
                Message::Port(port)
            }
        };

        Ok(Some(msg))
    }
}
