// crates/faststreams/src/lib.rs
#![forbid(unsafe_code)]
use bytes::{BufMut, BytesMut};
use serde::{Deserialize, Serialize};
use std::io::{self, Read, Write};

pub const FRAME_MAGIC: u32 = 0x4653_5452; // 'FSTR'
pub const FRAME_VERSION: u16 = 1;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AccountUpdate {
    pub slot: u64,
    pub is_startup: bool,
    pub pubkey: [u8; 32],
    pub lamports: u64,
    pub owner: [u8; 32],
    pub executable: bool,
    pub rent_epoch: u64,
    #[serde(with = "serde_bytes")]
    pub data: Vec<u8>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TxUpdate {
    pub slot: u64,
    pub signature_b58: String,
    pub err: Option<String>,
    pub vote: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BlockMeta {
    pub slot: u64,
    pub blockhash_b58: Option<String>,
    pub parent_slot: u64,
    pub rewards_len: u32,
    pub block_time_unix: Option<i64>,
    pub leader: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Record {
    Account(AccountUpdate),
    Tx(TxUpdate),
    Block(BlockMeta),
    Slot { slot: u64, parent: Option<u64>, status: u8 },
    EndOfStartup,
}

#[derive(thiserror::Error, Debug)]
pub enum StreamError {
    #[error("io: {0}")]
    Io(#[from] io::Error),
    #[error("deserialize: {0}")]
    De(#[from] Box<bincode::ErrorKind>),
    #[error("serialize: {0}")]
    Ser(#[from] bincode::Error),
    #[error("bad magic or version")]
    BadHeader,
}

pub fn encode_record(rec: &Record) -> Result<Vec<u8>, StreamError> {
    let payload = bincode::serialize(rec)?;
    let mut buf = BytesMut::with_capacity(4 + 2 + 4 + payload.len());
    buf.put_u32(FRAME_MAGIC);
    buf.put_u16(FRAME_VERSION);
    buf.put_u32(payload.len() as u32);
    buf.extend_from_slice(&payload);
    Ok(buf.to_vec())
}

pub fn decode_record(mut src: impl Read) -> Result<Record, StreamError> {
    let mut hdr = [0u8; 10];
    src.read_exact(&mut hdr)?;
    let magic = u32::from_be_bytes([hdr[0], hdr[1], hdr[2], hdr[3]]);
    let ver = u16::from_be_bytes([hdr[4], hdr[5]]);
    if magic != FRAME_MAGIC || ver != FRAME_VERSION {
        return Err(StreamError::BadHeader);
    }
    let len = u32::from_be_bytes([hdr[6], hdr[7], hdr[8], hdr[9]]) as usize;
    let mut payload = vec![0u8; len];
    src.read_exact(&mut payload)?;
    Ok(bincode::deserialize::<Record>(&payload).map_err(|e| e)?)
}

pub fn write_all_vectored(mut dst: impl Write, mut frames: Vec<Vec<u8>>) -> io::Result<()> {
    for frame in frames.iter_mut() {
        let mut written = 0usize;
        while written < frame.len() {
            let n = dst.write(&frame[written..])?;
            if n == 0 {
                return Err(io::Error::new(io::ErrorKind::WriteZero, "short write"));
            }
            written += n;
        }
    }
    Ok(())
}