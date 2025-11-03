// crates/faststreams/src/lib.rs
#![forbid(unsafe_code)]
use bytes::{BufMut, BytesMut};
use serde::{Deserialize, Serialize};
use std::io::{self, Read, Write};
use std::io::IoSlice;

const COMPRESS_THRESHOLD: usize = 2048;
const FLAG_LZ4: u16 = 0x0001;

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
    pub signature: [u8; 64],
    pub err: Option<String>,
    pub vote: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BlockMeta {
    pub slot: u64,
    pub blockhash: Option<[u8; 32]>,
    pub parent_slot: u64,
    pub rewards_len: u32,
    pub block_time_unix: Option<i64>,
    pub leader: Option<[u8; 32]>,
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
    let (flags, body): (u16, Vec<u8>) = if payload.len() >= COMPRESS_THRESHOLD {
        // use lz4 with size prepended to enable single-buffer decode
        let compressed = lz4_flex::block::compress_prepend_size(&payload);
        (FLAG_LZ4, compressed)
    } else {
        (0, payload)
    };

    // header: magic(4) | version(2) | flags(2) | len(4)
    let mut buf: Vec<u8> = Vec::with_capacity(4 + 2 + 2 + 4 + body.len());
    buf.extend_from_slice(&FRAME_MAGIC.to_be_bytes());
    buf.extend_from_slice(&FRAME_VERSION.to_be_bytes());
    buf.extend_from_slice(&flags.to_be_bytes());
    buf.extend_from_slice(&(body.len() as u32).to_be_bytes());
    buf.extend_from_slice(&body);
    Ok(buf)
}

pub fn decode_record(mut src: impl Read) -> Result<Record, StreamError> {
    let mut hdr = [0u8; 12];
    src.read_exact(&mut hdr)?;
    let magic = u32::from_be_bytes([hdr[0], hdr[1], hdr[2], hdr[3]]);
    let ver = u16::from_be_bytes([hdr[4], hdr[5]]);
    if magic != FRAME_MAGIC || ver != FRAME_VERSION {
        return Err(StreamError::BadHeader);
    }
    let flags = u16::from_be_bytes([hdr[6], hdr[7]]);
    let len = u32::from_be_bytes([hdr[8], hdr[9], hdr[10], hdr[11]]) as usize;
    let mut body = vec![0u8; len];
    src.read_exact(&mut body)?;
    let payload = if (flags & FLAG_LZ4) != 0 {
        lz4_flex::block::decompress_size_prepended(&body)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?
    } else {
        body
    };
    Ok(bincode::deserialize::<Record>(&payload).map_err(|e| e)?)
}

pub fn write_all_vectored(mut dst: impl Write, frames: &[Vec<u8>]) -> io::Result<()> {
    // Build IoSlice array over immutable frame data
    let mut slices: Vec<IoSlice<'_>> = frames.iter().map(|f| IoSlice::new(&f[..])).collect();
    let mut offset = 0usize;
    while offset < slices.len() {
        let n = dst.write_vectored(&slices[offset..])?;
        if n == 0 {
            return Err(io::Error::new(io::ErrorKind::WriteZero, "short write"));
        }

        // Advance IoSlice array by the number of bytes written
        let mut remaining = n;
        while remaining > 0 {
            if remaining >= slices[offset].len() {
                remaining -= slices[offset].len();
                offset += 1;
                if offset >= slices.len() { break; }
            } else {
                // Trim the current slice to account for partial write
                let cur = &slices[offset];
                let new = IoSlice::new(&cur[(remaining)..]);
                slices[offset] = new;
                remaining = 0;
            }
        }
    }
    Ok(())
}