// crates/faststreams/src/lib.rs
#![forbid(unsafe_code)]
use serde::{Deserialize, Serialize};
use bincode::Options;
use std::io::{self, Read, Write};
use std::io::IoSlice;
use smallvec::SmallVec;

const COMPRESS_THRESHOLD: usize = 2048;
const IOV_MAX_DEFAULT: usize = 1024; // typical on Linux/macOS
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
    #[serde(with = "serde_bytes")]
    pub signature: [u8; 64],
    pub err: Option<String>,
    pub vote: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BlockMeta {
    pub slot: u64,
    #[serde(with = "serde_bytes")]
    pub blockhash: Option<[u8; 32]>,
    pub parent_slot: Option<u64>,
    pub rewards_len: u32,
    pub block_time_unix: Option<i64>,
    #[serde(with = "serde_bytes")]
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

// Borrowing variants for zero-copy encoding on producers
#[derive(Debug, Serialize)]
pub struct AccountUpdateRef<'a> {
    pub slot: u64,
    pub is_startup: bool,
    pub pubkey: [u8; 32],
    pub lamports: u64,
    pub owner: [u8; 32],
    pub executable: bool,
    pub rent_epoch: u64,
    #[serde(with = "serde_bytes")]
    pub data: &'a [u8],
}

#[derive(Debug, Serialize)]
pub enum RecordRef<'a> {
    Account(AccountUpdateRef<'a>),
}

#[derive(thiserror::Error, Debug)]
pub enum StreamError {
    #[error("io: {0}")]
    Io(#[from] io::Error),
    #[error("deserialize: {0}")]
    De(Box<bincode::ErrorKind>),
    #[error("serialize: {0}")]
    Ser(#[from] bincode::Error),
    #[error("bad magic or version")]
    BadHeader,
}

#[derive(Clone, Copy, Debug)]
pub struct EncodeOptions {
    pub enable_compression: bool,
    pub compress_threshold: usize,
}

impl EncodeOptions {
    pub fn default_throughput() -> Self {
        Self { enable_compression: true, compress_threshold: COMPRESS_THRESHOLD }
    }
    pub fn latency_uds() -> Self {
        // Disable compression for low-latency local sockets
        Self { enable_compression: false, compress_threshold: usize::MAX }
    }
    /// Throughput-oriented remote hop: enable LZ4 with a low threshold to
    /// compress even relatively small payloads.
    pub fn throughput_lz4_low() -> Self {
        Self { enable_compression: true, compress_threshold: 512 }
    }
}

pub fn encode_record_with(rec: &Record, opts: EncodeOptions) -> Result<Vec<u8>, StreamError> {
    let bincode_opts = bincode::DefaultOptions::new()
        .with_fixint_encoding()
        .allow_trailing_bytes();
    if opts.enable_compression {
        // Compress only when enabled and above threshold. This path incurs a payload allocation,
        // which is acceptable for throughput-oriented configurations.
        let payload = bincode_opts.serialize(rec)?;
        let (flags, body): (u16, Vec<u8>) = if payload.len() >= opts.compress_threshold {
            let compressed = lz4_flex::block::compress_prepend_size(&payload);
            (FLAG_LZ4, compressed)
        } else {
            (0, payload)
        };

        // header: magic(4) | version(2) | flags(2) | len(4)
        let mut buf: Vec<u8> = Vec::with_capacity(12 + body.len());
        buf.extend_from_slice(&FRAME_MAGIC.to_be_bytes());
        buf.extend_from_slice(&FRAME_VERSION.to_be_bytes());
        buf.extend_from_slice(&flags.to_be_bytes());
        buf.extend_from_slice(&(body.len() as u32).to_be_bytes());
        buf.extend_from_slice(&body);
        Ok(buf)
    } else {
        // Low-latency path: avoid an intermediate payload allocation by sizing once and
        // serializing directly into the final buffer after writing the header.
        let payload_len = bincode_opts.serialized_size(rec)? as usize;
        let mut buf: Vec<u8> = Vec::with_capacity(12 + payload_len);
        buf.extend_from_slice(&FRAME_MAGIC.to_be_bytes());
        buf.extend_from_slice(&FRAME_VERSION.to_be_bytes());
        buf.extend_from_slice(&0u16.to_be_bytes());
        buf.extend_from_slice(&(payload_len as u32).to_be_bytes());
        bincode_opts.serialize_into(&mut buf, rec)?;
        Ok(buf)
    }
}

/// Encode a borrowed record (e.g. `RecordRef::Account`) avoiding intermediate copies.
pub fn encode_record_ref_with(rec: &RecordRef<'_>, opts: EncodeOptions) -> Result<Vec<u8>, StreamError> {
    let bincode_opts = bincode::DefaultOptions::new()
        .with_fixint_encoding()
        .allow_trailing_bytes();
    if opts.enable_compression {
        let payload = bincode_opts.serialize(rec)?;
        let (flags, body): (u16, Vec<u8>) = if payload.len() >= opts.compress_threshold {
            let compressed = lz4_flex::block::compress_prepend_size(&payload);
            (FLAG_LZ4, compressed)
        } else {
            (0, payload)
        };

        let mut buf: Vec<u8> = Vec::with_capacity(12 + body.len());
        buf.extend_from_slice(&FRAME_MAGIC.to_be_bytes());
        buf.extend_from_slice(&FRAME_VERSION.to_be_bytes());
        buf.extend_from_slice(&flags.to_be_bytes());
        buf.extend_from_slice(&(body.len() as u32).to_be_bytes());
        buf.extend_from_slice(&body);
        Ok(buf)
    } else {
        let payload_len = bincode_opts.serialized_size(rec)? as usize;
        let mut buf: Vec<u8> = Vec::with_capacity(12 + payload_len);
        buf.extend_from_slice(&FRAME_MAGIC.to_be_bytes());
        buf.extend_from_slice(&FRAME_VERSION.to_be_bytes());
        buf.extend_from_slice(&0u16.to_be_bytes());
        buf.extend_from_slice(&(payload_len as u32).to_be_bytes());
        bincode_opts.serialize_into(&mut buf, rec)?;
        Ok(buf)
    }
}

/// Encode a borrowed record directly into the provided buffer, avoiding an intermediate allocation.
pub fn encode_record_ref_into_with(rec: &RecordRef<'_>, buf: &mut Vec<u8>, opts: EncodeOptions) -> Result<(), StreamError> {
    let bincode_opts = bincode::DefaultOptions::new()
        .with_fixint_encoding()
        .allow_trailing_bytes();
    buf.clear();
    if opts.enable_compression {
        let payload = bincode_opts.serialize(rec)?;
        let (flags, body): (u16, Vec<u8>) = if payload.len() >= opts.compress_threshold {
            let compressed = lz4_flex::block::compress_prepend_size(&payload);
            (FLAG_LZ4, compressed)
        } else { (0, payload) };
        buf.reserve(12 + body.len());
        buf.extend_from_slice(&FRAME_MAGIC.to_be_bytes());
        buf.extend_from_slice(&FRAME_VERSION.to_be_bytes());
        buf.extend_from_slice(&flags.to_be_bytes());
        buf.extend_from_slice(&(body.len() as u32).to_be_bytes());
        buf.extend_from_slice(&body);
        Ok(())
    } else {
        let payload_len = bincode_opts.serialized_size(rec)? as usize;
        buf.reserve(12 + payload_len);
        buf.extend_from_slice(&FRAME_MAGIC.to_be_bytes());
        buf.extend_from_slice(&FRAME_VERSION.to_be_bytes());
        buf.extend_from_slice(&0u16.to_be_bytes());
        buf.extend_from_slice(&(payload_len as u32).to_be_bytes());
        bincode_opts.serialize_into(buf, rec)?;
        Ok(())
    }
}

/// Encode into the provided buffer, reusing its capacity when possible.
/// The buffer is cleared before writing and will contain one full frame on success.
pub fn encode_into_with(rec: &Record, buf: &mut Vec<u8>, opts: EncodeOptions) -> Result<(), StreamError> {
    let bincode_opts = bincode::DefaultOptions::new()
        .with_fixint_encoding()
        .allow_trailing_bytes();
    buf.clear();
    if opts.enable_compression {
        let payload = bincode_opts.serialize(rec)?;
        let (flags, body): (u16, Vec<u8>) = if payload.len() >= opts.compress_threshold {
            let compressed = lz4_flex::block::compress_prepend_size(&payload);
            (FLAG_LZ4, compressed)
        } else {
            (0, payload)
        };
        buf.reserve(12 + body.len());
        buf.extend_from_slice(&FRAME_MAGIC.to_be_bytes());
        buf.extend_from_slice(&FRAME_VERSION.to_be_bytes());
        buf.extend_from_slice(&flags.to_be_bytes());
        buf.extend_from_slice(&(body.len() as u32).to_be_bytes());
        buf.extend_from_slice(&body);
        Ok(())
    } else {
        let payload_len = bincode_opts.serialized_size(rec)? as usize;
        buf.reserve(12 + payload_len);
        buf.extend_from_slice(&FRAME_MAGIC.to_be_bytes());
        buf.extend_from_slice(&FRAME_VERSION.to_be_bytes());
        buf.extend_from_slice(&0u16.to_be_bytes());
        buf.extend_from_slice(&(payload_len as u32).to_be_bytes());
        bincode_opts.serialize_into(buf, rec)?;
        Ok(())
    }
}

pub fn encode_record(rec: &Record) -> Result<Vec<u8>, StreamError> {
    encode_record_with(rec, EncodeOptions::default_throughput())
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
    let bincode_opts = bincode::DefaultOptions::new()
        .with_fixint_encoding()
        .allow_trailing_bytes();
    let payload = if (flags & FLAG_LZ4) != 0 {
        lz4_flex::block::decompress_size_prepended(&body)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?
    } else {
        body
    };
    Ok(bincode_opts.deserialize::<Record>(&payload)?)
}

/// Decode without copying the body when uncompressed; returns (record, bytes_consumed).
pub fn decode_record_from_slice(src: &[u8], scratch: &mut Vec<u8>) -> Result<(Record, usize), StreamError> {
    if src.len() < 12 {
        return Err(StreamError::De(Box::new(bincode::ErrorKind::SizeLimit)));
    }
    let magic = u32::from_be_bytes([src[0], src[1], src[2], src[3]]);
    let ver = u16::from_be_bytes([src[4], src[5]]);
    if magic != FRAME_MAGIC || ver != FRAME_VERSION {
        return Err(StreamError::BadHeader);
    }
    let flags = u16::from_be_bytes([src[6], src[7]]);
    let len = u32::from_be_bytes([src[8], src[9], src[10], src[11]]) as usize;
    let total = 12 + len;
    if src.len() < total {
        return Err(StreamError::De(Box::new(bincode::ErrorKind::SizeLimit)));
    }
    let body = &src[12..total];
    let bincode_opts = bincode::DefaultOptions::new()
        .with_fixint_encoding()
        .allow_trailing_bytes();
    if (flags & FLAG_LZ4) != 0 {
        match lz4_flex::block::decompress_size_prepended(body) {
            Ok(mut decompressed) => {
                // Move decompressed buffer into scratch to avoid a copy
                std::mem::swap(scratch, &mut decompressed);
                let rec = bincode_opts.deserialize::<Record>(&scratch[..])?;
                Ok((rec, total))
            }
            Err(e) => Err(StreamError::Io(io::Error::new(io::ErrorKind::InvalidData, e)))
        }
    } else {
        let rec = bincode_opts.deserialize::<Record>(body)?;
        Ok((rec, total))
    }
}

pub fn write_all_vectored(mut dst: impl Write, frames: &[Vec<u8>]) -> io::Result<()> {
    // NOTE: We currently map each frame to one IoSlice. For tiny frames, we could coalesce
    // adjacent frames into larger IoSlices to reduce syscall overhead.
    let mut frame_idx: usize = 0;
    let mut first_offset: usize = 0; // byte offset inside frames[frame_idx]
    let iov_max = IOV_MAX_DEFAULT;

    while frame_idx < frames.len() {
        let remaining_frames = frames.len() - frame_idx;
        let pre_cap = if remaining_frames < iov_max { remaining_frames } else { iov_max };
        let mut iovecs: SmallVec<[IoSlice<'_>; 256]> = SmallVec::with_capacity(pre_cap);
        let mut added = 0usize;
        let mut idx = frame_idx;
        while idx < frames.len() && added < iov_max {
            if idx == frame_idx && first_offset != 0 {
                let s = &frames[idx][first_offset..];
                if !s.is_empty() { iovecs.push(IoSlice::new(s)); }
            } else {
                let s = &frames[idx];
                if !s.is_empty() { iovecs.push(IoSlice::new(s)); }
            }
            added += 1;
            idx += 1;
        }

        let n = dst.write_vectored(&iovecs)?;
        if n == 0 { return Err(io::Error::new(io::ErrorKind::WriteZero, "short write")); }

        let mut remaining = n;
        if remaining == 0 { return Err(io::Error::new(io::ErrorKind::WriteZero, "short write")); }
        if first_offset != 0 {
            let slice_len = frames[frame_idx].len() - first_offset;
            if remaining >= slice_len {
                remaining -= slice_len;
                frame_idx += 1;
                first_offset = 0;
            } else {
                first_offset += remaining;
                continue;
            }
        }
        while remaining > 0 {
            if frame_idx >= frames.len() { break; }
            let slice_len = frames[frame_idx].len();
            if remaining >= slice_len {
                remaining -= slice_len;
                frame_idx += 1;
            } else {
                first_offset = remaining;
                remaining = 0;
            }
        }
    }
    Ok(())
}

/// Write a batch of byte slices using vectored IO and handle partial writes.
pub fn write_all_vectored_slices(mut dst: impl Write, frames: &[&[u8]]) -> io::Result<()> {
    let mut frame_idx: usize = 0;
    let mut first_offset: usize = 0; // byte offset inside frames[frame_idx]
    let iov_max = IOV_MAX_DEFAULT;

    while frame_idx < frames.len() {
        let remaining_frames = frames.len() - frame_idx;
        let pre_cap = if remaining_frames < iov_max { remaining_frames } else { iov_max };
        let mut iovecs: SmallVec<[IoSlice<'_>; 256]> = SmallVec::with_capacity(pre_cap);
        let mut added = 0usize;
        let mut idx = frame_idx;
        while idx < frames.len() && added < iov_max {
            if idx == frame_idx && first_offset != 0 {
                let s = &frames[idx][first_offset..];
                if !s.is_empty() { iovecs.push(IoSlice::new(s)); }
            } else {
                let s = frames[idx];
                if !s.is_empty() { iovecs.push(IoSlice::new(s)); }
            }
            added += 1;
            idx += 1;
        }

        let n = dst.write_vectored(&iovecs)?;
        if n == 0 { return Err(io::Error::new(io::ErrorKind::WriteZero, "short write")); }

        let mut remaining = n;
        if remaining == 0 { return Err(io::Error::new(io::ErrorKind::WriteZero, "short write")); }
        if first_offset != 0 {
            let slice_len = frames[frame_idx].len() - first_offset;
            if remaining >= slice_len {
                remaining -= slice_len;
                frame_idx += 1;
                first_offset = 0;
            } else {
                first_offset += remaining;
                continue;
            }
        }
        while remaining > 0 {
            if frame_idx >= frames.len() { break; }
            let slice_len = frames[frame_idx].len();
            if remaining >= slice_len {
                remaining -= slice_len;
                frame_idx += 1;
            } else {
                first_offset = remaining;
                remaining = 0;
            }
        }
    }
    Ok(())
}

// (intentionally left empty)