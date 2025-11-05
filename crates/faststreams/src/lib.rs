// Numan Thabit 2025
// crates/faststreams/src/lib.rs
#![forbid(unsafe_code)]
use bincode::Options;
use serde::{Deserialize, Serialize};
use smallvec::SmallVec;
use std::io::IoSlice;
use std::io::{self, Read, Write};
use std::sync::atomic::{AtomicUsize, Ordering};

const COMPRESS_THRESHOLD: usize = 2048;
const IOV_MAX_DEFAULT: usize = 1024; // typical on Linux/macOS
const INLINE_IOVEC_CAP: usize = IOV_MAX_DEFAULT;
pub const FLAG_LZ4: u8 = 0x01;
pub const FLAG_RKYV: u8 = 0x02;
/// Header checksum present (CRC16 over bytes [0..8) is set in header)
pub const FLAG_HAS_CHECKSUM: u8 = 0x04;
/// Endianness indicator: if set, fields are little-endian (reserved; we currently write BE)
pub const FLAG_ENDIAN_LE: u8 = 0x80;

pub const FRAME_VERSION: u8 = 1;

// New 12-byte header layout:
// [0]  u8  version
// [1]  u8  flags
// [2..4) u16 type (big-endian)
// [4..8) u32 payload_len (big-endian)
// [8..10) u16 header_crc16 over bytes [0..8) (big-endian)
// [10..12) u16 reserved (zero)
const FRAME_HEADER_TEMPLATE: [u8; 12] = [
    FRAME_VERSION, // version
    0,             // flags
    0, 0,          // type
    0, 0, 0, 0,    // len
    0, 0,          // hdr_crc16
    0, 0,          // reserved
];

fn crc16_ccitt(data: &[u8]) -> u16 {
    // CRC-16/CCITT-FALSE (poly 0x1021, init 0xFFFF, refin=false, refout=false, xorout=0x0000)
    let mut crc: u16 = 0xFFFF;
    for &b in data {
        crc ^= (b as u16) << 8;
        for _ in 0..8 {
            if (crc & 0x8000) != 0 {
                crc = (crc << 1) ^ 0x1021;
            } else {
                crc <<= 1;
            }
        }
    }
    crc
}

fn record_type_tag(rec: &Record) -> u16 {
    match rec {
        Record::Account(_) => 1,
        Record::Tx(_) => 2,
        Record::Block(_) => 3,
        Record::Slot { .. } => 4,
        Record::EndOfStartup => 5,
    }
}

fn record_ref_type_tag(rec: &RecordRef<'_>) -> u16 {
    match rec {
        RecordRef::Account(_) => 1,
    }
}

// Exponentially weighted moving average for recent payload lengths
static AVG_LEN: AtomicUsize = AtomicUsize::new(512);

#[cfg_attr(
    feature = "rkyv",
    derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize)
)]
#[cfg_attr(feature = "rkyv", archive_attr(derive(bytecheck::CheckBytes)))]
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

#[cfg_attr(
    feature = "rkyv",
    derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize)
)]
#[cfg_attr(feature = "rkyv", archive_attr(derive(bytecheck::CheckBytes)))]
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TxUpdate {
    pub slot: u64,
    #[serde(with = "serde_bytes")]
    pub signature: [u8; 64],
    pub err: Option<String>,
    pub vote: bool,
}

#[cfg_attr(
    feature = "rkyv",
    derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize)
)]
#[cfg_attr(feature = "rkyv", archive_attr(derive(bytecheck::CheckBytes)))]
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

#[cfg_attr(
    feature = "rkyv",
    derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize)
)]
#[cfg_attr(feature = "rkyv", archive_attr(derive(bytecheck::CheckBytes)))]
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Record {
    Account(AccountUpdate),
    Tx(TxUpdate),
    Block(BlockMeta),
    Slot {
        slot: u64,
        parent: Option<u64>,
        status: u8,
    },
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
    pub payload_hint: Option<usize>,
    pub format: PayloadFormat,
}

#[derive(Clone, Copy, Debug)]
pub enum PayloadFormat {
    Bincode,
    #[cfg(feature = "rkyv")]
    Rkyv,
}

impl EncodeOptions {
    pub fn default_throughput() -> Self {
        Self {
            enable_compression: true,
            compress_threshold: COMPRESS_THRESHOLD,
            payload_hint: Some(AVG_LEN.load(Ordering::Relaxed)),
            format: PayloadFormat::Bincode,
        }
    }
    pub fn latency_uds() -> Self {
        // Disable compression for low-latency local sockets
        Self {
            enable_compression: false,
            compress_threshold: usize::MAX,
            payload_hint: Some(AVG_LEN.load(Ordering::Relaxed)),
            #[cfg(feature = "rkyv")]
            format: PayloadFormat::Rkyv,
            #[cfg(not(feature = "rkyv"))]
            format: PayloadFormat::Bincode,
        }
    }
    /// Throughput-oriented remote hop: enable LZ4 with a low threshold to
    /// compress even relatively small payloads.
    pub fn throughput_lz4_low() -> Self {
        Self {
            enable_compression: true,
            compress_threshold: 512,
            payload_hint: Some(AVG_LEN.load(Ordering::Relaxed)),
            format: PayloadFormat::Bincode,
        }
    }
}

pub fn encode_record_with(rec: &Record, opts: EncodeOptions) -> Result<Vec<u8>, StreamError> {
    let mut buf = Vec::new();
    encode_value_with_type(rec, &mut buf, opts, record_type_tag(rec))?;
    Ok(buf)
}

/// Encode a borrowed record (e.g. `RecordRef::Account`) avoiding intermediate copies.
pub fn encode_record_ref_with(
    rec: &RecordRef<'_>,
    opts: EncodeOptions,
) -> Result<Vec<u8>, StreamError> {
    let mut buf = Vec::new();
    encode_value_with_type(rec, &mut buf, opts, record_ref_type_tag(rec))?;
    Ok(buf)
}

/// Encode a borrowed record directly into the provided buffer, avoiding an intermediate allocation.
pub fn encode_record_ref_into_with(
    rec: &RecordRef<'_>,
    buf: &mut Vec<u8>,
    opts: EncodeOptions,
) -> Result<(), StreamError> {
    encode_value_with_type(rec, buf, opts, record_ref_type_tag(rec))
}

/// Encode into the provided buffer, reusing its capacity when possible.
/// The buffer is cleared before writing and will contain one full frame on success.
pub fn encode_into_with(
    rec: &Record,
    buf: &mut Vec<u8>,
    opts: EncodeOptions,
) -> Result<(), StreamError> {
    encode_value_with_type(rec, buf, opts, record_type_tag(rec))
}

fn encode_value_with_type<T: Serialize>(
    val: &T,
    buf: &mut Vec<u8>,
    opts: EncodeOptions,
    typ: u16,
) -> Result<(), StreamError> {
    let bincode_opts = bincode::DefaultOptions::new()
        .with_fixint_encoding()
        .allow_trailing_bytes();
    buf.clear();
    if opts.enable_compression {
        let payload = bincode_opts.serialize(val)?;
        let mut flags: u8;
        let body: Vec<u8> = if payload.len() >= opts.compress_threshold {
            flags = FLAG_LZ4;
            lz4_flex::block::compress_prepend_size(&payload)
        } else {
            flags = 0;
            payload
        };
        #[cfg(feature = "rkyv")]
        if matches!(opts.format, PayloadFormat::Rkyv) {
            flags |= FLAG_RKYV;
        }
        flags |= FLAG_HAS_CHECKSUM;
        buf.reserve(12 + body.len());
        buf.extend_from_slice(&FRAME_HEADER_TEMPLATE);
        // version already set at [0]
        buf[1] = flags; // flags (includes checksum bit)
        buf[2..4].copy_from_slice(&typ.to_be_bytes());
        buf[4..8].copy_from_slice(&(body.len() as u32).to_be_bytes());
        let crc = crc16_ccitt(&buf[0..8]);
        buf[8..10].copy_from_slice(&crc.to_be_bytes());
        buf.extend_from_slice(&body);
        return Ok(());
    }
    let hint = opts
        .payload_hint
        .unwrap_or_else(|| AVG_LEN.load(Ordering::Relaxed));
    buf.reserve(12 + hint);
    buf.extend_from_slice(&FRAME_HEADER_TEMPLATE);
    // Fill flags and type early; length will be filled post-serialize
    let mut flags: u8 = 0;
    #[cfg(feature = "rkyv")]
    if matches!(opts.format, PayloadFormat::Rkyv) {
        flags |= FLAG_RKYV;
    }
    flags |= FLAG_HAS_CHECKSUM;
    buf[1] = flags;
    buf[2..4].copy_from_slice(&typ.to_be_bytes());
    bincode_opts.serialize_into(&mut *buf, val)?;
    let payload_len = (buf.len() - 12) as u32;
    buf[4..8].copy_from_slice(&payload_len.to_be_bytes());
    let crc = crc16_ccitt(&buf[0..8]);
    buf[8..10].copy_from_slice(&crc.to_be_bytes());
    let len = payload_len as usize;
    let prev = AVG_LEN.load(Ordering::Relaxed);
    let next = ((prev.saturating_mul(7) + len) / 8).max(64);
    AVG_LEN.store(next, Ordering::Relaxed);
    Ok(())
}

pub fn encode_record(rec: &Record) -> Result<Vec<u8>, StreamError> {
    encode_record_with(rec, EncodeOptions::default_throughput())
}

#[cfg(feature = "rkyv")]
pub fn decode_record_archived_from_slice<'a>(
    src: &'a [u8],
) -> Result<(&'a ArchivedRecord, usize), StreamError> {
    if src.len() < 12 {
        return Err(StreamError::De(Box::new(bincode::ErrorKind::SizeLimit)));
    }
    let ver = src[0];
    if ver != FRAME_VERSION {
        return Err(StreamError::BadHeader);
    }
    let hdr_crc = u16::from_be_bytes([src[8], src[9]]);
    let calc = crc16_ccitt(&src[0..8]);
    if hdr_crc != calc {
        return Err(StreamError::BadHeader);
    }
    let flags = src[1];
    let _typ = u16::from_be_bytes([src[2], src[3]]);
    let len = u32::from_be_bytes([src[4], src[5], src[6], src[7]]) as usize;
    let total = 12 + len;
    if src.len() < total {
        return Err(StreamError::De(Box::new(bincode::ErrorKind::SizeLimit)));
    }
    if (flags & FLAG_LZ4) != 0 {
        return Err(StreamError::De(Box::new(bincode::ErrorKind::SizeLimit)));
    }
    let body = &src[12..total];
    let rec = rkyv::check_archived_root::<Record>(body)
        .map_err(|e| StreamError::Io(io::Error::new(io::ErrorKind::InvalidData, e.to_string())))?;
    Ok((rec, total))
}

#[cfg(feature = "rkyv")]
/// Trusted zero-copy rkyv decode: skips bytecheck validation. Use only when both ends are trusted.
pub fn decode_record_archived_trusted_from_slice<'a>(
    src: &'a [u8],
) -> Result<(&'a ArchivedRecord, usize), StreamError> {
    if src.len() < 12 {
        return Err(StreamError::De(Box::new(bincode::ErrorKind::SizeLimit)));
    }
    let ver = src[0];
    if ver != FRAME_VERSION {
        return Err(StreamError::BadHeader);
    }
    let hdr_crc = u16::from_be_bytes([src[8], src[9]]);
    let calc = crc16_ccitt(&src[0..8]);
    if hdr_crc != calc {
        return Err(StreamError::BadHeader);
    }
    let flags = src[1];
    let _typ = u16::from_be_bytes([src[2], src[3]]);
    let len = u32::from_be_bytes([src[4], src[5], src[6], src[7]]) as usize;
    let total = 12 + len;
    if src.len() < total {
        return Err(StreamError::De(Box::new(bincode::ErrorKind::SizeLimit)));
    }
    if (flags & FLAG_LZ4) != 0 {
        return Err(StreamError::De(Box::new(bincode::ErrorKind::SizeLimit)));
    }
    let body = &src[12..total];
    let rec = rkyv::check_archived_root::<Record>(body)
        .map_err(|e| StreamError::Io(io::Error::new(io::ErrorKind::InvalidData, e.to_string())))?;
    Ok((rec, total))
}

pub fn decode_record(mut src: impl Read) -> Result<Record, StreamError> {
    let mut hdr = [0u8; 12];
    src.read_exact(&mut hdr)?;
    let ver = hdr[0];
    if ver != FRAME_VERSION {
        return Err(StreamError::BadHeader);
    }
    let hdr_crc = u16::from_be_bytes([hdr[8], hdr[9]]);
    let calc = crc16_ccitt(&hdr[0..8]);
    if hdr_crc != calc {
        return Err(StreamError::BadHeader);
    }
    let flags = hdr[1];
    let _typ = u16::from_be_bytes([hdr[2], hdr[3]]);
    let len = u32::from_be_bytes([hdr[4], hdr[5], hdr[6], hdr[7]]) as usize;
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
pub fn decode_record_from_slice(
    src: &[u8],
    scratch: &mut Vec<u8>,
) -> Result<(Record, usize), StreamError> {
    if src.len() < 12 {
        return Err(StreamError::De(Box::new(bincode::ErrorKind::SizeLimit)));
    }
    let ver = src[0];
    if ver != FRAME_VERSION {
        return Err(StreamError::BadHeader);
    }
    let hdr_crc = u16::from_be_bytes([src[8], src[9]]);
    let calc = crc16_ccitt(&src[0..8]);
    if hdr_crc != calc {
        return Err(StreamError::BadHeader);
    }
    let flags = src[1];
    let _typ = u16::from_be_bytes([src[2], src[3]]);
    let len = u32::from_be_bytes([src[4], src[5], src[6], src[7]]) as usize;
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
            Err(e) => Err(StreamError::Io(io::Error::new(
                io::ErrorKind::InvalidData,
                e,
            ))),
        }
    } else {
        let rec = bincode_opts.deserialize::<Record>(body)?;
        Ok((rec, total))
    }
}

/// Decode using a caller-provided buffer for the body to avoid per-record allocations.
pub fn decode_record_with_scratch(
    mut src: impl Read,
    body_buf: &mut Vec<u8>,
) -> Result<Record, StreamError> {
    let mut hdr = [0u8; 12];
    src.read_exact(&mut hdr)?;
    let ver = hdr[0];
    if ver != FRAME_VERSION {
        return Err(StreamError::BadHeader);
    }
    let hdr_crc = u16::from_be_bytes([hdr[8], hdr[9]]);
    let calc = crc16_ccitt(&hdr[0..8]);
    if hdr_crc != calc {
        return Err(StreamError::BadHeader);
    }
    let flags = hdr[1];
    let _typ = u16::from_be_bytes([hdr[2], hdr[3]]);
    let len = u32::from_be_bytes([hdr[4], hdr[5], hdr[6], hdr[7]]) as usize;
    body_buf.clear();
    body_buf.resize(len, 0);
    src.read_exact(body_buf)?;
    let bincode_opts = bincode::DefaultOptions::new()
        .with_fixint_encoding()
        .allow_trailing_bytes();
    if (flags & FLAG_LZ4) != 0 {
        match lz4_flex::block::decompress_size_prepended(body_buf) {
            Ok(mut decompressed) => {
                std::mem::swap(body_buf, &mut decompressed);
            }
            Err(e) => {
                return Err(StreamError::Io(io::Error::new(
                    io::ErrorKind::InvalidData,
                    e,
                )));
            }
        }
    }
    Ok(bincode_opts.deserialize::<Record>(&body_buf[..])?)
}

/// Reusable decoder that keeps internal buffers to avoid per-record allocations across batches.
#[derive(Default)]
pub struct Decoder {
    body: Vec<u8>,
    scratch: Vec<u8>,
}

impl Decoder {
    pub fn with_capacities(body_cap: usize, scratch_cap: usize) -> Self {
        Self {
            body: Vec::with_capacity(body_cap),
            scratch: Vec::with_capacity(scratch_cap),
        }
    }

    #[inline]
    pub fn decode_from_slice(&mut self, src: &[u8]) -> Result<(Record, usize), StreamError> {
        decode_record_from_slice(src, &mut self.scratch)
    }

    #[inline]
    pub fn decode_from_reader(&mut self, src: impl Read) -> Result<Record, StreamError> {
        decode_record_with_scratch(src, &mut self.body)
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
        let pre_cap = if remaining_frames < iov_max {
            remaining_frames
        } else {
            iov_max
        };
        let mut iovecs: SmallVec<[IoSlice<'_>; INLINE_IOVEC_CAP]> =
            SmallVec::with_capacity(pre_cap);
        let mut added = 0usize;
        let mut idx = frame_idx;
        while idx < frames.len() && added < iov_max {
            if idx == frame_idx && first_offset != 0 {
                let s = &frames[idx][first_offset..];
                if !s.is_empty() {
                    iovecs.push(IoSlice::new(s));
                }
            } else {
                let s = &frames[idx];
                if !s.is_empty() {
                    iovecs.push(IoSlice::new(s));
                }
            }
            added += 1;
            idx += 1;
        }

        let n = dst.write_vectored(&iovecs)?;
        if n == 0 {
            return Err(io::Error::new(io::ErrorKind::WriteZero, "short write"));
        }

        let mut remaining = n;
        if remaining == 0 {
            return Err(io::Error::new(io::ErrorKind::WriteZero, "short write"));
        }
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
            if frame_idx >= frames.len() {
                break;
            }
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
pub fn write_all_vectored_slices(
    dst: &mut impl Write,
    slices: &mut [IoSlice<'_>],
) -> io::Result<()> {
    let mut offset = 0usize;
    while offset < slices.len() {
        let n = dst.write_vectored(&slices[offset..])?;
        if n == 0 {
            return Err(io::ErrorKind::WriteZero.into());
        }
        let mut rem: &mut [IoSlice<'_>] = &mut slices[offset..];
        IoSlice::advance_slices(&mut rem, n);
        let remaining = rem.len();
        let consumed = (slices.len() - offset) - remaining;
        offset += consumed;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::{self, Write};
    use std::sync::atomic::Ordering;

    fn sample_account(slot: u64) -> Record {
        Record::Account(AccountUpdate {
            slot,
            is_startup: slot == 0,
            pubkey: [1u8; 32],
            lamports: 42,
            owner: [2u8; 32],
            executable: false,
            rent_epoch: 5,
            data: vec![3u8; 16],
        })
    }

    #[test]
    fn encode_decode_roundtrip_default_opts() {
        let record = sample_account(123);
        let encoded = encode_record(&record).expect("encode succeeds");
        let mut cursor = io::Cursor::new(encoded);
        let decoded = decode_record(&mut cursor).expect("decode succeeds");
        match decoded {
            Record::Account(acc) => {
                assert_eq!(acc.slot, 123);
                assert!(!acc.is_startup);
                assert_eq!(acc.pubkey, [1u8; 32]);
                assert_eq!(acc.owner, [2u8; 32]);
                assert_eq!(acc.data, vec![3u8; 16]);
            }
            other => panic!("unexpected record variant: {other:?}"),
        }
    }

    #[test]
    fn encode_sets_lz4_flag_when_threshold_exceeded() {
        // Prepare a payload that will certainly exceed 512 bytes when serialized.
        let record = Record::Block(BlockMeta {
            slot: 99,
            blockhash: Some([9u8; 32]),
            parent_slot: Some(88),
            rewards_len: 1024,
            block_time_unix: Some(123456789),
            leader: Some([7u8; 32]),
        });
        let opts = EncodeOptions {
            enable_compression: true,
            compress_threshold: 1,
            payload_hint: None,
            format: PayloadFormat::Bincode,
        };
        let mut buf = Vec::new();
        encode_into_with(&record, &mut buf, opts).expect("encode succeeds");
        assert_eq!(buf[0], FRAME_VERSION);
        let flags = buf[1];
        assert_eq!(flags & FLAG_LZ4, FLAG_LZ4, "lz4 flag not set");

        let mut cursor = io::Cursor::new(buf);
        let decoded = decode_record(&mut cursor).expect("decode succeeds");
        match decoded {
            Record::Block(meta) => {
                assert_eq!(meta.slot, 99);
                assert_eq!(meta.rewards_len, 1024);
                assert_eq!(meta.leader, Some([7u8; 32]));
            }
            other => panic!("unexpected record variant: {other:?}"),
        }
    }

    #[test]
    fn decode_from_slice_handles_compressed_payloads() {
        let record = sample_account(777);
        let opts = EncodeOptions {
            enable_compression: true,
            compress_threshold: 1,
            payload_hint: None,
            format: PayloadFormat::Bincode,
        };
        let encoded = encode_record_with(&record, opts).expect("encode succeeds");
        let mut scratch = Vec::new();
        let (decoded, consumed) =
            decode_record_from_slice(&encoded, &mut scratch).expect("decode succeeds");
        assert_eq!(consumed, encoded.len());
        match decoded {
            Record::Account(acc) => {
                assert_eq!(acc.slot, 777);
                assert!(!acc.is_startup);
                assert_eq!(acc.data.len(), 16);
            }
            other => panic!("unexpected record variant: {other:?}"),
        }
        assert!(
            scratch.len() >= 16,
            "scratch buffer should retain decompressed payload"
        );
    }

    #[test]
    fn encode_record_ref_into_reuses_buffer_capacity() {
        let mut buf = Vec::with_capacity(16);
        let rec = RecordRef::Account(AccountUpdateRef {
            slot: 1,
            is_startup: false,
            pubkey: [4u8; 32],
            lamports: 55,
            owner: [5u8; 32],
            executable: true,
            rent_epoch: 9,
            data: &[1, 2, 3],
        });
        encode_record_ref_into_with(&rec, &mut buf, EncodeOptions::default_throughput())
            .expect("encode succeeds");
        let initial_capacity = buf.capacity();
        assert!(initial_capacity >= buf.len());
        encode_record_ref_into_with(&rec, &mut buf, EncodeOptions::default_throughput())
            .expect("second encode succeeds");
        assert_eq!(
            buf.capacity(),
            initial_capacity,
            "buffer should reuse capacity"
        );
    }

    struct ChunkedWriter {
        chunk: usize,
        body: Vec<u8>,
    }

    impl ChunkedWriter {
        fn new(chunk: usize) -> Self {
            Self {
                chunk: chunk.max(1),
                body: Vec::new(),
            }
        }
    }

    impl Write for ChunkedWriter {
        fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
            let take = buf.len().min(self.chunk);
            self.body.extend_from_slice(&buf[..take]);
            Ok(take)
        }

        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }

    #[test]
    fn write_all_vectored_handles_partial_writes() {
        let frames = vec![
            encode_record_with(&sample_account(1), EncodeOptions::default_throughput()).unwrap(),
            encode_record_with(&sample_account(2), EncodeOptions::default_throughput()).unwrap(),
        ];
        let mut writer = ChunkedWriter::new(7);
        write_all_vectored(&mut writer, &frames).expect("write succeeds");
        let expected: Vec<u8> = frames.iter().flatten().copied().collect();
        assert_eq!(writer.body, expected);
    }

    #[test]
    fn write_all_vectored_slices_advances_offsets() {
        let frames = vec![
            encode_record_with(&sample_account(10), EncodeOptions::default_throughput()).unwrap(),
            encode_record_with(&sample_account(11), EncodeOptions::default_throughput()).unwrap(),
        ];
        let expected: Vec<u8> = frames.iter().flatten().copied().collect();
        let mut slices: Vec<IoSlice<'_>> = frames.iter().map(|f| IoSlice::new(f)).collect();
        let mut writer = ChunkedWriter::new(5);
        write_all_vectored_slices(&mut writer, &mut slices).expect("write succeeds");
        assert_eq!(writer.body, expected);
    }

    #[test]
    fn avg_len_updates_on_encode() {
        super::AVG_LEN.store(64, Ordering::Relaxed);
        let record = sample_account(42);
        let mut buf = Vec::with_capacity(0);
        encode_into_with(&record, &mut buf, EncodeOptions::default_throughput())
            .expect("encode succeeds");
        let observed = super::AVG_LEN.load(Ordering::Relaxed);
        assert!(observed >= 64, "avg len should grow after encode");
    }

    #[test]
    fn decode_rejects_bad_header_crc() {
        let record = sample_account(5);
        let mut buf = Vec::new();
        encode_into_with(&record, &mut buf, EncodeOptions::default_throughput())
            .expect("encode succeeds");
        // Flip one header byte within [0..8) so CRC mismatches
        if !buf.is_empty() {
            buf[1] ^= 0xFF; // toggle flags byte
        }
        let res = decode_record_from_slice(&buf, &mut Vec::new());
        assert!(matches!(res, Err(StreamError::BadHeader)));
    }
}
