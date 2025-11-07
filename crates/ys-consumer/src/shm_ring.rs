// Numan Thabit 2025
// crates/ys-consumer/src/shm_ring.rs
#![deny(unsafe_code)]
use memmap2::{MmapMut, MmapOptions};
use metrics::counter;
use std::fs::OpenOptions;
use std::io;
use std::path::{Path, PathBuf};

const HDR_LEN: usize = 64;
const MAGIC: u32 = 0x59534D52; // 'YSMR'
const VERSION: u32 = 1;

// Header layout (little-endian):
// 0..4   magic 'YSMR'
// 4..8   version = 1
// 8..16  capacity_bytes (u64)
// 16..24 head (u64) - writer offset into body (0..capacity)
// 24..32 tail (u64) - reader offset into body (0..capacity)
// 32..64 reserved

fn read_u32_le(buf: &[u8], off: usize) -> u32 {
    u32::from_le_bytes([buf[off], buf[off + 1], buf[off + 2], buf[off + 3]])
}

fn write_u32_le(buf: &mut [u8], off: usize, v: u32) {
    buf[off..off + 4].copy_from_slice(&v.to_le_bytes());
}

fn read_u64_le(buf: &[u8], off: usize) -> u64 {
    u64::from_le_bytes([
        buf[off],
        buf[off + 1],
        buf[off + 2],
        buf[off + 3],
        buf[off + 4],
        buf[off + 5],
        buf[off + 6],
        buf[off + 7],
    ])
}

fn write_u64_le(buf: &mut [u8], off: usize, v: u64) {
    buf[off..off + 8].copy_from_slice(&v.to_le_bytes());
}

#[inline]
#[allow(unsafe_code)]
fn map_writable_with_len(file: &std::fs::File, total: usize) -> io::Result<MmapMut> {
    // Ensure file is large enough for requested mapping length
    let curr_len = file.metadata()?.len();
    if curr_len < total as u64 {
        file.set_len(total as u64)?;
    }
    if total == 0 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "mapping length must be > 0",
        ));
    }
    // SAFETY: offset is 0 and length <= file length (ensured above). The FD is opened read+write.
    let mmap = unsafe { MmapOptions::new().len(total).map_mut(file)? };
    Ok(mmap)
}

pub struct ShmRingWriter {
    _path: PathBuf,
    mmap: MmapMut,
    cap: usize,
}

impl ShmRingWriter {
    pub fn open_or_create(path: impl AsRef<Path>, capacity_bytes: usize) -> io::Result<Self> {
        let path = path.as_ref().to_path_buf();
        let file = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .truncate(false)
            .open(&path)?;
        let total = HDR_LEN + capacity_bytes;
        let mut mmap = map_writable_with_len(&file, total)?;
        // Initialize header if empty or mismatched
        let magic = read_u32_le(&mmap, 0);
        let version = read_u32_le(&mmap, 4);
        let cap_le = read_u64_le(&mmap, 8) as usize;
        if magic != MAGIC || version != VERSION || cap_le != capacity_bytes {
            write_u32_le(&mut mmap, 0, MAGIC);
            write_u32_le(&mut mmap, 4, VERSION);
            write_u64_le(&mut mmap, 8, capacity_bytes as u64);
            write_u64_le(&mut mmap, 16, 0);
            write_u64_le(&mut mmap, 24, 0);
            mmap.flush()?;
        }
        Ok(Self {
            _path: path,
            mmap,
            cap: capacity_bytes,
        })
    }

    #[inline]
    fn body_off(&self) -> usize {
        HDR_LEN
    }

    fn head(&self) -> usize {
        read_u64_le(&self.mmap, 16) as usize
    }

    fn set_head(&mut self, head: usize) {
        write_u64_le(&mut self.mmap, 16, head as u64);
    }

    fn tail(&self) -> usize {
        // Reader-owned; writer only reads
        read_u64_le(&self.mmap, 24) as usize
    }

    #[inline]
    fn used_bytes(&self, head: usize, tail: usize) -> usize {
        if head >= tail {
            head - tail
        } else {
            self.cap - (tail - head)
        }
    }

    #[inline]
    fn free_bytes(&self, head: usize, tail: usize) -> usize {
        // Leave 1 byte sentinel to distinguish full vs empty
        self.cap.saturating_sub(self.used_bytes(head, tail) + 1)
    }

    /// Try to push a frame into the ring. Returns true on success, false if insufficient space.
    pub fn try_push(&mut self, frame: &[u8]) -> bool {
        let need = 4usize + frame.len();
        if need > self.cap {
            counter!("ys_consumer_shm_drop_oversized_total").increment(1);
            return false;
        }
        let mut head = self.head();
        let tail = self.tail();
        if self.free_bytes(head, tail) < need {
            counter!("ys_consumer_shm_dropped_total", "reason" => "no_space").increment(1);
            return false;
        }
        // Ensure contiguous space at end; if not, write wrap marker (len=0) and wrap to 0
        let cont = self.cap - head;
        if cont < need {
            if cont >= 4 {
                let off = self.body_off() + head;
                write_u32_le(&mut self.mmap, off, 0);
            }
            head = 0;
        }
        // Write len and payload
        let off = self.body_off() + head;
        write_u32_le(&mut self.mmap, off, frame.len() as u32);
        let dst = &mut self.mmap[off + 4..off + 4 + frame.len()];
        dst.copy_from_slice(frame);
        head += need;
        self.set_head(head);
        counter!("ys_consumer_shm_written_total").increment(1);
        true
    }
}
