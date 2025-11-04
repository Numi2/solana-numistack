// Numan Thabit 2025
use std::sync::Arc;

use crossbeam_queue::ArrayQueue;
use metrics::{counter, gauge};

/// Lock-free pool of reusable `Vec<u8>` buffers.
#[derive(Debug)]
pub struct BufferPool {
    q: ArrayQueue<Vec<u8>>,
    default_capacity: usize,
}

impl BufferPool {
    pub fn new(max_items: usize, default_capacity: usize) -> Arc<Self> {
        Arc::new(Self {
            q: ArrayQueue::new(max_items),
            default_capacity,
        })
    }

    /// Get a pooled buffer if available. Returns `None` when pool is empty to keep memory bounded.
    pub fn try_get(self: &Arc<Self>) -> Option<PooledBuf> {
        let buf = match self.q.pop() {
            Some(b) => Some(b),
            None => {
                counter!("ultra_pool_get_miss_total").increment(1);
                None
            }
        };
        gauge!("ultra_pool_len").set(self.q.len() as f64);
        buf.map(|b| PooledBuf {
            inner: Some(b),
            pool: Arc::clone(self),
        })
    }

    fn put(&self, mut buf: Vec<u8>) {
        // Replace excessively large buffers to prevent bloat under pressure.
        if buf.capacity() > (self.default_capacity.saturating_mul(2)) {
            buf = Vec::with_capacity(self.default_capacity);
        }
        buf.clear();
        if self.q.push(buf).is_err() {
            counter!("ultra_pool_full_total").increment(1);
        }
        gauge!("ultra_pool_len").set(self.q.len() as f64);
    }
}

/// An owned buffer that returns to its originating pool on drop.
#[derive(Debug)]
pub struct PooledBuf {
    inner: Option<Vec<u8>>, // set to None when taken
    pool: Arc<BufferPool>,
}

impl PooledBuf {
    #[inline]
    pub fn inner_mut(&mut self) -> Option<&mut Vec<u8>> {
        if cfg!(debug_assertions) {
            debug_assert!(self.inner.is_some(), "pooled buffer already taken");
        }
        self.inner.as_mut()
    }

    #[inline]
    pub fn as_slice(&self) -> Option<&[u8]> {
        if cfg!(debug_assertions) {
            debug_assert!(self.inner.is_some(), "pooled buffer already taken");
        }
        self.inner.as_deref()
    }
}

impl AsRef<[u8]> for PooledBuf {
    #[inline]
    fn as_ref(&self) -> &[u8] {
        match &self.inner {
            Some(v) => v.as_slice(),
            None => &[],
        }
    }
}

impl Drop for PooledBuf {
    fn drop(&mut self) {
        if let Some(buf) = self.inner.take() {
            self.pool.put(buf);
        }
    }
}
