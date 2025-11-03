use std::sync::Arc;

use crossbeam_queue::ArrayQueue;

/// Lock-free pool of reusable `Vec<u8>` buffers.
#[derive(Debug)]
pub struct BufferPool {
    q: ArrayQueue<Vec<u8>>,
    default_capacity: usize,
}

impl BufferPool {
    pub fn new(max_items: usize, default_capacity: usize) -> Arc<Self> {
        Arc::new(Self { q: ArrayQueue::new(max_items), default_capacity })
    }

    /// Get a pooled buffer wrapped in `PooledBuf`. The buffer is empty and ready to write.
    pub fn get(self: &Arc<Self>) -> PooledBuf {
        let buf = self.q.pop().unwrap_or_else(|| Vec::with_capacity(self.default_capacity));
        PooledBuf { inner: Some(buf), pool: Arc::clone(self) }
    }

    fn put(&self, mut buf: Vec<u8>) {
        // Keep capacity; just clear contents for reuse.
        buf.clear();
        let _ = self.q.push(buf);
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
    pub fn inner_mut(&mut self) -> &mut Vec<u8> {
        self.inner.as_mut().expect("pooled buffer already taken")
    }

    #[inline]
    pub fn as_slice(&self) -> &[u8] {
        self.inner.as_ref().expect("pooled buffer already taken").as_slice()
    }
}

impl AsRef<[u8]> for PooledBuf {
    fn as_ref(&self) -> &[u8] { self.as_slice() }
}

impl Drop for PooledBuf {
    fn drop(&mut self) {
        if let Some(buf) = self.inner.take() {
            self.pool.put(buf);
        }
    }
}


