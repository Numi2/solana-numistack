// Numan Thabit 1337
use crossbeam_utils::CachePadded;
use std::cell::UnsafeCell;
use std::mem::MaybeUninit;
use std::sync::atomic::{fence, AtomicUsize, Ordering};
use std::sync::Arc;

/// Lock-free single-producer single-consumer ring buffer.
pub struct SpscRing<T> {
    inner: Arc<Inner<T>>,
}

pub struct Producer<T> {
    inner: Arc<Inner<T>>,
}

pub struct Consumer<T> {
    inner: Arc<Inner<T>>,
}

struct Inner<T> {
    buffer: Box<[UnsafeCell<MaybeUninit<T>>]>,
    capacity: usize,
    mask_val: usize,
    head: CachePadded<AtomicUsize>,
    tail: CachePadded<AtomicUsize>,
}

unsafe impl<T: Send> Send for Producer<T> {}
unsafe impl<T: Send> Send for Consumer<T> {}
unsafe impl<T: Send> Send for SpscRing<T> {}
unsafe impl<T: Send> Sync for Producer<T> {}
unsafe impl<T: Send> Sync for Consumer<T> {}
unsafe impl<T: Send> Sync for SpscRing<T> {}

impl<T> Clone for Producer<T> {
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
        }
    }
}

impl<T> SpscRing<T> {
    /// Create a ring buffer with the requested capacity. Capacity must be > 0.
    pub fn with_capacity(capacity: usize) -> Self {
        assert!(capacity > 0, "capacity must be positive");
        let cap_pow2 = capacity.next_power_of_two();
        let mut buffer = Vec::with_capacity(cap_pow2);
        for _ in 0..cap_pow2 {
            buffer.push(UnsafeCell::new(MaybeUninit::uninit()));
        }
        let inner = Arc::new(Inner {
            buffer: buffer.into_boxed_slice(),
            capacity: cap_pow2,
            mask_val: cap_pow2 - 1,
            head: CachePadded::new(AtomicUsize::new(0)),
            tail: CachePadded::new(AtomicUsize::new(0)),
        });
        Self { inner }
    }

    /// Split the queue into producer and consumer handles.
    pub fn split(self) -> (Producer<T>, Consumer<T>) {
        let producer = Producer {
            inner: Arc::clone(&self.inner),
        };
        let consumer = Consumer { inner: self.inner };
        (producer, consumer)
    }
}

impl<T> Inner<T> {
    #[inline]
    fn mask(&self, idx: usize) -> usize {
        idx & self.mask_val
    }

    #[inline]
    fn len(&self) -> usize {
        let head = self.head.load(Ordering::Acquire);
        let tail = self.tail.load(Ordering::Acquire);
        head.wrapping_sub(tail)
    }

    #[inline]
    fn drop_oldest(&self) -> bool {
        loop {
            let tail = self.tail.load(Ordering::Acquire);
            let head = self.head.load(Ordering::Acquire);
            if head == tail {
                return false;
            }
            let idx = self.mask(tail);
            match self.tail.compare_exchange(
                tail,
                tail.wrapping_add(1),
                Ordering::Release,
                Ordering::Acquire,
            ) {
                Ok(_) => {
                    fence(Ordering::Acquire);
                    unsafe {
                        (*self.buffer[idx].get()).assume_init_drop();
                    }
                    return true;
                }
                Err(_) => continue,
            }
        }
    }
}

impl<T> Producer<T> {
    /// Attempt to push a value without blocking. Returns `Err(value)` when full.
    #[inline]
    pub fn try_push(&self, value: T) -> Result<(), T> {
        let inner = &self.inner;
        let capacity = inner.capacity;
        let head = inner.head.load(Ordering::Relaxed);
        let tail = inner.tail.load(Ordering::Acquire);
        if head.wrapping_sub(tail) == capacity {
            return Err(value);
        }

        let idx = inner.mask(head);
        unsafe {
            (*inner.buffer[idx].get()).write(value);
        }
        inner.head.store(head.wrapping_add(1), Ordering::Release);
        Ok(())
    }

    /// Attempt to push, dropping the oldest item when the buffer is full.
    #[inline]
    pub fn push_drop_oldest(&self, value: T) -> Result<(), T> {
        let inner = &self.inner;
        loop {
            let head = inner.head.load(Ordering::Relaxed);
            let tail = inner.tail.load(Ordering::Acquire);
            if head.wrapping_sub(tail) == inner.capacity {
                if !inner.drop_oldest() {
                    return Err(value);
                }
                continue;
            }
            let idx = inner.mask(head);
            unsafe {
                (*inner.buffer[idx].get()).write(value);
            }
            inner.head.store(head.wrapping_add(1), Ordering::Release);
            return Ok(());
        }
    }

    /// Capacity of the ring.
    #[inline]
    #[allow(dead_code)]
    pub fn capacity(&self) -> usize {
        self.inner.capacity
    }

    /// Current number of items buffered.
    #[inline]
    pub fn len(&self) -> usize {
        self.inner.len()
    }
}

impl<T> Consumer<T> {
    /// Pop the next value if available.
    #[inline]
    pub fn pop(&self) -> Option<T> {
        let inner = &self.inner;
        loop {
            let tail = inner.tail.load(Ordering::Acquire);
            let head = inner.head.load(Ordering::Acquire);
            if head == tail {
                return None;
            }
            let idx = inner.mask(tail);
            match inner.tail.compare_exchange(
                tail,
                tail.wrapping_add(1),
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => {
                    let value = unsafe { (*inner.buffer[idx].get()).assume_init_read() };
                    return Some(value);
                }
                Err(_) => continue,
            }
        }
    }

    /// Current number of items buffered.
    #[inline]
    pub fn len(&self) -> usize {
        self.inner.len()
    }

    /// Capacity of the ring.
    #[inline]
    #[allow(dead_code)]
    pub fn capacity(&self) -> usize {
        self.inner.capacity
    }
}

impl<T> Drop for Inner<T> {
    fn drop(&mut self) {
        let mut tail = self.tail.load(Ordering::Relaxed);
        let head = self.head.load(Ordering::Relaxed);
        while tail != head {
            let idx = self.mask(tail);
            unsafe {
                (*self.buffer[idx].get()).assume_init_drop();
            }
            tail = tail.wrapping_add(1);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::SpscRing;

    #[test]
    fn drops_on_full() {
        let ring = SpscRing::with_capacity(2);
        let (producer, consumer) = ring.split();
        producer.try_push(1usize).unwrap();
        producer.try_push(2usize).unwrap();
        assert!(producer.try_push(3usize).is_err());
        assert_eq!(consumer.pop(), Some(1));
        assert_eq!(consumer.pop(), Some(2));
        assert!(consumer.pop().is_none());
    }

    #[test]
    fn push_drop_oldest_replaces_head() {
        let ring = SpscRing::with_capacity(2);
        let (producer, consumer) = ring.split();
        producer.try_push(1u32).unwrap();
        producer.try_push(2u32).unwrap();
        producer.push_drop_oldest(3u32).unwrap();
        assert_eq!(consumer.pop(), Some(2));
        assert_eq!(consumer.pop(), Some(3));
    }
}
