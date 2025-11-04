// Numan Thabit 2025
//! Adaptive batching utilities for coalescing high-frequency RPC calls.

use std::sync::Arc;
use std::time::Duration;

use crossbeam_queue::ArrayQueue;
use tokio::sync::Notify;
use tokio::time::{self, Instant};

/// Adaptive micro-batcher that coalesces items up to a configured limit or timeout.
pub struct AdaptiveBatcher<T> {
    queue: Arc<ArrayQueue<T>>,
    notify: Arc<Notify>,
    max_batch_size: usize,
    max_delay: Duration,
}

impl<T> Clone for AdaptiveBatcher<T> {
    fn clone(&self) -> Self {
        Self {
            queue: self.queue.clone(),
            notify: self.notify.clone(),
            max_batch_size: self.max_batch_size,
            max_delay: self.max_delay,
        }
    }
}

impl<T> AdaptiveBatcher<T> {
    /// Create a new batcher with the provided queue depth, batch size and deadline.
    pub fn new(queue_depth: usize, max_batch_size: usize, max_delay: Duration) -> Self {
        Self {
            queue: Arc::new(ArrayQueue::new(queue_depth)),
            notify: Arc::new(Notify::new()),
            max_batch_size,
            max_delay,
        }
    }

    /// Attempt to enqueue a new item. Returns the item back if the queue is full.
    pub fn enqueue(&self, item: T) -> Result<(), T> {
        match self.queue.push(item) {
            Ok(_) => {
                self.notify.notify_one();
                Ok(())
            }
            Err(item) => Err(item),
        }
    }

    /// Wait for the next batch of items according to batch size and delay hints.
    pub async fn next_batch(&self) -> Vec<T> {
        let mut deadline = Instant::now() + self.max_delay;
        let mut batch = Vec::with_capacity(self.max_batch_size);

        loop {
            while batch.len() < self.max_batch_size {
                match self.queue.pop() {
                    Some(item) => batch.push(item),
                    None => break,
                }
            }

            if !batch.is_empty() {
                return batch;
            }

            let notified = self.notify.notified();
            tokio::select! {
                _ = notified => {
                    deadline = Instant::now() + self.max_delay;
                    continue;
                }
                _ = time::sleep_until(deadline) => {
                    if let Some(item) = self.queue.pop() {
                        batch.push(item);
                        while batch.len() < self.max_batch_size {
                            if let Some(item) = self.queue.pop() {
                                batch.push(item);
                            } else {
                                break;
                            }
                        }
                        return batch;
                    }
                    deadline = Instant::now() + self.max_delay;
                }
            }
        }
    }

    /// Current length of the queue for metrics purposes.
    pub fn len(&self) -> usize {
        self.queue.len()
    }

    /// Returns true when the queue holds no pending items.
    pub fn is_empty(&self) -> bool {
        self.queue.is_empty()
    }

    /// Remaining capacity in the queue.
    pub fn remaining_capacity(&self) -> usize {
        self.queue.capacity() - self.queue.len()
    }
}
