use crossbeam_channel::{bounded, Receiver, Sender, TrySendError};
use std::time::Duration;

pub const DEFAULT_CAPACITY: usize = 1024;

/// A typed, bounded pipe between two operators.
pub struct Pipe<T> {
    tx: Sender<T>,
    rx: Receiver<T>,
}

impl<T: Send + 'static> Pipe<T> {
    pub fn new(capacity: usize) -> Self {
        let (tx, rx) = bounded(capacity);
        Self { tx, rx }
    }

    pub fn split(self) -> (PipeSender<T>, PipeReceiver<T>) {
        (PipeSender { tx: self.tx }, PipeReceiver { rx: self.rx })
    }
}

#[derive(Clone)]
pub struct PipeSender<T> {
    tx: Sender<T>,
}

pub struct PipeReceiver<T> {
    rx: Receiver<T>,
}

/// Backpressure strategy when the downstream buffer is full.
#[derive(Clone, Copy, Debug)]
pub enum BackpressurePolicy {
    /// Block until space is available (up to `timeout`).
    Block { timeout: Duration },
    /// Drop the record immediately and count it as lost.
    Drop,
}

impl<T: Send> PipeSender<T> {