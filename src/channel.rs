use crossbeam_channel::{bounded, Receiver, Sender, TrySendError};
use std::time::Duration;

pub const DEFAULT_CAPACITY: usize = 1024;

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

#[derive(Clone, Copy, Debug)]
pub enum BackpressurePolicy {
    Block { timeout: Duration },
    Drop,
}

impl<T: Send> PipeSender<T> {
    pub fn send(&self, item: T, policy: BackpressurePolicy) -> Result<(), T> {
        match policy {
            BackpressurePolicy::Block { timeout } => {
                self.tx.send_timeout(item, timeout).map_err(|e| e.into_inner())
            }
            BackpressurePolicy::Drop => match self.tx.try_send(item) {
                Ok(()) => Ok(()),
                Err(TrySendError::Full(v)) => Err(v),
                Err(TrySendError::Disconnected(v)) => Err(v),
            },
        }
    }

    pub fn is_full(&self) -> bool {
        self.tx.is_full()
    }

    pub fn len(&self) -> usize {
        self.tx.len()
    }

    pub fn capacity(&self) -> usize {
        self.tx.capacity().unwrap_or(0)
    }
}

impl<T: Send> PipeReceiver<T> {
    pub fn recv(&self) -> Option<T> {
        self.rx.recv().ok()
    }

    pub fn recv_timeout(&self, timeout: Duration) -> Option<T> {
        self.rx.recv_timeout(timeout).ok()
    }

    pub fn try_recv(&self) -> Option<T> {
        self.rx.try_recv().ok()
    }

    pub fn is_empty(&self) -> bool {
        self.rx.is_empty()
    }

    pub fn len(&self) -> usize {
        self.rx.len()
    }
}
