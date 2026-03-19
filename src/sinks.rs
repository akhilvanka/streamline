/// Built-in event sinks.
use crate::channel::PipeReceiver;
use crate::metrics::OperatorMetrics;
use crate::operator::StopToken;
use std::sync::{Arc, Mutex};
use std::time::Duration;

/// Collect all output records into a Vec (for testing / small demos)
pub struct CollectSink<T> {
    pub collected: Arc<Mutex<Vec<T>>>,
}

impl<T: Send + 'static> CollectSink<T> {
    pub fn new() -> (Self, Arc<Mutex<Vec<T>>>) {
        let v = Arc::new(Mutex::new(Vec::new()));
        (Self { collected: Arc::clone(&v) }, v)
    }

    pub fn run(self, rx: PipeReceiver<T>, metrics: Arc<OperatorMetrics>, stop: StopToken) {
        loop {
            if stop.is_stopped() {
                // Drain remaining
                while let Some(item) = rx.try_recv() {
                    self.collected.lock().unwrap().push(item);
                    metrics.record_in();
                }
                break;
            }
            if let Some(item) = rx.recv_timeout(Duration::from_millis(1)) {
                self.collected.lock().unwrap().push(item);
                metrics.record_in();
            }
        }
    }
}

impl<T: Send + 'static> Default for CollectSink<T> {
    fn default() -> Self {
        Self { collected: Arc::new(Mutex::new(Vec::new())) }
    }
}

/// Count records (for benchmarking without I/O overhead)
pub struct CountSink {
    pub count: Arc<std::sync::atomic::AtomicU64>,
}

impl CountSink {
    pub fn new() -> (Self, Arc<std::sync::atomic::AtomicU64>) {
        let c = Arc::new(std::sync::atomic::AtomicU64::new(0));
        (Self { count: Arc::clone(&c) }, c)
    }

    pub fn run<T: Send + 'static>(
        self, rx: PipeReceiver<T>, metrics: Arc<OperatorMetrics>, stop: StopToken,
    ) {
        loop {
            if stop.is_stopped() {
                while rx.try_recv().is_some() {
                    self.count.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    metrics.record_in();
                }
                break;
            }
            if let Some(_) = rx.recv_timeout(Duration::from_millis(1)) {
                self.count.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                metrics.record_in();
            }
        }
    }
}

impl Default for CountSink {
    fn default() -> Self {
        let (s, _) = Self::new();
        s
    }
}

/// Print records to stdout (debugging)
pub struct PrintSink<T: std::fmt::Debug + Send + 'static> {
    pub prefix: String,
    _phantom: std::marker::PhantomData<T>,
}

impl<T: std::fmt::Debug + Send + 'static> PrintSink<T> {
    pub fn new(prefix: &str) -> Self {
        Self { prefix: prefix.to_owned(), _phantom: std::marker::PhantomData }
    }

    pub fn run(self, rx: PipeReceiver<T>, metrics: Arc<OperatorMetrics>, stop: StopToken) {
        loop {
            if stop.is_stopped() { break; }
            if let Some(item) = rx.recv_timeout(Duration::from_millis(5)) {
                println!("{}: {:?}", self.prefix, item);
                metrics.record_in();
            }
        }
    }
}
