use crate::channel::{BackpressurePolicy, PipeReceiver, PipeSender, SendResult};
use crate::metrics::OperatorMetrics;
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};
use std::time::{Duration, Instant};

/// Shared stop signal
pub struct StopToken(Arc<AtomicBool>);

impl StopToken {
    pub fn new() -> (Self, StopHandle) {
        let flag = Arc::new(AtomicBool::new(false));
        (StopToken(Arc::clone(&flag)), StopHandle(flag))
    }

    pub fn is_stopped(&self) -> bool {
        self.0.load(Ordering::Acquire)
    }
}

impl Default for StopToken {
    fn default() -> Self {
        let (tok, _) = Self::new();
        tok
    }
}

pub struct StopHandle(Arc<AtomicBool>);

impl StopHandle {
    pub fn stop(&self) {
        self.0.store(true, Ordering::Release);
    }
}

// ---- Core processing trait ----

pub trait Process: Send + 'static {
    type Input:  Send + 'static;
    type Output: Send + 'static;

    fn process(&mut self, input: Self::Input) -> ProcessResult<Self::Output>;

    /// Called once per tick even when no input arrives (for watermark/timer work)
    fn on_tick(&mut self) -> Vec<Self::Output> { vec![] }
}

pub enum ProcessResult<T> {
    Emit(Vec<T>),
    EmitOne(T),
    Drop,
    Error(String),
}

// ---- Runnable operator context ----

pub struct OperatorRunner<P: Process>
where P::Output: Clone
{
    pub name:       String,
    pub processor:  P,
    pub rx:         PipeReceiver<P::Input>,
    pub senders:    Vec<PipeSender<P::Output>>,
    pub metrics:    Arc<OperatorMetrics>,
    pub stop:       StopToken,
    pub policy:     BackpressurePolicy,
    pub tick_ms:    u64,
}

impl<P: Process> OperatorRunner<P>
where P::Output: Clone
{
    pub fn run(mut self) {
        let tick = Duration::from_millis(self.tick_ms);
        let mut last_tick = Instant::now();

        loop {
            if self.stop.is_stopped() { break; }

            // Drain all available records without blocking
            let mut processed_any = false;
            loop {
                match self.rx.try_recv() {
                    Some(record) => {
                        self.metrics.record_in();
                        let t0 = Instant::now();
                        let result = self.processor.process(record);
                        self.metrics.add_processing_ns(t0.elapsed().as_nanos() as u64);
                        self.dispatch(result);
                        processed_any = true;
                    }
                    None => break,
                }
            }

            // Periodic tick for watermark advancement and timer-based operators
            if last_tick.elapsed() >= tick {
                let outputs = self.processor.on_tick();
                for out in outputs {
                    self.emit(out);
                }
                last_tick = Instant::now();
            }

            if !processed_any {
                // Brief yield to avoid busy-spinning when idle
                std::thread::sleep(Duration::from_micros(50));
            }
        }
    }

    fn dispatch(&mut self, result: ProcessResult<P::Output>) {
        match result {
            ProcessResult::Emit(items) => {
                for item in items { self.emit(item); }
            }
            ProcessResult::EmitOne(item) => { self.emit(item); }
            ProcessResult::Drop => {}
            ProcessResult::Error(e) => {
                self.metrics.record_error();
                eprintln!("[{}] processing error: {}", self.name, e);
            }
        }
    }

    fn emit(&mut self, item: P::Output) {
        for sender in &self.senders {
            match sender.send(item.clone_for_fanout(), self.policy) {
                SendResult::Sent => { self.metrics.record_out(); }
                SendResult::Dropped => { self.metrics.drop_record(); }
                SendResult::Disconnected => {}
            }
        }
    }
}

// Hack: we need Clone for fanout but don't want to require it on all outputs.
// This trait provides a no-op clone for the common single-sink case.
pub trait CloneForFanout: Sized {
    fn clone_for_fanout(&self) -> Self;
}

// Blanket impl for Clone types
impl<T: Clone> CloneForFanout for T {
    fn clone_for_fanout(&self) -> Self { self.clone() }
}

// ---- Built-in processors ----

/// Map: transforms each record 1:1
pub struct MapProcessor<I, O, F: FnMut(I) -> O + Send + 'static> {
    pub f: F,
    _phantom: std::marker::PhantomData<(I, O)>,
}

impl<I, O, F> MapProcessor<I, O, F>
where
    I: Send + 'static,
    O: Send + 'static,
    F: FnMut(I) -> O + Send + 'static,
{
    pub fn new(f: F) -> Self {
        Self { f, _phantom: std::marker::PhantomData }
    }
}

impl<I, O, F> Process for MapProcessor<I, O, F>
where
    I: Send + 'static,
    O: Send + Clone + 'static,
    F: FnMut(I) -> O + Send + 'static,
{
    type Input  = I;
    type Output = O;
    fn process(&mut self, input: I) -> ProcessResult<O> {
        ProcessResult::EmitOne((self.f)(input))
    }
}

/// Filter: passes records that satisfy predicate
pub struct FilterProcessor<T, F: FnMut(&T) -> bool + Send + 'static> {
    pub pred: F,
    _phantom: std::marker::PhantomData<T>,
}

impl<T, F> FilterProcessor<T, F>
where
    T: Send + Clone + 'static,
    F: FnMut(&T) -> bool + Send + 'static,
{
    pub fn new(pred: F) -> Self {
        Self { pred, _phantom: std::marker::PhantomData }
    }
}

impl<T, F> Process for FilterProcessor<T, F>
where
    T: Send + Clone + 'static,
    F: FnMut(&T) -> bool + Send + 'static,
{
    type Input  = T;
    type Output = T;
    fn process(&mut self, input: T) -> ProcessResult<T> {
        if (self.pred)(&input) { ProcessResult::EmitOne(input) }
        else                   { ProcessResult::Drop }
    }
}

/// FlatMap: expands each record into zero or more outputs
pub struct FlatMapProcessor<I, O, F: FnMut(I) -> Vec<O> + Send + 'static> {
    pub f: F,
    _phantom: std::marker::PhantomData<(I, O)>,
}

impl<I, O, F> FlatMapProcessor<I, O, F>
where
    I: Send + 'static,
    O: Send + Clone + 'static,
    F: FnMut(I) -> Vec<O> + Send + 'static,
{
    pub fn new(f: F) -> Self {
        Self { f, _phantom: std::marker::PhantomData }
    }
}

impl<I, O, F> Process for FlatMapProcessor<I, O, F>
where
    I: Send + 'static,
    O: Send + Clone + 'static,
    F: FnMut(I) -> Vec<O> + Send + 'static,
{
    type Input  = I;
    type Output = O;
    fn process(&mut self, input: I) -> ProcessResult<O> {
        ProcessResult::Emit((self.f)(input))
    }
}
