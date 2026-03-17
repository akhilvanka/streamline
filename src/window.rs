use std::collections::BTreeMap;
use std::time::Duration;

pub type Timestamp = u64; // milliseconds since epoch

/// All windows operate on events tagged with a timestamp.
#[derive(Debug, Clone)]
pub struct TimedRecord<T> {
    pub ts:   Timestamp,
    pub data: T,
}

/// A closed window result
#[derive(Debug, Clone)]
pub struct WindowResult<A> {
    pub start_ms:  Timestamp,
    pub end_ms:    Timestamp,
    pub aggregate: A,
    pub count:     u64,
}

// ---- Aggregator trait ----

pub trait Aggregator: Clone + Send + 'static {
    type Input:  Clone + Send + 'static;
    type Output: Clone + Send + 'static;

    fn new_accumulator() -> Self;
    fn add(&mut self, item: &Self::Input);
    fn merge(&mut self, other: &Self);
    fn emit(&self) -> Self::Output;
}

// ---- Tumbling window ----

pub struct TumblingWindow<A: Aggregator> {
    size_ms:     u64,
    watermark:   Timestamp,
    /// window_start → accumulator
    buckets:     BTreeMap<Timestamp, (A, u64)>,
}

impl<A: Aggregator> TumblingWindow<A> {
    pub fn new(size: Duration) -> Self {
        Self {
            size_ms:   size.as_millis() as u64,
            watermark: 0,
            buckets:   BTreeMap::new(),
        }
    }

    pub fn add(&mut self, record: &TimedRecord<A::Input>) {
        let bucket = (record.ts / self.size_ms) * self.size_ms;
        let entry  = self.buckets.entry(bucket).or_insert_with(|| (A::new_accumulator(), 0));
        entry.0.add(&record.data);
        entry.1 += 1;
    }

    /// Advance watermark. Returns closed windows whose end <= new watermark.
    pub fn advance_watermark(&mut self, wm: Timestamp) -> Vec<WindowResult<A::Output>> {
        self.watermark = self.watermark.max(wm);
        let mut closed = Vec::new();

        // Drain all buckets whose end is <= watermark
        while let Some((&start, _)) = self.buckets
            .iter()
            .find(|(&k, _)| k + self.size_ms <= self.watermark)
        {
            if let Some((acc, count)) = self.buckets.remove(&start) {
                closed.push(WindowResult {
                    start_ms: start,
                    end_ms:   start + self.size_ms,
                    aggregate: acc.emit(),
                    count,
                });
            }
        }
        closed
    }

    pub fn watermark(&self) -> Timestamp { self.watermark }
    pub fn open_windows(&self) -> usize  { self.buckets.len() }
}

// ---- Sliding window ----

pub struct SlidingWindow<A: Aggregator> {
    size_ms:   u64,
    step_ms:   u64,
    watermark: Timestamp,
    windows:   BTreeMap<Timestamp, (A, u64)>,
}

impl<A: Aggregator> SlidingWindow<A> {
    pub fn new(size: Duration, step: Duration) -> Self {
        let step_ms = step.as_millis() as u64;
        assert!(step_ms > 0 && step_ms <= size.as_millis() as u64);
        Self {
            size_ms:   size.as_millis() as u64,
            step_ms,
            watermark: 0,
            windows:   BTreeMap::new(),
        }
    }

    pub fn add(&mut self, record: &TimedRecord<A::Input>) {
        // An event at ts falls into all windows [start, start+size) where
        // start = align_down(ts - size + 1, step) ... align_down(ts, step)
        let ts = record.ts;
        let first_start = if ts >= self.size_ms {
            ((ts - self.size_ms + 1) / self.step_ms) * self.step_ms
        } else { 0 };
        let last_start  = (ts / self.step_ms) * self.step_ms;

        let mut start = first_start;
        while start <= last_start {
            let entry = self.windows.entry(start)
                .or_insert_with(|| (A::new_accumulator(), 0));
            entry.0.add(&record.data);
            entry.1 += 1;
            start += self.step_ms;
        }
    }

    pub fn advance_watermark(&mut self, wm: Timestamp) -> Vec<WindowResult<A::Output>> {
        self.watermark = self.watermark.max(wm);
        let mut closed = Vec::new();

        while let Some((&start, _)) = self.windows
            .iter()
            .find(|(&k, _)| k + self.size_ms <= self.watermark)
        {
            if let Some((acc, count)) = self.windows.remove(&start) {
                closed.push(WindowResult {
                    start_ms: start,
                    end_ms:   start + self.size_ms,
                    aggregate: acc.emit(),
                    count,
                });
            }
        }
        closed
    }