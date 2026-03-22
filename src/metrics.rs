/// Per-operator runtime metrics, tracked with lock-free atomics.
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

#[derive(Debug, Default)]
pub struct OperatorMetrics {
    pub records_in:      AtomicU64,
    pub records_out:     AtomicU64,
    pub records_dropped: AtomicU64,
    pub errors:          AtomicU64,
    pub processing_ns:   AtomicU64, // total nanoseconds spent processing
}

impl OperatorMetrics {
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    pub fn record_in(&self)  { self.records_in.fetch_add(1, Ordering::Relaxed); }
    pub fn record_out(&self) { self.records_out.fetch_add(1, Ordering::Relaxed); }
    pub fn drop_record(&self){ self.records_dropped.fetch_add(1, Ordering::Relaxed); }
    pub fn record_error(&self){ self.errors.fetch_add(1, Ordering::Relaxed); }

    pub fn add_processing_ns(&self, ns: u64) {
        self.processing_ns.fetch_add(ns, Ordering::Relaxed);
    }

    pub fn snapshot(&self) -> MetricsSnapshot {
        MetricsSnapshot {
            records_in:      self.records_in.load(Ordering::Relaxed),
            records_out:     self.records_out.load(Ordering::Relaxed),
            records_dropped: self.records_dropped.load(Ordering::Relaxed),
            errors:          self.errors.load(Ordering::Relaxed),
            processing_ns:   self.processing_ns.load(Ordering::Relaxed),
        }
    }
}

#[derive(Debug, Clone)]
pub struct MetricsSnapshot {
    pub records_in:      u64,
    pub records_out:     u64,
    pub records_dropped: u64,
    pub errors:          u64,
    pub processing_ns:   u64,
}

impl MetricsSnapshot {
    pub fn throughput_per_sec(&self, elapsed: Duration) -> f64 {
        if elapsed.as_secs_f64() < 1e-9 { return 0.0; }
        self.records_out as f64 / elapsed.as_secs_f64()
    }

    pub fn avg_latency_us(&self) -> f64 {
        if self.records_out == 0 { return 0.0; }
        self.processing_ns as f64 / self.records_out as f64 / 1000.0
    }

    pub fn drop_rate(&self) -> f64 {
        let total = self.records_in + self.records_dropped;
        if total == 0 { return 0.0; }
        self.records_dropped as f64 / total as f64
    }
}

/// Lightweight latency histogram with fixed power-of-2 buckets (ns).
pub struct LatencyHistogram {
    /// Buckets: [0-1ns), [1-2ns), [2-4ns), ... [2^30-2^31ns)
    buckets: [AtomicU64; 32],
    count:   AtomicU64,
    sum_ns:  AtomicU64,
}

impl LatencyHistogram {
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            buckets: std::array::from_fn(|_| AtomicU64::new(0)),
            count:   AtomicU64::new(0),
            sum_ns:  AtomicU64::new(0),
        })
    }

    pub fn record(&self, latency: Duration) {
        let ns = latency.as_nanos() as u64;
        self.sum_ns.fetch_add(ns, Ordering::Relaxed);
        self.count.fetch_add(1, Ordering::Relaxed);

        let bucket = if ns == 0 { 0 } else { 64 - ns.leading_zeros() as usize };
        let bucket = bucket.min(31);
        self.buckets[bucket].fetch_add(1, Ordering::Relaxed);
    }

    /// Approximate percentile (returns lower bound of the bucket)
    pub fn percentile(&self, p: f64) -> Duration {
        let count = self.count.load(Ordering::Relaxed);
        if count == 0 { return Duration::ZERO; }

        let target = (count as f64 * p / 100.0).ceil() as u64;
        let mut cumulative = 0u64;

        for (i, bucket) in self.buckets.iter().enumerate() {
            cumulative += bucket.load(Ordering::Relaxed);
            if cumulative >= target {
                let ns = if i == 0 { 0u64 } else { 1u64 << (i - 1) };
                return Duration::from_nanos(ns);
            }
        }
        Duration::from_nanos(1 << 30)
    }

    pub fn mean(&self) -> Duration {
        let count = self.count.load(Ordering::Relaxed);
        if count == 0 { return Duration::ZERO; }
        Duration::from_nanos(self.sum_ns.load(Ordering::Relaxed) / count)
    }
}

impl Default for LatencyHistogram {
    fn default() -> Self {
        Self {
            buckets: std::array::from_fn(|_| AtomicU64::new(0)),
            count:   AtomicU64::new(0),
            sum_ns:  AtomicU64::new(0),
        }
    }
}

/// Timed scope helper — records duration on drop.
pub struct TimedScope<'a> {
    histogram: &'a LatencyHistogram,
    start: Instant,
}

impl<'a> TimedScope<'a> {
    pub fn new(h: &'a LatencyHistogram) -> Self {
        Self { histogram: h, start: Instant::now() }
    }
}

impl<'a> Drop for TimedScope<'a> {
    fn drop(&mut self) {
        self.histogram.record(self.start.elapsed());
    }
}
