use crate::metrics::OperatorMetrics;
use crate::operator::{StopHandle, StopToken};
use std::collections::HashMap;
use std::sync::Arc;
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

pub struct PipelineHandle {
    stop_handle: StopHandle,
    threads:     Vec<(String, JoinHandle<()>)>,
    metrics:     HashMap<String, Arc<OperatorMetrics>>,
    start_time:  Instant,
}

impl PipelineHandle {
    pub fn new(
        stop_handle: StopHandle,
        threads: Vec<(String, JoinHandle<()>)>,
        metrics: HashMap<String, Arc<OperatorMetrics>>,
    ) -> Self {
        Self { stop_handle, threads, metrics, start_time: Instant::now() }
    }

    /// Signal all operators to stop and wait for them to drain.
    pub fn shutdown(self) -> ShutdownReport {
        self.stop_handle.stop();
        let elapsed = self.start_time.elapsed();

        let mut thread_results = Vec::new();
        for (name, handle) in self.threads {
            let ok = handle.join().is_ok();
            thread_results.push((name, ok));
        }

        let snapshots: HashMap<String, _> = self.metrics
            .iter()
            .map(|(k, v)| (k.clone(), v.snapshot()))
            .collect();

        ShutdownReport { elapsed, thread_results, metrics: snapshots }
    }

    pub fn metrics(&self) -> &HashMap<String, Arc<OperatorMetrics>> {
        &self.metrics
    }

    pub fn elapsed(&self) -> Duration { self.start_time.elapsed() }

    /// Wait until a condition is met or timeout expires, checking every `poll_ms`.
    pub fn wait_until<F: Fn(&HashMap<String, Arc<OperatorMetrics>>) -> bool>(
        &self,
        condition: F,
        timeout: Duration,
        poll_ms: u64,
    ) -> bool {
        let deadline = Instant::now() + timeout;
        while Instant::now() < deadline {
            if condition(&self.metrics) { return true; }
            thread::sleep(Duration::from_millis(poll_ms));
        }
        condition(&self.metrics)
    }

    /// Stop signal only (don't join threads — use when you want async shutdown)
    pub fn signal_stop(&self) {
        self.stop_handle.stop();
    }
}

pub struct ShutdownReport {
    pub elapsed:        Duration,
    pub thread_results: Vec<(String, bool)>,
    pub metrics:        HashMap<String, crate::metrics::MetricsSnapshot>,
}

impl ShutdownReport {
    pub fn total_records_out(&self) -> u64 {
        self.metrics.values().map(|m| m.records_out).sum()
    }

    pub fn total_dropped(&self) -> u64 {
        self.metrics.values().map(|m| m.records_dropped).sum()
    }

    pub fn print_summary(&self) {
        println!("\n  Pipeline Summary ({:.2}s)", self.elapsed.as_secs_f64());
        println!("  {:-<50}", "");
        println!("  {:30} {:>10} {:>10} {:>8}", "Operator", "In", "Out", "Dropped");
        for (name, snap) in &self.metrics {
            println!("  {:30} {:>10} {:>10} {:>8}",
                name, snap.records_in, snap.records_out, snap.records_dropped);
        }
        let elapsed = self.elapsed;
        let total_out = self.total_records_out();
        let throughput = total_out as f64 / elapsed.as_secs_f64();
        println!("  {:-<50}", "");
        println!("  Total output:   {}", total_out);
        println!("  Throughput:     {:.0} records/sec", throughput);
        println!("  Total dropped:  {}", self.total_dropped());
    }

    pub fn all_threads_ok(&self) -> bool {
        self.thread_results.iter().all(|(_, ok)| *ok)
    }
}

// ---- Thread spawn helpers ----

pub fn spawn_named<F>(name: impl Into<String>, f: F) -> (String, JoinHandle<()>)
where F: FnOnce() + Send + 'static
{
    let name = name.into();
    let n = name.clone();
    let handle = thread::Builder::new()
        .name(name.clone())
        .spawn(f)
        .unwrap_or_else(|_| panic!("failed to spawn thread {}", n));
    (name, handle)
}

pub fn make_stop_pair() -> (StopToken, StopHandle) {
    StopToken::new()
}
