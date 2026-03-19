/// Built-in event sources.
use crate::channel::{BackpressurePolicy, PipeSender, SendResult};
use crate::metrics::OperatorMetrics;
use crate::operator::StopToken;
use std::sync::Arc;
use std::time::{Duration, Instant};

/// Generate records at a fixed rate (tokens-per-second)
pub struct RateLimitedSource<T, F: FnMut(u64) -> T + Send + 'static> {
    pub generator: F,
    pub rate_per_sec: u64,
    pub max_records: Option<u64>,
}

impl<T: Send + Clone + 'static, F: FnMut(u64) -> T + Send + 'static>
    RateLimitedSource<T, F>
{
    pub fn run(
        mut self,
        sender: PipeSender<T>,
        metrics: Arc<OperatorMetrics>,
        stop: StopToken,
        policy: BackpressurePolicy,
    ) {
        let interval = Duration::from_nanos(1_000_000_000 / self.rate_per_sec.max(1));
        let mut seq = 0u64;
        let mut next = Instant::now();

        loop {
            if stop.is_stopped() { break; }
            if let Some(max) = self.max_records {
                if seq >= max { break; }
            }

            let now = Instant::now();
            if now >= next {
                let record = (self.generator)(seq);
                seq += 1;
                match sender.send(record, policy) {
                    SendResult::Sent    => { metrics.record_out(); }
                    SendResult::Dropped => { metrics.drop_record(); }
                    SendResult::Disconnected => break,
                }
                next += interval;
            } else {
                std::thread::sleep(next - now);
            }
        }
    }
}

/// Generate records as fast as possible (throughput testing)
pub struct BurstSource<T, F: FnMut(u64) -> T + Send + 'static> {
    pub generator:   F,
    pub max_records: u64,
}

impl<T: Send + Clone + 'static, F: FnMut(u64) -> T + Send + 'static>
    BurstSource<T, F>
{
    pub fn run(
        mut self,
        sender: PipeSender<T>,
        metrics: Arc<OperatorMetrics>,
        stop: StopToken,
        policy: BackpressurePolicy,
    ) {
        for seq in 0..self.max_records {
            if stop.is_stopped() { break; }
            let record = (self.generator)(seq);
            match sender.send(record, policy) {
                SendResult::Sent    => { metrics.record_out(); }
                SendResult::Dropped => { metrics.drop_record(); }
                SendResult::Disconnected => break,
            }
        }
    }
}
