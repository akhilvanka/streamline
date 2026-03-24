use streamline::channel::{BackpressurePolicy, Pipe, SendResult, DEFAULT_CAPACITY};
use streamline::checkpoint::CheckpointStore;
use streamline::operator::{Process, ProcessResult};
use streamline::window::{StatsAggregator, TimedRecord, TumblingWindow};

use rand::{Rng, SeedableRng};
use rand::rngs::StdRng;
use std::collections::HashMap;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::{Duration, Instant};

// ---- Domain types ----

#[derive(Clone, Debug)]
struct RequestEvent {
    ts_ms:      u64,
    endpoint:   &'static str,
    status:     u16,
    latency_ms: f32,
}

#[derive(Clone, Debug)]
struct ValidEvent(RequestEvent);

#[derive(Clone, Debug)]
struct WindowedStats {
    endpoint:   &'static str,
    window_end: u64,
    mean_ms:    f64,
    std_ms:     f64,
    min_ms:     f64,
    max_ms:     f64,
    count:      u64,
    error_rate: f64,
}

#[derive(Clone, Debug)]
struct Alert {
    kind:       &'static str,
    endpoint:   &'static str,
    value:      f64,
    window_end: u64,
    sample_n:   u64,
}

// ---- Synthetic generator ----

static ENDPOINTS: &[&str] = &[
    "/api/v1/users", "/api/v1/orders", "/api/v1/products",
    "/api/v1/search", "/api/v1/cart", "/health", "/metrics",
];

fn gen_event(seq: u64, rng: &mut StdRng) -> RequestEvent {
    let ep = ENDPOINTS[rng.gen_range(0..ENDPOINTS.len())];
    let base: f32 = match ep {
        "/api/v1/search"   => 80.0,
        "/api/v1/orders"   => 50.0,
        "/api/v1/products" => 30.0,
        _                  => 15.0,
    };
    let lat = if rng.gen_bool(0.02) {
        base * rng.gen_range(5.0..20.0_f32)
    } else {
        base * rng.gen_range(0.5..2.0_f32)
    };
    let status: u16 = if rng.gen_bool(0.03) { 500 }
                      else if rng.gen_bool(0.05) { 404 }
                      else { 200 };
    // Use seq-based timestamp so runs are deterministic even without wall clock
    RequestEvent { ts_ms: seq, endpoint: ep, status, latency_ms: lat }
}

// ---- Operator implementations ----

struct Validator;
impl Process for Validator {
    type Input  = RequestEvent;
    type Output = ValidEvent;
    fn process(&mut self, e: RequestEvent) -> ProcessResult<ValidEvent> {
        if e.latency_ms < 0.0 || e.latency_ms > 60_000.0 {
            ProcessResult::Error(format!("invalid latency {}", e.latency_ms))
        } else {
            ProcessResult::EmitOne(ValidEvent(e))
        }
    }
}

struct HealthFilter;
impl Process for HealthFilter {
    type Input  = ValidEvent;
    type Output = ValidEvent;
    fn process(&mut self, e: ValidEvent) -> ProcessResult<ValidEvent> {
        match e.0.endpoint {
            "/health" | "/metrics" => ProcessResult::Drop,
            _                      => ProcessResult::EmitOne(e),
        }
    }
}

struct EndpointStats {
    windows:    HashMap<&'static str, TumblingWindow<StatsAggregator>>,
    err_counts: HashMap<&'static str, (u64, u64)>,
    window_ms:  u64,
}

impl EndpointStats {
    fn new(window_ms: u64) -> Self {
        Self { windows: HashMap::new(), err_counts: HashMap::new(), window_ms }
    }
}

impl Process for EndpointStats {
    type Input  = ValidEvent;
    type Output = WindowedStats;

    fn process(&mut self, e: ValidEvent) -> ProcessResult<WindowedStats> {
        let ep = e.0.endpoint;
        let ts = e.0.ts_ms;

        let win = self.windows.entry(ep)
            .or_insert_with(|| TumblingWindow::new(Duration::from_millis(self.window_ms)));
        win.add(&TimedRecord { ts, data: e.0.latency_ms as f64 });

        let ec = self.err_counts.entry(ep).or_insert((0, 0));
        if e.0.status >= 400 { ec.0 += 1; }
        ec.1 += 1;

        let mut closed = win.advance_watermark(ts.saturating_sub(50));
        let err_rate = if ec.1 > 0 { ec.0 as f64 / ec.1 as f64 } else { 0.0 };

        match closed.len() {
            0 => ProcessResult::Drop,
            1 => {
                let w = closed.remove(0);
                ProcessResult::EmitOne(WindowedStats {
                    endpoint: ep, window_end: w.end_ms,
                    mean_ms: w.aggregate.mean, std_ms: w.aggregate.std_dev,
                    min_ms: w.aggregate.min,   max_ms: w.aggregate.max,
                    count: w.count, error_rate: err_rate,
                })
            }
            _ => ProcessResult::Emit(closed.into_iter().map(|w| WindowedStats {
                endpoint: ep, window_end: w.end_ms,
                mean_ms: w.aggregate.mean, std_ms: w.aggregate.std_dev,
                min_ms: w.aggregate.min,   max_ms: w.aggregate.max,
                count: w.count, error_rate: err_rate,
            }).collect()),
        }
    }
}

struct AnomalyDetector {
    lat_threshold:  f64,
    err_threshold:  f64,
}

impl Process for AnomalyDetector {
    type Input  = WindowedStats;
    type Output = Alert;

    fn process(&mut self, s: WindowedStats) -> ProcessResult<Alert> {
        // Ignore windows with too few samples — std_ms would be unreliable
        if s.count < 3 { return ProcessResult::Drop; }

        let mut alerts = Vec::new();
        // HIGH_LATENCY only when min_ms is also elevated — rules out a single spike skewing the mean
        if s.mean_ms > self.lat_threshold && s.min_ms > self.lat_threshold * 0.25 {
            alerts.push(Alert { kind: "HIGH_LATENCY", endpoint: s.endpoint,
                value: s.mean_ms, window_end: s.window_end, sample_n: s.count });
        }
        if s.error_rate > self.err_threshold {
            alerts.push(Alert { kind: "HIGH_ERROR_RATE", endpoint: s.endpoint,
                value: s.error_rate * 100.0, window_end: s.window_end, sample_n: s.count });
        }
        // Only spike-alert when std_ms is elevated too (not just a single outlier in a huge window)
        if s.max_ms > self.lat_threshold * 5.0 && s.std_ms > self.lat_threshold * 0.5 {
            alerts.push(Alert { kind: "LATENCY_SPIKE",  endpoint: s.endpoint,
                value: s.max_ms,           window_end: s.window_end, sample_n: s.count });
        }
        match alerts.len() {
            0 => ProcessResult::Drop,
            1 => ProcessResult::EmitOne(alerts.remove(0)),
            _ => ProcessResult::Emit(alerts),
        }
    }
}

// ---- Direct-wired pipeline ----

fn run_pipeline(n_events: u64, seed: u64) -> (Duration, u64, u64, Vec<Alert>) {
    let policy = BackpressurePolicy::Drop;

    let (raw_tx,    raw_rx)    = Pipe::<RequestEvent>::new(DEFAULT_CAPACITY * 4).split();
    let (valid_tx,  valid_rx)  = Pipe::<ValidEvent>::new(DEFAULT_CAPACITY * 4).split();
    let (filter_tx, filter_rx) = Pipe::<ValidEvent>::new(DEFAULT_CAPACITY * 4).split();
    let (stats_tx,  stats_rx)  = Pipe::<WindowedStats>::new(DEFAULT_CAPACITY).split();
    let (alert_tx,  alert_rx)  = Pipe::<Alert>::new(DEFAULT_CAPACITY).split();

    let in_count    = Arc::new(std::sync::atomic::AtomicU64::new(0));
    let alert_count = Arc::new(std::sync::atomic::AtomicU64::new(0));
    let samples     = Arc::new(std::sync::Mutex::new(Vec::<Alert>::new()));

    let t0 = Instant::now();

    // Source: generate n_events as fast as possible
    let src = std::thread::spawn(move || {
        let mut rng = StdRng::seed_from_u64(seed);
        for i in 0..n_events {
            let ev = gen_event(i, &mut rng);
            match raw_tx.send(ev, policy) {
                SendResult::Disconnected => break,
                _ => {}
            }
        }
    });

    // Validate
    let validate = std::thread::spawn(move || {
        let mut proc = Validator;
        while let Some(ev) = raw_rx.recv() {
            if let ProcessResult::EmitOne(v) = proc.process(ev) {
                if valid_tx.send(v, policy) == SendResult::Disconnected { break; }
            }
        }
    });

    // Filter health checks
    let filter = std::thread::spawn(move || {
        let mut proc = HealthFilter;
        while let Some(ev) = valid_rx.recv() {
            if let ProcessResult::EmitOne(v) = proc.process(ev) {
                if filter_tx.send(v, policy) == SendResult::Disconnected { break; }
            }
        }
    });

    // Windowed stats
    let stats = std::thread::spawn(move || {
        let mut proc = EndpointStats::new(1000);
        while let Some(ev) = filter_rx.recv() {
            match proc.process(ev) {
                ProcessResult::EmitOne(s) => { let _ = stats_tx.send(s, policy); }
                ProcessResult::Emit(vs)   => { for s in vs { let _ = stats_tx.send(s, policy); } }
                _ => {}
            }
        }
    });

    // Anomaly detection
    let anomaly = std::thread::spawn(move || {
        let mut proc = AnomalyDetector { lat_threshold: 200.0, err_threshold: 0.1 };
        while let Some(s) = stats_rx.recv() {
            match proc.process(s) {
                ProcessResult::EmitOne(a) => { let _ = alert_tx.send(a, policy); }
                ProcessResult::Emit(vs)   => { for a in vs { let _ = alert_tx.send(a, policy); } }
                _ => {}
            }
        }
    });

    // Sink: collect first 5 sample alerts, count the rest
    let ic = Arc::clone(&in_count);
    let ac = Arc::clone(&alert_count);
    let sp = Arc::clone(&samples);
    let sink = std::thread::spawn(move || {
        while let Some(alert) = alert_rx.recv() {
            ic.fetch_add(1, Ordering::Relaxed);
            ac.fetch_add(1, Ordering::Relaxed);
            let mut s = sp.lock().unwrap();
            if s.len() < 5 {
                s.push(alert);
            }
        }
    });

    src.join().unwrap();
    validate.join().unwrap();
    filter.join().unwrap();
    stats.join().unwrap();
    anomaly.join().unwrap();
    sink.join().unwrap();

    let elapsed      = t0.elapsed();
    let total_alerts = alert_count.load(Ordering::Relaxed);
    let sample_alerts = std::mem::take(&mut *samples.lock().unwrap());
    (elapsed, n_events, total_alerts, sample_alerts)
}

// ---- Demo functions ----

fn demo_throughput() {
    println!("=== Streamline: Throughput Benchmark ===");
    println!("Pipeline: Source → Validate → Filter → Window(1s) → Anomaly → Sink\n");

    let configs: &[(u64, &str)] = &[
        (100_000,   "100k events"),
        (500_000,   "500k events"),
        (1_000_000, "1M events"),
        (5_000_000, "5M events"),
    ];

    println!("  {:22} {:>10} {:>10} {:>10}", "Config", "Time(ms)", "Mev/sec", "Alerts");
    println!("  {}", "-".repeat(58));

    let mut last_samples = Vec::new();
    for &(n, label) in configs {
        let (elapsed, _, alerts, samples) = run_pipeline(n, 42);
        let throughput = n as f64 / elapsed.as_secs_f64() / 1_000_000.0;
        println!("  {:22} {:>10} {:>10.2} {:>10}", label, elapsed.as_millis(), throughput, alerts);
        last_samples = samples;
    }

    println!("\n  Sample alerts from last run:");
    println!("  {:20} {:28} {:>15} {:>7} {:>12}", "Kind", "Endpoint", "Value", "n", "window_ms");
    println!("  {}", "-".repeat(86));
    for a in &last_samples {
        let value_fmt = match a.kind {
            "HIGH_ERROR_RATE" => format!("{:.1}% errors", a.value),
            _                 => format!("{:.1} ms",      a.value),
        };
        println!("  {:20} {:28} {:>15} {:>7} {:>12}",
            a.kind, a.endpoint, value_fmt, a.sample_n, a.window_end);
    }
}

fn demo_montecarlo() {
    println!("=== Streamline: Monte Carlo (20 runs × 1M events) ===\n");

    const RUNS: usize = 20;
    const N:    u64   = 1_000_000;

    let mut throughputs = Vec::with_capacity(RUNS);
    let mut alert_counts = Vec::with_capacity(RUNS);

    print!("  Running");
    for seed in 0..RUNS as u64 {
        let (elapsed, _, alerts, _) = run_pipeline(N, seed);
        let mevs = N as f64 / elapsed.as_secs_f64() / 1_000_000.0;
        throughputs.push(mevs);
        alert_counts.push(alerts);
        print!(".");
        use std::io::Write;
        std::io::stdout().flush().ok();
    }
    println!(" done\n");

    throughputs.sort_by(|a, b| a.partial_cmp(b).unwrap());
    alert_counts.sort();

    let mean_tp  = throughputs.iter().sum::<f64>() / RUNS as f64;
    let p50_tp   = throughputs[RUNS / 2];
    let p95_tp   = throughputs[(RUNS as f64 * 0.95) as usize];

    let mean_alt = alert_counts.iter().sum::<u64>() as f64 / RUNS as f64;

    println!("  Throughput (Mev/sec):");
    println!("    min={:.2}  p50={:.2}  mean={:.2}  p95={:.2}  max={:.2}",
        throughputs[0], p50_tp, mean_tp, p95_tp, throughputs[RUNS - 1]);

    println!("\n  Alerts fired per run:");
    println!("    min={}  mean={:.1}  max={}",
        alert_counts[0], mean_alt, alert_counts[RUNS - 1]);

    // Determinism check
    let (_, _, a1, _) = run_pipeline(100_000, 7777);
    let (_, _, a2, _) = run_pipeline(100_000, 7777);
    println!("\n  Determinism (seed=7777): run1={} run2={} → {}",
        a1, a2, if a1 == a2 { "PASS" } else { "FAIL" });
}

fn demo_checkpoint() {
    println!("=== Streamline: Checkpoint / Restore ===\n");

    let dir = std::env::temp_dir().join("streamline_ckpt");
    let store = CheckpointStore::new(&dir, Duration::from_secs(5)).unwrap();

    let mut state = HashMap::new();
    state.insert("stats_op".to_owned(),
        serde_json::json!({ "windows_closed": 42, "events_in": 1_000_000 }).to_string());
    state.insert("anomaly_op".to_owned(),
        serde_json::json!({ "alerts_fired": 7, "last_wm": 999_000 }).to_string());

    let id1 = store.save(state.clone()).unwrap();
    println!("  Saved checkpoint {}", id1);

    // Simulate processing more events
    state.insert("stats_op".to_owned(),
        serde_json::json!({ "windows_closed": 43, "events_in": 2_000_000 }).to_string());
    let id2 = store.save(state).unwrap();
    println!("  Saved checkpoint {}", id2);

    store.gc(1).unwrap();

    let latest = store.load_latest().unwrap().unwrap();
    println!("  Restored checkpoint {} (expected {})", latest.id, id2);
    for (op, json) in &latest.operators {
        let v: serde_json::Value = serde_json::from_str(json).unwrap();
        println!("    {}: {}", op, v);
    }

    let pass = latest.id == id2;
    println!("\n  [{}] Latest checkpoint id after GC is {}", if pass { "PASS" } else { "FAIL" }, id2);
}

fn demo_show_data() {
    println!("=== Simulated request event stream (seed=42, first 40 events) ===\n");
    println!("  {:<4} {:<28} {:<6} {:<7} {:<12} {:<8}",
        "seq", "endpoint", "method", "status", "latency_ms", "bytes");
    println!("  {}", "-".repeat(72));

    let mut rng = StdRng::seed_from_u64(42);
    for i in 0u64..40 {
        let ev = gen_event(i, &mut rng);
        let flag = if ev.status >= 500       { " ← 5xx error" }
                   else if ev.status >= 400  { " ← 4xx" }
                   else if ev.latency_ms > 500.0 { " ← SPIKE" }
                   else                      { "" };
        println!("  {:<4} {:<28} {:<6} {:<7} {:<12.1} {:<8}{}",
            i, ev.endpoint, "GET", ev.status, ev.latency_ms,
            rng.gen_range(100u32..50000), flag);
    }

    println!("\n  Distribution over 100k events:");
    let mut endpoint_counts: std::collections::HashMap<&str, u64> = std::collections::HashMap::new();
    let mut error_count   = 0u64;
    let mut spike_count   = 0u64;
    let mut health_count  = 0u64;
    let mut rng2 = StdRng::seed_from_u64(42);
    for i in 0..100_000u64 {
        let ev = gen_event(i, &mut rng2);
        *endpoint_counts.entry(ev.endpoint).or_insert(0) += 1;
        if ev.status >= 400    { error_count  += 1; }
        if ev.latency_ms > 200.0 { spike_count += 1; }
        if ev.endpoint == "/health" || ev.endpoint == "/metrics" { health_count += 1; }
    }
    let mut counts: Vec<_> = endpoint_counts.iter().collect();
    counts.sort_by_key(|(_, &v)| std::cmp::Reverse(v));
    println!("\n  {:28} {:>8}  {:>6}", "Endpoint", "Count", "%");
    println!("  {}", "-".repeat(48));
    for (ep, count) in &counts {
        println!("  {:28} {:>8}  {:>5.1}%", ep, count, **count as f64 / 1000.0);
    }
    println!("\n  Error rate (4xx+5xx): {:.2}%  ({} events)",
        error_count as f64 / 1000.0, error_count);
    println!("  High-latency (>200ms): {:.2}%  ({} events)",
        spike_count as f64 / 1000.0, spike_count);
    println!("  Health/metrics (filtered out): {:.1}%  ({} events)",
        health_count as f64 / 1000.0, health_count);
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    match args.get(1).map(|s| s.as_str()) {
        Some("data") => demo_show_data(),
        _ => {
            demo_throughput();
            println!();
            demo_montecarlo();
            println!();
            demo_checkpoint();
        }
    }
}

// ---- Unit tests ----

#[cfg(test)]
mod tests {
    use super::*;
    use streamline::window::{SessionWindow, SlidingWindow, SumAggregator, TimedRecord};

    #[test]
    fn validator_rejects_negative_latency() {
        let ev = RequestEvent { ts_ms: 0, endpoint: "/api/v1/users", status: 200, latency_ms: -1.0 };
        match (Validator).process(ev) {
            ProcessResult::Error(_) => {}
            _ => panic!("should error on negative latency"),
        }
    }

    #[test]
    fn validator_accepts_valid_event() {
        let ev = RequestEvent { ts_ms: 0, endpoint: "/api/v1/users", status: 200, latency_ms: 42.0 };
        match (Validator).process(ev) {
            ProcessResult::EmitOne(_) => {}
            _ => panic!("should accept valid event"),
        }
    }

    #[test]
    fn health_filter_drops_health_endpoint() {
        let ev = RequestEvent { ts_ms: 0, endpoint: "/health", status: 200, latency_ms: 1.0 };
        match (HealthFilter).process(ValidEvent(ev)) {
            ProcessResult::Drop => {}
            _ => panic!("should drop /health"),
        }
    }

    #[test]
    fn health_filter_passes_api_endpoint() {
        let ev = RequestEvent { ts_ms: 0, endpoint: "/api/v1/users", status: 200, latency_ms: 10.0 };
        match (HealthFilter).process(ValidEvent(ev)) {
            ProcessResult::EmitOne(_) => {}
            _ => panic!("should pass /api/v1/users"),
        }
    }

    #[test]
    fn tumbling_window_closes_on_watermark() {
        use streamline::window::TumblingWindow;
        let mut w: TumblingWindow<SumAggregator> = TumblingWindow::new(Duration::from_millis(1000));

        for ts in [100u64, 200, 300, 500, 900] {
            w.add(&TimedRecord { ts, data: 1.0 });
        }
        // Advance watermark past end of window [0, 1000)
        let closed = w.advance_watermark(1100);
        assert_eq!(closed.len(), 1, "exactly one window should close");
        assert_eq!(closed[0].count, 5);
        assert!((closed[0].aggregate - 5.0).abs() < 1e-9);
    }

    #[test]
    fn sliding_window_overlaps_correctly() {
        let mut w: SlidingWindow<SumAggregator> = SlidingWindow::new(
            Duration::from_millis(200),
            Duration::from_millis(100),
        );

        // Add events at t=150
        w.add(&TimedRecord { ts: 150, data: 1.0 });
        let closed = w.advance_watermark(400);
        // Windows covering ts=150: [0,200) and [100,300)
        // Both should be closed by wm=400
        assert!(closed.len() >= 1, "at least one window should close");
    }

    #[test]
    fn session_window_groups_close_events() {
        let mut w: SessionWindow<SumAggregator> = SessionWindow::new(Duration::from_millis(500));

        // Events within 500ms gap → same session
        for ts in [100u64, 200, 400, 700] {
            w.add(&TimedRecord { ts, data: 1.0 });
        }
        assert_eq!(w.open_sessions(), 1, "all events within gap → one session");

        // Event far in the future → new session
        w.add(&TimedRecord { ts: 2000, data: 1.0 });
        assert_eq!(w.open_sessions(), 2);

        let closed = w.advance_watermark(3000);
        assert_eq!(closed.len(), 2, "both sessions should close at wm=3000");
    }

    #[test]
    fn stats_aggregator_welford_correctness() {
        use streamline::window::{Aggregator, StatsAggregator};
        let mut agg = StatsAggregator::new_accumulator();
        let values = [2.0f64, 4.0, 4.0, 4.0, 5.0, 5.0, 7.0, 9.0];
        for v in &values { agg.add(v); }
        let stats = agg.emit();
        assert_eq!(stats.count, 8);
        assert!((stats.mean - 5.0).abs() < 1e-9, "mean={}", stats.mean);
        // Sample std dev (Welford, n-1 denominator) for [2,4,4,4,5,5,7,9]:
        // variance = 32/7 ≈ 4.571, std ≈ 2.138
        let expected_std = (32.0f64 / 7.0).sqrt();
        assert!((stats.std_dev - expected_std).abs() < 1e-6, "std={}", stats.std_dev);
        assert!((stats.min - 2.0).abs() < 1e-9);
        assert!((stats.max - 9.0).abs() < 1e-9);
    }

    #[test]
    fn anomaly_detector_fires_on_high_latency() {
        let mut proc = AnomalyDetector { lat_threshold: 100.0, err_threshold: 0.5 };
        let s = WindowedStats {
            endpoint: "/api/v1/search", window_end: 1000,
            mean_ms: 250.0, std_ms: 20.0, min_ms: 200.0, max_ms: 300.0,
            count: 10, error_rate: 0.05,
        };
        match proc.process(s) {
            ProcessResult::EmitOne(a) => {
                assert_eq!(a.kind, "HIGH_LATENCY");
                assert_eq!(a.sample_n, 10);
                assert_eq!(a.window_end, 1000);
            }
            _ => panic!("should emit HIGH_LATENCY alert"),
        }
    }

    #[test]
    fn anomaly_detector_drops_normal_window() {
        let mut proc = AnomalyDetector { lat_threshold: 500.0, err_threshold: 0.5 };
        let s = WindowedStats {
            endpoint: "/api/v1/users", window_end: 1000,
            mean_ms: 15.0, std_ms: 3.0, min_ms: 5.0, max_ms: 40.0,
            count: 100, error_rate: 0.02,
        };
        match proc.process(s) {
            ProcessResult::Drop => {}
            _ => panic!("should drop normal window"),
        }
    }

    #[test]
    fn pipeline_deterministic() {
        let (_, _, a1, _) = run_pipeline(10_000, 42);
        let (_, _, a2, _) = run_pipeline(10_000, 42);
        assert_eq!(a1, a2, "same seed must produce same alert count");
    }

    #[test]
    fn checkpoint_save_restore() {
        let dir = std::env::temp_dir().join(format!("sl_test_{}", std::process::id()));
        let store = CheckpointStore::new(&dir, Duration::from_secs(60)).unwrap();

        let mut state = std::collections::HashMap::new();
        state.insert("op1".to_owned(), r#"{"count":42}"#.to_owned());

        let id = store.save(state).unwrap();
        let m  = store.load_latest().unwrap().unwrap();
        assert_eq!(m.id, id);
        assert!(m.operators.contains_key("op1"));

        let _ = std::fs::remove_dir_all(&dir);
    }
}
