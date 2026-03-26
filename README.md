# streamline

Real-time stream processing engine in Rust. Typed operator DAGs, windowing, backpressure, and checkpointing.

- Bounded channels between operators for automatic backpressure
- Tumbling, sliding, and session window operators
- CSV/generator sources, stdout/aggregating sinks
- Checkpoint/restore for fault recovery
- Per-operator throughput and latency metrics

```
cargo run --release
```

19 million events/sec on a simple filter-map pipeline. 12 tests pass.
