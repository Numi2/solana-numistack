# solana-numistack

Solana ingestion, streaming, RPC, and observability workspace built around faststreams encoding.

## Build

- `cargo build --release`
- `cargo test`
- `cargo clippy --workspace --all-targets --all-features`
- `cargo fmt`

## Crates

### faststreams
- Defines `Record` enums for account, transaction, block, and slot updates.
- Encodes frames with a fixed 12-byte header, optional LZ4 compression, and optional `rkyv` archives.
- Provides decode helpers, vectored write utilities, and batching helpers.
- Tech: `serde`, `bincode::Options`, `lz4_flex`, `smallvec`, `std::sync::atomic`, optional `rkyv` + `bytecheck`.
- Benchmark target: `cargo bench -p faststreams encode_decode`.

### geyser-plugin-ultra
- `cdylib` implementing the Agave Geyser plugin interface.
- Converts replica updates into `faststreams` frames and writes them to sharded Unix socket queues.
- Queue size, backpressure policy, batching, CPU affinity, and metrics endpoint come from JSON (see `ops/geyser-plugin-ultra.json`).
- Exports counters via `metrics`/Prometheus when enabled.
- Tech: `agave-geyser-plugin-interface`, `solana-sdk`, `faststreams`, `crossbeam-queue`, `parking_lot`, `socket2`, `metrics` + `metrics-exporter-prometheus`, `nix`, `libc`, `tracing`.

### ultra-aggregator
- Tokio service that reads `faststreams` frames from Unix sockets.
- Emits JSON to stdout and can send decoded records to Kafka when built with `--features kafka`.
- Rejects oversize frames, tracks drops, and updates Prometheus gauges.
- Config file example: `crates/ultra-aggregator/configs/aggregator.json`.
- Tech: `tokio`, `faststreams`, `serde_json`, `metrics`, `metrics-exporter-prometheus`, `socket2`, `bs58`, optional `rkyv`, optional `rdkafka`, `tracing`, `bytes`.

### solana-ultra-rpc
- Library that exposes `launch_server` returning `UltraRpcServerHandle`.
- Loads an initial snapshot stream, applies delta stream updates, and keeps an account cache.
- Uses a configurable scheduler (`UltraRpcConfig`) to batch QUIC JSON-RPC requests.
- Serves `/metrics` over HTTP and shuts down via the handle.
- Tech: `quinn` for QUIC transport, self-signed certs via `rcgen`, JSON parsing with `simd-json`, async runtime `tokio`, HTTP metrics via `axum`, tracing with `tracing`, metrics wiring in `telemetry` module.

### solana-quic-proxy
- Axum HTTP proxy that forwards JSON-RPC requests to a Solana QUIC upstream using `QuicRpcClient`.
- Enforces request and response size limits, request timeout, hedged attempts, and optional 0-RTT.
- Config is supplied via CLI or TOML (`ops/solana-quic-proxy.toml`).
- Metrics endpoint at `/metrics`.
- Tech: `axum`, `tokio`, `quinn`, `rustls-native-certs`, `tower-http` tracing, `arc-swap` for connection state, `metrics`/Prometheus, `serde_json`, `clap` CLI.

### solana-validator-observer
- CLI daemon that scrapes validator gossip, QUIC, RPC, and optional eBPF telemetry feeds.
- Maintains per-validator state, exposes Prometheus metrics, and renders a flamegraph view.
- Sends webhook alerts on slot lag when configured; can export a Grafana dashboard JSON.
- Configuration uses TOML (`ops/solana-validator-observer.example.toml`).
- Tech: `tokio`, `reqwest` (Rustls TLS), `axum` + `tower` for HTTP, `prometheus`, `pprof` flamegraph output, optional `aya` eBPF integration, `dashmap`, `serde_with`, `clap`, `tracing`.

### ys-consumer
- Yellowstone gRPC client that subscribes to updates and re-encodes them with `faststreams`.
- Writes frames to Unix sockets or SPSC queues with backpressure handling.
- Keeps a dead-letter queue for oversize frames and emits Prometheus metrics.
- Uses buffer pools to reuse allocations.
- Tech: `tokio`, `yellowstone-grpc-client` + `tonic` transport, `faststreams`, `crossbeam-channel`, `crossbeam-queue`, `event-listener`, `metrics`, `socket2`, `bs58`, `tracing`.

### jito-client
- Library wrapping `SearcherServiceClient` with retry logic, optional gzip, and bearer auth.
- Functions include `send_bundle`, `get_tip_accounts`, and `subscribe_bundle_results_stream`.
- Builder exposes connect timeout, HTTP/2 window sizes, keepalive, and retry backoff knobs.
- Binary `jito-bundle` submits bundles from CLI input.
- Tech: `tonic` gRPC, `prost` generated types, `http::Uri`, `tokio` runtime, `tokio-stream`, `futures-util`, `CompressionEncoding::Gzip`, TLS via `tonic::transport::ClientTlsConfig`, `thiserror`, `tracing`.

### ultra-rpc-bench
- Harness that starts `solana-ultra-rpc`, drives load via `wrk`/`wrk-quic`, and stores run artifacts.
- Controls server args/env, warmup, cooldown, and per-iteration JSON exports.
- `cargo run -p ultra-rpc-bench --bin uds_burst_soak` runs the Unix socket burst/soak generator.
- Tech: `tokio` subprocess management, `clap` CLI, `humantime` parsing, `serde_json` reporting, `faststreams` for frame generation, `tracing` logging.

## Operations

- `ops/` directory stores example deployment configs for the plugin, proxy, and observer.
- `ultra-aggregator.service` is a systemd unit file for running the aggregator.

## Development

- `cargo fmt`
- `cargo clippy --workspace --all-targets --all-features`
- `cargo test --workspace`