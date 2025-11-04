// Numan Thabit 2020
#![deny(missing_docs)]
//! solana-ultra-rpc: High-throughput JSON-RPC server for Solana with lock-free hot path.

/// Cache implementation primitives.
pub mod cache;
/// Server configuration structures.
pub mod config;
/// Geyser ingestion utilities.
pub mod ingest;
/// JSON-RPC routing and helpers.
pub mod rpc;
/// Adaptive micro-batching scheduler.
pub mod scheduler;
/// Telemetry and metrics wiring.
pub mod telemetry;
/// QUIC transport implementation.
pub mod transport;

mod server;

pub use server::{launch_server, UltraRpcServerHandle};
