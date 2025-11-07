// Numan Thabit 2025
use std::{
    fs,
    net::SocketAddr,
    path::{Path, PathBuf},
    time::Duration,
};

use anyhow::{bail, Context, Result};
use clap::Parser;
use quinn::VarInt;
use serde::Deserialize;
use tracing::info;

const DEFAULT_LISTEN: &str = "0.0.0.0:8898";
const DEFAULT_UPSTREAM: &str = "127.0.0.1:8899";
const DEFAULT_SERVER_NAME: &str = "solana-ultra-rpc";
const DEFAULT_MAX_REQUEST_BYTES: usize = 4 * 1024 * 1024;
const DEFAULT_MAX_RESPONSE_BYTES: usize = 8 * 1024 * 1024;
const DEFAULT_MAX_STREAMS: u32 = 1024;
const DEFAULT_KEEP_ALIVE_MS: u64 = 500;
const DEFAULT_MAX_IDLE_TIMEOUT_MS: u64 = 15_000;
const DEFAULT_INITIAL_MTU: u16 = 1_400;
const DEFAULT_STREAM_WINDOW: u64 = 4 * 1024 * 1024;
const DEFAULT_CONNECTION_WINDOW: u64 = 64 * 1024 * 1024;
const DEFAULT_SEND_WINDOW: u64 = 64 * 1024 * 1024;
const DEFAULT_DATAGRAM_BUFFER: usize = 2 * 1024 * 1024;
const DEFAULT_CONFIG_PATH: &str = "ops/solana-quic-proxy.toml";
const DEFAULT_REQUEST_TIMEOUT_MS: u64 = 1200;
const DEFAULT_HEDGED_ATTEMPTS: u32 = 1;
const DEFAULT_HEDGE_JITTER_MS: u64 = 25;
const DEFAULT_ENABLE_EARLY_DATA: bool = true;
const DEFAULT_PREOPEN_STREAMS: u32 = 0;

#[derive(Parser, Debug, Clone)]
#[command(
    author,
    version,
    about = "QUIC → JSON-RPC proxy for solana-ultra-rpc",
    rename_all = "kebab-case"
)]
pub struct CliArgs {
    /// Path to a TOML configuration file.
    #[arg(long, value_name = "PATH", env = "SOLANA_QUIC_PROXY_CONFIG")]
    pub config: Option<PathBuf>,

    /// Socket address to bind the HTTP ingress server on.
    #[arg(long)]
    pub listen: Option<SocketAddr>,

    /// QUIC upstream (solana-ultra-rpc) socket address.
    #[arg(long)]
    pub upstream: Option<SocketAddr>,

    /// TLS server name used for SNI when connecting upstream.
    #[arg(long)]
    pub server_name: Option<String>,

    /// Optional PEM bundle with trusted CA certificates.
    #[arg(long, value_name = "PATH")]
    pub ca_cert: Option<PathBuf>,

    /// Maximum accepted JSON-RPC request body size in bytes.
    #[arg(long)]
    pub max_request_bytes: Option<usize>,

    /// Maximum JSON-RPC response body size in bytes.
    #[arg(long)]
    pub max_response_bytes: Option<usize>,

    /// Maximum number of concurrent bi-directional streams per QUIC connection.
    #[arg(long)]
    pub max_streams: Option<u32>,

    /// Interval for QUIC keep-alive pings in milliseconds (0 disables keep-alives).
    #[arg(long)]
    pub keep_alive_ms: Option<u64>,

    /// Maximum idle timeout for the QUIC connection in milliseconds (0 for infinite).
    #[arg(long)]
    pub max_idle_timeout_ms: Option<u64>,

    /// Initial QUIC MTU advertised to the upstream.
    #[arg(long)]
    pub initial_mtu: Option<u16>,

    /// Stream-level receive window in bytes.
    #[arg(long)]
    pub stream_receive_window: Option<u64>,

    /// Connection-level receive window in bytes.
    #[arg(long)]
    pub connection_receive_window: Option<u64>,

    /// Connection-level send window in bytes.
    #[arg(long)]
    pub send_window: Option<u64>,

    /// Size of the QUIC datagram send buffer in bytes (0 uses Quinn default).
    #[arg(long)]
    pub datagram_send_buffer: Option<usize>,

    /// Size of the QUIC datagram receive buffer in bytes (0 uses Quinn default).
    #[arg(long)]
    pub datagram_recv_buffer: Option<usize>,

    #[arg(long, default_value_t = false)]
    pub lazy_connect: bool,

    /// Enable per-request HTTP tracing logs (adds overhead; default off).
    #[arg(long, default_value_t = false)]
    pub http_trace: bool,

    /// Per-request deadline in milliseconds (0 disables timeout).
    #[arg(long)]
    pub request_timeout_ms: Option<u64>,

    /// Number of hedged attempts per request (1 disables hedging).
    #[arg(long)]
    pub hedged_attempts: Option<u32>,

    /// Delay before launching a hedged attempt in milliseconds.
    #[arg(long)]
    pub hedge_jitter_ms: Option<u64>,

    /// Allow TLS early data (0-RTT) for idempotent RPCs.
    #[arg(long)]
    pub enable_early_data: Option<bool>,

    /// Number of bi-directional streams to pre-open during warmup.
    #[arg(long)]
    pub preopen_streams: Option<u32>,
}

#[derive(Debug, Clone)]
pub struct Config {
    pub listen: SocketAddr,
    pub upstream: SocketAddr,
    pub server_name: String,
    pub ca_cert: Option<PathBuf>,
    pub max_request_bytes: usize,
    pub max_response_bytes: usize,
    pub max_streams: u32,
    pub keep_alive: Option<Duration>,
    pub max_idle_timeout: Option<Duration>,
    pub initial_mtu: u16,
    pub stream_receive_window: u64,
    pub connection_receive_window: u64,
    pub send_window: u64,
    pub datagram_send_buffer: Option<usize>,
    pub datagram_recv_buffer: Option<usize>,
    pub lazy_connect: bool,
    pub config_path: Option<PathBuf>,
    pub http_trace: bool,
    pub request_timeout: Option<Duration>,
    pub hedged_attempts: u32,
    pub hedge_jitter: Duration,
    pub enable_early_data: bool,
    pub preopen_streams: u32,
}

#[derive(Debug, Deserialize, Default)]
struct FileConfig {
    listen: Option<SocketAddr>,
    upstream: Option<SocketAddr>,
    server_name: Option<String>,
    ca_cert: Option<PathBuf>,
    max_request_bytes: Option<usize>,
    max_response_bytes: Option<usize>,
    max_streams: Option<u32>,
    keep_alive_ms: Option<u64>,
    max_idle_timeout_ms: Option<u64>,
    initial_mtu: Option<u16>,
    stream_receive_window: Option<u64>,
    connection_receive_window: Option<u64>,
    send_window: Option<u64>,
    datagram_send_buffer: Option<usize>,
    datagram_recv_buffer: Option<usize>,
    lazy_connect: Option<bool>,
    http_trace: Option<bool>,
    request_timeout_ms: Option<u64>,
    hedged_attempts: Option<u32>,
    hedge_jitter_ms: Option<u64>,
    enable_early_data: Option<bool>,
    preopen_streams: Option<u32>,
}

impl Config {
    pub fn from_cli(cli: &CliArgs) -> Result<Self> {
        let file_cfg =
            load_file_config(cli.config.as_deref()).context("failed to load config file")?;
        let config = merge(cli, file_cfg)?;
        config.validate()?;
        config.log_summary();
        Ok(config)
    }

    fn validate(&self) -> Result<()> {
        if self.max_request_bytes == 0 {
            bail!("max_request_bytes must be greater than 0");
        }
        if self.max_request_bytes > u32::MAX as usize {
            bail!("max_request_bytes must not exceed 4GiB (u32 frame limit)");
        }
        if self.max_response_bytes == 0 {
            bail!("max_response_bytes must be greater than 0");
        }
        if self.max_response_bytes > u32::MAX as usize {
            bail!("max_response_bytes must not exceed 4GiB (u32 frame limit)");
        }
        if self.max_streams == 0 {
            bail!("max_streams must be greater than 0");
        }
        if self.initial_mtu < 1200 {
            bail!("initial_mtu must be at least 1200 bytes");
        }
        if VarInt::try_from(self.stream_receive_window).is_err() {
            bail!("stream_receive_window exceeds QUIC VarInt maximum");
        }
        if VarInt::try_from(self.connection_receive_window).is_err() {
            bail!("connection_receive_window exceeds QUIC VarInt maximum");
        }
        if self.send_window == 0 {
            bail!("send_window must be greater than 0");
        }
        if let Some(buf) = self.datagram_send_buffer {
            if buf == 0 {
                bail!("datagram_send_buffer must be greater than 0 when specified");
            }
        }
        if let Some(buf) = self.datagram_recv_buffer {
            if buf == 0 {
                bail!("datagram_recv_buffer must be greater than 0 when specified");
            }
        }
        Ok(())
    }

    fn log_summary(&self) {
        info!(
            listen = %self.listen,
            upstream = %self.upstream,
            server_name = %self.server_name,
            keep_alive = ?self.keep_alive,
            idle_timeout = ?self.max_idle_timeout,
            max_streams = self.max_streams,
            mtu = self.initial_mtu,
            stream_window = self.stream_receive_window,
            connection_window = self.connection_receive_window,
            send_window = self.send_window,
            datagram_send = ?self.datagram_send_buffer,
            datagram_recv = ?self.datagram_recv_buffer,
            lazy_connect = self.lazy_connect,
            http_trace = self.http_trace,
            request_timeout = ?self.request_timeout,
            hedged_attempts = self.hedged_attempts,
            hedge_jitter_ms = self.hedge_jitter.as_millis(),
            enable_early_data = self.enable_early_data,
            "solana-quic-proxy configuration"
        );
    }
}

fn merge(cli: &CliArgs, file_cfg: Option<(PathBuf, FileConfig)>) -> Result<Config> {
    let (cfg_path, file_cfg) = file_cfg.unzip();
    let file_cfg = file_cfg.unwrap_or_default();

    let listen = pick(cli.listen, file_cfg.listen, DEFAULT_LISTEN.parse().unwrap());
    let upstream = pick(
        cli.upstream,
        file_cfg.upstream,
        DEFAULT_UPSTREAM.parse().unwrap(),
    );
    let server_name = pick(
        cli.server_name.clone(),
        file_cfg.server_name,
        DEFAULT_SERVER_NAME.to_string(),
    );
    let ca_cert = cli.ca_cert.clone().or(file_cfg.ca_cert);
    let max_request_bytes = pick(
        cli.max_request_bytes,
        file_cfg.max_request_bytes,
        DEFAULT_MAX_REQUEST_BYTES,
    );
    let max_response_bytes = pick(
        cli.max_response_bytes,
        file_cfg.max_response_bytes,
        DEFAULT_MAX_RESPONSE_BYTES,
    );
    let max_streams = pick(cli.max_streams, file_cfg.max_streams, DEFAULT_MAX_STREAMS);

    let keep_alive_ms = pick(
        cli.keep_alive_ms,
        file_cfg.keep_alive_ms,
        DEFAULT_KEEP_ALIVE_MS,
    );
    let keep_alive = if keep_alive_ms == 0 {
        None
    } else {
        Some(Duration::from_millis(keep_alive_ms))
    };

    let max_idle_timeout_ms = pick(
        cli.max_idle_timeout_ms,
        file_cfg.max_idle_timeout_ms,
        DEFAULT_MAX_IDLE_TIMEOUT_MS,
    );
    let max_idle_timeout = if max_idle_timeout_ms == 0 {
        None
    } else {
        Some(Duration::from_millis(max_idle_timeout_ms))
    };

    let initial_mtu = pick(cli.initial_mtu, file_cfg.initial_mtu, DEFAULT_INITIAL_MTU);
    let stream_receive_window = pick(
        cli.stream_receive_window,
        file_cfg.stream_receive_window,
        DEFAULT_STREAM_WINDOW,
    );
    let connection_receive_window = pick(
        cli.connection_receive_window,
        file_cfg.connection_receive_window,
        DEFAULT_CONNECTION_WINDOW,
    );
    let send_window = pick(cli.send_window, file_cfg.send_window, DEFAULT_SEND_WINDOW);

    let datagram_send_buffer =
        resolve_datagram(cli.datagram_send_buffer, file_cfg.datagram_send_buffer);
    let datagram_recv_buffer =
        resolve_datagram(cli.datagram_recv_buffer, file_cfg.datagram_recv_buffer);

    let lazy_connect = cli.lazy_connect || file_cfg.lazy_connect.unwrap_or(false);
    let http_trace = cli.http_trace || file_cfg.http_trace.unwrap_or(false);

    let request_timeout_ms = pick(
        cli.request_timeout_ms,
        file_cfg.request_timeout_ms,
        DEFAULT_REQUEST_TIMEOUT_MS,
    );
    let request_timeout = if request_timeout_ms == 0 {
        None
    } else {
        Some(Duration::from_millis(request_timeout_ms))
    };
    let hedged_attempts = pick(
        cli.hedged_attempts,
        file_cfg.hedged_attempts,
        DEFAULT_HEDGED_ATTEMPTS,
    );
    let hedge_jitter_ms = pick(
        cli.hedge_jitter_ms,
        file_cfg.hedge_jitter_ms,
        DEFAULT_HEDGE_JITTER_MS,
    );
    let enable_early_data = pick(
        cli.enable_early_data,
        file_cfg.enable_early_data,
        DEFAULT_ENABLE_EARLY_DATA,
    );
    let preopen_streams = pick(
        cli.preopen_streams,
        file_cfg.preopen_streams,
        DEFAULT_PREOPEN_STREAMS,
    );

    Ok(Config {
        listen,
        upstream,
        server_name,
        ca_cert,
        max_request_bytes,
        max_response_bytes,
        max_streams,
        keep_alive,
        max_idle_timeout,
        initial_mtu,
        stream_receive_window,
        connection_receive_window,
        send_window,
        datagram_send_buffer,
        datagram_recv_buffer,
        lazy_connect,
        config_path: cfg_path,
        http_trace,
        request_timeout,
        hedged_attempts,
        hedge_jitter: Duration::from_millis(hedge_jitter_ms),
        enable_early_data,
        preopen_streams,
    })
}

fn pick<T: Clone>(cli: Option<T>, file: Option<T>, default: T) -> T {
    cli.or(file).unwrap_or(default)
}

fn resolve_datagram(cli: Option<usize>, file: Option<usize>) -> Option<usize> {
    match cli.or(file) {
        Some(0) => None,
        Some(value) => Some(value),
        None => Some(DEFAULT_DATAGRAM_BUFFER),
    }
}

fn load_file_config(path: Option<&Path>) -> Result<Option<(PathBuf, FileConfig)>> {
    if let Some(path) = path {
        return read_config(path).map(|cfg| Some((path.to_path_buf(), cfg)));
    }

    let default_path = PathBuf::from(DEFAULT_CONFIG_PATH);
    if default_path.exists() {
        return read_config(&default_path).map(|cfg| Some((default_path, cfg)));
    }

    Ok(None)
}

fn read_config(path: &Path) -> Result<FileConfig> {
    if !path.exists() {
        bail!("config file {} does not exist", path.display());
    }
    let data = fs::read_to_string(path)
        .with_context(|| format!("failed to read config file {}", path.display()))?;
    let cfg: FileConfig = toml::from_str(&data)
        .with_context(|| format!("failed to parse config file {}", path.display()))?;
    Ok(cfg)
}
