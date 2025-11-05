// Numan Thabit 2025
#![forbid(unsafe_code)]
pub mod jito {
    // pub mod auth { tonic::include_proto!("auth"); } // Empty proto file
    pub mod bundle {
        tonic::include_proto!("bundle");
    }
    pub mod packet {
        tonic::include_proto!("packet");
    }
    pub mod shared {
        tonic::include_proto!("shared");
    }
    pub mod searcher {
        tonic::include_proto!("searcher");
    }
    // pub mod block_engine { tonic::include_proto!("block_engine"); } // Empty proto file
    // pub mod relayer { tonic::include_proto!("relayer"); } // Empty proto file
}

use futures_util::StreamExt;
use http::Uri;
use jito::bundle::{Bundle, BundleResult};
use jito::packet::{Meta, Packet, PacketFlags};
use jito::searcher::searcher_service_client::SearcherServiceClient;
use jito::searcher::{GetTipAccountsRequest, SendBundleRequest};
use prost_types::Timestamp;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use thiserror::Error;
use tokio::sync::mpsc;
use tokio::time::sleep;
use tokio_stream::wrappers::ReceiverStream;
use tonic::codec::CompressionEncoding;
use tonic::metadata::MetadataValue;
use tonic::transport::{Channel, ClientTlsConfig, Endpoint};
use tonic::Request;
use tracing::instrument;

#[derive(Debug, Error)]
pub enum Error {
    #[error("transport error: {0}")]
    Transport(#[from] tonic::transport::Error),
    #[error("rpc status: {0}")]
    Rpc(Box<tonic::Status>),
    #[error("invalid endpoint URI: {0}")]
    InvalidEndpoint(String),
    #[error("invalid metadata value: {0}")]
    InvalidMetadata(String),
}

pub type Result<T> = std::result::Result<T, Error>;

impl From<tonic::Status> for Error {
    fn from(status: tonic::Status) -> Self {
        Self::Rpc(Box::new(status))
    }
}

#[derive(Clone)]
pub struct JitoClient {
    inner: SearcherServiceClient<Channel>,
    shared: Arc<SharedClientState>,
}

#[derive(Debug)]
struct SharedClientState {
    config: ConnectConfig,
    retry: RetryConfig,
    endpoint: Endpoint,
}

#[derive(Clone, Debug)]
struct ConnectConfig {
    endpoint: String,
    bearer: Option<MetadataValue<tonic::metadata::Ascii>>,
    connect_timeout: Duration,
    rpc_timeout: Duration,
    init_conn_window: u32,
    init_stream_window: u32,
    keepalive_interval_ms: u64,
    keepalive_timeout_ms: u64,
    tcp_keepalive_secs: u64,
    concurrency_limit: usize,
    compression: bool,
}

#[derive(Clone, Debug)]
pub struct JitoClientBuilder {
    endpoint: String,
    bearer: Option<String>,
    connect_timeout: Duration,
    rpc_timeout: Duration,
    init_conn_window: u32,
    init_stream_window: u32,
    keepalive_interval_ms: u64,
    keepalive_timeout_ms: u64,
    tcp_keepalive_secs: u64,
    concurrency_limit: usize,
    compression: bool,
    retry_max_retries: u32,
    retry_initial_ms: u64,
    retry_max_ms: u64,
    retry_jitter_ms: u64,
}

impl JitoClientBuilder {
    pub fn new(endpoint: impl Into<String>) -> Self {
        let endpoint = endpoint.into();
        // Defaults + env overrides
        let env_u32 = |k: &str, d: u32| -> u32 {
            std::env::var(k)
                .ok()
                .and_then(|v| v.parse::<u32>().ok())
                .unwrap_or(d)
        };
        let env_u64 = |k: &str, d: u64| -> u64 {
            std::env::var(k)
                .ok()
                .and_then(|v| v.parse::<u64>().ok())
                .unwrap_or(d)
        };
        let env_usize = |k: &str, d: usize| -> usize {
            std::env::var(k)
                .ok()
                .and_then(|v| v.parse::<usize>().ok())
                .unwrap_or(d)
        };
        let env_bool = |k: &str, d: bool| -> bool {
            std::env::var(k)
                .ok()
                .and_then(|v| v.parse::<bool>().ok())
                .unwrap_or(d)
        };
        let connect_timeout = Duration::from_secs(env_u64("JITO_CONNECT_TIMEOUT_SECS", 3));
        let rpc_timeout = Duration::from_secs(env_u64("JITO_RPC_TIMEOUT_SECS", 5));
        Self {
            endpoint,
            bearer: std::env::var("JITO_BEARER").ok(),
            connect_timeout,
            rpc_timeout,
            init_conn_window: env_u32("JITO_INIT_CONN_WINDOW", 8 * 1024 * 1024),
            init_stream_window: env_u32("JITO_INIT_STREAM_WINDOW", 8 * 1024 * 1024),
            keepalive_interval_ms: env_u64("JITO_HTTP2_KEEPALIVE_INTERVAL_MS", 1_000),
            keepalive_timeout_ms: env_u64("JITO_HTTP2_KEEPALIVE_TIMEOUT_MS", 3_000),
            tcp_keepalive_secs: env_u64("JITO_TCP_KEEPALIVE_SECS", 30),
            concurrency_limit: env_usize("JITO_CONCURRENCY_LIMIT", 64),
            compression: env_bool("JITO_GZIP", false),
            retry_max_retries: env_u32("JITO_RETRIES", 5),
            retry_initial_ms: env_u64("JITO_RETRY_INITIAL_MS", 100),
            retry_max_ms: env_u64("JITO_RETRY_MAX_MS", 3_000),
            retry_jitter_ms: env_u64("JITO_RETRY_JITTER_MS", 13),
        }
    }

    pub fn bearer(mut self, bearer: impl Into<String>) -> Self {
        self.bearer = Some(bearer.into());
        self
    }

    pub fn connect_timeout(mut self, timeout: Duration) -> Self {
        self.connect_timeout = timeout;
        self
    }
    pub fn rpc_timeout(mut self, timeout: Duration) -> Self {
        self.rpc_timeout = timeout;
        self
    }
    pub fn http2_windows(mut self, conn: u32, stream: u32) -> Self {
        self.init_conn_window = conn;
        self.init_stream_window = stream;
        self
    }
    pub fn keepalive(mut self, interval_ms: u64, timeout_ms: u64) -> Self {
        self.keepalive_interval_ms = interval_ms;
        self.keepalive_timeout_ms = timeout_ms;
        self
    }
    pub fn tcp_keepalive(mut self, secs: u64) -> Self {
        self.tcp_keepalive_secs = secs;
        self
    }
    pub fn concurrency_limit(mut self, limit: usize) -> Self {
        self.concurrency_limit = limit;
        self
    }
    pub fn compression(mut self, enable_gzip: bool) -> Self {
        self.compression = enable_gzip;
        self
    }
    pub fn retries(mut self, max_retries: u32) -> Self {
        self.retry_max_retries = max_retries;
        self
    }
    pub fn retry_backoff(mut self, initial_ms: u64, max_ms: u64) -> Self {
        self.retry_initial_ms = initial_ms;
        self.retry_max_ms = max_ms;
        self
    }

    pub fn retry_jitter(mut self, jitter_ms: u64) -> Self {
        self.retry_jitter_ms = jitter_ms;
        self
    }

    #[instrument(name = "jito_client_connect", skip(self))]
    pub async fn connect(self) -> Result<JitoClient> {
        // Validate endpoint early
        let _uri: Uri = self
            .endpoint
            .parse()
            .map_err(|e: http::uri::InvalidUri| Error::InvalidEndpoint(e.to_string()))?;

        let bearer_md = match self.bearer {
            Some(token) => Some(
                MetadataValue::try_from(format!("Bearer {}", token))
                    .map_err(|e| Error::InvalidMetadata(e.to_string()))?,
            ),
            None => None,
        };

        let cfg = ConnectConfig {
            endpoint: self.endpoint,
            bearer: bearer_md,
            connect_timeout: self.connect_timeout,
            rpc_timeout: self.rpc_timeout,
            init_conn_window: self.init_conn_window,
            init_stream_window: self.init_stream_window,
            keepalive_interval_ms: self.keepalive_interval_ms,
            keepalive_timeout_ms: self.keepalive_timeout_ms,
            tcp_keepalive_secs: self.tcp_keepalive_secs,
            concurrency_limit: self.concurrency_limit,
            compression: self.compression,
        };

        let retry = RetryConfig {
            max_retries: self.retry_max_retries,
            initial_backoff_ms: self.retry_initial_ms,
            max_backoff_ms: self.retry_max_ms,
            fixed_jitter_ms: self.retry_jitter_ms,
        };

        JitoClient::connect_with_config_and_retry(cfg, retry).await
    }
}

impl JitoClient {
    async fn connect_with_config_and_retry(cfg: ConnectConfig, retry: RetryConfig) -> Result<Self> {
        let uri: Uri = cfg
            .endpoint
            .parse()
            .map_err(|e: http::uri::InvalidUri| Error::InvalidEndpoint(e.to_string()))?;
        let host = uri.host().unwrap_or("").to_string();
        let endpoint = Self::build_endpoint(&cfg, &host)?;
        let shared = Arc::new(SharedClientState {
            config: cfg,
            retry,
            endpoint,
        });

        Self::connect_with_shared(shared).await
    }

    async fn connect_with_shared(shared: Arc<SharedClientState>) -> Result<Self> {
        let channel = Self::dial_channel(&shared.endpoint, &shared.retry).await?;
        let inner = Self::make_client(channel, &shared.config);
        Ok(JitoClient { inner, shared })
    }

    async fn dial_channel(endpoint: &Endpoint, retry: &RetryConfig) -> Result<Channel> {
        let mut attempt: u32 = 0;
        let mut backoff_ms = retry.initial_backoff_ms.max(1);
        loop {
            match endpoint.clone().connect().await {
                Ok(channel) => return Ok(channel),
                Err(err) => {
                    if attempt >= retry.max_retries {
                        return Err(Error::Transport(err));
                    }
                    attempt += 1;
                    sleep(Duration::from_millis(
                        backoff_ms.saturating_add(retry.fixed_jitter_ms),
                    ))
                    .await;
                    backoff_ms = (backoff_ms.saturating_mul(2)).min(retry.max_backoff_ms.max(1));
                }
            }
        }
    }

    fn make_client(channel: Channel, cfg: &ConnectConfig) -> SearcherServiceClient<Channel> {
        let mut client = SearcherServiceClient::new(channel);
        if cfg.compression {
            client = client
                .send_compressed(CompressionEncoding::Gzip)
                .accept_compressed(CompressionEncoding::Gzip);
        }
        client
    }

    fn build_endpoint(cfg: &ConnectConfig, host: &str) -> Result<Endpoint> {
        let endpoint = Channel::from_shared(cfg.endpoint.clone())
            .map_err(|e: tonic::codegen::http::uri::InvalidUri| {
                Error::InvalidEndpoint(e.to_string())
            })?
            .tls_config(if !host.is_empty() {
                ClientTlsConfig::new().domain_name(host.to_owned())
            } else {
                ClientTlsConfig::new()
            })?
            .tcp_nodelay(true)
            .tcp_keepalive(Some(Duration::from_secs(cfg.tcp_keepalive_secs)))
            .http2_adaptive_window(true)
            .initial_connection_window_size(cfg.init_conn_window)
            .initial_stream_window_size(cfg.init_stream_window)
            .http2_keep_alive_interval(Duration::from_millis(cfg.keepalive_interval_ms))
            .keep_alive_timeout(Duration::from_millis(cfg.keepalive_timeout_ms))
            .keep_alive_while_idle(true)
            .concurrency_limit(cfg.concurrency_limit)
            .connect_timeout(cfg.connect_timeout)
            .timeout(cfg.rpc_timeout);
        Ok(endpoint)
    }
}

impl JitoClient {
    pub async fn connect(endpoint: &str) -> Result<Self> {
        JitoClientBuilder::new(endpoint).connect().await
    }

    pub async fn connect_with_bearer(endpoint: &str, bearer: &str) -> Result<Self> {
        JitoClientBuilder::new(endpoint)
            .bearer(bearer.to_string())
            .connect()
            .await
    }

    pub async fn get_tip_accounts(&mut self) -> Result<Vec<String>> {
        let mut attempt: u32 = 0;
        let mut backoff_ms = self.shared.retry.initial_backoff_ms;
        loop {
            let mut req = Request::new(GetTipAccountsRequest {});
            if let Some(auth) = self.shared.config.bearer.clone() {
                req.metadata_mut().insert("authorization", auth);
            }
            req.set_timeout(self.shared.config.rpc_timeout);
            match self.inner.get_tip_accounts(req).await {
                Ok(resp) => return Ok(resp.into_inner().accounts),
                Err(status) => {
                    if !is_retryable(status.code()) || attempt >= self.shared.retry.max_retries {
                        return Err(status.into());
                    }
                    attempt += 1;
                    if matches!(
                        status.code(),
                        tonic::Code::Unavailable | tonic::Code::Unknown
                    ) {
                        let _ = self.reconnect_in_place().await;
                    }
                    sleep(Duration::from_millis(
                        backoff_ms.saturating_add(self.shared.retry.fixed_jitter_ms),
                    ))
                    .await;
                    backoff_ms =
                        (backoff_ms.saturating_mul(2)).min(self.shared.retry.max_backoff_ms);
                }
            }
        }
    }

    /// Build a Jito bundle from raw signed transactions (wire-format, not base64)
    pub fn build_bundle_from_signed_txs(raw_txs: Vec<Vec<u8>>) -> Bundle {
        let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap();
        let header = jito::shared::Header {
            ts: Some(Timestamp {
                seconds: now.as_secs() as i64,
                nanos: now.subsec_nanos() as i32,
            }),
        };
        let mut packets = Vec::with_capacity(raw_txs.len());
        let flags = PacketFlags {
            discard: false,
            forwarded: false,
            repair: false,
            simple_vote_tx: false,
            tracer_packet: false,
            from_staked_node: true,
        };
        for tx in raw_txs {
            let tx_len = tx.len() as u64;
            packets.push(Packet {
                data: tx,
                meta: Some(Meta {
                    size: tx_len,
                    addr: String::new(),
                    port: 0,
                    flags: Some(flags),
                    sender_stake: 0,
                }),
            });
        }
        Bundle {
            header: Some(header),
            packets,
        }
    }

    pub async fn send_bundle(&mut self, bundle: Bundle) -> Result<String> {
        const HEDGE_DELAY_MS: u64 = 15;
        let mut attempt: u32 = 0;
        let mut backoff_ms = self.shared.retry.initial_backoff_ms;
        loop {
            // Build primary request with per-request deadline
            let mut req_primary = Request::new(SendBundleRequest {
                bundle: Some(bundle.clone()),
            });
            if let Some(auth) = self.shared.config.bearer.clone() {
                req_primary.metadata_mut().insert("authorization", auth);
            }
            req_primary.set_timeout(self.shared.config.rpc_timeout);

            // Clone client for primary path
            let mut primary_client = self.inner.clone();

            // Prepare secondary (hedged) future with a separate channel
            let cfg = self.shared.config.clone();
            let endpoint = self.shared.endpoint.clone();
            let retry = self.shared.retry.clone();
            let mut req_secondary = Request::new(SendBundleRequest {
                bundle: Some(bundle.clone()),
            });
            if let Some(auth) = self.shared.config.bearer.clone() {
                req_secondary.metadata_mut().insert("authorization", auth);
            }
            req_secondary.set_timeout(self.shared.config.rpc_timeout);

            let secondary_fut = async move {
                sleep(Duration::from_millis(HEDGE_DELAY_MS)).await;
                // Try to dial a fresh channel for true hedging
                match JitoClient::dial_channel(&endpoint, &retry).await {
                    Ok(ch) => {
                        let mut client = JitoClient::make_client(ch, &cfg);
                        client.send_bundle(req_secondary).await
                    }
                    Err(e) => Err(tonic::Status::unavailable(format!(
                        "hedge dial failed: {}",
                        e
                    ))),
                }
            };

            // Race primary vs secondary
            let res = tokio::select! {
                r = primary_client.send_bundle(req_primary) => r,
                r = secondary_fut => r,
            };

            match res {
                Ok(resp) => return Ok(resp.into_inner().uuid),
                Err(status) => {
                    if !is_retryable(status.code()) || attempt >= self.shared.retry.max_retries {
                        return Err(status.into());
                    }
                    attempt += 1;
                    if matches!(
                        status.code(),
                        tonic::Code::Unavailable | tonic::Code::Unknown
                    ) {
                        let _ = self.reconnect_in_place().await;
                    }
                    sleep(Duration::from_millis(
                        backoff_ms.saturating_add(self.shared.retry.fixed_jitter_ms),
                    ))
                    .await;
                    backoff_ms =
                        (backoff_ms.saturating_mul(2)).min(self.shared.retry.max_backoff_ms);
                }
            }
        }
    }

    pub async fn subscribe_bundle_results(&mut self) -> Result<tonic::Streaming<BundleResult>> {
        let resp = self
            .inner
            .subscribe_bundle_results(jito::searcher::SubscribeBundleResultsRequest {})
            .await?;
        Ok(resp.into_inner())
    }

    /// Auto-reconnecting stream wrapper for bundle results.
    /// Returns a stream that will keep trying to reconnect on transient errors/EOF,
    /// yielding `Result<BundleResult, Error>` items. Drop the stream to stop.
    pub fn subscribe_bundle_results_stream(&self) -> ReceiverStream<Result<BundleResult>> {
        let (tx, rx) = mpsc::channel::<Result<BundleResult>>(1024);
        let shared = Arc::clone(&self.shared);
        tokio::spawn(async move {
            let mut backoff_ms = shared.retry.initial_backoff_ms;
            loop {
                if tx.is_closed() {
                    break;
                }
                // Establish a fresh client
                let client_res = JitoClient::connect_with_shared(Arc::clone(&shared)).await;
                let mut client = match client_res {
                    Ok(c) => c,
                    Err(e) => {
                        let _ = tx.send(Err(e)).await;
                        sleep(Duration::from_millis(
                            backoff_ms.saturating_add(shared.retry.fixed_jitter_ms),
                        ))
                        .await;
                        backoff_ms =
                            (backoff_ms.saturating_mul(2)).min(shared.retry.max_backoff_ms);
                        continue;
                    }
                };

                let req = jito::searcher::SubscribeBundleResultsRequest {};
                let stream_res = client.inner.subscribe_bundle_results(req).await;
                let mut stream = match stream_res {
                    Ok(resp) => {
                        backoff_ms = shared.retry.initial_backoff_ms; // reset on success
                        resp.into_inner()
                    }
                    Err(status) => {
                        let _ = tx.send(Err(status.into())).await;
                        sleep(Duration::from_millis(
                            backoff_ms.saturating_add(shared.retry.fixed_jitter_ms),
                        ))
                        .await;
                        backoff_ms =
                            (backoff_ms.saturating_mul(2)).min(shared.retry.max_backoff_ms);
                        continue;
                    }
                };

                // Drain the stream until error or closed
                while let Some(item) = stream.next().await {
                    match item {
                        Ok(msg) => {
                            if tx.send(Ok(msg)).await.is_err() {
                                return;
                            }
                        }
                        Err(status) => {
                            let _ = tx.send(Err(status.into())).await;
                            break; // reconnect
                        }
                    }
                    if tx.is_closed() {
                        return;
                    }
                }
                // EOF or channel closed → reconnect with backoff
                sleep(Duration::from_millis(
                    backoff_ms.saturating_add(shared.retry.fixed_jitter_ms),
                ))
                .await;
                backoff_ms = (backoff_ms.saturating_mul(2)).min(shared.retry.max_backoff_ms);
            }
        });

        ReceiverStream::new(rx)
    }

    async fn reconnect_in_place(&mut self) -> Result<()> {
        let channel = Self::dial_channel(&self.shared.endpoint, &self.shared.retry).await?;
        self.inner = Self::make_client(channel, &self.shared.config);
        Ok(())
    }

    // call_with_retry removed; explicit retry logic is implemented per-RPC
}

#[derive(Clone, Debug)]
struct RetryConfig {
    max_retries: u32,
    initial_backoff_ms: u64,
    max_backoff_ms: u64,
    fixed_jitter_ms: u64,
}

fn is_retryable(code: tonic::Code) -> bool {
    use tonic::Code::*;
    matches!(
        code,
        Unavailable | DeadlineExceeded | ResourceExhausted | Aborted | Internal | Unknown
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_build_bundle_from_signed_txs() {
        let raw_txs = vec![vec![1u8, 2, 3], vec![4u8; 64]];
        let bundle = JitoClient::build_bundle_from_signed_txs(raw_txs.clone());
        assert!(bundle.header.is_some());
        assert_eq!(bundle.packets.len(), 2);
        let p0 = &bundle.packets[0];
        assert_eq!(p0.data, raw_txs[0]);
        let meta = p0.meta.as_ref().expect("meta");
        assert_eq!(meta.size, raw_txs[0].len() as u64);
        let flags = meta.flags.as_ref().expect("flags");
        assert!(flags.from_staked_node);
    }
}
