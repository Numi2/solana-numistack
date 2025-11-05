// Numan Thabit 2025
use std::{io::IoSlice, net::SocketAddr, sync::Arc, time::Duration};

use anyhow::{bail, Context, Result};
use arc_swap::ArcSwapOption;
use bytes::{Bytes, BytesMut};
use quinn::congestion::BbrConfig;
use quinn::crypto::rustls::QuicClientConfig;
use quinn::rustls::{
    client::{ClientSessionMemoryCache, Resumption},
    pki_types::CertificateDer,
    ClientConfig as RustlsClientConfig, RootCertStore,
};
use quinn::{ClientConfig, Connection, Endpoint, IdleTimeout, VarInt};
use rustls_native_certs::load_native_certs;
use tokio::io::AsyncWriteExt;
use tokio::sync::Mutex;
use tokio::time::Instant;
use tracing::warn;

use crate::config::Config;
use crate::metrics::ProxyMetrics;

const FRAME_HEADER: usize = 4;

pub struct QuicRpcClient {
    endpoint: Endpoint,
    server_addr: SocketAddr,
    server_name: String,
    max_response_bytes: usize,
    metrics: Arc<ProxyMetrics>,
    connection: ArcSwapOption<Connection>,
    connect_lock: Mutex<()>,
    recv_buf: Mutex<BytesMut>,
}

pub struct ClientResponse {
    pub payload: Bytes,
    pub latency: Duration,
}

impl QuicRpcClient {
    pub fn new(config: Arc<Config>, metrics: Arc<ProxyMetrics>) -> Result<Self> {
        let client_config = build_client_config(&config)?;
        let bind_addr = SocketAddr::from(([0, 0, 0, 0], 0));
        let mut endpoint = Endpoint::client(bind_addr).context("failed to create QUIC endpoint")?;
        endpoint.set_default_client_config(client_config);
        let initial_recv_capacity = config.max_response_bytes.clamp(4 * 1024, 8 * 1024 * 1024);

        Ok(Self {
            endpoint,
            server_addr: config.upstream,
            server_name: config.server_name.clone(),
            max_response_bytes: config.max_response_bytes,
            metrics,
            connection: ArcSwapOption::from(None),
            connect_lock: Mutex::new(()),
            recv_buf: Mutex::new(BytesMut::with_capacity(initial_recv_capacity)),
        })
    }

    pub async fn warmup(&self) -> Result<(), ProxyError> {
        let _ = self.connection().await?;
        Ok(())
    }

    pub async fn request(&self, payload: &[u8]) -> Result<ClientResponse, ProxyError> {
        let connection = self.connection().await?;
        match self.request_inner(&connection, payload).await {
            Ok(response) => Ok(response),
            Err(err @ ProxyError::Connection(_))
            | Err(err @ ProxyError::Read(_))
            | Err(err @ ProxyError::Write(_))
            | Err(err @ ProxyError::IoWrite(_))
            | Err(err @ ProxyError::Protocol(_)) => {
                self.invalidate();
                Err(err)
            }
            Err(err) => Err(err),
        }
    }

    async fn connection(&self) -> Result<Connection, ProxyError> {
        if let Some(conn) = self.connection.load_full() {
            return Ok((*conn).clone());
        }

        let _guard = self.connect_lock.lock().await;

        if let Some(conn) = self.connection.load_full() {
            return Ok((*conn).clone());
        }

        let connecting = self
            .endpoint
            .connect(self.server_addr, &self.server_name)
            .map_err(ProxyError::Connect)?;

        // Try 0-RTT if we have a cached session; fall back to full handshake on failure.
        let connection = match connecting.into_0rtt() {
            Ok((conn, _zero_rtt)) => conn,
            Err(connecting) => connecting.await.map_err(ProxyError::Connection)?,
        };
        self.connection.store(Some(Arc::new(connection.clone())));
        Ok(connection)
    }

    fn invalidate(&self) {
        if let Some(conn) = self.connection.swap(None) {
            conn.close(0u32.into(), b"proxy reset");
            self.metrics.record_connection_reset();
        }
    }

    async fn request_inner(
        &self,
        connection: &Connection,
        payload: &[u8],
    ) -> Result<ClientResponse, ProxyError> {
        let start = Instant::now();
        let (mut send, mut recv) = connection.open_bi().await.map_err(ProxyError::Connection)?;

        // Write header + payload with a single vectored syscall, handling partials explicitly.
        let header = (payload.len() as u32).to_be_bytes();
        let mut header_pos = 0usize;
        let mut payload_pos = 0usize;
        while header_pos < header.len() || payload_pos < payload.len() {
            let bytes_written = if header_pos < header.len() && payload_pos < payload.len() {
                let slices = [
                    IoSlice::new(&header[header_pos..]),
                    IoSlice::new(&payload[payload_pos..]),
                ];
                send.write_vectored(&slices)
                    .await
                    .map_err(ProxyError::IoWrite)?
            } else if header_pos < header.len() {
                let slices = [IoSlice::new(&header[header_pos..])];
                send.write_vectored(&slices)
                    .await
                    .map_err(ProxyError::IoWrite)?
            } else {
                let slices = [IoSlice::new(&payload[payload_pos..])];
                send.write_vectored(&slices)
                    .await
                    .map_err(ProxyError::IoWrite)?
            };

            if bytes_written == 0 {
                return Err(ProxyError::IoWrite(std::io::Error::new(
                    std::io::ErrorKind::WriteZero,
                    "short write",
                )));
            }

            if header_pos < header.len() {
                let header_remaining = header.len() - header_pos;
                if bytes_written >= header_remaining {
                    header_pos = header.len();
                    payload_pos += bytes_written - header_remaining;
                } else {
                    header_pos += bytes_written;
                    continue;
                }
            } else {
                payload_pos += bytes_written;
            }
        }
        send.finish()
            .map_err(|err| ProxyError::Protocol(format!("stream closed: {err:?}")))?;

        let mut header = [0u8; FRAME_HEADER];
        recv.read_exact(&mut header)
            .await
            .map_err(ProxyError::from)?;
        let len = u32::from_be_bytes(header) as usize;
        if len > self.max_response_bytes {
            return Err(ProxyError::ResponseTooLarge {
                size: len,
                max: self.max_response_bytes,
            });
        }

        let mut buf = self.recv_buf.lock().await;
        let capacity = buf.capacity();
        if capacity < len {
            buf.reserve(len - capacity);
        }
        buf.resize(len, 0);
        recv.read_exact(&mut buf[..])
            .await
            .map_err(ProxyError::from)?;

        let payload = buf.split_to(len).freeze();
        Ok(ClientResponse {
            payload,
            latency: start.elapsed(),
        })
    }
}

fn build_client_config(config: &Config) -> Result<ClientConfig> {
    let mut roots = RootCertStore::empty();

    if let Some(path) = &config.ca_cert {
        let data = std::fs::read(path)
            .with_context(|| format!("failed to open CA bundle {}", path.display()))?;
        let mut reader = std::io::Cursor::new(data);
        let certs = rustls_pemfile::certs(&mut reader).context("failed to parse CA bundle")?;
        let der: Vec<CertificateDer<'static>> =
            certs.into_iter().map(CertificateDer::from).collect();
        let (added, skipped) = roots.add_parsable_certificates(der.iter().cloned());
        if added == 0 {
            bail!("no valid certificates found in CA bundle (skipped {skipped} entries)");
        }
    } else {
        let native: Vec<CertificateDer<'static>> = load_native_certs()
            .context("failed to load native root certificates")?
            .into_iter()
            .map(|cert| CertificateDer::from(cert.0))
            .collect();
        let (added, skipped) = roots.add_parsable_certificates(native.iter().cloned());
        if added == 0 {
            bail!("no usable certificates found in system store (skipped {skipped} entries)");
        }
    }

    let mut crypto = RustlsClientConfig::builder()
        .with_root_certificates(roots)
        .with_no_client_auth();
    crypto.alpn_protocols = vec![b"jsonrpc-quic".to_vec()];
    // Enable 0-RTT: cache session tickets and allow early data (safe only for idempotent RPCs)
    crypto.resumption = Resumption::store(Arc::new(ClientSessionMemoryCache::new(256)));
    crypto.enable_early_data = true;

    let crypto = QuicClientConfig::try_from(crypto)
        .context("failed to convert TLS client config for QUIC")?;
    let mut client_config = ClientConfig::new(Arc::new(crypto));

    let mut transport = quinn::TransportConfig::default();
    if let Some(keep_alive) = config.keep_alive {
        transport.keep_alive_interval(Some(keep_alive));
    }
    if let Some(timeout) = config.max_idle_timeout {
        match IdleTimeout::try_from(timeout) {
            Ok(idle) => {
                transport.max_idle_timeout(Some(idle));
            }
            Err(err) => warn!(%err, "invalid max idle timeout; using default"),
        }
    }
    transport.max_concurrent_bidi_streams(VarInt::from_u32(config.max_streams));
    transport.max_concurrent_uni_streams(VarInt::from_u32(0));
    transport.send_fairness(false);

    let stream_window =
        VarInt::try_from(config.stream_receive_window).expect("stream_receive_window validated");
    let connection_window = VarInt::try_from(config.connection_receive_window)
        .expect("connection_receive_window validated");

    transport.stream_receive_window(stream_window);
    transport.receive_window(connection_window);
    transport.send_window(config.send_window);

    transport.initial_mtu(config.initial_mtu);

    if let Some(buffer) = config.datagram_send_buffer {
        transport.datagram_send_buffer_size(buffer);
    }
    transport.datagram_receive_buffer_size(config.datagram_recv_buffer);

    transport.congestion_controller_factory(Arc::new(BbrConfig::default()));

    client_config.transport_config(Arc::new(transport));

    Ok(client_config)
}

#[derive(Debug, thiserror::Error)]
pub enum ProxyError {
    #[error("upstream connect failed: {0}")]
    Connect(quinn::ConnectError),
    #[error("connection error: {0}")]
    Connection(quinn::ConnectionError),
    #[error("stream write failed: {0}")]
    Write(quinn::WriteError),
    #[error("stream write failed: {0}")]
    IoWrite(std::io::Error),
    #[error("stream read failed: {0}")]
    Read(quinn::ReadError),
    #[error("response too large: {size} bytes (max {max})")]
    ResponseTooLarge { size: usize, max: usize },
    #[error("protocol violation: {0}")]
    Protocol(String),
}

impl From<quinn::ReadExactError> for ProxyError {
    fn from(err: quinn::ReadExactError) -> Self {
        match err {
            quinn::ReadExactError::FinishedEarly(_) => {
                ProxyError::Protocol("stream finished early".into())
            }
            quinn::ReadExactError::ReadError(read) => ProxyError::Read(read),
        }
    }
}
