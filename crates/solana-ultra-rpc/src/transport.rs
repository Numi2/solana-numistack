// Numan Thabit 201337

use std::sync::Arc;

use anyhow::Result;
use quinn::crypto::rustls::QuicServerConfig;
use quinn::{Connection, Endpoint, ReadExactError, ServerConfig, TransportConfig, VarInt};
use rcgen::{CertificateParams, DistinguishedName, DnType, SanType};
use rustls::pki_types::{CertificateDer, PrivateKeyDer};
use serde::ser::SerializeStruct;
use serde::{Deserialize, Serialize};
use serde_json::value::RawValue;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, instrument};

use crate::config::UltraRpcConfig;
use crate::rpc::{RpcCallError, RpcRouter};

/// Length prefix size for framing (u32 big endian).
const FRAME_HEADER: usize = 4;
/// Hard limit on JSON-RPC payload size to guard against adversarial allocations.
const MAX_FRAME_LEN: usize = 1 << 20; // 1 MiB
/// Default allocation size for inbound/outbound frame buffers.
const DEFAULT_FRAME_CAPACITY: usize = 16 * 1024;

/// RPC server bound to a QUIC endpoint.
pub struct QuicRpcServer {
    endpoint: Endpoint,
    shutdown: CancellationToken,
    join: JoinHandle<()>,
}

impl QuicRpcServer {
    /// Bind a new QUIC listener and start accepting JSON-RPC traffic.
    pub async fn bind(config: &UltraRpcConfig, router: Arc<RpcRouter>) -> Result<Self> {
        let server_config = build_server_config(config.max_streams)?;
        let endpoint = Endpoint::server(server_config, config.rpc_bind)?;
        info!(addr = %config.rpc_bind, "solana-ultra-rpc listening on QUIC");

        let shutdown = CancellationToken::new();
        let accept_shutdown = shutdown.clone();
        let listener = endpoint.clone();
        let join = tokio::spawn(async move {
            accept_loop(listener, router, accept_shutdown).await;
        });

        Ok(Self {
            endpoint,
            shutdown,
            join,
        })
    }

    /// Initiate shutdown and wait for the accept loop to finish.
    pub async fn close(self) {
        self.shutdown.cancel();
        self.endpoint.close(0u32.into(), b"shutdown");
        let _ = self.join.await;
    }
}

async fn accept_loop(endpoint: Endpoint, router: Arc<RpcRouter>, shutdown: CancellationToken) {
    loop {
        tokio::select! {
            biased;
            _ = shutdown.cancelled() => {
                info!("quic accept loop exiting");
                break;
            }
            incoming = endpoint.accept() => {
                match incoming {
                    Some(connecting) => {
                        let router = router.clone();
                        let shutdown = shutdown.clone();
                        tokio::spawn(async move {
                            match connecting.await {
                                Ok(connection) => {
                                    if let Err(err) = handle_connection(connection, router, shutdown).await {
                                        error!(error = %err, "connection task failed");
                                    }
                                }
                                Err(err) => {
                                    error!(error = %err, "failed to establish quic connection");
                                }
                            }
                        });
                    }
                    None => break,
                }
            }
        }
    }
}

#[instrument(skip(connection, router, shutdown))]
async fn handle_connection(
    connection: Connection,
    router: Arc<RpcRouter>,
    shutdown: CancellationToken,
) -> Result<()> {
    loop {
        tokio::select! {
            _ = shutdown.cancelled() => {
                debug!("connection cancelled");
                break;
            }
            stream = connection.accept_bi() => {
                match stream {
                    Ok((mut send, mut recv)) => {
                        let router = router.clone();
                        tokio::spawn(async move {
                            if let Err(err) = handle_stream(&router, &mut send, &mut recv).await {
                                error!(error = %err, "stream handler error");
                            }
                            let _ = send.finish();
                        });
                    }
                    Err(err) => {
                        debug!(error = %err, "bi stream accept ended");
                        break;
                    }
                }
            }
        }
    }
    Ok(())
}

struct StreamBuffers {
    payload: Vec<u8>,
    response: Vec<u8>,
}

impl StreamBuffers {
    #[inline]
    fn new() -> Self {
        Self {
            payload: Vec::with_capacity(DEFAULT_FRAME_CAPACITY),
            response: Vec::with_capacity(DEFAULT_FRAME_CAPACITY + FRAME_HEADER),
        }
    }

    #[inline]
    fn begin_response(&mut self) {
        self.response.clear();
        self.response.resize(FRAME_HEADER, 0);
    }

    async fn read_payload(&mut self, recv: &mut quinn::RecvStream, len: usize) -> Result<()> {
        self.payload.clear();
        self.payload.resize(len, 0);
        recv.read_exact(&mut self.payload).await?;
        Ok(())
    }
}

async fn handle_stream(
    router: &RpcRouter,
    send: &mut quinn::SendStream,
    recv: &mut quinn::RecvStream,
) -> Result<()> {
    let mut header = [0u8; FRAME_HEADER];
    let mut buffers = StreamBuffers::new();

    loop {
        match recv.read_exact(&mut header).await {
            Ok(()) => {}
            Err(ReadExactError::FinishedEarly(_)) => break,
            Err(ReadExactError::ReadError(err)) => return Err(err.into()),
        }

        let len = u32::from_be_bytes(header) as usize;
        if len == 0 || len > MAX_FRAME_LEN {
            // Send a JSON-RPC error instead of closing abruptly, to avoid client read errors.
            let id = JsonRpcId::from_raw(None);
            let response: JsonRpcMessage<()> = JsonRpcMessage::error(id, RpcCallError::invalid_request());
            buffers.begin_response();
            serde_json::to_writer(&mut buffers.response, &response)?;
            let frame_len = buffers.response.len() - FRAME_HEADER;
            buffers.response[..FRAME_HEADER].copy_from_slice(&(frame_len as u32).to_be_bytes());
            send.write_all(&buffers.response).await?;
            continue;
        }

        buffers.read_payload(recv, len).await?;

        // Decide if this is a batch (first non-whitespace is '[')
        let is_batch = buffers
            .payload
            .iter()
            .copied()
            .skip_while(|b| matches!(b, b' ' | b'\n' | b'\r' | b'\t'))
            .next()
            .map(|b| b == b'[')
            .unwrap_or(false);

        // Parse request(s); fall back to an error response on malformed JSON
        // Use serde_json for robustness; performance is dominated by cache lookup
        buffers.begin_response();
        if is_batch {
            // Batch request: parse an array of requests
            let parsed: Result<Vec<JsonRpcRequest<'_>>, _> =
                serde_json::from_slice(&buffers.payload);
            match parsed {
                Ok(reqs) if !reqs.is_empty() => {
                    debug!(count = reqs.len(), bytes = buffers.payload.len(), "rpc batch received");
                    let mut out: Vec<JsonRpcMessage<_>> = Vec::with_capacity(reqs.len());
                    for JsonRpcRequest { id, method, params, .. } in reqs {
                        let id = JsonRpcId::from_raw(id);
                        let msg = match router.handle(method, params).await {
                            Ok(result) => JsonRpcMessage::success(id.clone(), result),
                            Err(err) => JsonRpcMessage::error(id, err),
                        };
                        out.push(msg);
                    }
                    serde_json::to_writer(&mut buffers.response, &out)?;
                }
                Ok(_empty) => {
                    // Empty batch is an invalid request per JSON-RPC 2.0
                    let id = JsonRpcId::from_raw(None);
                    let resp: JsonRpcMessage<()> =
                        JsonRpcMessage::error(id, RpcCallError::invalid_request());
                    serde_json::to_writer(&mut buffers.response, &resp)?;
                }
                Err(_) => {
                    // Try a best-effort generic parse to salvage
                    match serde_json::from_slice::<serde_json::Value>(&buffers.payload) {
                        Ok(val) => {
                            // If it is an array but contents are unusable, still return invalid request
                            if val.is_array() {
                                let id = JsonRpcId::from_raw(None);
                                let resp: JsonRpcMessage<()> =
                                    JsonRpcMessage::error(id, RpcCallError::invalid_request());
                                serde_json::to_writer(&mut buffers.response, &resp)?;
                            } else {
                                let method = val
                                    .get("method")
                                    .and_then(|m| m.as_str())
                                    .unwrap_or("");
                                let id = JsonRpcId::from_json_value(
                                    val.get("id").unwrap_or(&serde_json::Value::Null),
                                );
                                let resp = match router.handle(method, None).await {
                                    Ok(result) => JsonRpcMessage::success(id.clone(), result),
                                    Err(err) => JsonRpcMessage::error(id, err),
                                };
                                simd_json::to_writer(&mut buffers.response, &resp)?;
                            }
                        }
                        Err(_) => {
                            let preview = if buffers.payload.len() <= 256 {
                                String::from_utf8_lossy(&buffers.payload).into_owned()
                            } else {
                                let mut s =
                                    String::from_utf8_lossy(&buffers.payload[..256]).into_owned();
                                s.push_str("…");
                                s
                            };
                            debug!(len = buffers.payload.len(), preview = %preview, "json parse failed");
                            let id = JsonRpcId::from_raw(None);
                            let resp: JsonRpcMessage<()> =
                                JsonRpcMessage::error(id, RpcCallError::invalid_request());
                            serde_json::to_writer(&mut buffers.response, &resp)?;
                        }
                    }
                }
            }
        } else {
            // Single-request path
            let parsed: Result<JsonRpcRequest<'_>, _> =
                serde_json::from_slice(&buffers.payload);
            match parsed {
                Ok(JsonRpcRequest { id, method, params, .. }) => {
                    debug!(method = %method, bytes = buffers.payload.len(), "rpc request received");
                    let id = JsonRpcId::from_raw(id);
                    let resp = match router.handle(method, params).await {
                        Ok(result) => JsonRpcMessage::success(id.clone(), result),
                        Err(err) => JsonRpcMessage::error(id, err),
                    };
                    serde_json::to_writer(&mut buffers.response, &resp)?;
                }
                Err(_) => {
                    // Try a best-effort generic parse to salvage the request
                    match serde_json::from_slice::<serde_json::Value>(&buffers.payload) {
                        Ok(val) => {
                            let method = val
                                .get("method")
                                .and_then(|m| m.as_str())
                                .unwrap_or("");
                            let id = JsonRpcId::from_json_value(
                                val.get("id").unwrap_or(&serde_json::Value::Null),
                            );
                            let resp = match router.handle(method, None).await {
                                Ok(result) => JsonRpcMessage::success(id.clone(), result),
                                Err(err) => JsonRpcMessage::error(id, err),
                            };
                            serde_json::to_writer(&mut buffers.response, &resp)?;
                        }
                        Err(_) => {
                            let preview = if buffers.payload.len() <= 256 {
                                String::from_utf8_lossy(&buffers.payload).into_owned()
                            } else {
                                let mut s =
                                    String::from_utf8_lossy(&buffers.payload[..256]).into_owned();
                                s.push_str("…");
                                s
                            };
                            debug!(len = buffers.payload.len(), preview = %preview, "json parse failed");
                            let id = JsonRpcId::from_raw(None);
                            let resp: JsonRpcMessage<()> =
                                JsonRpcMessage::error(id, RpcCallError::invalid_request());
                            serde_json::to_writer(&mut buffers.response, &resp)?;
                        }
                    }
                }
            }
        }
        let frame_len = buffers.response.len() - FRAME_HEADER;
        anyhow::ensure!(
            frame_len <= MAX_FRAME_LEN,
            "response frame length {} exceeds max {}",
            frame_len,
            MAX_FRAME_LEN
        );
        buffers.response[..FRAME_HEADER].copy_from_slice(&(frame_len as u32).to_be_bytes());
        send.write_all(&buffers.response).await?;
    }

    Ok(())
}

fn build_server_config(max_streams: u32) -> Result<ServerConfig> {
    // Self-signed cert for embedded server (sufficient for QUIC RPC in trusted networks).
    let mut params = CertificateParams::new(vec![]);
    params.distinguished_name = DistinguishedName::new();
    params
        .distinguished_name
        .push(DnType::CommonName, "solana-ultra-rpc");
    params
        .subject_alt_names
        .push(SanType::IpAddress("127.0.0.1".parse()?));
    let cert = rcgen::Certificate::from_params(params)?;
    let key = PrivateKeyDer::Pkcs8(cert.serialize_private_key_der().into());
    let cert_der: CertificateDer<'static> = CertificateDer::from(cert.serialize_der()?);

    // Optionally export the self-signed certificate for clients to trust (development only)
    if let Ok(path) = std::env::var("ULTRA_RPC_CERT_OUT") {
        if let Ok(pem) = cert.serialize_pem() {
            let _ = std::fs::write(&path, pem);
            info!(path = %path, "wrote self-signed certificate (PEM)");
        }
    }

    // Build rustls server config to set ALPN and (implicitly) allow safe 0-RTT reads.
    let mut tls_config = rustls::ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(vec![cert_der], key)?;
    tls_config.alpn_protocols = vec![b"jsonrpc-quic".to_vec()];

    // Convert to Quinn server config with custom transport.
    let mut server_config =
        ServerConfig::with_crypto(Arc::new(QuicServerConfig::try_from(tls_config)?));
    let mut transport = TransportConfig::default();
    transport.max_concurrent_bidi_streams(VarInt::from_u32(max_streams));
    transport.keep_alive_interval(Some(std::time::Duration::from_secs(3)));
    server_config.transport_config(Arc::new(transport));
    Ok(server_config)
}

#[derive(Debug, Deserialize)]
struct JsonRpcRequest<'a> {
    #[serde(default = "default_jsonrpc")]
    _jsonrpc: &'a str,
    #[serde(default)]
    #[serde(borrow)]
    id: Option<&'a RawValue>,
    #[serde(borrow)]
    method: &'a str,
    #[serde(default)]
    #[serde(borrow)]
    params: Option<&'a RawValue>,
}

enum JsonRpcMessage<T>
where
    T: Serialize,
{
    Success { id: JsonRpcId, result: T },
    Error { id: JsonRpcId, error: RpcCallError },
}

impl<T> JsonRpcMessage<T>
where
    T: Serialize,
{
    fn success(id: JsonRpcId, result: T) -> Self {
        Self::Success { id, result }
    }

    fn error(id: JsonRpcId, error: RpcCallError) -> Self {
        Self::Error { id, error }
    }
}

impl<T> Serialize for JsonRpcMessage<T>
where
    T: Serialize,
{
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut state = serializer.serialize_struct("JsonRpcResponse", 3)?;
        state.serialize_field("jsonrpc", "2.0")?;
        match self {
            JsonRpcMessage::Success { id, result } => {
                state.serialize_field("id", id)?;
                state.serialize_field("result", result)?;
            }
            JsonRpcMessage::Error { id, error } => {
                state.serialize_field("id", id)?;
                state.serialize_field("error", error)?;
            }
        }
        state.end()
    }
}

#[derive(Clone)]
struct JsonRpcId {
    raw: Option<Box<RawValue>>,
}

impl JsonRpcId {
    fn from_raw(raw: Option<&RawValue>) -> Self {
        match raw {
            Some(value) => {
                let owned =
                    RawValue::from_string(value.get().to_owned()).expect("valid JSON-RPC id");
                Self { raw: Some(owned) }
            }
            None => Self { raw: None },
        }
    }
}

impl Serialize for JsonRpcId {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        match &self.raw {
            Some(raw) => raw.serialize(serializer),
            None => serializer.serialize_unit(),
        }
    }
}

fn default_jsonrpc() -> &'static str {
    "2.0"
}

impl JsonRpcId {
    #[inline]
    fn from_json_value(value: &serde_json::Value) -> Self {
        match serde_json::to_string(value)
            .ok()
            .and_then(|s| serde_json::value::RawValue::from_string(s).ok())
        {
            Some(raw) => Self { raw: Some(raw) },
            None => Self { raw: None },
        }
    }
}
