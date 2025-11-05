// Numan Thabit 201337

use std::sync::Arc;

use anyhow::Result;
use quinn::{Connection, Endpoint, ReadExactError, ServerConfig, TransportConfig, VarInt};
use quinn::crypto::rustls::QuicServerConfig;
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
            anyhow::bail!("invalid frame length {len}; max allowed {MAX_FRAME_LEN}");
        }

        buffers.read_payload(recv, len).await?;

        let request: JsonRpcRequest<'_> = simd_json::from_slice(buffers.payload.as_mut_slice())?;
        let JsonRpcRequest {
            id,
            method,
            params,
            ..
        } = request;
        // Only idempotent read-only methods are supported; future mutating methods should be
        // rejected on early-data paths.
        let response = match router.handle(method, params).await {
            Ok(result) => JsonRpcMessage::success(id, result),
            Err(err) => JsonRpcMessage::error(id, err),
        };
        buffers.begin_response();
        simd_json::to_writer(&mut buffers.response, &response)?;
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

    // Build rustls server config to set ALPN and (implicitly) allow safe 0-RTT reads.
    let mut tls_config = rustls::ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(vec![cert_der], key)?;
    tls_config.alpn_protocols = vec![b"jsonrpc-quic".to_vec()];

    // Convert to Quinn server config with custom transport.
    let mut server_config = ServerConfig::with_crypto(Arc::new(QuicServerConfig::try_from(
        tls_config,
    )?));
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

enum JsonRpcMessage<'a, T>
where
    T: Serialize,
{
    Success {
        id: Option<&'a RawValue>,
        result: T,
    },
    Error {
        id: Option<&'a RawValue>,
        error: RpcCallError,
    },
}

impl<'a, T> JsonRpcMessage<'a, T>
where
    T: Serialize,
{
    fn success(id: Option<&'a RawValue>, result: T) -> Self {
        Self::Success { id, result }
    }

    fn error(id: Option<&'a RawValue>, error: RpcCallError) -> Self {
        Self::Error { id, error }
    }
}

impl<'a, T> Serialize for JsonRpcMessage<'a, T>
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
                match id {
                    Some(raw) => state.serialize_field("id", raw)?,
                    None => state.serialize_field("id", &())?,
                }
                state.serialize_field("result", result)?;
            }
            JsonRpcMessage::Error { id, error } => {
                match id {
                    Some(raw) => state.serialize_field("id", raw)?,
                    None => state.serialize_field("id", &())?,
                }
                state.serialize_field("error", error)?;
            }
        }
        state.end()
    }
}

fn default_jsonrpc() -> &'static str {
    "2.0"
}
