// Numan Thabit 2025
use std::{
    io::Write,
    net::SocketAddr,
    sync::{Arc, Once},
    time::Duration,
};

use anyhow::Result;
use clap::Parser;
use quinn::crypto::rustls::QuicServerConfig;
use rcgen::{BasicConstraints, Certificate, CertificateParams, IsCa};
use solana_quic_proxy::{
    client::QuicRpcClient,
    config::{CliArgs, Config},
    metrics::ProxyMetrics,
};
use tempfile::NamedTempFile;
use tokio::time::timeout;

fn install_crypto_provider() {
    static INIT: Once = Once::new();
    INIT.call_once(|| {
        rustls::crypto::ring::default_provider()
            .install_default()
            .expect("install ring crypto provider");
    });
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn eager_preconnect_succeeds() -> Result<()> {
    install_crypto_provider();

    // Generate a transient test CA
    let mut ca_params = CertificateParams::default();
    ca_params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
    let ca_cert = Certificate::from_params(ca_params)?;

    // Generate a server cert for localhost, signed by the CA
    let mut server_params = CertificateParams::new(["localhost".into()]);
    server_params.is_ca = IsCa::NoCa;
    let server_cert = Certificate::from_params(server_params)?;
    let server_der = server_cert.serialize_der_with_signer(&ca_cert)?;
    let cert_der = quinn::rustls::pki_types::CertificateDer::from(server_der);
    let key_der =
        quinn::rustls::pki_types::PrivatePkcs8KeyDer::from(server_cert.serialize_private_key_der());

    let mut tls_config = quinn::rustls::ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(vec![cert_der.clone()], key_der.into())?;
    tls_config.alpn_protocols = vec![b"jsonrpc-quic".to_vec()];

    let mut server_config =
        quinn::ServerConfig::with_crypto(Arc::new(QuicServerConfig::try_from(tls_config)?));
    let transport = Arc::get_mut(&mut server_config.transport).expect("unique transport");
    transport.keep_alive_interval(Some(Duration::from_secs(1)));
    transport.max_concurrent_bidi_streams(quinn::VarInt::from_u32(16));

    let server_addr: SocketAddr = "127.0.0.1:0".parse()?;
    let endpoint = quinn::Endpoint::server(server_config, server_addr)?;
    let upstream = endpoint.local_addr()?;
    let endpoint_task = endpoint.clone();

    tokio::spawn(async move {
        if let Some(connecting) = endpoint_task.accept().await {
            let _ = connecting.await;
        }
        endpoint_task.wait_idle().await;
    });

    let listen_addr: SocketAddr = "127.0.0.1:0".parse()?;
    let mut ca_file = NamedTempFile::new()?;
    ca_file.write_all(ca_cert.serialize_pem()?.as_bytes())?;
    ca_file.flush()?;
    let cli = CliArgs::parse_from([
        "test",
        "--listen",
        &listen_addr.to_string(),
        "--upstream",
        &upstream.to_string(),
        "--server-name",
        "localhost",
        "--ca-cert",
        ca_file.path().to_str().expect("temp path utf8"),
    ]);
    let config = Arc::new(Config::from_cli(&cli)?);
    let metrics = Arc::new(ProxyMetrics::new()?);
    let client = Arc::new(QuicRpcClient::new(config.clone(), metrics)?);

    timeout(Duration::from_secs(5), client.warmup()).await??;

    Ok(())
}
