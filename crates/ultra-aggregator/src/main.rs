// crates/ultra-aggregator/src/main.rs
#![forbid(unsafe_code)]
use anyhow::Result;
use faststreams::{decode_record, Record};
use metrics::{counter};
use metrics_exporter_prometheus::PrometheusBuilder;
use std::path::Path;
use tokio::io::AsyncReadExt;
use tokio::net::{UnixListener, UnixStream};
use tokio::signal;
use tracing::{error, info};
use tracing_subscriber::EnvFilter;

#[derive(Debug, serde::Deserialize)]
struct KafkaCfg {
    brokers: String,
    topic_accounts: String,
    topic_txs: String,
    topic_blocks: String,
    topic_slots: String,
}

#[derive(Debug, serde::Deserialize)]
struct Cfg {
    uds_path: String,
    stdout_json: bool,
    metrics_addr: Option<String>,
    #[cfg(feature = "kafka")]
    kafka: Option<KafkaCfg>,
}

#[cfg(feature = "kafka")]
struct KafkaSink {
    prod: rdkafka::producer::FutureProducer,
    cfg: KafkaCfg,
}
#[cfg(feature = "kafka")]
impl KafkaSink {
    fn new(cfg: KafkaCfg) -> Result<Self> {
        use rdkafka::ClientConfig;
        let prod = ClientConfig::new()
            .set("bootstrap.servers", &cfg.brokers)
            .set("queue.buffering.max.messages", "2000000")
            .set("queue.buffering.max.kbytes", "1048576")
            .set("message.timeout.ms", "5000")
            .create()?;
        Ok(Self { prod, cfg })
    }
    async fn send(&self, rec: &Record) -> Result<()> {
        use rdkafka::producer::FutureRecord;
        let (topic, key) = match rec {
            Record::Account(a) => (&self.cfg.topic_accounts, hex::encode(&a.pubkey)),
            Record::Tx(t) => (&self.cfg.topic_txs, t.signature_b58.clone()),
            Record::Block(b) => (&self.cfg.topic_blocks, b.blockhash_b58.clone().unwrap_or_default()),
            Record::Slot { slot, .. } => (&self.cfg.topic_slots, slot.to_string()),
            Record::EndOfStartup => (&self.cfg.topic_slots, "eos".to_string()),
        };
        let payload = bincode::serialize(rec)?;
        let _ = self.prod.send(FutureRecord::to(topic).key(&key).payload(&payload), std::time::Duration::from_secs(1)).await;
        Ok(())
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env().add_directive("info".parse()?))
        .init();

    let cfg_path = std::env::args().nth(1).unwrap_or_else(|| "configs/aggregator.json".to_string());
    let cfg: Cfg = {
        let raw = std::fs::read_to_string(&cfg_path)?;
        serde_json::from_str(&raw)?
    };

    if let Some(addr) = &cfg.metrics_addr {
        let _ = PrometheusBuilder::new().with_http_listener(addr.parse().unwrap()).install();
    }

    if Path::new(&cfg.uds_path).exists() {
        let _ = std::fs::remove_file(&cfg.uds_path);
    }
    let listener = UnixListener::bind(&cfg.uds_path)?;
    info!("listening UDS {}", cfg.uds_path);

    #[cfg(feature = "kafka")]
    let kafka_sink = if let Some(k) = cfg.kafka.clone() {
        Some(KafkaSink::new(k)?)
    } else { None };

    let shutdown = signal::ctrl_c();
    tokio::pin!(shutdown);

    loop {
        tokio::select! {
            _ = &mut shutdown => {
                info!("shutting down");
                break;
            }
            Ok((sock, _)) = listener.accept() => {
                let cfg_clone = cfg.clone();
                #[cfg(feature = "kafka")] let ks = kafka_sink.clone();
                tokio::spawn(async move {
                    if let Err(e) = handle_client(sock, cfg_clone, ks).await {
                        error!("client error: {e:?}");
                    }
                });
            }
        }
    }
    Ok(())
}

async fn handle_client(mut sock: UnixStream, cfg: Cfg, #[cfg(feature = "kafka")] ks: Option<KafkaSink>) -> Result<()> {
    let mut buf = Vec::with_capacity(1 << 20);
    loop {
        // read one frame at a time using the blocking decode API over a memory buffer
        // read available bytes
        let mut tmp = [0u8; 65536];
        let n = sock.read(&mut tmp).await?;
        if n == 0 { break; }
        buf.extend_from_slice(&tmp[..n]);

        // Try to peel records out
        let mut cursor = std::io::Cursor::new(&buf);
        loop {
            let pos = cursor.position() as usize;
            match faststreams::decode_record(&mut cursor) {
                Ok(rec) => {
                    if cfg.stdout_json {
                        let line = serde_json::to_string(&rec)?;
                        println!("{line}");
                    }
                    counter!("ultra_records_ingested_total").increment(1);
                    #[cfg(feature = "kafka")]
                    if let Some(k) = &ks { let _ = k.send(&rec).await; }
                }
                Err(faststreams::StreamError::BadHeader) => {
                    // desync; drop up to pos+1
                    let drop_to = pos.saturating_add(1);
                    buf.drain(..drop_to);
                    break;
                }
                Err(faststreams::StreamError::Io(_)) => break,
                Err(faststreams::StreamError::De(_)) => {
                    // not enough bytes for payload, wait for more
                    break;
                }
                Err(faststreams::StreamError::Ser(_)) => break,
            }
        }
        // drain consumed bytes
        let consumed = cursor.position() as usize;
        if consumed > 0 && consumed <= buf.len() {
            buf.drain(..consumed);
        }
    }
    Ok(())
}