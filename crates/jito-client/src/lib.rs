#![forbid(unsafe_code)]
pub mod jito {
    // pub mod auth { tonic::include_proto!("auth"); } // Empty proto file
    pub mod bundle { tonic::include_proto!("bundle"); }
    pub mod packet { tonic::include_proto!("packet"); }
    pub mod shared { tonic::include_proto!("shared"); }
    pub mod searcher { tonic::include_proto!("searcher"); }
    // pub mod block_engine { tonic::include_proto!("block_engine"); } // Empty proto file
    // pub mod relayer { tonic::include_proto!("relayer"); } // Empty proto file
}

use anyhow::Result;
use jito::bundle::{Bundle, BundleResult};
use jito::packet::{Meta, Packet, PacketFlags};
use jito::searcher::searcher_service_client::SearcherServiceClient;
use jito::searcher::{GetTipAccountsRequest, SendBundleRequest};
use prost_types::Timestamp;
use std::time::{SystemTime, UNIX_EPOCH};
use tonic::transport::{Channel, ClientTlsConfig};

#[derive(Clone)]
pub struct JitoClient {
    inner: SearcherServiceClient<Channel>,
}

impl JitoClient {
    pub async fn connect(endpoint: &str) -> Result<Self> {
        let channel = Channel::from_shared(endpoint.to_string())?
            .tls_config(ClientTlsConfig::new())?
            .connect()
            .await?;
        Ok(Self { inner: SearcherServiceClient::new(channel) })
    }

    pub async fn connect_with_bearer(endpoint: &str, _bearer: &str) -> Result<Self> {
        let channel = Channel::from_shared(endpoint.to_string())?
            .tls_config(ClientTlsConfig::new())?
            .connect()
            .await?;
        let client = Self { inner: SearcherServiceClient::new(channel) };
        // Note: Bearer token could be added to individual requests via interceptor
        Ok(client)
    }

    pub async fn get_tip_accounts(&mut self) -> Result<Vec<String>> {
        let resp = self.inner.get_tip_accounts(GetTipAccountsRequest {}).await?;
        Ok(resp.into_inner().accounts)
    }

    /// Build a Jito bundle from raw signed transactions (wire-format, not base64)
    pub fn build_bundle_from_signed_txs(raw_txs: Vec<Vec<u8>>) -> Bundle {
        let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap();
        let header = jito::shared::Header {
            ts: Some(Timestamp { seconds: now.as_secs() as i64, nanos: now.subsec_nanos() as i32 }),
        };
        let packets = raw_txs
            .into_iter()
            .map(|tx| Packet {
                data: tx.clone(),
                meta: Some(Meta {
                    size: tx.len() as u64,
                    addr: "".into(),
                    port: 0,
                    flags: Some(PacketFlags {
                        discard: false,
                        forwarded: false,
                        repair: false,
                        simple_vote_tx: false,
                        tracer_packet: false,
                        from_staked_node: true,
                    }),
                    sender_stake: 0,
                }),
            })
            .collect::<Vec<_>>();
        Bundle { header: Some(header), packets }
    }

    pub async fn send_bundle(&mut self, bundle: Bundle) -> Result<String> {
        let req = SendBundleRequest { bundle: Some(bundle) };
        let resp = self.inner.send_bundle(req).await?;
        Ok(resp.into_inner().uuid)
    }

    pub async fn subscribe_bundle_results(
        &mut self,
    ) -> Result<tonic::Streaming<BundleResult>> {
        let resp = self.inner.subscribe_bundle_results(jito::searcher::SubscribeBundleResultsRequest {}).await?;
        Ok(resp.into_inner())
    }
}


