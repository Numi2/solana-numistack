// Numan Thabit 2025
//! JSON-RPC routing atop the lock-free cache.

use std::fmt;
use std::str::FromStr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Instant;

use base64::engine::general_purpose::STANDARD as BASE64_ENGINE;
use base64::Engine as _;
use serde::{ser::SerializeTuple, ser::Serializer, Deserialize, Serialize};
use serde_json::{json, Value};
use solana_sdk::pubkey::Pubkey;

use crate::cache::{AccountCache, AccountRecord};
use crate::telemetry::RpcMetrics;

/// Tracks most recent root slot applied by the ingest pipeline.
#[derive(Default)]
pub struct SlotTracker {
    current: AtomicU64,
}

impl SlotTracker {
    /// Create a tracker initialised at slot 0.
    pub fn new() -> Self {
        Self::default()
    }

    /// Update to the provided slot if it is greater than the current value.
    pub fn update(&self, slot: u64) {
        self.current.fetch_max(slot, Ordering::Relaxed);
    }

    /// Get the latest observed slot.
    pub fn load(&self) -> u64 {
        self.current.load(Ordering::Relaxed)
    }
}

/// Minimal JSON-RPC router with async handlers.
pub struct RpcRouter {
    cache: Arc<AccountCache>,
    metrics: RpcMetrics,
    slots: Arc<SlotTracker>,
}

impl RpcRouter {
    /// Create a router bound to shared cache and metrics.
    pub fn new(cache: Arc<AccountCache>, metrics: RpcMetrics, slots: Arc<SlotTracker>) -> Self {
        Self {
            cache,
            metrics,
            slots,
        }
    }

    /// Dispatch a request and return either a JSON result or an RPC error object.
    pub async fn handle(&self, method: &str, params: &Value) -> Result<RpcResult, RpcCallError> {
        match method {
            "getAccountInfo" => self.get_account_info(params).await,
            "getMultipleAccounts" => self.get_multiple_accounts(params).await,
            "getSlot" => {
                let start = Instant::now();
                let slot = self.slots.load();
                self.metrics
                    .record_request("getSlot", start.elapsed().as_secs_f64(), 0);
                Ok(RpcResult::Slot(RpcResponse::new(slot, slot)))
            }
            other => {
                let start = Instant::now();
                self.metrics
                    .record_request(other, start.elapsed().as_secs_f64(), 0);
                Err(RpcCallError::method_not_found(other))
            }
        }
    }

    async fn get_account_info(&self, params: &Value) -> Result<RpcResult, RpcCallError> {
        let start = Instant::now();
        let (pubkey, cfg) = match parse_account_params(params) {
            Ok(v) => v,
            Err(err) => {
                self.metrics
                    .record_request("getAccountInfo", start.elapsed().as_secs_f64(), 0);
                return Err(err);
            }
        };

        // Validate supported config
        if let Some(enc) = &cfg.encoding {
            if enc != "base64" {
                self.metrics
                    .record_request("getAccountInfo", start.elapsed().as_secs_f64(), 0);
                return Err(RpcCallError::invalid_params(
                    "unsupported encoding; only base64 is supported",
                ));
            }
        }
        if let Some(commitment) = &cfg.commitment {
            match commitment.as_str() {
                "processed" | "confirmed" | "finalized" => {}
                _ => {
                    self.metrics
                        .record_request("getAccountInfo", start.elapsed().as_secs_f64(), 0);
                    return Err(RpcCallError::invalid_params("unsupported commitment"));
                }
            }
        }
        if let Some(required_slot) = cfg.min_context_slot {
            let observed = self.slots.load();
            if observed < required_slot {
                self.metrics
                    .record_request("getAccountInfo", start.elapsed().as_secs_f64(), 0);
                return Err(RpcCallError::min_context_slot_not_reached(
                    required_slot,
                    observed,
                ));
            }
        }

        // Build response with a fast path for the common case (no dataSlice)
        let value = if let Some(slice) = cfg.data_slice.as_ref() {
            self.cache
                .get(&pubkey)
                .map(|record| account_to_response_with_slice(record.as_ref(), Some(slice)))
        } else {
            self.cache
                .get(&pubkey)
                .map(|record| account_to_response(record.as_ref()))
        };

        let bytes = value.as_ref().map(data_size).unwrap_or(0);
        self.metrics
            .record_request("getAccountInfo", start.elapsed().as_secs_f64(), bytes);
        let response = RpcResponse::new(self.slots.load(), value);
        Ok(RpcResult::AccountInfo(response))
    }

    async fn get_multiple_accounts(&self, params: &Value) -> Result<RpcResult, RpcCallError> {
        let start = Instant::now();
        let (pubkeys, cfg) = match parse_multiple_account_params(params) {
            Ok(v) => v,
            Err(err) => {
                self.metrics.record_request(
                    "getMultipleAccounts",
                    start.elapsed().as_secs_f64(),
                    0,
                );
                return Err(err);
            }
        };

        // Validate supported config
        if let Some(enc) = &cfg.encoding {
            if enc != "base64" {
                self.metrics.record_request(
                    "getMultipleAccounts",
                    start.elapsed().as_secs_f64(),
                    0,
                );
                return Err(RpcCallError::invalid_params(
                    "unsupported encoding; only base64 is supported",
                ));
            }
        }
        if let Some(commitment) = &cfg.commitment {
            match commitment.as_str() {
                "processed" | "confirmed" | "finalized" => {}
                _ => {
                    self.metrics.record_request(
                        "getMultipleAccounts",
                        start.elapsed().as_secs_f64(),
                        0,
                    );
                    return Err(RpcCallError::invalid_params("unsupported commitment"));
                }
            }
        }
        if let Some(required_slot) = cfg.min_context_slot {
            let observed = self.slots.load();
            if observed < required_slot {
                self.metrics.record_request(
                    "getMultipleAccounts",
                    start.elapsed().as_secs_f64(),
                    0,
                );
                return Err(RpcCallError::min_context_slot_not_reached(
                    required_slot,
                    observed,
                ));
            }
        }

        // Conservative per-key lookups to avoid coupling to shard math in this layer,
        // with a fast path when no dataSlice is requested.
        let mut total_bytes = 0usize;
        let mut results = Vec::with_capacity(pubkeys.len());
        if let Some(slice) = cfg.data_slice.as_ref() {
            for key in pubkeys {
                let entry = self
                    .cache
                    .get(&key)
                    .map(|record| account_to_response_with_slice(record.as_ref(), Some(slice)));
                total_bytes += entry.as_ref().map(data_size).unwrap_or(0);
                results.push(entry);
            }
        } else {
            for key in pubkeys {
                let entry = self
                    .cache
                    .get(&key)
                    .map(|record| account_to_response(record.as_ref()));
                total_bytes += entry.as_ref().map(data_size).unwrap_or(0);
                results.push(entry);
            }
        }
        self.metrics.record_request(
            "getMultipleAccounts",
            start.elapsed().as_secs_f64(),
            total_bytes,
        );
        let response = RpcResponse::new(self.slots.load(), results);
        Ok(RpcResult::MultipleAccounts(response))
    }
}

/// Pre-serialized RPC payload variants.
pub enum RpcResult {
    /// Response payload for `getAccountInfo` requests.
    AccountInfo(RpcResponse<Option<AccountInfoValue>>),
    /// Response payload for `getMultipleAccounts` requests.
    MultipleAccounts(RpcResponse<Vec<Option<AccountInfoValue>>>),
    /// Response payload for `getSlot` requests.
    Slot(RpcResponse<u64>),
}

impl Serialize for RpcResult {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match self {
            Self::AccountInfo(response) => response.serialize(serializer),
            Self::MultipleAccounts(response) => response.serialize(serializer),
            Self::Slot(response) => response.serialize(serializer),
        }
    }
}

fn parse_account_params(params: &Value) -> Result<(Pubkey, AccountConfig), RpcCallError> {
    let arr = params
        .as_array()
        .ok_or_else(|| RpcCallError::invalid_params("expected params array"))?;
    let pubkey_str = arr
        .first()
        .and_then(Value::as_str)
        .ok_or_else(|| RpcCallError::invalid_params("missing pubkey"))?;
    let pubkey =
        Pubkey::from_str(pubkey_str).map_err(|_| RpcCallError::invalid_params("invalid pubkey"))?;
    let cfg = arr
        .get(1)
        .cloned()
        .map(|value| serde_json::from_value(value).unwrap_or_default())
        .unwrap_or_default();
    Ok((pubkey, cfg))
}

fn parse_multiple_account_params(
    params: &Value,
) -> Result<(Vec<Pubkey>, MultipleAccountConfig), RpcCallError> {
    let arr = params
        .as_array()
        .ok_or_else(|| RpcCallError::invalid_params("expected params array"))?;
    let keys_value = arr
        .first()
        .ok_or_else(|| RpcCallError::invalid_params("missing pubkeys"))?;
    let keys_array = keys_value
        .as_array()
        .ok_or_else(|| RpcCallError::invalid_params("pubkeys must be array"))?;
    let mut pubkeys = Vec::with_capacity(keys_array.len());
    for value in keys_array {
        let key = value
            .as_str()
            .ok_or_else(|| RpcCallError::invalid_params("pubkey must be base58 string"))?;
        let pubkey =
            Pubkey::from_str(key).map_err(|_| RpcCallError::invalid_params("invalid pubkey"))?;
        pubkeys.push(pubkey);
    }
    let cfg = arr
        .get(1)
        .cloned()
        .map(|value| serde_json::from_value(value).unwrap_or_default())
        .unwrap_or_default();
    Ok((pubkeys, cfg))
}

fn data_size(info: &AccountInfoValue) -> usize {
    info.space()
}

fn account_to_response(record: &AccountRecord) -> AccountInfoValue {
    AccountInfoValue::from_record(record)
}

fn account_to_response_with_slice(
    record: &AccountRecord,
    data_slice: Option<&DataSliceConfig>,
) -> AccountInfoValue {
    if let Some(slice) = data_slice {
        let data = record.data_slice();
        let start = slice.offset.min(data.len());
        let end = start.saturating_add(slice.length).min(data.len());
        let window = &data[start..end];
        let encoded: Arc<str> = if window.is_empty() {
            Arc::<str>::from("")
        } else {
            Arc::<str>::from(BASE64_ENGINE.encode(window))
        };
        AccountInfoValue::from_record_with_data(record, encoded)
    } else {
        AccountInfoValue::from_record(record)
    }
}

#[derive(Deserialize, Default)]
struct AccountConfig {
    #[allow(dead_code)]
    encoding: Option<String>,
    #[serde(rename = "minContextSlot")]
    #[allow(dead_code)]
    min_context_slot: Option<u64>,
    #[allow(dead_code)]
    commitment: Option<String>,
    #[serde(rename = "dataSlice")]
    #[allow(dead_code)]
    data_slice: Option<DataSliceConfig>,
}

#[derive(Deserialize, Default)]
struct MultipleAccountConfig {
    #[allow(dead_code)]
    encoding: Option<String>,
    #[serde(rename = "minContextSlot")]
    #[allow(dead_code)]
    min_context_slot: Option<u64>,
    #[allow(dead_code)]
    commitment: Option<String>,
    #[serde(rename = "dataSlice")]
    #[allow(dead_code)]
    data_slice: Option<DataSliceConfig>,
}

#[derive(Deserialize, Default)]
struct DataSliceConfig {
    offset: usize,
    length: usize,
}

#[derive(Clone, Serialize)]
/// JSON-RPC ready account payload built from cache records.
pub struct AccountInfoValue {
    lamports: u64,
    owner: OwnerString,
    data: EncodedAccountData,
    executable: bool,
    #[serde(rename = "rentEpoch")]
    rent_epoch: u64,
    #[serde(rename = "space")]
    space: usize,
}

impl AccountInfoValue {
    #[inline]
    /// Construct a payload from a cached account record.
    pub(crate) fn from_record(record: &AccountRecord) -> Self {
        Self::from_record_with_data(record, record.data_base64())
    }

    #[inline]
    /// Construct a payload from a cached account record with custom encoded data.
    pub(crate) fn from_record_with_data(record: &AccountRecord, encoded_data: Arc<str>) -> Self {
        Self {
            lamports: record.lamports(),
            owner: OwnerString::from(record.owner_arc()),
            data: EncodedAccountData::new(encoded_data),
            executable: record.executable(),
            rent_epoch: record.rent_epoch(),
            space: record.data_len(),
        }
    }

    #[inline]
    /// Lamport balance associated with the account.
    pub fn lamports(&self) -> u64 {
        self.lamports
    }

    #[inline]
    /// Program owner rendered as base58.
    pub fn owner(&self) -> &str {
        self.owner.as_str()
    }

    #[inline]
    /// Reference-counted owner string for zero-copy propagation.
    pub fn owner_arc(&self) -> Arc<str> {
        self.owner.clone().into_inner()
    }

    #[inline]
    /// Raw account data encoded as `[base64, "base64"]` tuple.
    pub fn data(&self) -> &EncodedAccountData {
        &self.data
    }

    #[inline]
    /// Whether the account has executable flag set by the runtime.
    pub fn executable(&self) -> bool {
        self.executable
    }

    #[inline]
    /// Rent epoch reported by the ledger for this account snapshot.
    pub fn rent_epoch(&self) -> u64 {
        self.rent_epoch
    }

    #[inline]
    /// Length of the original binary account data before base64 encoding.
    pub fn space(&self) -> usize {
        self.space
    }
}

#[derive(Clone)]
/// Base64 encoded account data with metadata required by the RPC spec.
pub struct EncodedAccountData {
    payload: Arc<str>,
}

impl EncodedAccountData {
    #[inline]
    /// Wrap an already base64-encoded payload.
    pub fn new(payload: Arc<str>) -> Self {
        Self { payload }
    }

    #[inline]
    /// Borrow the encoded payload as a string slice.
    pub fn as_str(&self) -> &str {
        &self.payload
    }

    #[inline]
    /// Encoding label advertised to clients.
    pub fn encoding(&self) -> &'static str {
        "base64"
    }

    #[inline]
    /// Length of the encoded payload in bytes.
    pub fn len(&self) -> usize {
        self.payload.len()
    }

    #[inline]
    /// Returns true when the encoded payload is empty.
    pub fn is_empty(&self) -> bool {
        self.payload.is_empty()
    }

    #[inline]
    /// Extract the underlying `Arc<str>` for reuse.
    pub fn into_inner(self) -> Arc<str> {
        self.payload
    }
}

impl fmt::Debug for EncodedAccountData {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("EncodedAccountData")
            .field("encoding", &self.encoding())
            .field("len", &self.len())
            .finish()
    }
}

impl Serialize for EncodedAccountData {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut seq = serializer.serialize_tuple(2)?;
        seq.serialize_element(self.payload.as_ref())?;
        seq.serialize_element(self.encoding())?;
        seq.end()
    }
}

#[derive(Clone)]
/// Wrapper around shared owner strings to provide custom serialization.
pub struct OwnerString(Arc<str>);

impl OwnerString {
    #[inline]
    /// Create an owner string wrapper from an `Arc<str>`.
    pub fn new(value: Arc<str>) -> Self {
        Self(value)
    }

    #[inline]
    /// Borrow the raw base58 owner string.
    pub fn as_str(&self) -> &str {
        &self.0
    }

    #[inline]
    /// Consume the wrapper and return the shared string.
    pub fn into_inner(self) -> Arc<str> {
        self.0
    }
}

impl From<Arc<str>> for OwnerString {
    #[inline]
    fn from(value: Arc<str>) -> Self {
        Self::new(value)
    }
}

impl fmt::Debug for OwnerString {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_tuple("OwnerString").field(&self.as_str()).finish()
    }
}

impl Serialize for OwnerString {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(self.as_str())
    }
}

#[derive(Clone, Copy, Debug, Serialize)]
/// Minimal RPC metadata describing the slot context of a response.
pub struct RpcContext {
    slot: u64,
}

impl RpcContext {
    #[inline]
    /// Build a context wrapper for the provided slot.
    pub fn new(slot: u64) -> Self {
        Self { slot }
    }

    #[inline]
    /// Slot number associated with the response payload.
    pub fn slot(&self) -> u64 {
        self.slot
    }
}

#[derive(Serialize)]
/// Generic RPC response envelope mirroring Solana's JSON-RPC schema.
pub struct RpcResponse<T> {
    context: RpcContext,
    value: T,
}

impl<T> RpcResponse<T> {
    #[inline]
    /// Compose a response using the given slot and value payload.
    pub fn new(slot: u64, value: T) -> Self {
        Self {
            context: RpcContext::new(slot),
            value,
        }
    }

    #[inline]
    /// Inspect the contextual metadata for this response.
    pub fn context(&self) -> &RpcContext {
        &self.context
    }

    #[inline]
    /// Borrow the payload contained in the response.
    pub fn value(&self) -> &T {
        &self.value
    }

    #[inline]
    /// Split the response into its context and payload components.
    pub fn into_parts(self) -> (RpcContext, T) {
        (self.context, self.value)
    }
}

/// Application-level error object for JSON-RPC responses.
#[derive(Debug)]
pub struct RpcCallError {
    code: i32,
    message: String,
    data: Option<Value>,
}

impl RpcCallError {
    fn invalid_params(message: impl Into<String>) -> Self {
        Self {
            code: -32602,
            message: message.into(),
            data: None,
        }
    }

    fn method_not_found(method: &str) -> Self {
        Self {
            code: -32601,
            message: format!("method {} not found", method),
            data: None,
        }
    }

    fn min_context_slot_not_reached(required: u64, observed: u64) -> Self {
        Self {
            code: -32016,
            message: "minimum context slot not reached".into(),
            data: Some(json!({ "required": required, "observed": observed })),
        }
    }

    /// Convert into JSON-RPC error object.
    pub fn into_error_object(self) -> Value {
        json!({
            "code": self.code,
            "message": self.message,
            "data": self.data,
        })
    }
}

impl From<serde_json::Error> for RpcCallError {
    fn from(err: serde_json::Error) -> Self {
        Self {
            code: -32602,
            message: "invalid params".into(),
            data: Some(json!({"details": err.to_string()})),
        }
    }
}
