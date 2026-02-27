use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering as AtomicOrdering};
use std::sync::Arc;
use std::sync::Once;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use futures_util::{SinkExt, StreamExt};
use hmac::{Hmac, Mac};
use reqwest::Method;
use reqwest::StatusCode;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use serde_json::{Map as JsonMap, Value as JsonValue};
use sha2::Sha256;
use thiserror::Error;
use tokio::sync::{oneshot, Mutex};
use tokio::time::{sleep, timeout};
use tokio_tungstenite::connect_async;
use tokio_tungstenite::tungstenite::Message;
use tracing::{info, warn};

use crate::config::ExecutionConfig;
use crate::strategy::Signal;

const BINANCE_API_KEY_ENV: &str = "BINANCE_API_KEY";
const BINANCE_API_SECRET_ENV: &str = "BINANCE_API_SECRET";
const MBX_API_KEY_HEADER: &str = "X-MBX-APIKEY";
const USER_STREAM_KEEPALIVE_SECS: u64 = 30 * 60;
const USER_STREAM_RECONNECT_BACKOFF_MS: u64 = 1_000;
const ACK_TIMEOUT_MS: u64 = 5_000;
const FALLBACK_ACK_TIMEOUT_MIN_MS: u64 = 300;
const FALLBACK_ACK_TIMEOUT_MAX_MS: u64 = 800;
const FALLBACK_ACK_POLL_INTERVAL_MS: u64 = 50;
const SPIKE_LOG_THRESHOLD_MS: u64 = 500;
const HTTP_POOL_MAX_IDLE_PER_HOST: usize = 64;
const HTTP_POOL_IDLE_TIMEOUT_SECS: u64 = 300;
const HTTP_TCP_KEEPALIVE_SECS: u64 = 30;
const WS_API_RECONNECT_BACKOFF_MS: u64 = 500;
const ERROR_BODY_SNIPPET_MAX_CHARS: usize = 300;

type HmacSha256 = Hmac<Sha256>;

#[derive(Debug, Clone, Copy)]
pub enum Side {
    Buy,
    Sell,
}

impl Side {
    pub fn from_signal(signal: Signal) -> Option<Self> {
        match signal {
            Signal::Buy => Some(Self::Buy),
            Signal::Sell => Some(Self::Sell),
            Signal::Hold => None,
        }
    }

    pub fn signed_qty(self, qty: f64) -> f64 {
        match self {
            Self::Buy => qty,
            Self::Sell => -qty,
        }
    }

    fn as_binance(self) -> &'static str {
        match self {
            Self::Buy => "BUY",
            Self::Sell => "SELL",
        }
    }
}

#[derive(Debug, Clone)]
pub struct OrderRequest {
    pub symbol: String,
    pub side: Side,
    pub qty: f64,
    pub price: f64,
    pub order_type: OrderType,
}

#[derive(Debug, Clone, Copy)]
pub enum OrderType {
    Limit,
    LimitMaker,
}

impl OrderType {
    fn as_binance(self) -> &'static str {
        match self {
            Self::Limit => "LIMIT",
            Self::LimitMaker => "LIMIT_MAKER",
        }
    }

    fn needs_time_in_force(self) -> bool {
        matches!(self, Self::Limit)
    }
}

#[derive(Debug, Clone)]
pub struct Fill {
    pub symbol: String,
    pub side: Side,
    pub qty: f64,
    pub price: f64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OrderStatus {
    PendingNew,
    New,
    PartiallyFilled,
    Filled,
    Canceled,
    Rejected,
}

#[derive(Debug, Clone)]
pub struct ManagedOrder {
    pub client_order_id: String,
    pub order_id: Option<u64>,
    pub status: OrderStatus,
}

#[derive(Debug, Clone)]
pub struct OrderSnapshot {
    pub order: ManagedOrder,
    pub orig_qty: f64,
    pub executed_qty: f64,
}

#[derive(Debug, Clone)]
pub struct SymbolFilters {
    pub symbol: String,
    pub tick_size: f64,
    pub step_size: f64,
    pub min_notional: Option<f64>,
}

#[derive(Debug, Clone)]
pub struct PlaceOrderResult {
    pub order: ManagedOrder,
    pub place_http_us: u64,
    pub place_to_ack_us: Option<u64>,
    pub place_ack_at: Option<Instant>,
}

#[derive(Debug, Clone)]
pub struct CancelOrderResult {
    pub order: ManagedOrder,
    pub cancel_http_us: u64,
    pub cancel_to_ack_us: Option<u64>,
    pub cancel_ack_at: Option<Instant>,
}

#[derive(Debug, Default)]
pub struct OrderManager {
    orders: HashMap<String, ManagedOrder>,
}

impl OrderManager {
    pub fn mark_pending_new(&mut self, client_order_id: String) -> ManagedOrder {
        let order = ManagedOrder {
            client_order_id: client_order_id.clone(),
            order_id: None,
            status: OrderStatus::PendingNew,
        };
        self.orders.insert(client_order_id, order.clone());
        order
    }

    pub fn mark_new(&mut self, client_order_id: String, order_id: u64) -> ManagedOrder {
        self.set_status(client_order_id, Some(order_id), OrderStatus::New)
    }

    pub fn mark_partially_filled(
        &mut self,
        client_order_id: String,
        order_id: Option<u64>,
    ) -> ManagedOrder {
        self.set_status(client_order_id, order_id, OrderStatus::PartiallyFilled)
    }

    pub fn mark_filled(&mut self, client_order_id: String, order_id: Option<u64>) -> ManagedOrder {
        self.set_status(client_order_id, order_id, OrderStatus::Filled)
    }

    pub fn mark_canceled(
        &mut self,
        client_order_id: String,
        order_id: Option<u64>,
    ) -> ManagedOrder {
        self.set_status(client_order_id, order_id, OrderStatus::Canceled)
    }

    pub fn mark_rejected(
        &mut self,
        client_order_id: String,
        order_id: Option<u64>,
    ) -> ManagedOrder {
        self.set_status(client_order_id, order_id, OrderStatus::Rejected)
    }

    pub fn apply_execution_report(
        &mut self,
        client_order_id: String,
        order_id: Option<u64>,
        status: OrderStatus,
    ) -> ManagedOrder {
        self.set_status(client_order_id, order_id, status)
    }

    fn set_status(
        &mut self,
        client_order_id: String,
        order_id: Option<u64>,
        status: OrderStatus,
    ) -> ManagedOrder {
        let order = ManagedOrder {
            client_order_id: client_order_id.clone(),
            order_id,
            status,
        };
        self.orders.insert(client_order_id, order.clone());
        order
    }

    pub fn get(&self, client_order_id: &str) -> Option<ManagedOrder> {
        self.orders.get(client_order_id).cloned()
    }
}

#[derive(Debug)]
struct LatencyWindow {
    samples_us: Vec<u64>,
    total_operations: u64,
}

impl LatencyWindow {
    fn new() -> Self {
        Self {
            samples_us: Vec::with_capacity(10),
            total_operations: 0,
        }
    }

    fn record(&mut self, elapsed: Duration) -> Option<LatencySnapshot> {
        self.samples_us.push(elapsed.as_micros() as u64);
        self.total_operations = self.total_operations.saturating_add(1);

        if self.samples_us.len() < 10 {
            return None;
        }

        self.samples_us.sort_unstable();
        let snapshot = LatencySnapshot {
            total_operations: self.total_operations,
            p50_us: percentile(&self.samples_us, 0.50),
            p95_us: percentile(&self.samples_us, 0.95),
            p99_us: percentile(&self.samples_us, 0.99),
        };
        self.samples_us.clear();

        Some(snapshot)
    }
}

#[derive(Debug, Clone, Copy)]
struct LatencySnapshot {
    total_operations: u64,
    p50_us: u64,
    p95_us: u64,
    p99_us: u64,
}

#[derive(Debug)]
struct ParsedApiError {
    error: ExecutionError,
    status: u16,
    code: Option<i64>,
    body_snippet: String,
}

#[derive(Debug, Clone, Copy)]
struct AckMeasurement {
    latency_us: u64,
    ack_instant: Instant,
}

#[derive(Debug, Default)]
struct AckWaiters {
    place_waiters: HashMap<String, oneshot::Sender<Instant>>,
    cancel_waiters: HashMap<String, oneshot::Sender<Instant>>,
}

impl AckWaiters {
    fn register_place_waiter(&mut self, client_order_id: String) -> oneshot::Receiver<Instant> {
        let (tx, rx) = oneshot::channel();
        self.place_waiters.insert(client_order_id, tx);
        rx
    }

    fn register_cancel_waiter(&mut self, client_order_id: String) -> oneshot::Receiver<Instant> {
        let (tx, rx) = oneshot::channel();
        self.cancel_waiters.insert(client_order_id, tx);
        rx
    }

    fn notify_place_waiter(&mut self, client_order_id: &str, ack_instant: Instant) {
        if let Some(waiter) = self.place_waiters.remove(client_order_id) {
            let _ = waiter.send(ack_instant);
        }
    }

    fn notify_cancel_waiter(&mut self, client_order_id: &str, ack_instant: Instant) {
        if let Some(waiter) = self.cancel_waiters.remove(client_order_id) {
            let _ = waiter.send(ack_instant);
        }
    }

    fn clear_for_client_order_id(&mut self, client_order_id: &str) {
        self.place_waiters.remove(client_order_id);
        self.cancel_waiters.remove(client_order_id);
    }
}

#[derive(Debug)]
struct SimpleRateLimiter {
    min_interval: Duration,
    next_allowed: Instant,
}

impl SimpleRateLimiter {
    fn new(rate_limit_per_s: u32) -> Self {
        let per_second = rate_limit_per_s.max(1) as f64;
        let min_interval = Duration::from_secs_f64(1.0 / per_second);
        Self {
            min_interval,
            next_allowed: Instant::now(),
        }
    }

    fn reserve_delay(&mut self) -> Duration {
        let now = Instant::now();
        let delay = if self.next_allowed > now {
            self.next_allowed - now
        } else {
            Duration::ZERO
        };
        let base = if self.next_allowed > now {
            self.next_allowed
        } else {
            now
        };
        self.next_allowed = base + self.min_interval;

        delay
    }
}

#[derive(Debug, Error)]
pub enum ExecutionError {
    #[error("order rejected: {0}")]
    Rejected(String),
    #[error("missing required env var {0}")]
    MissingCredentials(&'static str),
    #[error("http request error: {source}")]
    Http {
        #[source]
        source: Box<reqwest::Error>,
        path: Option<String>,
        request_id: Option<u64>,
        retry_count: u32,
        body_snippet: Option<String>,
    },
    #[error("json parse error: {0}")]
    Json(#[source] Box<serde_json::Error>),
    #[error("signature error: {0}")]
    Signature(String),
    #[error("binance api error (status={status}, code={code:?}): {msg}")]
    Api {
        status: u16,
        code: Option<i64>,
        msg: String,
        path: Option<String>,
        body_snippet: Option<String>,
        request_id: Option<u64>,
        retry_count: u32,
    },
    #[error("binance rate limit: {0}")]
    RateLimited(String),
    #[error("exchange info missing symbol {0}")]
    ExchangeInfoSymbolMissing(String),
    #[error("exchange info missing filter {filter} for symbol {symbol}")]
    ExchangeInfoFilterMissing {
        symbol: String,
        filter: &'static str,
    },
    #[error("invalid exchange info filter value for symbol {symbol}: {field}={value}")]
    InvalidExchangeInfoFilter {
        symbol: String,
        field: &'static str,
        value: String,
    },
    #[error("user stream websocket error: {0}")]
    UserStreamSocket(#[source] Box<tokio_tungstenite::tungstenite::Error>),
    #[error("user stream parse error: {0}")]
    UserStreamParse(#[source] Box<serde_json::Error>),
    #[error("user stream missing listenKey")]
    MissingListenKey,
    #[error("ws-api websocket error: {0}")]
    WsApiSocket(#[source] Box<tokio_tungstenite::tungstenite::Error>),
    #[error("ws-api protocol error: {0}")]
    WsApiProtocol(String),
}

impl From<reqwest::Error> for ExecutionError {
    fn from(error: reqwest::Error) -> Self {
        Self::Http {
            source: Box::new(error),
            path: None,
            request_id: None,
            retry_count: 0,
            body_snippet: None,
        }
    }
}

impl From<serde_json::Error> for ExecutionError {
    fn from(error: serde_json::Error) -> Self {
        Self::Json(Box::new(error))
    }
}

impl From<tokio_tungstenite::tungstenite::Error> for ExecutionError {
    fn from(error: tokio_tungstenite::tungstenite::Error) -> Self {
        Self::UserStreamSocket(Box::new(error))
    }
}

type BinanceWsSocket =
    tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>;

#[derive(Clone)]
pub struct BinanceTestnetExecutor {
    http: reqwest::Client,
    api_key: String,
    api_secret: String,
    rest_url: String,
    ws_url: String,
    recv_window_ms: u64,
    ack_timeout: Duration,
    fallback_ack_timeout: Duration,
    fallback_ack_poll_interval: Duration,
    order_manager: Arc<Mutex<OrderManager>>,
    prep_latency_window: Arc<Mutex<LatencyWindow>>,
    latency_window: Arc<Mutex<LatencyWindow>>,
    rate_limiter: Arc<Mutex<SimpleRateLimiter>>,
    rate_limit_sleep_count_total: Arc<AtomicU64>,
    rate_limit_sleep_ms_total: Arc<AtomicU64>,
    rate_limit_sleep_count_window: Arc<AtomicU64>,
    rate_limit_sleep_ms_window: Arc<AtomicU64>,
    request_counter: Arc<AtomicU64>,
    connection_warm: Arc<AtomicBool>,
    ack_waiters: Arc<Mutex<AckWaiters>>,
    user_stream_started: Arc<Mutex<bool>>,
    user_stream_supported: Arc<AtomicBool>,
    user_stream_disabled_logged: Arc<AtomicBool>,
}

impl BinanceTestnetExecutor {
    pub fn from_config(cfg: &ExecutionConfig) -> Result<Self, ExecutionError> {
        ensure_rustls_crypto_provider();

        let api_key = std::env::var(BINANCE_API_KEY_ENV)
            .map_err(|_| ExecutionError::MissingCredentials(BINANCE_API_KEY_ENV))?;
        let api_secret = std::env::var(BINANCE_API_SECRET_ENV)
            .map_err(|_| ExecutionError::MissingCredentials(BINANCE_API_SECRET_ENV))?;

        let http = reqwest::Client::builder()
            .timeout(Duration::from_millis(cfg.timeout_ms()))
            .connect_timeout(Duration::from_millis(cfg.timeout_ms().min(1_000)))
            .pool_max_idle_per_host(HTTP_POOL_MAX_IDLE_PER_HOST)
            .pool_idle_timeout(Some(Duration::from_secs(HTTP_POOL_IDLE_TIMEOUT_SECS)))
            .tcp_keepalive(Some(Duration::from_secs(HTTP_TCP_KEEPALIVE_SECS)))
            .tcp_nodelay(true)
            .http1_only()
            .build()?;

        let (user_stream_supported, user_stream_disabled_logged) =
            initial_user_stream_state(cfg.testnet, cfg.enable_user_stream);
        if cfg.testnet && cfg.enable_user_stream {
            warn!("testnet user stream not supported; disabled");
        }

        Ok(Self {
            http,
            api_key,
            api_secret,
            rest_url: cfg.rest_url.trim_end_matches('/').to_string(),
            ws_url: cfg.ws_url.trim_end_matches('/').to_string(),
            recv_window_ms: cfg.recv_window_ms(),
            ack_timeout: Duration::from_millis(ACK_TIMEOUT_MS),
            fallback_ack_timeout: Duration::from_millis(
                cfg.timeout_ms()
                    .clamp(FALLBACK_ACK_TIMEOUT_MIN_MS, FALLBACK_ACK_TIMEOUT_MAX_MS),
            ),
            fallback_ack_poll_interval: Duration::from_millis(FALLBACK_ACK_POLL_INTERVAL_MS),
            order_manager: Arc::new(Mutex::new(OrderManager::default())),
            prep_latency_window: Arc::new(Mutex::new(LatencyWindow::new())),
            latency_window: Arc::new(Mutex::new(LatencyWindow::new())),
            rate_limiter: Arc::new(Mutex::new(SimpleRateLimiter::new(cfg.rate_limit_per_s()))),
            rate_limit_sleep_count_total: Arc::new(AtomicU64::new(0)),
            rate_limit_sleep_ms_total: Arc::new(AtomicU64::new(0)),
            rate_limit_sleep_count_window: Arc::new(AtomicU64::new(0)),
            rate_limit_sleep_ms_window: Arc::new(AtomicU64::new(0)),
            request_counter: Arc::new(AtomicU64::new(0)),
            connection_warm: Arc::new(AtomicBool::new(false)),
            ack_waiters: Arc::new(Mutex::new(AckWaiters::default())),
            user_stream_started: Arc::new(Mutex::new(false)),
            user_stream_supported: Arc::new(AtomicBool::new(user_stream_supported)),
            user_stream_disabled_logged: Arc::new(AtomicBool::new(user_stream_disabled_logged)),
        })
    }

    pub async fn fetch_symbol_filters(
        &self,
        symbol: &str,
    ) -> Result<SymbolFilters, ExecutionError> {
        let symbol_upper = symbol.to_uppercase();
        let response = self
            .send_public_get(
                "/api/v3/exchangeInfo",
                vec![("symbol", symbol_upper.clone())],
            )
            .await?;
        let payload: BinanceExchangeInfoResponse = response.json().await?;

        let symbol_info = payload
            .symbols
            .into_iter()
            .find(|item| item.symbol.eq_ignore_ascii_case(&symbol_upper))
            .ok_or_else(|| ExecutionError::ExchangeInfoSymbolMissing(symbol_upper.clone()))?;

        let mut tick_size: Option<f64> = None;
        let mut step_size: Option<f64> = None;
        let mut min_notional: Option<f64> = None;

        for filter in symbol_info.filters {
            match filter.filter_type.as_str() {
                "PRICE_FILTER" => {
                    if let Some(raw) = filter.tick_size {
                        let parsed =
                            parse_exchange_filter_number(&symbol_upper, "tickSize", &raw, true)?;
                        tick_size = Some(parsed);
                    }
                }
                "LOT_SIZE" => {
                    if let Some(raw) = filter.step_size {
                        let parsed =
                            parse_exchange_filter_number(&symbol_upper, "stepSize", &raw, true)?;
                        step_size = Some(parsed);
                    }
                }
                "MIN_NOTIONAL" | "NOTIONAL" => {
                    if let Some(raw) = filter.min_notional {
                        let parsed = parse_exchange_filter_number(
                            &symbol_upper,
                            "minNotional",
                            &raw,
                            false,
                        )?;
                        min_notional = Some(parsed);
                    }
                }
                _ => {}
            }
        }

        let tick_size = tick_size.ok_or_else(|| ExecutionError::ExchangeInfoFilterMissing {
            symbol: symbol_upper.clone(),
            filter: "PRICE_FILTER.tickSize",
        })?;
        let step_size = step_size.ok_or_else(|| ExecutionError::ExchangeInfoFilterMissing {
            symbol: symbol_upper.clone(),
            filter: "LOT_SIZE.stepSize",
        })?;

        Ok(SymbolFilters {
            symbol: symbol_upper,
            tick_size,
            step_size,
            min_notional,
        })
    }

    pub async fn new_limit_order(
        &self,
        request: &OrderRequest,
        client_order_id: String,
    ) -> Result<ManagedOrder, ExecutionError> {
        let outcome = self
            .new_limit_order_with_metrics(request, client_order_id)
            .await?;
        Ok(outcome.order)
    }

    pub async fn new_limit_order_with_metrics(
        &self,
        request: &OrderRequest,
        client_order_id: String,
    ) -> Result<PlaceOrderResult, ExecutionError> {
        self.ensure_user_stream_started().await;

        {
            let mut manager = self.order_manager.lock().await;
            manager.mark_pending_new(client_order_id.clone());
        }

        let place_ack_rx = if self.is_user_stream_enabled() {
            Some(self.register_place_waiter(client_order_id.clone()).await)
        } else {
            None
        };
        let place_started = Instant::now();

        let mut params = vec![
            ("symbol", request.symbol.to_uppercase()),
            ("side", request.side.as_binance().to_string()),
            ("type", request.order_type.as_binance().to_string()),
            ("quantity", decimal_string(request.qty)),
            ("price", decimal_string(request.price)),
            ("newClientOrderId", client_order_id.clone()),
        ];
        if request.order_type.needs_time_in_force() {
            params.push(("timeInForce", "GTC".to_string()));
        }

        let response = self
            .send_signed_with_latency(Method::POST, "/api/v3/order", params)
            .await;

        match response {
            Ok((resp, place_http_us)) => {
                let payload: BinanceOrderResponse = resp.json().await?;
                let status = map_binance_status(&payload.status);
                let final_client_order_id = payload
                    .orig_client_order_id
                    .clone()
                    .unwrap_or_else(|| payload.client_order_id.clone());

                let managed = {
                    let mut manager = self.order_manager.lock().await;
                    apply_status(
                        &mut manager,
                        final_client_order_id.clone(),
                        Some(payload.order_id),
                        status,
                    )
                };

                let place_to_ack_us = if matches!(status, OrderStatus::Rejected) {
                    self.clear_waiters_for(&final_client_order_id).await;
                    None
                } else if let Some(place_ack_rx) = place_ack_rx {
                    let measured = self.wait_for_ack(place_ack_rx, place_started).await;
                    if measured.is_none() {
                        self.clear_waiters_for(&final_client_order_id).await;
                    }
                    measured
                } else {
                    self.wait_for_place_ack_fallback(
                        &request.symbol,
                        &final_client_order_id,
                        place_started,
                    )
                    .await
                };
                let (place_to_ack_us, place_ack_at) = match place_to_ack_us {
                    Some(value) => (Some(value.latency_us), Some(value.ack_instant)),
                    None => (None, None),
                };

                Ok(PlaceOrderResult {
                    order: managed,
                    place_http_us,
                    place_to_ack_us,
                    place_ack_at,
                })
            }
            Err(error) => {
                self.clear_waiters_for(&client_order_id).await;
                let mut manager = self.order_manager.lock().await;
                manager.mark_rejected(client_order_id, None);
                Err(error)
            }
        }
    }

    pub async fn cancel_order(
        &self,
        symbol: &str,
        client_order_id: &str,
    ) -> Result<ManagedOrder, ExecutionError> {
        let outcome = self
            .cancel_order_with_metrics(symbol, client_order_id)
            .await?;
        Ok(outcome.order)
    }

    pub async fn cancel_order_with_metrics(
        &self,
        symbol: &str,
        client_order_id: &str,
    ) -> Result<CancelOrderResult, ExecutionError> {
        self.ensure_user_stream_started().await;

        let cancel_ack_rx = if self.is_user_stream_enabled() {
            Some(
                self.register_cancel_waiter(client_order_id.to_string())
                    .await,
            )
        } else {
            None
        };
        let cancel_started = Instant::now();

        let params = vec![
            ("symbol", symbol.to_uppercase()),
            ("origClientOrderId", client_order_id.to_string()),
        ];

        let response = self
            .send_signed_with_latency(Method::DELETE, "/api/v3/order", params)
            .await;
        let (response, cancel_http_us) = match response {
            Ok(value) => value,
            Err(error) => {
                self.clear_waiters_for(client_order_id).await;
                return Err(error);
            }
        };
        let payload: BinanceOrderResponse = response.json().await?;
        let status = map_binance_status(&payload.status);
        let final_client_order_id = payload
            .orig_client_order_id
            .clone()
            .unwrap_or_else(|| payload.client_order_id.clone());

        let managed = {
            let mut manager = self.order_manager.lock().await;
            apply_status(
                &mut manager,
                final_client_order_id.clone(),
                Some(payload.order_id),
                status,
            )
        };

        let cancel_to_ack_us = if matches!(status, OrderStatus::Canceled) {
            if let Some(cancel_ack_rx) = cancel_ack_rx {
                let measured = self.wait_for_ack(cancel_ack_rx, cancel_started).await;
                if measured.is_none() {
                    self.clear_waiters_for(&final_client_order_id).await;
                }
                measured
            } else {
                self.wait_for_cancel_ack_fallback(symbol, &final_client_order_id, cancel_started)
                    .await
            }
        } else {
            self.clear_waiters_for(&final_client_order_id).await;
            None
        };
        let (cancel_to_ack_us, cancel_ack_at) = match cancel_to_ack_us {
            Some(value) => (Some(value.latency_us), Some(value.ack_instant)),
            None => (None, None),
        };

        Ok(CancelOrderResult {
            order: managed,
            cancel_http_us,
            cancel_to_ack_us,
            cancel_ack_at,
        })
    }

    pub async fn query_order(
        &self,
        symbol: &str,
        client_order_id: &str,
    ) -> Result<ManagedOrder, ExecutionError> {
        let snapshot = self.query_order_snapshot(symbol, client_order_id).await?;
        Ok(snapshot.order)
    }

    pub async fn query_order_snapshot(
        &self,
        symbol: &str,
        client_order_id: &str,
    ) -> Result<OrderSnapshot, ExecutionError> {
        let params = vec![
            ("symbol", symbol.to_uppercase()),
            ("origClientOrderId", client_order_id.to_string()),
        ];

        let response = self
            .send_signed(Method::GET, "/api/v3/order", params)
            .await?;
        let payload: BinanceOrderResponse = response.json().await?;
        let status = map_binance_status(&payload.status);
        let final_client_order_id = payload
            .orig_client_order_id
            .clone()
            .unwrap_or_else(|| payload.client_order_id.clone());

        let managed = {
            let mut manager = self.order_manager.lock().await;
            apply_status(
                &mut manager,
                final_client_order_id,
                Some(payload.order_id),
                status,
            )
        };

        Ok(OrderSnapshot {
            order: managed,
            orig_qty: parse_non_negative_decimal_or_zero(&payload.orig_qty),
            executed_qty: parse_non_negative_decimal_or_zero(&payload.executed_qty),
        })
    }

    pub async fn order_state(&self, client_order_id: &str) -> Option<ManagedOrder> {
        let manager = self.order_manager.lock().await;
        manager.get(client_order_id)
    }

    async fn ensure_user_stream_started(&self) {
        if !self.is_user_stream_enabled() {
            return;
        }

        let mut started = self.user_stream_started.lock().await;
        if *started || !self.is_user_stream_enabled() {
            return;
        }

        *started = true;
        let this = self.clone();
        tokio::spawn(async move {
            this.user_data_stream_loop().await;
        });
    }

    async fn register_place_waiter(&self, client_order_id: String) -> oneshot::Receiver<Instant> {
        let mut waiters = self.ack_waiters.lock().await;
        waiters.register_place_waiter(client_order_id)
    }

    async fn register_cancel_waiter(&self, client_order_id: String) -> oneshot::Receiver<Instant> {
        let mut waiters = self.ack_waiters.lock().await;
        waiters.register_cancel_waiter(client_order_id)
    }

    async fn clear_waiters_for(&self, client_order_id: &str) {
        let mut waiters = self.ack_waiters.lock().await;
        waiters.clear_for_client_order_id(client_order_id);
    }

    async fn wait_for_ack(
        &self,
        ack_rx: oneshot::Receiver<Instant>,
        started_at: Instant,
    ) -> Option<AckMeasurement> {
        match timeout(self.ack_timeout, ack_rx).await {
            Ok(Ok(ack_instant)) => Some(AckMeasurement {
                latency_us: ack_instant.duration_since(started_at).as_micros() as u64,
                ack_instant,
            }),
            Ok(Err(_)) => None,
            Err(_) => None,
        }
    }

    async fn wait_for_place_ack_fallback(
        &self,
        symbol: &str,
        client_order_id: &str,
        started_at: Instant,
    ) -> Option<AckMeasurement> {
        self.wait_for_order_visibility_fallback(
            symbol,
            client_order_id,
            started_at,
            is_place_visible_status,
        )
        .await
    }

    async fn wait_for_cancel_ack_fallback(
        &self,
        symbol: &str,
        client_order_id: &str,
        started_at: Instant,
    ) -> Option<AckMeasurement> {
        self.wait_for_order_visibility_fallback(
            symbol,
            client_order_id,
            started_at,
            is_cancel_visible_status,
        )
        .await
    }

    async fn wait_for_order_visibility_fallback(
        &self,
        symbol: &str,
        client_order_id: &str,
        started_at: Instant,
        matches_status: fn(OrderStatus) -> bool,
    ) -> Option<AckMeasurement> {
        let deadline = Instant::now() + self.fallback_ack_timeout;

        loop {
            match self.query_order(symbol, client_order_id).await {
                Ok(order) if matches_status(order.status) => {
                    let ack_instant = Instant::now();
                    return Some(AckMeasurement {
                        latency_us: ack_instant.duration_since(started_at).as_micros() as u64,
                        ack_instant,
                    });
                }
                Ok(_) => {}
                Err(error) => {
                    if !is_poll_retryable_error(&error) {
                        return None;
                    }
                }
            }

            let now = Instant::now();
            if now >= deadline {
                return None;
            }

            sleep(
                self.fallback_ack_poll_interval
                    .min(deadline.saturating_duration_since(now)),
            )
            .await;
        }
    }

    fn is_user_stream_enabled(&self) -> bool {
        self.user_stream_supported.load(AtomicOrdering::Relaxed)
    }

    async fn maybe_disable_user_stream_from_error(&self, error: &ExecutionError) {
        if let Some(status_code) = user_stream_unavailable_status(error) {
            self.disable_user_stream(status_code).await;
        }
    }

    async fn disable_user_stream(&self, status_code: u16) {
        self.user_stream_supported
            .store(false, AtomicOrdering::Relaxed);

        {
            let mut waiters = self.ack_waiters.lock().await;
            waiters.place_waiters.clear();
            waiters.cancel_waiters.clear();
        }

        if !self
            .user_stream_disabled_logged
            .swap(true, AtomicOrdering::Relaxed)
        {
            warn!(
                status_code,
                "user data stream disabled (listenKey endpoints unavailable)"
            );
        }
    }

    async fn user_data_stream_loop(self) {
        let reconnect_backoff = Duration::from_millis(USER_STREAM_RECONNECT_BACKOFF_MS);

        loop {
            if !self.is_user_stream_enabled() {
                return;
            }

            let listen_key = match self.create_listen_key().await {
                Ok(value) => value,
                Err(error) => {
                    if !self.is_user_stream_enabled() {
                        return;
                    }
                    warn!(error = %error, "failed to create user data stream listenKey");
                    sleep(reconnect_backoff).await;
                    continue;
                }
            };

            let keepalive_executor = self.clone();
            let keepalive_key = listen_key.clone();
            let keepalive_handle = tokio::spawn(async move {
                keepalive_executor
                    .user_stream_keepalive_loop(keepalive_key)
                    .await;
            });

            let ws_url = self.user_stream_url(&listen_key);
            match connect_async(&ws_url).await {
                Ok((socket, _)) => {
                    if let Err(error) = self.run_user_data_socket(socket).await {
                        warn!(error = %error, "user data stream disconnected");
                    }
                }
                Err(error) => {
                    warn!(error = %error, "failed to connect user data stream websocket");
                }
            }

            keepalive_handle.abort();
            let _ = keepalive_handle.await;
            if !self.is_user_stream_enabled() {
                return;
            }
            sleep(reconnect_backoff).await;
        }
    }

    async fn run_user_data_socket(
        &self,
        mut socket: BinanceWsSocket,
    ) -> Result<(), ExecutionError> {
        loop {
            let maybe_msg = socket.next().await;
            let Some(msg) = maybe_msg else {
                return Err(ExecutionError::UserStreamSocket(Box::new(
                    tokio_tungstenite::tungstenite::Error::ConnectionClosed,
                )));
            };

            let msg = msg.map_err(ExecutionError::from)?;

            if msg.is_ping() {
                socket
                    .send(Message::Pong(msg.into_data()))
                    .await
                    .map_err(ExecutionError::from)?;
                continue;
            }

            if msg.is_pong() {
                continue;
            }

            if msg.is_close() {
                return Err(ExecutionError::UserStreamSocket(Box::new(
                    tokio_tungstenite::tungstenite::Error::ConnectionClosed,
                )));
            }

            if !msg.is_text() {
                continue;
            }

            let payload = msg.into_data();
            let event: BinanceUserDataEvent = match serde_json::from_slice(&payload) {
                Ok(value) => value,
                Err(_) => continue,
            };

            match event {
                BinanceUserDataEvent::ExecutionReport(report) => {
                    self.apply_execution_report(report).await;
                }
                BinanceUserDataEvent::Other => {}
            }
        }
    }

    async fn apply_execution_report(&self, report: BinanceExecutionReport) {
        if report.client_order_id.trim().is_empty() {
            return;
        }

        let status = map_execution_report_status(&report.order_status, &report.execution_type);
        let ack_instant = Instant::now();

        {
            let mut manager = self.order_manager.lock().await;
            apply_status(
                &mut manager,
                report.client_order_id.clone(),
                Some(report.order_id),
                status,
            );
        }

        let mut waiters = self.ack_waiters.lock().await;
        match status {
            OrderStatus::New => {
                waiters.notify_place_waiter(&report.client_order_id, ack_instant);
            }
            OrderStatus::PartiallyFilled | OrderStatus::Filled => {
                waiters.place_waiters.remove(&report.client_order_id);
            }
            OrderStatus::Canceled => {
                waiters.notify_cancel_waiter(&report.client_order_id, ack_instant);
                waiters.place_waiters.remove(&report.client_order_id);
            }
            OrderStatus::Rejected => {
                waiters.notify_cancel_waiter(&report.client_order_id, ack_instant);
                waiters.place_waiters.remove(&report.client_order_id);
            }
            OrderStatus::PendingNew => {}
        }

        if matches!(
            status,
            OrderStatus::Filled | OrderStatus::Canceled | OrderStatus::Rejected
        ) {
            waiters.clear_for_client_order_id(&report.client_order_id);
        }
    }

    async fn create_listen_key(&self) -> Result<String, ExecutionError> {
        let (response, _) = match self
            .send_api_key_only_with_latency(Method::POST, "/api/v3/userDataStream", vec![])
            .await
        {
            Ok(value) => value,
            Err(error) => {
                self.maybe_disable_user_stream_from_error(&error).await;
                return Err(error);
            }
        };
        let status_code = response.status().as_u16();
        let body = response.text().await?;
        if looks_like_html(&body) {
            self.disable_user_stream(status_code).await;
            let snippet = body_snippet(&body);
            return Err(ExecutionError::Api {
                status: status_code,
                code: None,
                msg: body,
                path: Some("/api/v3/userDataStream".to_string()),
                body_snippet: Some(snippet),
                request_id: None,
                retry_count: 0,
            });
        }

        let payload: BinanceListenKeyResponse =
            serde_json::from_str(&body).map_err(|error| ExecutionError::Json(Box::new(error)))?;
        if payload.listen_key.trim().is_empty() {
            return Err(ExecutionError::MissingListenKey);
        }

        Ok(payload.listen_key)
    }

    async fn user_stream_keepalive_loop(&self, listen_key: String) {
        let mut interval = tokio::time::interval(Duration::from_secs(USER_STREAM_KEEPALIVE_SECS));
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

        loop {
            interval.tick().await;
            if let Err(error) = self.keepalive_listen_key(&listen_key).await {
                warn!(error = %error, "failed to keepalive listenKey; forcing user stream reconnect");
                break;
            }
        }
    }

    async fn keepalive_listen_key(&self, listen_key: &str) -> Result<(), ExecutionError> {
        let result = self
            .send_api_key_only_with_latency(
                Method::PUT,
                "/api/v3/userDataStream",
                vec![("listenKey", listen_key.to_string())],
            )
            .await;
        if let Err(error) = result {
            self.maybe_disable_user_stream_from_error(&error).await;
            return Err(error);
        }
        Ok(())
    }

    fn user_stream_url(&self, listen_key: &str) -> String {
        format!("{}/{}", self.ws_url, listen_key)
    }

    async fn send_public_get(
        &self,
        path: &str,
        params: Vec<(&str, String)>,
    ) -> Result<reqwest::Response, ExecutionError> {
        let (request_id, dns_cached_inferred, connection_reused_inferred) =
            self.next_request_hints();
        let prep_started = Instant::now();
        let query = build_query_string(&params);
        let url = build_url_with_query(&self.rest_url, path, &query);
        self.record_before_send_latency(prep_started.elapsed())
            .await;

        let rate_limiter_delay = self.wait_rate_limit().await;

        let started = Instant::now();
        let response_result = self.http.get(url).send().await;
        let elapsed = started.elapsed();
        self.record_latency(elapsed).await;

        match response_result {
            Ok(response) => {
                self.mark_connection_warm();
                if response.status().is_success() {
                    self.log_spike_if_needed(
                        path,
                        elapsed,
                        Some(response.status().as_u16()),
                        None,
                        0,
                        rate_limiter_delay,
                        dns_cached_inferred,
                        connection_reused_inferred,
                    );
                    return Ok(response);
                }

                let parsed = parse_api_error_with_meta(response, path, request_id, 0).await;
                self.log_rest_http_error(
                    path,
                    request_id,
                    0,
                    Some(parsed.status),
                    parsed.code,
                    Some(&parsed.body_snippet),
                );
                self.log_spike_if_needed(
                    path,
                    elapsed,
                    Some(parsed.status),
                    parsed.code,
                    0,
                    rate_limiter_delay,
                    dns_cached_inferred,
                    connection_reused_inferred,
                );
                Err(parsed.error)
            }
            Err(error) => {
                let snippet = body_snippet(&error.to_string());
                self.log_rest_http_error(path, request_id, 0, None, None, Some(&snippet));
                self.log_spike_if_needed(
                    path,
                    elapsed,
                    None,
                    None,
                    0,
                    rate_limiter_delay,
                    dns_cached_inferred,
                    connection_reused_inferred,
                );
                Err(ExecutionError::Http {
                    source: Box::new(error),
                    path: Some(path.to_string()),
                    request_id: Some(request_id),
                    retry_count: 0,
                    body_snippet: Some(snippet),
                })
            }
        }
    }

    async fn send_api_key_only_with_latency(
        &self,
        method: Method,
        path: &str,
        params: Vec<(&str, String)>,
    ) -> Result<(reqwest::Response, u64), ExecutionError> {
        let (request_id, dns_cached_inferred, connection_reused_inferred) =
            self.next_request_hints();
        let prep_started = Instant::now();
        let query = build_query_string(&params);
        let url = build_url_with_query(&self.rest_url, path, &query);
        self.record_before_send_latency(prep_started.elapsed())
            .await;

        let rate_limiter_delay = self.wait_rate_limit().await;

        let started = Instant::now();
        let response_result = self
            .http
            .request(method, url)
            .header(MBX_API_KEY_HEADER, &self.api_key)
            .send()
            .await;
        let elapsed = started.elapsed();
        self.record_latency(elapsed).await;

        match response_result {
            Ok(response) => {
                self.mark_connection_warm();
                if response.status().is_success() {
                    self.log_spike_if_needed(
                        path,
                        elapsed,
                        Some(response.status().as_u16()),
                        None,
                        0,
                        rate_limiter_delay,
                        dns_cached_inferred,
                        connection_reused_inferred,
                    );
                    return Ok((response, elapsed.as_micros() as u64));
                }

                let parsed = parse_api_error_with_meta(response, path, request_id, 0).await;
                self.log_rest_http_error(
                    path,
                    request_id,
                    0,
                    Some(parsed.status),
                    parsed.code,
                    Some(&parsed.body_snippet),
                );
                self.log_spike_if_needed(
                    path,
                    elapsed,
                    Some(parsed.status),
                    parsed.code,
                    0,
                    rate_limiter_delay,
                    dns_cached_inferred,
                    connection_reused_inferred,
                );
                Err(parsed.error)
            }
            Err(error) => {
                let snippet = body_snippet(&error.to_string());
                self.log_rest_http_error(path, request_id, 0, None, None, Some(&snippet));
                self.log_spike_if_needed(
                    path,
                    elapsed,
                    None,
                    None,
                    0,
                    rate_limiter_delay,
                    dns_cached_inferred,
                    connection_reused_inferred,
                );
                Err(ExecutionError::Http {
                    source: Box::new(error),
                    path: Some(path.to_string()),
                    request_id: Some(request_id),
                    retry_count: 0,
                    body_snippet: Some(snippet),
                })
            }
        }
    }

    async fn send_signed(
        &self,
        method: Method,
        path: &str,
        params: Vec<(&str, String)>,
    ) -> Result<reqwest::Response, ExecutionError> {
        let (response, _) = self.send_signed_with_latency(method, path, params).await?;
        Ok(response)
    }

    async fn send_signed_with_latency(
        &self,
        method: Method,
        path: &str,
        mut params: Vec<(&str, String)>,
    ) -> Result<(reqwest::Response, u64), ExecutionError> {
        let (request_id, dns_cached_inferred, connection_reused_inferred) =
            self.next_request_hints();
        let prep_started = Instant::now();
        params.push(("timestamp", now_unix_ms().to_string()));
        params.push(("recvWindow", self.recv_window_ms.to_string()));

        let mut query = build_query_string(&params);
        append_signature_param(&self.api_secret, &mut query)?;
        let url = build_url_with_query(&self.rest_url, path, &query);
        self.record_before_send_latency(prep_started.elapsed())
            .await;

        let rate_limiter_delay = self.wait_rate_limit().await;

        let started = Instant::now();
        let response_result = self
            .http
            .request(method, url)
            .header(MBX_API_KEY_HEADER, &self.api_key)
            .send()
            .await;
        let elapsed = started.elapsed();
        self.record_latency(elapsed).await;

        match response_result {
            Ok(response) => {
                self.mark_connection_warm();
                if response.status().is_success() {
                    self.log_spike_if_needed(
                        path,
                        elapsed,
                        Some(response.status().as_u16()),
                        None,
                        0,
                        rate_limiter_delay,
                        dns_cached_inferred,
                        connection_reused_inferred,
                    );
                    return Ok((response, elapsed.as_micros() as u64));
                }

                let parsed = parse_api_error_with_meta(response, path, request_id, 0).await;
                self.log_rest_http_error(
                    path,
                    request_id,
                    0,
                    Some(parsed.status),
                    parsed.code,
                    Some(&parsed.body_snippet),
                );
                self.log_spike_if_needed(
                    path,
                    elapsed,
                    Some(parsed.status),
                    parsed.code,
                    0,
                    rate_limiter_delay,
                    dns_cached_inferred,
                    connection_reused_inferred,
                );
                Err(parsed.error)
            }
            Err(error) => {
                let snippet = body_snippet(&error.to_string());
                self.log_rest_http_error(path, request_id, 0, None, None, Some(&snippet));
                self.log_spike_if_needed(
                    path,
                    elapsed,
                    None,
                    None,
                    0,
                    rate_limiter_delay,
                    dns_cached_inferred,
                    connection_reused_inferred,
                );
                Err(ExecutionError::Http {
                    source: Box::new(error),
                    path: Some(path.to_string()),
                    request_id: Some(request_id),
                    retry_count: 0,
                    body_snippet: Some(snippet),
                })
            }
        }
    }

    async fn wait_rate_limit(&self) -> Duration {
        let sleep_for = {
            let mut limiter = self.rate_limiter.lock().await;
            limiter.reserve_delay()
        };

        if !sleep_for.is_zero() {
            self.note_rate_limit_sleep(sleep_for);
            sleep(sleep_for).await;
        }

        sleep_for
    }

    async fn record_latency(&self, elapsed: Duration) {
        let snapshot = {
            let mut window = self.latency_window.lock().await;
            window.record(elapsed)
        };

        if let Some(snapshot) = snapshot {
            let rate_limiter_slept_count_window = self
                .rate_limit_sleep_count_window
                .swap(0, AtomicOrdering::Relaxed);
            let rate_limiter_slept_ms_window = self
                .rate_limit_sleep_ms_window
                .swap(0, AtomicOrdering::Relaxed);
            let rate_limiter_slept_count_total = self
                .rate_limit_sleep_count_total
                .load(AtomicOrdering::Relaxed);
            let rate_limiter_slept_ms_total =
                self.rate_limit_sleep_ms_total.load(AtomicOrdering::Relaxed);

            info!(
                ops = snapshot.total_operations,
                after_p50_us = snapshot.p50_us,
                after_p95_us = snapshot.p95_us,
                after_p99_us = snapshot.p99_us,
                rate_limiter_slept_count_window,
                rate_limiter_slept_ms_window,
                rate_limiter_slept_count_total,
                rate_limiter_slept_ms_total,
                "execution after-send latency window"
            );
        }
    }

    fn next_request_hints(&self) -> (u64, bool, bool) {
        let request_number = self.request_counter.fetch_add(1, AtomicOrdering::Relaxed) + 1;
        let request_id = request_number;
        let dns_cached_inferred = request_number > 1;
        let connection_reused_inferred =
            request_number > 1 && self.connection_warm.load(AtomicOrdering::Relaxed);
        (request_id, dns_cached_inferred, connection_reused_inferred)
    }

    fn mark_connection_warm(&self) {
        self.connection_warm.store(true, AtomicOrdering::Relaxed);
    }

    fn note_rate_limit_sleep(&self, sleep_for: Duration) {
        let sleep_ms = sleep_for.as_millis() as u64;
        self.rate_limit_sleep_count_total
            .fetch_add(1, AtomicOrdering::Relaxed);
        self.rate_limit_sleep_ms_total
            .fetch_add(sleep_ms, AtomicOrdering::Relaxed);
        self.rate_limit_sleep_count_window
            .fetch_add(1, AtomicOrdering::Relaxed);
        self.rate_limit_sleep_ms_window
            .fetch_add(sleep_ms, AtomicOrdering::Relaxed);
    }

    fn log_rest_http_error(
        &self,
        path: &str,
        request_id: u64,
        retry_count: u32,
        http_status: Option<u16>,
        binance_code: Option<i64>,
        body_snippet: Option<&str>,
    ) {
        let error_reason = explicit_rest_error_reason(http_status, binance_code);
        warn!(
            transport = "rest",
            path,
            request_id,
            retry_count,
            http_status = ?http_status,
            binance_code = ?binance_code,
            error_reason = ?error_reason,
            last_error_body_snippet = ?body_snippet,
            "execution rest request failed"
        );
    }

    #[allow(clippy::too_many_arguments)]
    fn log_spike_if_needed(
        &self,
        path: &str,
        elapsed: Duration,
        http_status: Option<u16>,
        binance_code: Option<i64>,
        retry_count: u32,
        rate_limiter_delay: Duration,
        dns_cached_inferred: bool,
        connection_reused_inferred: bool,
    ) {
        if elapsed < Duration::from_millis(SPIKE_LOG_THRESHOLD_MS) {
            return;
        }

        info!(
            path,
            elapsed_ms = elapsed.as_millis() as u64,
            http_status = ?http_status,
            binance_code = ?binance_code,
            retry_count,
            rate_limiter_delay_ms = rate_limiter_delay.as_millis() as u64,
            dns_cached_inferred,
            connection_reused_inferred,
            rate_limiter_slept_count_total = self
                .rate_limit_sleep_count_total
                .load(AtomicOrdering::Relaxed),
            rate_limiter_slept_ms_total = self
                .rate_limit_sleep_ms_total
                .load(AtomicOrdering::Relaxed),
            "execution latency spike"
        );
    }

    async fn record_before_send_latency(&self, elapsed: Duration) {
        let snapshot = {
            let mut window = self.prep_latency_window.lock().await;
            window.record(elapsed)
        };

        if let Some(snapshot) = snapshot {
            info!(
                ops = snapshot.total_operations,
                before_p50_us = snapshot.p50_us,
                before_p95_us = snapshot.p95_us,
                before_p99_us = snapshot.p99_us,
                "execution before-send latency window"
            );
        }
    }
}

#[derive(Clone)]
pub struct BinanceWsExecutor {
    inner: BinanceTestnetExecutor,
    ws_api_url: String,
    ws_api_socket: Arc<Mutex<Option<BinanceWsSocket>>>,
    next_request_id: Arc<AtomicU64>,
}

impl BinanceWsExecutor {
    pub fn from_config(cfg: &ExecutionConfig) -> Result<Self, ExecutionError> {
        let inner = BinanceTestnetExecutor::from_config(cfg)?;
        let ws_api_url = cfg.ws_api_url.trim().trim_end_matches('/').to_string();
        if ws_api_url.is_empty() {
            return Err(ExecutionError::WsApiProtocol(
                "execution.ws_api_url must not be empty".to_string(),
            ));
        }

        Ok(Self {
            inner,
            ws_api_url,
            ws_api_socket: Arc::new(Mutex::new(None)),
            next_request_id: Arc::new(AtomicU64::new(1)),
        })
    }

    pub async fn fetch_symbol_filters(
        &self,
        symbol: &str,
    ) -> Result<SymbolFilters, ExecutionError> {
        self.inner.fetch_symbol_filters(symbol).await
    }

    pub async fn new_limit_order(
        &self,
        request: &OrderRequest,
        client_order_id: String,
    ) -> Result<ManagedOrder, ExecutionError> {
        let outcome = self
            .new_limit_order_with_metrics(request, client_order_id)
            .await?;
        Ok(outcome.order)
    }

    pub async fn new_limit_order_with_metrics(
        &self,
        request: &OrderRequest,
        client_order_id: String,
    ) -> Result<PlaceOrderResult, ExecutionError> {
        self.inner.ensure_user_stream_started().await;

        {
            let mut manager = self.inner.order_manager.lock().await;
            manager.mark_pending_new(client_order_id.clone());
        }

        let place_ack_rx = if self.inner.is_user_stream_enabled() {
            Some(
                self.inner
                    .register_place_waiter(client_order_id.clone())
                    .await,
            )
        } else {
            None
        };
        let place_started = Instant::now();

        let mut params = vec![
            ("symbol", JsonValue::String(request.symbol.to_uppercase())),
            (
                "side",
                JsonValue::String(request.side.as_binance().to_string()),
            ),
            (
                "type",
                JsonValue::String(request.order_type.as_binance().to_string()),
            ),
            ("quantity", JsonValue::String(decimal_string(request.qty))),
            ("price", JsonValue::String(decimal_string(request.price))),
            (
                "newClientOrderId",
                JsonValue::String(client_order_id.clone()),
            ),
            ("newOrderRespType", JsonValue::String("RESULT".to_string())),
        ];
        if request.order_type.needs_time_in_force() {
            params.push(("timeInForce", JsonValue::String("GTC".to_string())));
        }

        let response = self
            .send_signed_ws_with_latency::<BinanceOrderResponse>("order.place", params)
            .await;

        match response {
            Ok((payload, place_http_us)) => {
                let status = map_binance_status(&payload.status);
                let final_client_order_id = payload
                    .orig_client_order_id
                    .clone()
                    .unwrap_or_else(|| payload.client_order_id.clone());

                let managed = {
                    let mut manager = self.inner.order_manager.lock().await;
                    apply_status(
                        &mut manager,
                        final_client_order_id.clone(),
                        Some(payload.order_id),
                        status,
                    )
                };

                let place_to_ack_us = if matches!(status, OrderStatus::Rejected) {
                    self.inner.clear_waiters_for(&final_client_order_id).await;
                    None
                } else if let Some(place_ack_rx) = place_ack_rx {
                    let measured = self.inner.wait_for_ack(place_ack_rx, place_started).await;
                    if measured.is_none() {
                        self.inner.clear_waiters_for(&final_client_order_id).await;
                    }
                    measured
                } else {
                    self.inner
                        .wait_for_place_ack_fallback(
                            &request.symbol,
                            &final_client_order_id,
                            place_started,
                        )
                        .await
                };
                let (place_to_ack_us, place_ack_at) = match place_to_ack_us {
                    Some(value) => (Some(value.latency_us), Some(value.ack_instant)),
                    None => (None, None),
                };

                Ok(PlaceOrderResult {
                    order: managed,
                    place_http_us,
                    place_to_ack_us,
                    place_ack_at,
                })
            }
            Err(error) => {
                self.inner.clear_waiters_for(&client_order_id).await;
                let mut manager = self.inner.order_manager.lock().await;
                manager.mark_rejected(client_order_id, None);
                Err(error)
            }
        }
    }

    pub async fn cancel_order(
        &self,
        symbol: &str,
        client_order_id: &str,
    ) -> Result<ManagedOrder, ExecutionError> {
        let outcome = self
            .cancel_order_with_metrics(symbol, client_order_id)
            .await?;
        Ok(outcome.order)
    }

    pub async fn cancel_order_with_metrics(
        &self,
        symbol: &str,
        client_order_id: &str,
    ) -> Result<CancelOrderResult, ExecutionError> {
        self.inner.ensure_user_stream_started().await;

        let cancel_ack_rx = if self.inner.is_user_stream_enabled() {
            Some(
                self.inner
                    .register_cancel_waiter(client_order_id.to_string())
                    .await,
            )
        } else {
            None
        };
        let cancel_started = Instant::now();

        let params = vec![
            ("symbol", JsonValue::String(symbol.to_uppercase())),
            (
                "origClientOrderId",
                JsonValue::String(client_order_id.to_string()),
            ),
        ];

        let response = self
            .send_signed_ws_with_latency::<BinanceOrderResponse>("order.cancel", params)
            .await;
        let (payload, cancel_http_us) = match response {
            Ok(value) => value,
            Err(error) => {
                self.inner.clear_waiters_for(client_order_id).await;
                return Err(error);
            }
        };

        let status = map_binance_status(&payload.status);
        let final_client_order_id = payload
            .orig_client_order_id
            .clone()
            .unwrap_or_else(|| payload.client_order_id.clone());

        let managed = {
            let mut manager = self.inner.order_manager.lock().await;
            apply_status(
                &mut manager,
                final_client_order_id.clone(),
                Some(payload.order_id),
                status,
            )
        };

        let cancel_to_ack_us = if matches!(status, OrderStatus::Canceled) {
            if let Some(cancel_ack_rx) = cancel_ack_rx {
                let measured = self.inner.wait_for_ack(cancel_ack_rx, cancel_started).await;
                if measured.is_none() {
                    self.inner.clear_waiters_for(&final_client_order_id).await;
                }
                measured
            } else {
                self.inner
                    .wait_for_cancel_ack_fallback(symbol, &final_client_order_id, cancel_started)
                    .await
            }
        } else {
            self.inner.clear_waiters_for(&final_client_order_id).await;
            None
        };
        let (cancel_to_ack_us, cancel_ack_at) = match cancel_to_ack_us {
            Some(value) => (Some(value.latency_us), Some(value.ack_instant)),
            None => (None, None),
        };

        Ok(CancelOrderResult {
            order: managed,
            cancel_http_us,
            cancel_to_ack_us,
            cancel_ack_at,
        })
    }

    pub async fn query_order(
        &self,
        symbol: &str,
        client_order_id: &str,
    ) -> Result<ManagedOrder, ExecutionError> {
        let snapshot = self.query_order_snapshot(symbol, client_order_id).await?;
        Ok(snapshot.order)
    }

    pub async fn query_order_snapshot(
        &self,
        symbol: &str,
        client_order_id: &str,
    ) -> Result<OrderSnapshot, ExecutionError> {
        let params = vec![
            ("symbol", JsonValue::String(symbol.to_uppercase())),
            (
                "origClientOrderId",
                JsonValue::String(client_order_id.to_string()),
            ),
        ];

        let (payload, _) = self
            .send_signed_ws_with_latency::<BinanceOrderResponse>("order.status", params)
            .await?;
        let status = map_binance_status(&payload.status);
        let final_client_order_id = payload
            .orig_client_order_id
            .clone()
            .unwrap_or_else(|| payload.client_order_id.clone());

        let managed = {
            let mut manager = self.inner.order_manager.lock().await;
            apply_status(
                &mut manager,
                final_client_order_id,
                Some(payload.order_id),
                status,
            )
        };

        Ok(OrderSnapshot {
            order: managed,
            orig_qty: parse_non_negative_decimal_or_zero(&payload.orig_qty),
            executed_qty: parse_non_negative_decimal_or_zero(&payload.executed_qty),
        })
    }

    pub async fn order_state(&self, client_order_id: &str) -> Option<ManagedOrder> {
        self.inner.order_state(client_order_id).await
    }

    async fn send_signed_ws_with_latency<T>(
        &self,
        method: &str,
        params: Vec<(&str, JsonValue)>,
    ) -> Result<(T, u64), ExecutionError>
    where
        T: DeserializeOwned,
    {
        let (_request_id, dns_cached_inferred, connection_reused_inferred) =
            self.inner.next_request_hints();
        let prep_started = Instant::now();

        let timestamp = now_unix_ms();
        let recv_window_ms = self.inner.recv_window_ms;

        let mut payload_params = JsonMap::with_capacity(params.len() + 4);
        let mut signature_params: Vec<(&str, String)> = Vec::with_capacity(params.len() + 3);
        for (key, value) in params {
            signature_params.push((key, ws_param_to_string_for_signature(key, &value)?));
            payload_params.insert(key.to_string(), value);
        }

        signature_params.push(("apiKey", self.inner.api_key.clone()));
        signature_params.push(("recvWindow", recv_window_ms.to_string()));
        signature_params.push(("timestamp", timestamp.to_string()));
        signature_params.sort_unstable_by(|left, right| left.0.cmp(right.0));

        let signature_payload = build_query_string(&signature_params);
        let signature = signature_hex(&self.inner.api_secret, &signature_payload)?;

        payload_params.insert(
            "apiKey".to_string(),
            JsonValue::String(self.inner.api_key.clone()),
        );
        payload_params.insert(
            "recvWindow".to_string(),
            JsonValue::Number(serde_json::Number::from(recv_window_ms)),
        );
        payload_params.insert(
            "timestamp".to_string(),
            JsonValue::Number(serde_json::Number::from(timestamp)),
        );
        payload_params.insert("signature".to_string(), JsonValue::String(signature));

        let ws_request = BinanceWsApiRequest {
            id: self.next_request_id.fetch_add(1, AtomicOrdering::Relaxed),
            method: method.to_string(),
            params: payload_params,
        };
        let ws_payload = serde_json::to_string(&ws_request)?;
        self.inner
            .record_before_send_latency(prep_started.elapsed())
            .await;

        let rate_limiter_delay = self.inner.wait_rate_limit().await;

        let started = Instant::now();
        let (response_payload, retry_count) =
            match self.send_ws_request_with_retry(&ws_payload).await {
                Ok(value) => value,
                Err((error, retry_count)) => {
                    let elapsed = started.elapsed();
                    self.inner.record_latency(elapsed).await;
                    self.inner.log_spike_if_needed(
                        method,
                        elapsed,
                        None,
                        None,
                        retry_count,
                        rate_limiter_delay,
                        dns_cached_inferred,
                        connection_reused_inferred,
                    );
                    return Err(error);
                }
            };
        let elapsed = started.elapsed();
        self.inner.record_latency(elapsed).await;
        self.inner.mark_connection_warm();

        let response: BinanceWsApiResponse<T> = serde_json::from_str(&response_payload)?;
        if (200..300).contains(&response.status) {
            if let Some(result) = response.result {
                self.inner.log_spike_if_needed(
                    method,
                    elapsed,
                    Some(response.status),
                    None,
                    retry_count,
                    rate_limiter_delay,
                    dns_cached_inferred,
                    connection_reused_inferred,
                );
                return Ok((result, elapsed.as_micros() as u64));
            }

            self.inner.log_spike_if_needed(
                method,
                elapsed,
                Some(response.status),
                None,
                retry_count,
                rate_limiter_delay,
                dns_cached_inferred,
                connection_reused_inferred,
            );
            return Err(ExecutionError::WsApiProtocol(
                "ws-api success response missing result".to_string(),
            ));
        }

        let code = response.error.as_ref().and_then(|error| error.code);
        self.inner.log_spike_if_needed(
            method,
            elapsed,
            Some(response.status),
            code,
            retry_count,
            rate_limiter_delay,
            dns_cached_inferred,
            connection_reused_inferred,
        );
        let msg = response
            .error
            .and_then(|error| error.msg)
            .unwrap_or_else(|| "unknown ws-api error".to_string());

        if response.status == StatusCode::TOO_MANY_REQUESTS.as_u16()
            || code == Some(-1003)
            || code == Some(-1015)
        {
            return Err(ExecutionError::RateLimited(msg));
        }

        Err(ExecutionError::Api {
            status: response.status,
            code,
            msg,
            path: None,
            body_snippet: None,
            request_id: None,
            retry_count,
        })
    }

    async fn send_ws_request_with_retry(
        &self,
        ws_payload: &str,
    ) -> Result<(String, u32), (ExecutionError, u32)> {
        match self.send_ws_request_once(ws_payload).await {
            Ok(response) => Ok((response, 0)),
            Err(error @ ExecutionError::WsApiSocket(_)) => {
                warn!(error = %error, "ws-api request failed; reconnecting and retrying once");
                self.reset_ws_api_socket().await;
                sleep(Duration::from_millis(WS_API_RECONNECT_BACKOFF_MS)).await;
                match self.send_ws_request_once(ws_payload).await {
                    Ok(response) => Ok((response, 1)),
                    Err(error) => Err((error, 1)),
                }
            }
            Err(error) => Err((error, 0)),
        }
    }

    async fn send_ws_request_once(&self, ws_payload: &str) -> Result<String, ExecutionError> {
        let mut socket_guard = self.ws_api_socket.lock().await;
        if socket_guard.is_none() {
            let (socket, _) = connect_async(&self.ws_api_url)
                .await
                .map_err(ws_api_socket_error)?;
            *socket_guard = Some(socket);
        }

        let socket = socket_guard.as_mut().ok_or_else(|| {
            ExecutionError::WsApiProtocol("ws-api socket unavailable".to_string())
        })?;

        if let Err(error) = socket.send(Message::Text(ws_payload.to_string())).await {
            *socket_guard = None;
            return Err(ws_api_socket_error(error));
        }

        loop {
            let maybe_message = socket.next().await;
            let message = match maybe_message {
                Some(Ok(message)) => message,
                Some(Err(error)) => {
                    *socket_guard = None;
                    return Err(ws_api_socket_error(error));
                }
                None => {
                    *socket_guard = None;
                    return Err(ws_api_socket_error(
                        tokio_tungstenite::tungstenite::Error::ConnectionClosed,
                    ));
                }
            };

            match message {
                Message::Text(text) => return Ok(text.to_string()),
                Message::Binary(bytes) => {
                    if let Ok(text) = String::from_utf8(bytes.to_vec()) {
                        return Ok(text);
                    }
                }
                Message::Ping(payload) => {
                    if let Err(error) = socket.send(Message::Pong(payload)).await {
                        *socket_guard = None;
                        return Err(ws_api_socket_error(error));
                    }
                }
                Message::Pong(_) => {}
                Message::Close(_) => {
                    *socket_guard = None;
                    return Err(ws_api_socket_error(
                        tokio_tungstenite::tungstenite::Error::ConnectionClosed,
                    ));
                }
                Message::Frame(_) => {}
            }
        }
    }

    async fn reset_ws_api_socket(&self) {
        let mut socket_guard = self.ws_api_socket.lock().await;
        *socket_guard = None;
    }
}

#[derive(Debug, Clone, Default)]
pub struct PaperExecutor;

impl PaperExecutor {
    pub async fn execute(&self, request: &OrderRequest) -> Result<Fill, ExecutionError> {
        sleep(Duration::from_millis(3)).await;
        info!(
            symbol = %request.symbol,
            qty = request.qty,
            price = request.price,
            "paper order executed"
        );

        Ok(Fill {
            symbol: request.symbol.clone(),
            side: request.side,
            qty: request.qty,
            price: request.price,
        })
    }
}

#[derive(Debug, Clone)]
pub struct LiveExecutor {
    testnet: bool,
    venue: String,
}

impl LiveExecutor {
    pub fn new(testnet: bool, venue: impl Into<String>) -> Self {
        Self {
            testnet,
            venue: venue.into(),
        }
    }

    pub async fn execute(&self, request: &OrderRequest) -> Result<Fill, ExecutionError> {
        if !self.testnet {
            return Err(ExecutionError::Rejected(
                "live trading disabled: set execution.testnet=true".to_string(),
            ));
        }

        sleep(Duration::from_millis(8)).await;
        info!(
            venue = %self.venue,
            symbol = %request.symbol,
            qty = request.qty,
            price = request.price,
            "testnet order submitted (stub)"
        );

        Ok(Fill {
            symbol: request.symbol.clone(),
            side: request.side,
            qty: request.qty,
            price: request.price,
        })
    }
}

#[derive(Debug, Deserialize)]
struct BinanceOrderResponse {
    #[serde(rename = "orderId")]
    order_id: u64,
    #[serde(rename = "clientOrderId")]
    client_order_id: String,
    #[serde(rename = "origClientOrderId")]
    orig_client_order_id: Option<String>,
    #[serde(rename = "origQty", default)]
    orig_qty: String,
    #[serde(rename = "executedQty", default)]
    executed_qty: String,
    status: String,
}

#[derive(Debug, Deserialize)]
struct BinanceErrorResponse {
    code: Option<i64>,
    msg: Option<String>,
}

#[derive(Debug, Deserialize)]
struct BinanceExchangeInfoResponse {
    symbols: Vec<BinanceExchangeInfoSymbol>,
}

#[derive(Debug, Deserialize)]
struct BinanceExchangeInfoSymbol {
    symbol: String,
    filters: Vec<BinanceExchangeFilter>,
}

#[derive(Debug, Deserialize)]
struct BinanceExchangeFilter {
    #[serde(rename = "filterType")]
    filter_type: String,
    #[serde(rename = "tickSize")]
    tick_size: Option<String>,
    #[serde(rename = "stepSize")]
    step_size: Option<String>,
    #[serde(rename = "minNotional")]
    min_notional: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(tag = "e")]
enum BinanceUserDataEvent {
    #[serde(rename = "executionReport")]
    ExecutionReport(BinanceExecutionReport),
    #[serde(other)]
    Other,
}

#[derive(Debug, Deserialize)]
struct BinanceExecutionReport {
    #[serde(rename = "c")]
    client_order_id: String,
    #[serde(rename = "i")]
    order_id: u64,
    #[serde(rename = "x")]
    execution_type: String,
    #[serde(rename = "X")]
    order_status: String,
}

#[derive(Debug, Deserialize)]
struct BinanceListenKeyResponse {
    #[serde(rename = "listenKey")]
    listen_key: String,
}

#[derive(Debug, Serialize)]
struct BinanceWsApiRequest {
    id: u64,
    method: String,
    params: JsonMap<String, JsonValue>,
}

#[derive(Debug, Deserialize)]
struct BinanceWsApiResponse<T> {
    status: u16,
    result: Option<T>,
    error: Option<BinanceWsApiErrorResponse>,
}

#[derive(Debug, Deserialize)]
struct BinanceWsApiErrorResponse {
    code: Option<i64>,
    msg: Option<String>,
}

fn now_unix_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

fn decimal_string(value: f64) -> String {
    let mut out = format!("{value:.8}");
    while out.contains('.') && out.ends_with('0') {
        out.pop();
    }
    if out.ends_with('.') {
        out.pop();
    }
    out
}

fn parse_non_negative_decimal_or_zero(raw: &str) -> f64 {
    raw.parse::<f64>()
        .ok()
        .filter(|value| value.is_finite() && *value >= 0.0)
        .unwrap_or(0.0)
}

fn build_query_string(params: &[(&str, String)]) -> String {
    if params.is_empty() {
        return String::new();
    }

    let total_len = params
        .iter()
        .map(|(key, value)| key.len() + 1 + value.len())
        .sum::<usize>()
        + params.len().saturating_sub(1);

    let mut query = String::with_capacity(total_len);
    for (idx, (key, value)) in params.iter().enumerate() {
        if idx > 0 {
            query.push('&');
        }
        query.push_str(key);
        query.push('=');
        query.push_str(value);
    }

    query
}

fn build_url_with_query(base: &str, path: &str, query: &str) -> String {
    let extra = if query.is_empty() { 0 } else { 1 + query.len() };
    let mut url = String::with_capacity(base.len() + path.len() + extra);
    url.push_str(base);
    url.push_str(path);

    if query.is_empty() {
        url
    } else {
        url.push('?');
        url.push_str(query);
        url
    }
}

fn append_signature_param(secret: &str, query: &mut String) -> Result<(), ExecutionError> {
    let signature = signature_hex(secret, query)?;
    query.reserve("&signature=".len() + signature.len());
    query.push_str("&signature=");
    query.push_str(&signature);
    Ok(())
}

fn signature_hex(secret: &str, payload: &str) -> Result<String, ExecutionError> {
    let mut mac = HmacSha256::new_from_slice(secret.as_bytes())
        .map_err(|e| ExecutionError::Signature(e.to_string()))?;
    mac.update(payload.as_bytes());
    let digest = mac.finalize().into_bytes();

    let mut signature = String::with_capacity(digest.len() * 2);
    append_hex_lower(&digest, &mut signature);
    Ok(signature)
}

fn ws_api_socket_error(error: tokio_tungstenite::tungstenite::Error) -> ExecutionError {
    ExecutionError::WsApiSocket(Box::new(error))
}

fn ws_param_to_string_for_signature(
    key: &str,
    value: &JsonValue,
) -> Result<String, ExecutionError> {
    match value {
        JsonValue::String(raw) => Ok(raw.clone()),
        JsonValue::Number(raw) => Ok(raw.to_string()),
        JsonValue::Bool(raw) => Ok(if *raw {
            "true".to_string()
        } else {
            "false".to_string()
        }),
        _ => Err(ExecutionError::Signature(format!(
            "unsupported ws-api param type for key '{key}'"
        ))),
    }
}

fn append_hex_lower(bytes: &[u8], out: &mut String) {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    for byte in bytes {
        out.push(HEX[(byte >> 4) as usize] as char);
        out.push(HEX[(byte & 0x0f) as usize] as char);
    }
}

fn parse_exchange_filter_number(
    symbol: &str,
    field: &'static str,
    raw: &str,
    must_be_positive: bool,
) -> Result<f64, ExecutionError> {
    let parsed = raw
        .parse::<f64>()
        .map_err(|_| ExecutionError::InvalidExchangeInfoFilter {
            symbol: symbol.to_string(),
            field,
            value: raw.to_string(),
        })?;

    let valid = if must_be_positive {
        parsed.is_finite() && parsed > 0.0
    } else {
        parsed.is_finite() && parsed >= 0.0
    };
    if !valid {
        return Err(ExecutionError::InvalidExchangeInfoFilter {
            symbol: symbol.to_string(),
            field,
            value: raw.to_string(),
        });
    }

    Ok(parsed)
}

fn apply_status(
    manager: &mut OrderManager,
    client_order_id: String,
    order_id: Option<u64>,
    status: OrderStatus,
) -> ManagedOrder {
    match status {
        OrderStatus::PendingNew => manager.mark_pending_new(client_order_id),
        OrderStatus::New => match order_id {
            Some(value) => manager.mark_new(client_order_id, value),
            None => manager.mark_pending_new(client_order_id),
        },
        OrderStatus::PartiallyFilled => manager.mark_partially_filled(client_order_id, order_id),
        OrderStatus::Filled => manager.mark_filled(client_order_id, order_id),
        OrderStatus::Canceled => manager.mark_canceled(client_order_id, order_id),
        OrderStatus::Rejected => manager.mark_rejected(client_order_id, order_id),
    }
}

fn map_binance_status(status: &str) -> OrderStatus {
    match status {
        "NEW" => OrderStatus::New,
        "PARTIALLY_FILLED" => OrderStatus::PartiallyFilled,
        "FILLED" => OrderStatus::Filled,
        "CANCELED" | "EXPIRED" => OrderStatus::Canceled,
        "REJECTED" => OrderStatus::Rejected,
        _ => OrderStatus::PendingNew,
    }
}

fn map_execution_report_status(order_status: &str, execution_type: &str) -> OrderStatus {
    let mapped = map_binance_status(order_status);
    if !matches!(mapped, OrderStatus::PendingNew) {
        return mapped;
    }

    match execution_type {
        "NEW" => OrderStatus::New,
        "TRADE" => OrderStatus::PartiallyFilled,
        "CANCELED" => OrderStatus::Canceled,
        "REJECTED" => OrderStatus::Rejected,
        _ => OrderStatus::PendingNew,
    }
}

fn initial_user_stream_state(testnet: bool, enable_user_stream: bool) -> (bool, bool) {
    if testnet && enable_user_stream {
        (false, true)
    } else {
        (enable_user_stream, false)
    }
}

fn user_stream_unavailable_status(error: &ExecutionError) -> Option<u16> {
    let ExecutionError::Api { status, msg, .. } = error else {
        return None;
    };

    if *status == StatusCode::GONE.as_u16() || *status == StatusCode::NOT_FOUND.as_u16() {
        return Some(*status);
    }

    if looks_like_html(msg) {
        return Some(*status);
    }

    None
}

fn looks_like_html(body: &str) -> bool {
    let trimmed = body.trim_start();
    trimmed.starts_with("<!doctype html")
        || trimmed.starts_with("<!DOCTYPE html")
        || trimmed.starts_with("<html")
        || trimmed.starts_with("<HTML")
}

fn is_place_visible_status(status: OrderStatus) -> bool {
    matches!(
        status,
        OrderStatus::New
            | OrderStatus::PartiallyFilled
            | OrderStatus::Filled
            | OrderStatus::Canceled
            | OrderStatus::Rejected
    )
}

fn is_cancel_visible_status(status: OrderStatus) -> bool {
    matches!(
        status,
        OrderStatus::Canceled | OrderStatus::Rejected | OrderStatus::Filled
    )
}

fn is_poll_retryable_error(error: &ExecutionError) -> bool {
    match error {
        ExecutionError::Api { status, code, .. } => {
            (*status == StatusCode::BAD_REQUEST.as_u16() && *code == Some(-2013))
                || *status >= StatusCode::INTERNAL_SERVER_ERROR.as_u16()
        }
        ExecutionError::RateLimited(_) => true,
        ExecutionError::Http { source, .. } => source.is_timeout(),
        _ => false,
    }
}

pub fn explicit_rest_error_reason(
    http_status: Option<u16>,
    binance_code: Option<i64>,
) -> Option<&'static str> {
    match (http_status, binance_code) {
        (_, Some(-1022)) => Some("invalid_signature"),
        (_, Some(-1021)) => Some("invalid_timestamp"),
        (_, Some(-2015)) => Some("invalid_api_key_or_permissions"),
        (_, Some(-2014)) => Some("invalid_api_key_format"),
        (Some(401), _) => Some("unauthorized"),
        (Some(403), _) => Some("forbidden"),
        _ => None,
    }
}

fn body_snippet(body: &str) -> String {
    body.chars().take(ERROR_BODY_SNIPPET_MAX_CHARS).collect()
}

fn percentile(sorted_values: &[u64], pct: f64) -> u64 {
    if sorted_values.is_empty() {
        return 0;
    }
    let rank = ((sorted_values.len() as f64 - 1.0) * pct).round() as usize;
    sorted_values[rank.min(sorted_values.len() - 1)]
}

async fn parse_api_error_with_meta(
    response: reqwest::Response,
    path: &str,
    request_id: u64,
    retry_count: u32,
) -> ParsedApiError {
    let status = response.status();
    let body = response.text().await.unwrap_or_default();
    let body_snippet = body_snippet(&body);

    let parsed = serde_json::from_str::<BinanceErrorResponse>(&body);
    let (code, msg) = match parsed {
        Ok(payload) => (
            payload.code,
            payload
                .msg
                .unwrap_or_else(|| "unknown binance api error".to_string()),
        ),
        Err(_) => (None, body),
    };

    if status == StatusCode::TOO_MANY_REQUESTS || code == Some(-1003) {
        return ParsedApiError {
            error: ExecutionError::RateLimited(msg),
            status: status.as_u16(),
            code,
            body_snippet,
        };
    }

    ParsedApiError {
        error: ExecutionError::Api {
            status: status.as_u16(),
            code,
            msg,
            path: Some(path.to_string()),
            body_snippet: Some(body_snippet.clone()),
            request_id: Some(request_id),
            retry_count,
        },
        status: status.as_u16(),
        code,
        body_snippet,
    }
}

fn ensure_rustls_crypto_provider() {
    static INIT: Once = Once::new();
    INIT.call_once(|| {
        if rustls::crypto::CryptoProvider::install_default(rustls::crypto::ring::default_provider())
            .is_err()
        {
            warn!("could not install rustls ring provider (already installed)");
        }
    });
}

#[cfg(test)]
mod tests {
    use super::{
        explicit_rest_error_reason, initial_user_stream_state, map_binance_status,
        map_execution_report_status, user_stream_unavailable_status, ExecutionError, OrderManager,
        OrderStatus,
    };

    #[test]
    fn order_manager_transitions() {
        let mut manager = OrderManager::default();

        let pending = manager.mark_pending_new("abc".to_string());
        assert_eq!(pending.status, OrderStatus::PendingNew);

        let new = manager.mark_new("abc".to_string(), 42);
        assert_eq!(new.status, OrderStatus::New);
        assert_eq!(new.order_id, Some(42));

        let canceled = manager.mark_canceled("abc".to_string(), Some(42));
        assert_eq!(canceled.status, OrderStatus::Canceled);

        let fetched = manager.get("abc").expect("order should be present");
        assert_eq!(fetched.status, OrderStatus::Canceled);
    }

    #[test]
    fn maps_binance_statuses() {
        assert_eq!(map_binance_status("NEW"), OrderStatus::New);
        assert_eq!(
            map_binance_status("PARTIALLY_FILLED"),
            OrderStatus::PartiallyFilled
        );
        assert_eq!(map_binance_status("FILLED"), OrderStatus::Filled);
        assert_eq!(map_binance_status("CANCELED"), OrderStatus::Canceled);
        assert_eq!(map_binance_status("REJECTED"), OrderStatus::Rejected);
    }

    #[test]
    fn maps_execution_report_statuses() {
        assert_eq!(map_execution_report_status("NEW", "NEW"), OrderStatus::New);
        assert_eq!(
            map_execution_report_status("PARTIALLY_FILLED", "TRADE"),
            OrderStatus::PartiallyFilled
        );
        assert_eq!(
            map_execution_report_status("FILLED", "TRADE"),
            OrderStatus::Filled
        );
        assert_eq!(
            map_execution_report_status("UNKNOWN", "CANCELED"),
            OrderStatus::Canceled
        );
    }

    #[test]
    fn disables_user_stream_on_410() {
        let error = ExecutionError::Api {
            status: 410,
            code: None,
            msg: "Gone".to_string(),
            path: None,
            body_snippet: None,
            request_id: None,
            retry_count: 0,
        };
        assert_eq!(user_stream_unavailable_status(&error), Some(410));
    }

    #[test]
    fn testnet_defaults_user_stream_to_disabled() {
        assert_eq!(initial_user_stream_state(true, true), (false, true));
        assert_eq!(initial_user_stream_state(true, false), (false, false));
        assert_eq!(initial_user_stream_state(false, true), (true, false));
    }

    #[test]
    fn maps_explicit_rest_error_reasons() {
        assert_eq!(
            explicit_rest_error_reason(Some(401), None),
            Some("unauthorized")
        );
        assert_eq!(
            explicit_rest_error_reason(Some(400), Some(-1022)),
            Some("invalid_signature")
        );
        assert_eq!(
            explicit_rest_error_reason(Some(400), Some(-1021)),
            Some("invalid_timestamp")
        );
    }
}
