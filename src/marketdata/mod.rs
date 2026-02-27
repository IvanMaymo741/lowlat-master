use std::sync::Once;
use std::time::{Duration, Instant};

use futures_util::{SinkExt, StreamExt};
use ordered_float::OrderedFloat;
use serde::Deserialize;
use serde_json::Value as JsonValue;
use thiserror::Error;
use tokio::sync::mpsc;
use tokio::sync::mpsc::error::TrySendError;
use tokio::time;
use tokio_tungstenite::connect_async;
use tokio_tungstenite::tungstenite::Message;
use tracing::{debug, info, warn};

use crate::config::MarketDataConfig;
use crate::orderbook::{
    BinanceSpotOrderBook, DepthApplyResult, DepthDelta, DepthSnapshot, LevelUpdate, OrderBookError,
};
use crate::util::now_millis;

#[derive(Debug, Clone)]
pub struct MarketEvent {
    pub symbol: String,
    pub bid: f64,
    pub bid_qty: f64,
    pub ask: f64,
    pub ask_qty: f64,
    pub ts_ms: u128,
    pub recv_ts_ms: u128,
    pub parsed_ts_ms: u128,
    pub parse_latency_us: u64,
    pub inter_msg_us: Option<u64>,
    pub dropped_events: u64,
    pub yes_no_imbalance: Option<f64>,
    pub spread_pct: Option<f64>,
    pub depth_ratio: Option<f64>,
    pub yes_best_bid: Option<f64>,
    pub yes_best_ask: Option<f64>,
    pub no_best_bid: Option<f64>,
    pub no_best_ask: Option<f64>,
    pub yes_depth: Option<f64>,
    pub no_depth: Option<f64>,
}

impl MarketEvent {
    pub fn new(symbol: impl Into<String>, bid: f64, ask: f64, ts_ms: u128) -> Self {
        Self {
            symbol: symbol.into(),
            bid,
            bid_qty: 1.0,
            ask,
            ask_qty: 1.0,
            ts_ms,
            recv_ts_ms: ts_ms,
            parsed_ts_ms: ts_ms,
            parse_latency_us: 0,
            inter_msg_us: None,
            dropped_events: 0,
            yes_no_imbalance: None,
            spread_pct: None,
            depth_ratio: None,
            yes_best_bid: None,
            yes_best_ask: None,
            no_best_bid: None,
            no_best_ask: None,
            yes_depth: None,
            no_depth: None,
        }
    }
}

#[derive(Debug, Clone)]
pub struct MockMarketData {
    symbol: String,
    interval_ms: u64,
    channel_buffer: usize,
}

impl MockMarketData {
    pub fn new(symbol: impl Into<String>, interval_ms: u64) -> Self {
        Self {
            symbol: symbol.into(),
            interval_ms,
            channel_buffer: 1_024,
        }
    }

    pub fn with_channel_buffer(mut self, channel_buffer: usize) -> Self {
        self.channel_buffer = channel_buffer.max(1);
        self
    }

    pub fn spawn(self) -> mpsc::Receiver<MarketEvent> {
        let (tx, rx) = mpsc::channel(self.channel_buffer);
        tokio::spawn(async move {
            let mut interval = time::interval(Duration::from_millis(self.interval_ms));
            let mut mid = 100.0;
            let mut direction = 1.0;
            let mut last_recv_instant: Option<Instant> = None;

            loop {
                interval.tick().await;

                let recv_instant = Instant::now();
                let recv_ts_ms = now_millis();

                mid += 0.02 * direction;
                if mid > 101.0 {
                    direction = -1.0;
                } else if mid < 99.0 {
                    direction = 1.0;
                }

                let parsed_ts_ms = now_millis();
                let parse_latency_us = recv_instant.elapsed().as_micros() as u64;
                let inter_msg_us = last_recv_instant
                    .map(|prev| recv_instant.duration_since(prev).as_micros() as u64);
                last_recv_instant = Some(recv_instant);

                let event = MarketEvent {
                    symbol: self.symbol.clone(),
                    bid: mid - 0.01,
                    bid_qty: 1.0,
                    ask: mid + 0.01,
                    ask_qty: 1.0,
                    ts_ms: parsed_ts_ms,
                    recv_ts_ms,
                    parsed_ts_ms,
                    parse_latency_us,
                    inter_msg_us,
                    dropped_events: 0,
                    yes_no_imbalance: None,
                    spread_pct: None,
                    depth_ratio: None,
                    yes_best_bid: None,
                    yes_best_ask: None,
                    no_best_bid: None,
                    no_best_ask: None,
                    yes_depth: None,
                    no_depth: None,
                };

                if tx.send(event).await.is_err() {
                    break;
                }
            }
        });

        rx
    }
}

#[derive(Debug, Error)]
pub enum MarketDataError {
    #[error(
        "unsupported marketdata.mode '{0}'; expected 'mock', 'binance_ws', or 'polymarket_ws'"
    )]
    InvalidMode(String),
    #[error("websocket connection error: {0}")]
    Connection(#[source] Box<tokio_tungstenite::tungstenite::Error>),
    #[error("http request error: {0}")]
    Http(#[source] Box<reqwest::Error>),
    #[error("failed to parse json payload: {0}")]
    ParsePayload(#[source] Box<serde_json::Error>),
    #[error("websocket stream closed")]
    StreamClosed,
    #[error("market depth update is missing symbol")]
    MissingSymbol,
    #[error("invalid numeric value '{0}' in depth update")]
    InvalidNumber(String),
    #[error("invalid polymarket config: {0}")]
    InvalidPolymarketConfig(String),
    #[error("orderbook update error: {0}")]
    OrderBook(#[from] OrderBookError),
}

impl From<tokio_tungstenite::tungstenite::Error> for MarketDataError {
    fn from(error: tokio_tungstenite::tungstenite::Error) -> Self {
        Self::Connection(Box::new(error))
    }
}

impl From<reqwest::Error> for MarketDataError {
    fn from(error: reqwest::Error) -> Self {
        Self::Http(Box::new(error))
    }
}

impl From<serde_json::Error> for MarketDataError {
    fn from(error: serde_json::Error) -> Self {
        Self::ParsePayload(Box::new(error))
    }
}

#[derive(Debug, Clone)]
pub struct BinanceWsMarketData {
    symbol: String,
    ws_url: String,
    rest_url: String,
    depth_stream: String,
    snapshot_limit: u32,
    reconnect_backoff_ms: u64,
    ping_interval_s: u64,
    channel_buffer: usize,
}

impl BinanceWsMarketData {
    pub fn from_config(symbol: impl Into<String>, cfg: &MarketDataConfig) -> Self {
        Self {
            symbol: symbol.into(),
            ws_url: cfg.ws_url.clone(),
            rest_url: cfg.rest_url.clone(),
            depth_stream: cfg.depth_stream.clone(),
            snapshot_limit: cfg.snapshot_limit(),
            reconnect_backoff_ms: cfg.reconnect_backoff_ms(),
            ping_interval_s: cfg.ping_interval_s(),
            channel_buffer: cfg.channel_buffer(),
        }
    }

    pub fn spawn(self) -> mpsc::Receiver<MarketEvent> {
        ensure_rustls_crypto_provider();

        let (tx, rx) = mpsc::channel(self.channel_buffer);

        tokio::spawn(async move {
            let reconnect_backoff = Duration::from_millis(self.reconnect_backoff_ms);

            loop {
                if tx.is_closed() {
                    break;
                }

                let stream = self.stream_name();
                let url = self.stream_url(&stream);

                match connect_async(&url).await {
                    Ok((socket, _)) => {
                        info!(mode = "binance_ws", stream = %stream, "marketdata connected");

                        if let Err(error) = self.run_socket(socket, &tx).await {
                            warn!(error = %error, "marketdata connection dropped");
                        }
                    }
                    Err(error) => {
                        warn!(error = %error, "marketdata connect failed");
                    }
                }

                if tx.is_closed() {
                    break;
                }

                time::sleep(reconnect_backoff).await;
            }
        });

        rx
    }

    fn stream_name(&self) -> String {
        let depth_stream = self.depth_stream.trim_start_matches('@');
        format!("{}@{}", self.symbol.to_lowercase(), depth_stream)
    }

    fn stream_url(&self, stream: &str) -> String {
        format!("{}/{}", self.ws_url.trim_end_matches('/'), stream)
    }

    fn depth_snapshot_url(&self) -> String {
        format!("{}/api/v3/depth", self.rest_url.trim_end_matches('/'))
    }

    async fn run_socket(
        &self,
        mut socket: tokio_tungstenite::WebSocketStream<
            tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>,
        >,
        tx: &mpsc::Sender<MarketEvent>,
    ) -> Result<(), MarketDataError> {
        let http_client = reqwest::Client::builder()
            .timeout(Duration::from_secs(5))
            .build()?;

        let mut book = BinanceSpotOrderBook::new(self.symbol.clone());
        self.resync_from_snapshot(&http_client, &mut book, "initial")
            .await?;

        let mut ping_interval = time::interval(Duration::from_secs(self.ping_interval_s));
        ping_interval.set_missed_tick_behavior(time::MissedTickBehavior::Skip);
        let mut last_recv_instant: Option<Instant> = None;
        let mut dropped_events_total: u64 = 0;
        let mut dropped_events_since_emit: u64 = 0;
        let mut dropped_events_since_log: u64 = 0;
        let mut last_drop_log = Instant::now();

        loop {
            tokio::select! {
                _ = ping_interval.tick() => {
                    socket
                        .send(Message::Ping(Vec::new()))
                        .await
                        .map_err(MarketDataError::from)?;
                }
                maybe_msg = socket.next() => {
                    let Some(msg) = maybe_msg else {
                        return Err(MarketDataError::StreamClosed);
                    };

                    let msg = msg.map_err(MarketDataError::from)?;

                    if msg.is_ping() {
                        socket
                            .send(Message::Pong(msg.into_data()))
                            .await
                            .map_err(MarketDataError::from)?;
                        continue;
                    }

                    if msg.is_pong() {
                        continue;
                    }

                    if msg.is_close() {
                        return Err(MarketDataError::StreamClosed);
                    }

                    if !msg.is_text() {
                        continue;
                    }

                    let recv_instant = Instant::now();
                    let recv_ts_ms = now_millis();
                    let payload = msg.into_data();

                    let parsed = match parse_binance_depth_payload(&payload) {
                        Ok(Some(depth)) => depth,
                        Ok(None) => continue,
                        Err(error) => {
                            debug!(error = %error, "skipping malformed depth payload");
                            continue;
                        }
                    };

                    let ParsedDepthDelta {
                        symbol,
                        first_update_id,
                        final_update_id,
                        bids,
                        asks,
                        exchange_ts_ms,
                    } = parsed;

                    if !symbol.eq_ignore_ascii_case(book.symbol()) {
                        continue;
                    }

                    let delta = DepthDelta {
                        first_update_id,
                        final_update_id,
                        bids,
                        asks,
                    };

                    let top = match book.apply_delta(&delta)? {
                        DepthApplyResult::DiscardedOld => continue,
                        DepthApplyResult::Applied(top) => top,
                        DepthApplyResult::GapDetected => {
                            warn!(
                                expected_next_update_id = book.last_update_id().saturating_add(1),
                                first_update_id,
                                final_update_id,
                                "orderbook gap detected; resyncing"
                            );
                            self.resync_from_snapshot(&http_client, &mut book, "gap").await?;
                            continue;
                        }
                    };

                    let parsed_ts_ms = now_millis();
                    let parse_latency_us = recv_instant.elapsed().as_micros() as u64;
                    let inter_msg_us = last_recv_instant
                        .map(|prev| recv_instant.duration_since(prev).as_micros() as u64);
                    last_recv_instant = Some(recv_instant);

                    let event_ts_ms = exchange_ts_ms.map_or(parsed_ts_ms, u128::from);

                    let mut event = MarketEvent {
                        symbol,
                        bid: top.best_bid,
                        bid_qty: top.best_bid_qty,
                        ask: top.best_ask,
                        ask_qty: top.best_ask_qty,
                        ts_ms: event_ts_ms,
                        recv_ts_ms,
                        parsed_ts_ms,
                        parse_latency_us,
                        inter_msg_us,
                        dropped_events: 0,
                        yes_no_imbalance: None,
                        spread_pct: None,
                        depth_ratio: None,
                        yes_best_bid: None,
                        yes_best_ask: None,
                        no_best_bid: None,
                        no_best_ask: None,
                        yes_depth: None,
                        no_depth: None,
                    };
                    event.dropped_events = dropped_events_since_emit;

                    match tx.try_send(event) {
                        Ok(()) => {
                            dropped_events_since_emit = 0;
                        }
                        Err(TrySendError::Full(_)) => {
                            dropped_events_total = dropped_events_total.saturating_add(1);
                            dropped_events_since_emit = dropped_events_since_emit.saturating_add(1);
                            dropped_events_since_log = dropped_events_since_log.saturating_add(1);

                            if last_drop_log.elapsed() >= Duration::from_secs(5) {
                                warn!(
                                    dropped_events = dropped_events_since_log,
                                    dropped_events_total,
                                    "marketdata channel full; dropping events"
                                );
                                dropped_events_since_log = 0;
                                last_drop_log = Instant::now();
                            }
                        }
                        Err(TrySendError::Closed(_)) => break,
                    }
                }
            }
        }

        if dropped_events_since_log > 0 {
            warn!(
                dropped_events = dropped_events_since_log,
                dropped_events_total, "marketdata channel closed with pending dropped events"
            );
        }

        Ok(())
    }

    async fn resync_from_snapshot(
        &self,
        http_client: &reqwest::Client,
        book: &mut BinanceSpotOrderBook,
        reason: &'static str,
    ) -> Result<(), MarketDataError> {
        let snapshot = self.fetch_snapshot(http_client).await?;
        let top = book.apply_snapshot(snapshot)?;

        info!(
            reason,
            symbol = %book.symbol(),
            last_update_id = book.last_update_id(),
            best_bid = top.best_bid,
            best_ask = top.best_ask,
            mid = top.mid_price,
            spread_bps = top.spread_bps,
            "orderbook snapshot synced"
        );

        Ok(())
    }

    async fn fetch_snapshot(
        &self,
        http_client: &reqwest::Client,
    ) -> Result<DepthSnapshot, MarketDataError> {
        let snapshot_url = self.depth_snapshot_url();
        let symbol = self.symbol.to_uppercase();
        let snapshot_limit = self.snapshot_limit.to_string();

        let response = http_client
            .get(snapshot_url)
            .query(&[
                ("symbol", symbol.as_str()),
                ("limit", snapshot_limit.as_str()),
            ])
            .send()
            .await?
            .error_for_status()?;

        let payload: BinanceDepthSnapshotData = response.json().await?;
        let bids = parse_levels(payload.bids)?;
        let asks = parse_levels(payload.asks)?;

        Ok(DepthSnapshot {
            last_update_id: payload.last_update_id,
            bids,
            asks,
        })
    }
}

#[derive(Debug, Clone)]
pub struct PolymarketWsMarketData {
    symbol: String,
    ws_url: String,
    market_id: Option<String>,
    yes_asset_id: String,
    no_asset_id: String,
    depth_levels: usize,
    reconnect_backoff_ms: u64,
    ping_interval_s: u64,
    channel_buffer: usize,
}

impl PolymarketWsMarketData {
    pub fn from_config(
        symbol: impl Into<String>,
        cfg: &MarketDataConfig,
    ) -> Result<Self, MarketDataError> {
        let ws_url = cfg.polymarket_ws_url.trim().to_string();
        if ws_url.is_empty() {
            return Err(MarketDataError::InvalidPolymarketConfig(
                "marketdata.polymarket_ws_url cannot be empty".to_string(),
            ));
        }

        let yes_asset_id = cfg.polymarket_yes_asset_id.trim().to_string();
        let no_asset_id = cfg.polymarket_no_asset_id.trim().to_string();
        if yes_asset_id.is_empty() || no_asset_id.is_empty() {
            return Err(MarketDataError::InvalidPolymarketConfig(
                "marketdata.polymarket_yes_asset_id and polymarket_no_asset_id are required"
                    .to_string(),
            ));
        }
        if yes_asset_id == no_asset_id {
            return Err(MarketDataError::InvalidPolymarketConfig(
                "polymarket yes/no asset ids must be different".to_string(),
            ));
        }

        let market_id = {
            let trimmed = cfg.polymarket_market_id.trim();
            if trimmed.is_empty() {
                None
            } else {
                Some(trimmed.to_string())
            }
        };

        Ok(Self {
            symbol: symbol.into(),
            ws_url,
            market_id,
            yes_asset_id,
            no_asset_id,
            depth_levels: cfg.polymarket_depth_levels(),
            reconnect_backoff_ms: cfg.reconnect_backoff_ms(),
            ping_interval_s: cfg.ping_interval_s(),
            channel_buffer: cfg.channel_buffer(),
        })
    }

    pub fn spawn(self) -> mpsc::Receiver<MarketEvent> {
        ensure_rustls_crypto_provider();

        let (tx, rx) = mpsc::channel(self.channel_buffer);
        tokio::spawn(async move {
            let base_reconnect_backoff = Duration::from_millis(self.reconnect_backoff_ms.max(1));
            let max_reconnect_backoff = Duration::from_secs(30);
            let mut reconnect_attempt: u32 = 0;

            loop {
                if tx.is_closed() {
                    break;
                }

                let mut needs_reconnect = true;
                match connect_async(&self.ws_url).await {
                    Ok((mut socket, _)) => {
                        if let Err(error) = self.subscribe(&mut socket).await {
                            warn!(error = %error, "polymarket subscribe failed");
                        } else {
                            info!(
                                mode = "polymarket_ws",
                                symbol = %self.symbol,
                                yes_asset_id = %self.yes_asset_id,
                                no_asset_id = %self.no_asset_id,
                                "polymarket marketdata connected"
                            );
                            if let Err(error) = self.run_socket(socket, &tx).await {
                                warn!(error = %error, "polymarket connection dropped");
                            } else {
                                needs_reconnect = false;
                            }
                        }
                    }
                    Err(error) => {
                        warn!(error = %error, "polymarket connect failed");
                    }
                }

                if tx.is_closed() {
                    break;
                }

                if needs_reconnect {
                    reconnect_attempt = reconnect_attempt.saturating_add(1);
                } else {
                    reconnect_attempt = 0;
                }

                let reconnect_sleep = exponential_backoff_duration(
                    base_reconnect_backoff,
                    reconnect_attempt,
                    max_reconnect_backoff,
                );
                warn!(
                    reconnect_attempt,
                    reconnect_backoff_ms = reconnect_sleep.as_millis() as u64,
                    "polymarket reconnect scheduled"
                );
                time::sleep(reconnect_sleep).await;
            }
        });

        rx
    }

    async fn subscribe(
        &self,
        socket: &mut tokio_tungstenite::WebSocketStream<
            tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>,
        >,
    ) -> Result<(), MarketDataError> {
        let mut payload = serde_json::Map::new();
        payload.insert("type".to_string(), JsonValue::String("Market".to_string()));
        let asset_ids = vec![
            JsonValue::String(self.yes_asset_id.clone()),
            JsonValue::String(self.no_asset_id.clone()),
        ];
        payload.insert(
            "assets_ids".to_string(),
            JsonValue::Array(asset_ids.clone()),
        );
        payload.insert("asset_ids".to_string(), JsonValue::Array(asset_ids));
        if let Some(market_id) = &self.market_id {
            payload.insert("market".to_string(), JsonValue::String(market_id.clone()));
        }

        let text = serde_json::to_string(&payload).map_err(MarketDataError::from)?;
        socket
            .send(Message::Text(text))
            .await
            .map_err(MarketDataError::from)
    }

    async fn run_socket(
        &self,
        mut socket: tokio_tungstenite::WebSocketStream<
            tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>,
        >,
        tx: &mpsc::Sender<MarketEvent>,
    ) -> Result<(), MarketDataError> {
        let mut ping_interval = time::interval(Duration::from_secs(self.ping_interval_s));
        ping_interval.set_missed_tick_behavior(time::MissedTickBehavior::Skip);

        let mut yes_book = PolymarketAssetBook::default();
        let mut no_book = PolymarketAssetBook::default();
        let mut last_recv_instant: Option<Instant> = None;
        let mut dropped_events_total = 0u64;
        let mut dropped_events_since_emit = 0u64;
        let mut dropped_events_since_log = 0u64;
        let mut last_drop_log = Instant::now();

        loop {
            tokio::select! {
                _ = ping_interval.tick() => {
                    socket
                        .send(Message::Ping(Vec::new()))
                        .await
                        .map_err(MarketDataError::from)?;
                }
                maybe_msg = socket.next() => {
                    let Some(msg) = maybe_msg else {
                        return Err(MarketDataError::StreamClosed);
                    };
                    let msg = msg.map_err(MarketDataError::from)?;

                    if msg.is_ping() {
                        socket
                            .send(Message::Pong(msg.into_data()))
                            .await
                            .map_err(MarketDataError::from)?;
                        continue;
                    }
                    if msg.is_pong() {
                        continue;
                    }
                    if msg.is_close() {
                        return Err(MarketDataError::StreamClosed);
                    }
                    if !msg.is_text() {
                        continue;
                    }

                    let recv_instant = Instant::now();
                    let recv_ts_ms = now_millis();
                    let payload = msg.into_data();
                    let updates = match parse_polymarket_updates(&payload) {
                        Ok(items) => items,
                        Err(error) => {
                            debug!(error = %error, "skipping malformed polymarket payload");
                            continue;
                        }
                    };
                    if updates.is_empty() {
                        continue;
                    }

                    for update in updates {
                        let target_book = if update.asset_id == self.yes_asset_id {
                            &mut yes_book
                        } else if update.asset_id == self.no_asset_id {
                            &mut no_book
                        } else {
                            continue;
                        };

                        match update.kind {
                            PolymarketUpdateKind::Book { bids, asks } => {
                                target_book.apply_snapshot(bids, asks);
                            }
                            PolymarketUpdateKind::PriceChanges { changes } => {
                                for change in changes {
                                    target_book.apply_change(change.side, change.price, change.size);
                                }
                            }
                        }

                        let yes_top = yes_book.top_of_book();
                        let Some((yes_best_bid, yes_best_bid_qty, yes_best_ask, yes_best_ask_qty)) = yes_top else {
                            continue;
                        };

                        let no_top = no_book.top_of_book();
                        let no_best_bid = no_top.map(|(bid, _, _, _)| bid);
                        let no_best_ask = no_top.map(|(_, _, ask, _)| ask);

                        let yes_depth = yes_book.depth_sum(self.depth_levels);
                        let no_depth = no_book.depth_sum(self.depth_levels);
                        let depth_total = yes_depth + no_depth;
                        let yes_no_imbalance = if depth_total > 0.0 {
                            Some((yes_depth - no_depth) / depth_total)
                        } else {
                            None
                        };
                        let depth_ratio = if no_depth > 0.0 {
                            Some(yes_depth / no_depth)
                        } else {
                            None
                        };

                        let mid = (yes_best_bid + yes_best_ask) * 0.5;
                        let spread_pct = if mid > 0.0 {
                            Some(((yes_best_ask - yes_best_bid) / mid) * 100.0)
                        } else {
                            None
                        };

                        let parsed_ts_ms = now_millis();
                        let parse_latency_us = recv_instant.elapsed().as_micros() as u64;
                        let inter_msg_us = last_recv_instant
                            .map(|prev| recv_instant.duration_since(prev).as_micros() as u64);
                        last_recv_instant = Some(recv_instant);

                        let event = MarketEvent {
                            symbol: self.symbol.clone(),
                            bid: yes_best_bid,
                            bid_qty: yes_best_bid_qty,
                            ask: yes_best_ask,
                            ask_qty: yes_best_ask_qty,
                            ts_ms: update.exchange_ts_ms.unwrap_or(parsed_ts_ms),
                            recv_ts_ms,
                            parsed_ts_ms,
                            parse_latency_us,
                            inter_msg_us,
                            dropped_events: dropped_events_since_emit,
                            yes_no_imbalance,
                            spread_pct,
                            depth_ratio,
                            yes_best_bid: Some(yes_best_bid),
                            yes_best_ask: Some(yes_best_ask),
                            no_best_bid,
                            no_best_ask,
                            yes_depth: Some(yes_depth),
                            no_depth: Some(no_depth),
                        };

                        match tx.try_send(event) {
                            Ok(()) => {
                                dropped_events_since_emit = 0;
                            }
                            Err(TrySendError::Full(_)) => {
                                dropped_events_total = dropped_events_total.saturating_add(1);
                                dropped_events_since_emit = dropped_events_since_emit.saturating_add(1);
                                dropped_events_since_log = dropped_events_since_log.saturating_add(1);
                                if last_drop_log.elapsed() >= Duration::from_secs(5) {
                                    warn!(
                                        dropped_events = dropped_events_since_log,
                                        dropped_events_total,
                                        "polymarket marketdata channel full; dropping events"
                                    );
                                    dropped_events_since_log = 0;
                                    last_drop_log = Instant::now();
                                }
                            }
                            Err(TrySendError::Closed(_)) => return Ok(()),
                        }
                    }
                }
            }
        }
    }
}

pub fn spawn_marketdata(
    cfg: &MarketDataConfig,
    symbol: impl Into<String>,
) -> Result<mpsc::Receiver<MarketEvent>, MarketDataError> {
    let symbol = symbol.into();
    match cfg.mode.trim().to_ascii_lowercase().as_str() {
        "mock" => Ok(MockMarketData::new(symbol, cfg.interval_ms)
            .with_channel_buffer(cfg.channel_buffer())
            .spawn()),
        "binance_ws" => Ok(BinanceWsMarketData::from_config(symbol, cfg).spawn()),
        "polymarket_ws" => Ok(PolymarketWsMarketData::from_config(symbol, cfg)?.spawn()),
        _ => Err(MarketDataError::InvalidMode(cfg.mode.clone())),
    }
}

#[derive(Debug, Clone, Copy)]
enum PolymarketBookSide {
    Buy,
    Sell,
}

#[derive(Debug, Clone)]
struct ParsedPolymarketUpdate {
    asset_id: String,
    exchange_ts_ms: Option<u128>,
    kind: PolymarketUpdateKind,
}

#[derive(Debug, Clone)]
enum PolymarketUpdateKind {
    Book {
        bids: Vec<LevelUpdate>,
        asks: Vec<LevelUpdate>,
    },
    PriceChanges {
        changes: Vec<ParsedPolymarketPriceChange>,
    },
}

#[derive(Debug, Clone, Copy)]
struct ParsedPolymarketPriceChange {
    side: PolymarketBookSide,
    price: f64,
    size: f64,
}

#[derive(Debug, Default, Clone)]
struct PolymarketAssetBook {
    bids: std::collections::BTreeMap<OrderedFloat<f64>, f64>,
    asks: std::collections::BTreeMap<OrderedFloat<f64>, f64>,
}

impl PolymarketAssetBook {
    fn apply_snapshot(&mut self, bids: Vec<LevelUpdate>, asks: Vec<LevelUpdate>) {
        self.bids.clear();
        self.asks.clear();
        for level in bids {
            self.update_level(PolymarketBookSide::Buy, level.price, level.qty);
        }
        for level in asks {
            self.update_level(PolymarketBookSide::Sell, level.price, level.qty);
        }
    }

    fn apply_change(&mut self, side: PolymarketBookSide, price: f64, size: f64) {
        self.update_level(side, price, size);
    }

    fn update_level(&mut self, side: PolymarketBookSide, price: f64, size: f64) {
        if !is_valid_polymarket_price(price) || !size.is_finite() || size < 0.0 {
            return;
        }

        let book = match side {
            PolymarketBookSide::Buy => &mut self.bids,
            PolymarketBookSide::Sell => &mut self.asks,
        };

        let key = OrderedFloat(price);
        if size == 0.0 {
            book.remove(&key);
        } else {
            book.insert(key, size);
        }
    }

    fn top_of_book(&self) -> Option<(f64, f64, f64, f64)> {
        let (&best_bid, &best_bid_qty) = self.bids.iter().next_back()?;
        let (&best_ask, &best_ask_qty) = self.asks.iter().next()?;
        Some((best_bid.0, best_bid_qty, best_ask.0, best_ask_qty))
    }

    fn depth_sum(&self, levels: usize) -> f64 {
        let bid_depth = self
            .bids
            .iter()
            .rev()
            .take(levels)
            .map(|(_, qty)| *qty)
            .sum::<f64>();
        let ask_depth = self
            .asks
            .iter()
            .take(levels)
            .map(|(_, qty)| *qty)
            .sum::<f64>();
        bid_depth + ask_depth
    }
}

fn parse_polymarket_updates(
    payload: &[u8],
) -> Result<Vec<ParsedPolymarketUpdate>, MarketDataError> {
    let value: JsonValue = serde_json::from_slice(payload).map_err(MarketDataError::from)?;
    let mut updates = Vec::new();

    match value {
        JsonValue::Array(items) => {
            for item in items {
                if let Some(update) = parse_polymarket_update_value(&item)? {
                    updates.push(update);
                }
            }
        }
        other => {
            if let Some(update) = parse_polymarket_update_value(&other)? {
                updates.push(update);
            }
        }
    }

    Ok(updates)
}

fn parse_polymarket_update_value(
    value: &JsonValue,
) -> Result<Option<ParsedPolymarketUpdate>, MarketDataError> {
    let JsonValue::Object(obj) = value else {
        return Ok(None);
    };

    let event_type = obj
        .get("event_type")
        .or_else(|| obj.get("eventType"))
        .and_then(JsonValue::as_str)
        .unwrap_or_default()
        .to_ascii_lowercase();

    if event_type.is_empty() {
        return Ok(None);
    }

    let asset_id = obj
        .get("asset_id")
        .or_else(|| obj.get("assetId"))
        .and_then(JsonValue::as_str)
        .map(str::to_string)
        .unwrap_or_default();
    if asset_id.is_empty() {
        return Ok(None);
    }

    let exchange_ts_ms = parse_optional_u128(
        obj.get("timestamp")
            .or_else(|| obj.get("ts"))
            .or_else(|| obj.get("event_time"))
            .or_else(|| obj.get("eventTime")),
    );

    if event_type == "book" {
        let bids = parse_polymarket_levels(obj.get("bids"))?;
        let asks = parse_polymarket_levels(obj.get("asks"))?;
        return Ok(Some(ParsedPolymarketUpdate {
            asset_id,
            exchange_ts_ms,
            kind: PolymarketUpdateKind::Book { bids, asks },
        }));
    }

    if event_type == "price_change" {
        let changes_value = obj.get("changes").or_else(|| obj.get("price_changes"));
        let changes = parse_polymarket_price_changes(changes_value)?;
        return Ok(Some(ParsedPolymarketUpdate {
            asset_id,
            exchange_ts_ms,
            kind: PolymarketUpdateKind::PriceChanges { changes },
        }));
    }

    Ok(None)
}

fn parse_polymarket_levels(value: Option<&JsonValue>) -> Result<Vec<LevelUpdate>, MarketDataError> {
    let Some(JsonValue::Array(levels)) = value else {
        return Ok(Vec::new());
    };

    let mut out = Vec::with_capacity(levels.len());
    for level in levels {
        match level {
            JsonValue::Array(pair) if pair.len() >= 2 => {
                let Some(price) = parse_optional_f64(pair.first()) else {
                    continue;
                };
                let Some(qty) = parse_optional_f64(pair.get(1)) else {
                    continue;
                };
                if is_valid_polymarket_price(price) && qty.is_finite() && qty >= 0.0 {
                    out.push(LevelUpdate { price, qty });
                }
            }
            JsonValue::Object(obj) => {
                let price = parse_optional_f64(obj.get("price"))
                    .or_else(|| parse_optional_f64(obj.get("p")));
                let qty = parse_optional_f64(obj.get("size"))
                    .or_else(|| parse_optional_f64(obj.get("qty")))
                    .or_else(|| parse_optional_f64(obj.get("amount")));
                if let (Some(price), Some(qty)) = (price, qty) {
                    if is_valid_polymarket_price(price) && qty.is_finite() && qty >= 0.0 {
                        out.push(LevelUpdate { price, qty });
                    }
                }
            }
            _ => {}
        }
    }

    Ok(out)
}

fn parse_polymarket_price_changes(
    value: Option<&JsonValue>,
) -> Result<Vec<ParsedPolymarketPriceChange>, MarketDataError> {
    let Some(JsonValue::Array(changes)) = value else {
        return Ok(Vec::new());
    };

    let mut out = Vec::with_capacity(changes.len());
    for change in changes {
        let JsonValue::Object(obj) = change else {
            continue;
        };
        let Some(side_raw) = obj
            .get("side")
            .or_else(|| obj.get("taker_side"))
            .and_then(JsonValue::as_str)
        else {
            continue;
        };
        let side = match side_raw.to_ascii_uppercase().as_str() {
            "BUY" | "BID" => PolymarketBookSide::Buy,
            "SELL" | "ASK" => PolymarketBookSide::Sell,
            _ => continue,
        };

        let Some(price) = parse_optional_f64(obj.get("price").or_else(|| obj.get("p"))) else {
            continue;
        };
        let Some(size) = parse_optional_f64(
            obj.get("size")
                .or_else(|| obj.get("qty"))
                .or_else(|| obj.get("amount")),
        ) else {
            continue;
        };
        if !is_valid_polymarket_price(price) || !size.is_finite() || size < 0.0 {
            continue;
        }

        out.push(ParsedPolymarketPriceChange { side, price, size });
    }

    Ok(out)
}

fn parse_optional_f64(value: Option<&JsonValue>) -> Option<f64> {
    let value = value?;
    match value {
        JsonValue::Number(n) => n.as_f64(),
        JsonValue::String(s) => s.parse::<f64>().ok(),
        _ => None,
    }
}

fn parse_optional_u128(value: Option<&JsonValue>) -> Option<u128> {
    let value = value?;
    match value {
        JsonValue::Number(n) => n.as_u64().map(u128::from),
        JsonValue::String(s) => s.parse::<u128>().ok(),
        _ => None,
    }
}

fn is_valid_polymarket_price(price: f64) -> bool {
    price.is_finite() && (0.0..=1.0).contains(&price)
}

fn exponential_backoff_duration(base: Duration, attempt: u32, cap: Duration) -> Duration {
    let base_ms = base.as_millis().max(1) as u64;
    let cap_ms = cap.as_millis().max(1) as u64;
    let shift = attempt.saturating_sub(1).min(16);
    let multiplier = 1u64.checked_shl(shift).unwrap_or(u64::MAX);
    let sleep_ms = base_ms.saturating_mul(multiplier).min(cap_ms).max(1);
    Duration::from_millis(sleep_ms)
}

#[derive(Debug)]
struct ParsedDepthDelta {
    symbol: String,
    first_update_id: u64,
    final_update_id: u64,
    bids: Vec<LevelUpdate>,
    asks: Vec<LevelUpdate>,
    exchange_ts_ms: Option<u64>,
}

#[derive(Debug, Deserialize)]
struct BinanceDepthData {
    #[serde(rename = "e")]
    event_type: String,
    #[serde(rename = "E")]
    event_time_ms: Option<u64>,
    #[serde(rename = "s")]
    symbol: String,
    #[serde(rename = "U")]
    first_update_id: u64,
    #[serde(rename = "u")]
    final_update_id: u64,
    #[serde(rename = "b")]
    bids: Vec<[String; 2]>,
    #[serde(rename = "a")]
    asks: Vec<[String; 2]>,
}

#[derive(Debug, Deserialize)]
#[serde(untagged)]
enum BinanceDepthEnvelope {
    Combined(BinanceCombinedDepthData),
    Direct(BinanceDepthData),
}

#[derive(Debug, Deserialize)]
struct BinanceCombinedDepthData {
    data: BinanceDepthData,
}

#[derive(Debug, Deserialize)]
struct BinanceDepthSnapshotData {
    #[serde(rename = "lastUpdateId")]
    last_update_id: u64,
    bids: Vec<[String; 2]>,
    asks: Vec<[String; 2]>,
}

fn parse_binance_depth_payload(
    payload: &[u8],
) -> Result<Option<ParsedDepthDelta>, MarketDataError> {
    let envelope: BinanceDepthEnvelope =
        serde_json::from_slice(payload).map_err(MarketDataError::from)?;
    let data = match envelope {
        BinanceDepthEnvelope::Direct(data) => data,
        BinanceDepthEnvelope::Combined(combined) => combined.data,
    };

    if data.event_type != "depthUpdate" {
        return Ok(None);
    }

    if data.symbol.trim().is_empty() {
        return Err(MarketDataError::MissingSymbol);
    }

    let bids = parse_levels(data.bids)?;
    let asks = parse_levels(data.asks)?;

    Ok(Some(ParsedDepthDelta {
        symbol: data.symbol,
        first_update_id: data.first_update_id,
        final_update_id: data.final_update_id,
        bids,
        asks,
        exchange_ts_ms: data.event_time_ms,
    }))
}

fn parse_levels(levels: Vec<[String; 2]>) -> Result<Vec<LevelUpdate>, MarketDataError> {
    let mut parsed = Vec::with_capacity(levels.len());
    for [price_raw, qty_raw] in levels {
        let price = price_raw
            .parse::<f64>()
            .map_err(|_| MarketDataError::InvalidNumber(price_raw.clone()))?;
        let qty = qty_raw
            .parse::<f64>()
            .map_err(|_| MarketDataError::InvalidNumber(qty_raw.clone()))?;

        parsed.push(LevelUpdate { price, qty });
    }

    Ok(parsed)
}

fn ensure_rustls_crypto_provider() {
    static INIT: Once = Once::new();
    INIT.call_once(|| {
        let _ = rustls::crypto::CryptoProvider::install_default(
            rustls::crypto::ring::default_provider(),
        );
    });
}

#[cfg(test)]
mod tests {
    use super::{
        exponential_backoff_duration, parse_binance_depth_payload, parse_polymarket_updates,
        PolymarketAssetBook, PolymarketUpdateKind,
    };
    use std::time::Duration;

    #[test]
    fn parses_direct_depth_payload() {
        let payload = r#"{
            "e":"depthUpdate",
            "E":1700000000123,
            "s":"BTCUSDT",
            "U":100,
            "u":102,
            "b":[["43000.10","1.25"]],
            "a":[["43000.20","0.95"]]
        }"#;

        let parsed = parse_binance_depth_payload(payload.as_bytes()).expect("payload should parse");
        let parsed = parsed.expect("payload should include depth update");
        assert_eq!(parsed.symbol, "BTCUSDT");
        assert_eq!(parsed.first_update_id, 100);
        assert_eq!(parsed.final_update_id, 102);
        assert_eq!(parsed.bids.len(), 1);
        assert_eq!(parsed.asks.len(), 1);
        assert_eq!(parsed.exchange_ts_ms, Some(1700000000123));
    }

    #[test]
    fn parses_combined_depth_payload() {
        let payload = r#"{
            "stream":"btcusdt@depth@100ms",
            "data":{
                "e":"depthUpdate",
                "E":1700000000123,
                "s":"BTCUSDT",
                "U":100,
                "u":102,
                "b":[["43000.10","1.25"]],
                "a":[["43000.20","0.95"]]
            }
        }"#;

        let parsed = parse_binance_depth_payload(payload.as_bytes()).expect("payload should parse");
        let parsed = parsed.expect("payload should include depth update");
        assert_eq!(parsed.symbol, "BTCUSDT");
        assert_eq!(parsed.first_update_id, 100);
        assert_eq!(parsed.final_update_id, 102);
        assert_eq!(parsed.bids.len(), 1);
        assert_eq!(parsed.asks.len(), 1);
    }

    #[test]
    fn parses_polymarket_book_payload() {
        let payload = r#"{
            "event_type":"book",
            "asset_id":"yes_asset",
            "timestamp":"1700000000123",
            "bids":[{"price":"0.41","size":"120"}],
            "asks":[{"price":"0.44","size":"95"}]
        }"#;

        let updates = parse_polymarket_updates(payload.as_bytes()).expect("payload should parse");
        assert_eq!(updates.len(), 1);
        let update = &updates[0];
        assert_eq!(update.asset_id, "yes_asset");
        assert_eq!(update.exchange_ts_ms, Some(1700000000123));
        match &update.kind {
            PolymarketUpdateKind::Book { bids, asks } => {
                assert_eq!(bids.len(), 1);
                assert_eq!(asks.len(), 1);
                assert!((bids[0].price - 0.41).abs() < f64::EPSILON);
                assert!((asks[0].price - 0.44).abs() < f64::EPSILON);
            }
            _ => panic!("expected book update"),
        }
    }

    #[test]
    fn applies_polymarket_price_change_to_book() {
        let book_payload = r#"{
            "event_type":"book",
            "asset_id":"yes_asset",
            "timestamp":"1700000000123",
            "bids":[{"price":"0.41","size":"120"}],
            "asks":[{"price":"0.44","size":"95"}]
        }"#;
        let change_payload = r#"{
            "event_type":"price_change",
            "asset_id":"yes_asset",
            "timestamp":"1700000000222",
            "changes":[{"side":"BUY","price":"0.42","size":"100"}]
        }"#;

        let mut book = PolymarketAssetBook::default();
        let updates = parse_polymarket_updates(book_payload.as_bytes()).expect("payload parse");
        match &updates[0].kind {
            PolymarketUpdateKind::Book { bids, asks } => {
                book.apply_snapshot(bids.clone(), asks.clone());
            }
            _ => panic!("expected book update"),
        }

        let updates = parse_polymarket_updates(change_payload.as_bytes()).expect("payload parse");
        match &updates[0].kind {
            PolymarketUpdateKind::PriceChanges { changes } => {
                for change in changes {
                    book.apply_change(change.side, change.price, change.size);
                }
            }
            _ => panic!("expected price change update"),
        }

        let top = book.top_of_book().expect("top of book");
        assert!((top.0 - 0.42).abs() < f64::EPSILON);
        assert!((top.2 - 0.44).abs() < f64::EPSILON);
    }

    #[test]
    fn polymarket_backoff_is_exponential_and_capped() {
        let base = Duration::from_millis(250);
        let cap = Duration::from_secs(30);

        assert_eq!(
            exponential_backoff_duration(base, 0, cap),
            Duration::from_millis(250)
        );
        assert_eq!(
            exponential_backoff_duration(base, 1, cap),
            Duration::from_millis(250)
        );
        assert_eq!(
            exponential_backoff_duration(base, 2, cap),
            Duration::from_millis(500)
        );
        assert_eq!(
            exponential_backoff_duration(base, 3, cap),
            Duration::from_millis(1_000)
        );
        assert_eq!(
            exponential_backoff_duration(base, 50, cap),
            Duration::from_secs(30)
        );
    }
}
