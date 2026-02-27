use anyhow::{anyhow, bail, Context, Result};
use serde::Serialize;
use std::collections::BTreeMap;
use std::fs::{self, File};
use std::io::{BufWriter, Write};
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};
use tokio::sync::watch;
use tokio::time::{sleep, timeout};
use tracing::{error, info, warn};

use lowlat_bot::config::AppConfig;
use lowlat_bot::execution::{
    explicit_rest_error_reason, BinanceTestnetExecutor, BinanceWsExecutor, CancelOrderResult,
    ExecutionError, OrderRequest, OrderType, PlaceOrderResult, Side, SymbolFilters,
};
use lowlat_bot::marketdata::spawn_marketdata;
use lowlat_bot::telemetry;
use lowlat_bot::util::{now_millis, quantize_price_floor, quantize_qty_floor};

const DEFAULT_ITERATIONS: usize = 200;
const DEFAULT_WARMUP_ITERATIONS: usize = 10;
const DEFAULT_DISCOUNT_BPS: f64 = 1_000.0;
const INITIAL_MID_TIMEOUT_SECS: u64 = 15;
const DEFAULT_SUMMARY_WINDOW: usize = 10;
const BENCHMARK_LOG_EVERY: usize = 25;

#[derive(Debug, Clone)]
struct SmokeArgs {
    config_path: String,
    qty: Option<f64>,
    iterations: usize,
    warmup_iterations: usize,
    wait_cancel_ms: Option<u64>,
    discount_bps: DiscountBps,
    cancel_mode: Option<CancelMode>,
    transport: Option<ExecutionTransport>,
    liquidity_mode: LiquidityMode,
    out_json: Option<PathBuf>,
    summary_window: usize,
    summary_global: bool,
    log_level: Option<String>,
    profile: bool,
}

impl Default for SmokeArgs {
    fn default() -> Self {
        Self {
            config_path: "config/default.toml".to_string(),
            qty: None,
            iterations: DEFAULT_ITERATIONS,
            warmup_iterations: DEFAULT_WARMUP_ITERATIONS,
            wait_cancel_ms: None,
            discount_bps: DiscountBps::single(DEFAULT_DISCOUNT_BPS),
            cancel_mode: None,
            transport: None,
            liquidity_mode: LiquidityMode::Maker,
            out_json: None,
            summary_window: DEFAULT_SUMMARY_WINDOW,
            summary_global: true,
            log_level: None,
            profile: false,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CancelMode {
    Always,
    Never,
    Adaptive,
}

impl CancelMode {
    fn parse(raw: &str) -> Option<Self> {
        match raw.trim().to_ascii_lowercase().as_str() {
            "always" => Some(Self::Always),
            "never" => Some(Self::Never),
            "adaptive" => Some(Self::Adaptive),
            _ => None,
        }
    }

    fn as_str(self) -> &'static str {
        match self {
            Self::Always => "always",
            Self::Never => "never",
            Self::Adaptive => "adaptive",
        }
    }
}

#[derive(Debug, Clone, Copy)]
enum ExecutionTransport {
    Rest,
    WsApi,
}

impl ExecutionTransport {
    fn parse(raw: &str) -> Option<Self> {
        match raw.trim().to_ascii_lowercase().as_str() {
            "rest" => Some(Self::Rest),
            "ws_api" | "wsapi" | "ws" => Some(Self::WsApi),
            _ => None,
        }
    }

    fn as_str(self) -> &'static str {
        match self {
            Self::Rest => "rest",
            Self::WsApi => "ws_api",
        }
    }
}

#[derive(Debug, Clone, Copy)]
enum LiquidityMode {
    Maker,
    PostOnlyMaker,
    Taker,
}

impl LiquidityMode {
    fn parse(raw: &str) -> Option<Self> {
        match raw.trim().to_ascii_lowercase().as_str() {
            "maker" => Some(Self::Maker),
            "post_only_maker" | "post-only-maker" | "postonly" | "post_only" => {
                Some(Self::PostOnlyMaker)
            }
            "taker" => Some(Self::Taker),
            _ => None,
        }
    }

    fn as_str(self) -> &'static str {
        match self {
            Self::Maker => "maker",
            Self::PostOnlyMaker => "post_only_maker",
            Self::Taker => "taker",
        }
    }
}

#[derive(Debug, Clone, Copy)]
struct DiscountBps {
    min: f64,
    max: f64,
}

impl DiscountBps {
    fn single(value: f64) -> Self {
        Self {
            min: value,
            max: value,
        }
    }

    fn parse(raw: &str) -> Result<Self> {
        let trimmed = raw.trim();
        if let Some((left, right)) = trimmed.split_once("..") {
            let min = left
                .trim()
                .parse::<f64>()
                .with_context(|| format!("invalid --discount-bps '{}'", raw))?;
            let max = right
                .trim()
                .parse::<f64>()
                .with_context(|| format!("invalid --discount-bps '{}'", raw))?;
            if min <= 0.0 || max <= 0.0 || min >= max {
                bail!(
                    "invalid --discount-bps range '{}': expected min>0, max>0 and min<max",
                    raw
                );
            }
            return Ok(Self { min, max });
        }

        let value = trimmed
            .parse::<f64>()
            .with_context(|| format!("invalid --discount-bps '{}'", raw))?;
        if value <= 0.0 {
            bail!("--discount-bps must be > 0");
        }
        Ok(Self::single(value))
    }

    fn validate(self) -> Result<()> {
        if self.min <= 0.0 || self.max <= 0.0 || self.max >= 9_999.0 || self.min >= 9_999.0 {
            bail!("--discount-bps must be in (0, 9999)");
        }
        if self.min > self.max {
            bail!("--discount-bps range must satisfy min<=max");
        }
        Ok(())
    }

    fn value_for_iteration(self, iteration: usize, total_iterations: usize) -> f64 {
        if (self.max - self.min).abs() < f64::EPSILON {
            return self.min;
        }
        let steps = total_iterations.max(2) - 1;
        let idx = iteration.min(steps);
        let t = idx as f64 / steps as f64;
        self.min + (self.max - self.min) * t
    }

    fn display(self) -> String {
        if (self.max - self.min).abs() < f64::EPSILON {
            format!("{}", self.min)
        } else {
            format!("{}..{}", self.min, self.max)
        }
    }
}

#[derive(Debug, Clone, Copy)]
struct CancelPolicy {
    mode: CancelMode,
    wait_cancel_ms: u64,
    adaptive_mid_move_bps: f64,
    adaptive_spread_change_bps: f64,
}

impl CancelPolicy {
    fn is_benchmark_mode(self) -> bool {
        self.wait_cancel_ms == 0 && matches!(self.mode, CancelMode::Always)
    }
}

fn parse_args() -> Result<SmokeArgs> {
    let mut parsed = SmokeArgs::default();
    let raw_args: Vec<String> = std::env::args().skip(1).collect();
    let mut i = 0usize;
    let mut positional_config_set = false;

    while i < raw_args.len() {
        match raw_args[i].as_str() {
            "--config" => {
                i += 1;
                let Some(value) = raw_args.get(i) else {
                    bail!("missing value for --config");
                };
                parsed.config_path = value.clone();
            }
            "--qty" => {
                i += 1;
                let Some(value) = raw_args.get(i) else {
                    bail!("missing value for --qty");
                };
                parsed.qty = Some(
                    value
                        .parse::<f64>()
                        .with_context(|| format!("invalid --qty '{}'", value))?,
                );
            }
            "--iterations" | "--iters" => {
                i += 1;
                let Some(value) = raw_args.get(i) else {
                    bail!("missing value for --iterations");
                };
                parsed.iterations = value
                    .parse::<usize>()
                    .with_context(|| format!("invalid --iterations '{}'", value))?;
            }
            "--warmup-iterations" | "--warmup-iters" => {
                i += 1;
                let Some(value) = raw_args.get(i) else {
                    bail!("missing value for --warmup-iterations");
                };
                parsed.warmup_iterations = value
                    .parse::<usize>()
                    .with_context(|| format!("invalid --warmup-iterations '{}'", value))?;
            }
            "--wait-cancel-ms" => {
                i += 1;
                let Some(value) = raw_args.get(i) else {
                    bail!("missing value for --wait-cancel-ms");
                };
                parsed.wait_cancel_ms = Some(
                    value
                        .parse::<u64>()
                        .with_context(|| format!("invalid --wait-cancel-ms '{}'", value))?,
                );
            }
            "--cancel-mode" => {
                i += 1;
                let Some(value) = raw_args.get(i) else {
                    bail!("missing value for --cancel-mode");
                };
                parsed.cancel_mode = Some(
                    CancelMode::parse(value)
                        .ok_or_else(|| anyhow!("invalid --cancel-mode '{}'", value))?,
                );
            }
            "--discount-bps" => {
                i += 1;
                let Some(value) = raw_args.get(i) else {
                    bail!("missing value for --discount-bps");
                };
                parsed.discount_bps = DiscountBps::parse(value)?;
            }
            "--transport" => {
                i += 1;
                let Some(value) = raw_args.get(i) else {
                    bail!("missing value for --transport");
                };
                parsed.transport = Some(
                    ExecutionTransport::parse(value)
                        .ok_or_else(|| anyhow!("invalid --transport '{}'", value))?,
                );
            }
            "--liquidity-mode" => {
                i += 1;
                let Some(value) = raw_args.get(i) else {
                    bail!("missing value for --liquidity-mode");
                };
                parsed.liquidity_mode = LiquidityMode::parse(value)
                    .ok_or_else(|| anyhow!("invalid --liquidity-mode '{}'", value))?;
            }
            "--out-json" => {
                i += 1;
                let Some(value) = raw_args.get(i) else {
                    bail!("missing value for --out-json");
                };
                parsed.out_json = Some(PathBuf::from(value));
            }
            "--summary-window" => {
                i += 1;
                let Some(value) = raw_args.get(i) else {
                    bail!("missing value for --summary-window");
                };
                parsed.summary_window = value
                    .parse::<usize>()
                    .with_context(|| format!("invalid --summary-window '{}'", value))?;
            }
            "--summary-global" => {
                i += 1;
                let Some(value) = raw_args.get(i) else {
                    bail!("missing value for --summary-global");
                };
                parsed.summary_global = parse_bool_flag(value, "--summary-global")?;
            }
            "--log-level" => {
                i += 1;
                let Some(value) = raw_args.get(i) else {
                    bail!("missing value for --log-level");
                };
                parsed.log_level = Some(parse_log_level(value)?);
            }
            "--profile" => {
                parsed.profile = true;
            }
            "--help" | "-h" => {
                print_usage();
                std::process::exit(0);
            }
            value if !value.starts_with('-') && !positional_config_set => {
                parsed.config_path = value.to_string();
                positional_config_set = true;
            }
            value => bail!("unknown argument '{}'", value),
        }
        i += 1;
    }

    if parsed.iterations == 0 {
        bail!("--iterations must be > 0");
    }
    if parsed.summary_window == 0 {
        bail!("--summary-window must be > 0");
    }
    parsed.discount_bps.validate()?;

    Ok(parsed)
}

fn parse_bool_flag(raw: &str, flag_name: &str) -> Result<bool> {
    match raw.trim().to_ascii_lowercase().as_str() {
        "true" | "1" | "yes" | "y" => Ok(true),
        "false" | "0" | "no" | "n" => Ok(false),
        _ => bail!(
            "invalid value for {} '{}': expected true|false",
            flag_name,
            raw
        ),
    }
}

fn parse_log_level(raw: &str) -> Result<String> {
    let normalized = raw.trim().to_ascii_lowercase();
    match normalized.as_str() {
        "trace" | "debug" | "info" | "warn" | "error" => Ok(normalized),
        _ => bail!(
            "invalid --log-level '{}': expected trace|debug|info|warn|error",
            raw
        ),
    }
}

fn print_usage() {
    eprintln!(
        "Usage:
  cargo run --bin exec_smoke -- [config_path]
  cargo run --bin exec_smoke -- --config config/default.toml --qty 0.0001 --iterations 200 --warmup-iterations 10 --cancel-mode adaptive --wait-cancel-ms 200 --discount-bps 1..5 --transport ws_api --liquidity-mode post_only_maker --summary-window 10 --summary-global true --out-json runs/last.json --log-level info --profile"
    );
}

#[derive(Debug, Clone, Copy)]
struct MarketSnapshot {
    ask: f64,
    mid: f64,
    spread_bps: f64,
    rx_instant: Instant,
}

fn latest_market_snapshot(rx: &watch::Receiver<Option<MarketSnapshot>>) -> Option<MarketSnapshot> {
    *rx.borrow()
}

async fn wait_for_market_snapshot(
    rx: &mut watch::Receiver<Option<MarketSnapshot>>,
) -> Result<MarketSnapshot> {
    loop {
        if let Some(snapshot) = latest_market_snapshot(rx) {
            return Ok(snapshot);
        }
        if rx.changed().await.is_err() {
            return Err(anyhow!("marketdata feed closed before first mid"));
        }
    }
}

fn build_client_order_id(prefix: &str, iteration: usize) -> String {
    let mut out = String::with_capacity(prefix.len() + 20);
    out.push_str(prefix);
    push_usize_decimal(&mut out, iteration);
    out
}

fn push_usize_decimal(out: &mut String, mut value: usize) {
    if value == 0 {
        out.push('0');
        return;
    }

    let mut rev = [0u8; 20];
    let mut idx = 0usize;
    while value > 0 {
        rev[idx] = (value % 10) as u8;
        value /= 10;
        idx += 1;
    }
    for digit in rev[..idx].iter().rev() {
        out.push(char::from(b'0' + *digit));
    }
}

fn push_u128_decimal(out: &mut String, mut value: u128) {
    if value == 0 {
        out.push('0');
        return;
    }

    let mut rev = [0u8; 39];
    let mut idx = 0usize;
    while value > 0 {
        rev[idx] = (value % 10) as u8;
        value /= 10;
        idx += 1;
    }
    for digit in rev[..idx].iter().rev() {
        out.push(char::from(b'0' + *digit));
    }
}

fn price_from_mid(mid: f64, discount_bps: f64) -> Result<f64> {
    if !mid.is_finite() || mid <= 0.0 {
        bail!("invalid mid price {}", mid);
    }

    let factor = 1.0 - (discount_bps / 10_000.0);
    let px = mid * factor;
    if !px.is_finite() || px <= 0.0 {
        bail!(
            "invalid limit price derived from mid={} and discount_bps={}",
            mid,
            discount_bps
        );
    }

    Ok(px)
}

fn aggressive_price_from_ask(ask: f64, premium_bps: f64) -> Result<f64> {
    if !ask.is_finite() || ask <= 0.0 {
        bail!("invalid ask price {}", ask);
    }

    let factor = 1.0 + (premium_bps / 10_000.0);
    let px = ask * factor;
    if !px.is_finite() || px <= 0.0 {
        bail!(
            "invalid aggressive price derived from ask={} and premium_bps={}",
            ask,
            premium_bps
        );
    }

    Ok(px)
}

fn backoff_duration(error: &ExecutionError, failures: u32) -> Duration {
    let step = failures.min(10) as u64;
    match error {
        ExecutionError::RateLimited(_) => Duration::from_millis(500 + step * 250),
        _ => Duration::from_millis(200 + step * 150),
    }
}

fn is_retriable_error(error: &ExecutionError) -> bool {
    match error {
        ExecutionError::RateLimited(_) => true,
        ExecutionError::Api { status, .. } => *status >= 500,
        ExecutionError::Http { source, .. } => source.is_timeout(),
        ExecutionError::WsApiSocket(_) => true,
        _ => false,
    }
}

fn is_filter_reject_1013(error: &ExecutionError) -> bool {
    matches!(
        error,
        ExecutionError::Api {
            status: 400,
            code: Some(-1013),
            ..
        }
    )
}

fn quantized_order_values(
    computed_price: f64,
    qty: f64,
    filters: &SymbolFilters,
) -> Result<(f64, f64, f64, f64)> {
    let quantized_price =
        quantize_price_floor(computed_price, filters.tick_size).ok_or_else(|| {
            anyhow!(
                "failed to quantize price with tick_size={} (computed_price={})",
                filters.tick_size,
                computed_price
            )
        })?;

    let computed_qty = qty;
    let quantized_qty = quantize_qty_floor(computed_qty, filters.step_size).ok_or_else(|| {
        anyhow!(
            "failed to quantize qty with step_size={} (computed_qty={})",
            filters.step_size,
            computed_qty
        )
    })?;

    if quantized_price <= 0.0 || quantized_qty <= 0.0 {
        bail!(
            "quantized values are non-positive: quantized_price={}, quantized_qty={}",
            quantized_price,
            quantized_qty
        );
    }

    Ok((computed_price, quantized_price, computed_qty, quantized_qty))
}

fn resolve_cancel_policy(args: &SmokeArgs, cfg: &AppConfig) -> Result<CancelPolicy> {
    let config_mode = CancelMode::parse(&cfg.execution.cancel_mode).ok_or_else(|| {
        anyhow!(
            "invalid execution.cancel_mode='{}' (expected always|never|adaptive)",
            cfg.execution.cancel_mode
        )
    })?;

    let mode = args.cancel_mode.unwrap_or(config_mode);
    let wait_cancel_ms = args
        .wait_cancel_ms
        .unwrap_or(cfg.execution.wait_cancel_ms());

    Ok(CancelPolicy {
        mode,
        wait_cancel_ms,
        adaptive_mid_move_bps: cfg.execution.adaptive_mid_move_bps(),
        adaptive_spread_change_bps: cfg.execution.adaptive_spread_change_bps(),
    })
}

fn should_cancel_adaptive(
    placed: MarketSnapshot,
    latest: MarketSnapshot,
    mid_move_threshold_bps: f64,
    spread_change_threshold_bps: f64,
) -> bool {
    if !placed.mid.is_finite() || placed.mid <= 0.0 {
        return true;
    }

    let mid_move_bps = ((latest.mid - placed.mid).abs() / placed.mid) * 10_000.0;
    let spread_change_bps = (latest.spread_bps - placed.spread_bps).abs();

    mid_move_bps >= mid_move_threshold_bps || spread_change_bps >= spread_change_threshold_bps
}

fn order_type_for_mode(mode: LiquidityMode) -> OrderType {
    match mode {
        LiquidityMode::Maker | LiquidityMode::Taker => OrderType::Limit,
        LiquidityMode::PostOnlyMaker => OrderType::LimitMaker,
    }
}

fn resolve_transport(args: &SmokeArgs, cfg: &AppConfig) -> Result<ExecutionTransport> {
    if let Some(transport) = args.transport {
        return Ok(transport);
    }

    ExecutionTransport::parse(&cfg.execution.transport).ok_or_else(|| {
        anyhow!(
            "invalid execution.transport='{}' (expected rest|ws_api)",
            cfg.execution.transport
        )
    })
}

#[derive(Clone)]
enum SmokeExecutor {
    Rest(BinanceTestnetExecutor),
    WsApi(BinanceWsExecutor),
}

impl SmokeExecutor {
    async fn fetch_symbol_filters(
        &self,
        symbol: &str,
    ) -> std::result::Result<SymbolFilters, ExecutionError> {
        match self {
            Self::Rest(executor) => executor.fetch_symbol_filters(symbol).await,
            Self::WsApi(executor) => executor.fetch_symbol_filters(symbol).await,
        }
    }

    async fn new_limit_order_with_metrics(
        &self,
        request: &OrderRequest,
        client_order_id: String,
    ) -> std::result::Result<PlaceOrderResult, ExecutionError> {
        match self {
            Self::Rest(executor) => {
                executor
                    .new_limit_order_with_metrics(request, client_order_id)
                    .await
            }
            Self::WsApi(executor) => {
                executor
                    .new_limit_order_with_metrics(request, client_order_id)
                    .await
            }
        }
    }

    async fn cancel_order_with_metrics(
        &self,
        symbol: &str,
        client_order_id: &str,
    ) -> std::result::Result<CancelOrderResult, ExecutionError> {
        match self {
            Self::Rest(executor) => {
                executor
                    .cancel_order_with_metrics(symbol, client_order_id)
                    .await
            }
            Self::WsApi(executor) => {
                executor
                    .cancel_order_with_metrics(symbol, client_order_id)
                    .await
            }
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize)]
struct Quantiles {
    p50: u64,
    p95: u64,
    p99: u64,
    samples: usize,
}

#[derive(Debug, Default, Clone, Serialize)]
struct LatencyStatsReport {
    place_rtt_us: Option<Quantiles>,
    cancel_rtt_us: Option<Quantiles>,
    place_to_ack_us: Option<Quantiles>,
    cancel_to_ack_us: Option<Quantiles>,
    rx_to_decide_us: Option<Quantiles>,
    decide_to_ack_us: Option<Quantiles>,
    rx_to_ack_us: Option<Quantiles>,
}

impl LatencyStatsReport {
    fn is_empty(&self) -> bool {
        self.place_rtt_us.is_none()
            && self.cancel_rtt_us.is_none()
            && self.place_to_ack_us.is_none()
            && self.cancel_to_ack_us.is_none()
            && self.rx_to_decide_us.is_none()
            && self.decide_to_ack_us.is_none()
            && self.rx_to_ack_us.is_none()
    }
}

#[derive(Debug, Default, Clone, Copy)]
struct IterationMetrics {
    place_rtt_us: Option<u64>,
    cancel_rtt_us: Option<u64>,
    place_to_ack_us: Option<u64>,
    cancel_to_ack_us: Option<u64>,
    rx_to_decide_us: Option<u64>,
    decide_to_ack_us: Option<u64>,
    rx_to_ack_us: Option<u64>,
}

#[derive(Debug)]
struct BenchmarkStats {
    summary_window: usize,
    window_iterations: usize,
    place_rtt_window: Vec<u64>,
    cancel_rtt_window: Vec<u64>,
    place_to_ack_window: Vec<u64>,
    cancel_to_ack_window: Vec<u64>,
    rx_to_decide_window: Vec<u64>,
    decide_to_ack_window: Vec<u64>,
    rx_to_ack_window: Vec<u64>,
    place_rtt_global: Vec<u64>,
    cancel_rtt_global: Vec<u64>,
    place_to_ack_global: Vec<u64>,
    cancel_to_ack_global: Vec<u64>,
    rx_to_decide_global: Vec<u64>,
    decide_to_ack_global: Vec<u64>,
    rx_to_ack_global: Vec<u64>,
}

impl BenchmarkStats {
    fn new(summary_window: usize, global_capacity: usize) -> Self {
        Self {
            summary_window,
            window_iterations: 0,
            place_rtt_window: Vec::with_capacity(summary_window),
            cancel_rtt_window: Vec::with_capacity(summary_window),
            place_to_ack_window: Vec::with_capacity(summary_window),
            cancel_to_ack_window: Vec::with_capacity(summary_window),
            rx_to_decide_window: Vec::with_capacity(summary_window),
            decide_to_ack_window: Vec::with_capacity(summary_window),
            rx_to_ack_window: Vec::with_capacity(summary_window),
            place_rtt_global: Vec::with_capacity(global_capacity),
            cancel_rtt_global: Vec::with_capacity(global_capacity),
            place_to_ack_global: Vec::with_capacity(global_capacity),
            cancel_to_ack_global: Vec::with_capacity(global_capacity),
            rx_to_decide_global: Vec::with_capacity(global_capacity),
            decide_to_ack_global: Vec::with_capacity(global_capacity),
            rx_to_ack_global: Vec::with_capacity(global_capacity),
        }
    }

    fn record(&mut self, metrics: IterationMetrics) {
        self.window_iterations = self.window_iterations.saturating_add(1);
        Self::push_metric(
            metrics.place_rtt_us,
            &mut self.place_rtt_window,
            &mut self.place_rtt_global,
        );
        Self::push_metric(
            metrics.cancel_rtt_us,
            &mut self.cancel_rtt_window,
            &mut self.cancel_rtt_global,
        );
        Self::push_metric(
            metrics.place_to_ack_us,
            &mut self.place_to_ack_window,
            &mut self.place_to_ack_global,
        );
        Self::push_metric(
            metrics.cancel_to_ack_us,
            &mut self.cancel_to_ack_window,
            &mut self.cancel_to_ack_global,
        );
        Self::push_metric(
            metrics.rx_to_decide_us,
            &mut self.rx_to_decide_window,
            &mut self.rx_to_decide_global,
        );
        Self::push_metric(
            metrics.decide_to_ack_us,
            &mut self.decide_to_ack_window,
            &mut self.decide_to_ack_global,
        );
        Self::push_metric(
            metrics.rx_to_ack_us,
            &mut self.rx_to_ack_window,
            &mut self.rx_to_ack_global,
        );
    }

    fn maybe_log_window(
        &mut self,
        attempted_measured: usize,
        transport: ExecutionTransport,
        place_method: &str,
        cancel_method: &str,
    ) {
        if self.window_iterations < self.summary_window {
            return;
        }
        let report = self.take_window_report();
        log_latency_report(
            "window",
            attempted_measured,
            transport,
            place_method,
            cancel_method,
            &report,
        );
    }

    fn flush_window(
        &mut self,
        attempted_measured: usize,
        transport: ExecutionTransport,
        place_method: &str,
        cancel_method: &str,
    ) {
        if self.window_iterations == 0 {
            return;
        }
        let report = self.take_window_report();
        log_latency_report(
            "window_partial",
            attempted_measured,
            transport,
            place_method,
            cancel_method,
            &report,
        );
    }

    fn take_window_report(&mut self) -> LatencyStatsReport {
        self.window_iterations = 0;
        LatencyStatsReport {
            place_rtt_us: quantiles(&mut self.place_rtt_window),
            cancel_rtt_us: quantiles(&mut self.cancel_rtt_window),
            place_to_ack_us: quantiles(&mut self.place_to_ack_window),
            cancel_to_ack_us: quantiles(&mut self.cancel_to_ack_window),
            rx_to_decide_us: quantiles(&mut self.rx_to_decide_window),
            decide_to_ack_us: quantiles(&mut self.decide_to_ack_window),
            rx_to_ack_us: quantiles(&mut self.rx_to_ack_window),
        }
    }

    fn take_global_report(&mut self) -> LatencyStatsReport {
        LatencyStatsReport {
            place_rtt_us: quantiles(&mut self.place_rtt_global),
            cancel_rtt_us: quantiles(&mut self.cancel_rtt_global),
            place_to_ack_us: quantiles(&mut self.place_to_ack_global),
            cancel_to_ack_us: quantiles(&mut self.cancel_to_ack_global),
            rx_to_decide_us: quantiles(&mut self.rx_to_decide_global),
            decide_to_ack_us: quantiles(&mut self.decide_to_ack_global),
            rx_to_ack_us: quantiles(&mut self.rx_to_ack_global),
        }
    }

    fn push_metric(value: Option<u64>, window: &mut Vec<u64>, global: &mut Vec<u64>) {
        let Some(value) = value else {
            return;
        };
        window.push(value);
        global.push(value);
    }
}

#[derive(Debug, Clone, Copy, Serialize)]
#[serde(rename_all = "snake_case")]
#[allow(clippy::enum_variant_names)]
enum PlaceState {
    PlacedOk,
    PlacedErr,
}

#[derive(Debug, Clone, Copy, Serialize)]
#[serde(rename_all = "snake_case")]
#[allow(clippy::enum_variant_names)]
enum CancelState {
    CancelOk,
    CancelErr,
    CancelSkipped,
}

#[derive(Debug, Clone, Copy, Serialize)]
#[serde(rename_all = "snake_case")]
#[allow(clippy::enum_variant_names)]
enum AckState {
    AckOk,
    AckTimeout,
    AckNotApplicable,
}

#[derive(Debug, Default, Clone, Serialize)]
struct ErrorCounters {
    place_errors: u64,
    cancel_errors: u64,
    ack_timeouts: u64,
    ws_disconnects: u64,
}

#[derive(Debug, Clone, Serialize)]
struct FailedIteration {
    iteration: usize,
    measured_iteration: Option<usize>,
    warmup: bool,
    client_order_id: String,
    place_state: PlaceState,
    cancel_state: CancelState,
    ack_state: AckState,
    reason: String,
    transport: String,
    method: String,
    http_status: Option<u16>,
    binance_code: Option<i64>,
    error_kind: String,
}

#[derive(Debug, Clone, Serialize)]
struct EffectiveRunConfig {
    symbol: String,
    qty: f64,
    warmup_iterations: usize,
    measured_iterations: usize,
    summary_window: usize,
    summary_global: bool,
    transport: String,
    place_method: String,
    cancel_method: String,
    discount_bps: String,
    cancel_mode: String,
    liquidity_mode: String,
    execution_rest_url: String,
    execution_ws_api_url: String,
    execution_ws_url: String,
    marketdata_ws_url: String,
    marketdata_rest_url: String,
}

#[derive(Debug, Clone, Serialize)]
struct IterationCounts {
    attempted_total: usize,
    attempted_measured: usize,
    succeeded_measured: usize,
    placed_ok_measured: usize,
    canceled_ok_measured: usize,
}

#[derive(Debug, Clone, Serialize)]
struct ErrorSummary {
    total: ErrorCounters,
    measured: ErrorCounters,
    by_reason: BTreeMap<String, u64>,
}

#[derive(Debug, Clone, Serialize)]
struct RunReport {
    started_at_ms: u128,
    finished_at_ms: u128,
    effective_config: EffectiveRunConfig,
    iteration_counts: IterationCounts,
    stats_global: LatencyStatsReport,
    last_http_status: Option<u16>,
    last_binance_code: Option<i64>,
    last_error_body_snippet: Option<String>,
    last_path: Option<String>,
    errors: ErrorSummary,
    failed_iterations: Vec<FailedIteration>,
}

#[derive(Debug, Clone)]
struct ErrorClassification {
    error_kind: String,
    reason: String,
    http_status: Option<u16>,
    binance_code: Option<i64>,
    path: Option<String>,
    body_snippet: Option<String>,
    request_id: Option<u64>,
    retry_count: u32,
    explicit_error_reason: Option<String>,
    ws_disconnect: bool,
}

fn quantiles(values: &mut Vec<u64>) -> Option<Quantiles> {
    if values.is_empty() {
        return None;
    }

    values.sort_unstable();
    let samples = values.len();
    let p50 = percentile(values, 0.50);
    let p95 = percentile(values, 0.95);
    let p99 = percentile(values, 0.99);
    values.clear();

    Some(Quantiles {
        p50,
        p95,
        p99,
        samples,
    })
}

fn percentile(sorted_values: &[u64], pct: f64) -> u64 {
    if sorted_values.is_empty() {
        return 0;
    }
    let rank = ((sorted_values.len() as f64 - 1.0) * pct).round() as usize;
    sorted_values[rank.min(sorted_values.len() - 1)]
}

fn log_latency_report(
    scope: &str,
    attempted_measured: usize,
    transport: ExecutionTransport,
    place_method: &str,
    cancel_method: &str,
    report: &LatencyStatsReport,
) {
    if report.is_empty() {
        return;
    }

    info!(
        scope,
        attempted_measured,
        transport = transport.as_str(),
        place_method,
        cancel_method,
        place_rtt_samples = report.place_rtt_us.map_or(0, |q| q.samples),
        place_rtt_p50_us = report.place_rtt_us.map_or(0, |q| q.p50),
        place_rtt_p95_us = report.place_rtt_us.map_or(0, |q| q.p95),
        place_rtt_p99_us = report.place_rtt_us.map_or(0, |q| q.p99),
        cancel_rtt_samples = report.cancel_rtt_us.map_or(0, |q| q.samples),
        cancel_rtt_p50_us = report.cancel_rtt_us.map_or(0, |q| q.p50),
        cancel_rtt_p95_us = report.cancel_rtt_us.map_or(0, |q| q.p95),
        cancel_rtt_p99_us = report.cancel_rtt_us.map_or(0, |q| q.p99),
        place_to_ack_samples = report.place_to_ack_us.map_or(0, |q| q.samples),
        place_to_ack_p50_us = report.place_to_ack_us.map_or(0, |q| q.p50),
        place_to_ack_p95_us = report.place_to_ack_us.map_or(0, |q| q.p95),
        place_to_ack_p99_us = report.place_to_ack_us.map_or(0, |q| q.p99),
        cancel_to_ack_samples = report.cancel_to_ack_us.map_or(0, |q| q.samples),
        cancel_to_ack_p50_us = report.cancel_to_ack_us.map_or(0, |q| q.p50),
        cancel_to_ack_p95_us = report.cancel_to_ack_us.map_or(0, |q| q.p95),
        cancel_to_ack_p99_us = report.cancel_to_ack_us.map_or(0, |q| q.p99),
        rx_to_decide_samples = report.rx_to_decide_us.map_or(0, |q| q.samples),
        rx_to_decide_p50_us = report.rx_to_decide_us.map_or(0, |q| q.p50),
        rx_to_decide_p95_us = report.rx_to_decide_us.map_or(0, |q| q.p95),
        rx_to_decide_p99_us = report.rx_to_decide_us.map_or(0, |q| q.p99),
        decide_to_ack_samples = report.decide_to_ack_us.map_or(0, |q| q.samples),
        decide_to_ack_p50_us = report.decide_to_ack_us.map_or(0, |q| q.p50),
        decide_to_ack_p95_us = report.decide_to_ack_us.map_or(0, |q| q.p95),
        decide_to_ack_p99_us = report.decide_to_ack_us.map_or(0, |q| q.p99),
        rx_to_ack_samples = report.rx_to_ack_us.map_or(0, |q| q.samples),
        rx_to_ack_p50_us = report.rx_to_ack_us.map_or(0, |q| q.p50),
        rx_to_ack_p95_us = report.rx_to_ack_us.map_or(0, |q| q.p95),
        rx_to_ack_p99_us = report.rx_to_ack_us.map_or(0, |q| q.p99),
        "exec_smoke latency stats"
    );
}

fn classify_execution_error(error: &ExecutionError) -> ErrorClassification {
    match error {
        ExecutionError::Api {
            status,
            code,
            msg,
            path,
            body_snippet,
            request_id,
            retry_count,
        } => ErrorClassification {
            error_kind: "api_error".to_string(),
            reason: msg.clone(),
            http_status: Some(*status),
            binance_code: *code,
            path: path.clone(),
            body_snippet: body_snippet.clone(),
            request_id: *request_id,
            retry_count: *retry_count,
            explicit_error_reason: explicit_rest_error_reason(Some(*status), *code)
                .map(str::to_string),
            ws_disconnect: false,
        },
        ExecutionError::RateLimited(msg) => ErrorClassification {
            error_kind: "rate_limited".to_string(),
            reason: msg.clone(),
            http_status: None,
            binance_code: None,
            path: None,
            body_snippet: Some(msg.chars().take(300).collect()),
            request_id: None,
            retry_count: 0,
            explicit_error_reason: None,
            ws_disconnect: false,
        },
        ExecutionError::Http {
            source,
            path,
            request_id,
            retry_count,
            body_snippet,
        } => ErrorClassification {
            error_kind: "http_error".to_string(),
            reason: source.to_string(),
            http_status: source.status().map(|status| status.as_u16()),
            binance_code: None,
            path: path.clone(),
            body_snippet: body_snippet.clone(),
            request_id: *request_id,
            retry_count: *retry_count,
            explicit_error_reason: explicit_rest_error_reason(
                source.status().map(|status| status.as_u16()),
                None,
            )
            .map(str::to_string),
            ws_disconnect: false,
        },
        ExecutionError::WsApiSocket(source) => ErrorClassification {
            error_kind: "ws_api_socket".to_string(),
            reason: source.to_string(),
            http_status: None,
            binance_code: None,
            path: None,
            body_snippet: None,
            request_id: None,
            retry_count: 0,
            explicit_error_reason: None,
            ws_disconnect: true,
        },
        ExecutionError::UserStreamSocket(source) => ErrorClassification {
            error_kind: "user_stream_socket".to_string(),
            reason: source.to_string(),
            http_status: None,
            binance_code: None,
            path: None,
            body_snippet: None,
            request_id: None,
            retry_count: 0,
            explicit_error_reason: None,
            ws_disconnect: true,
        },
        ExecutionError::WsApiProtocol(msg) => ErrorClassification {
            error_kind: "ws_api_protocol".to_string(),
            reason: msg.clone(),
            http_status: None,
            binance_code: None,
            path: None,
            body_snippet: None,
            request_id: None,
            retry_count: 0,
            explicit_error_reason: None,
            ws_disconnect: false,
        },
        ExecutionError::Rejected(msg) => ErrorClassification {
            error_kind: "rejected".to_string(),
            reason: msg.clone(),
            http_status: None,
            binance_code: None,
            path: None,
            body_snippet: None,
            request_id: None,
            retry_count: 0,
            explicit_error_reason: None,
            ws_disconnect: false,
        },
        ExecutionError::MissingCredentials(name) => ErrorClassification {
            error_kind: "missing_credentials".to_string(),
            reason: format!("missing env var {}", name),
            http_status: None,
            binance_code: None,
            path: None,
            body_snippet: None,
            request_id: None,
            retry_count: 0,
            explicit_error_reason: None,
            ws_disconnect: false,
        },
        ExecutionError::Json(source) => ErrorClassification {
            error_kind: "json_error".to_string(),
            reason: source.to_string(),
            http_status: None,
            binance_code: None,
            path: None,
            body_snippet: None,
            request_id: None,
            retry_count: 0,
            explicit_error_reason: None,
            ws_disconnect: false,
        },
        ExecutionError::Signature(msg) => ErrorClassification {
            error_kind: "signature_error".to_string(),
            reason: msg.clone(),
            http_status: None,
            binance_code: None,
            path: None,
            body_snippet: None,
            request_id: None,
            retry_count: 0,
            explicit_error_reason: None,
            ws_disconnect: false,
        },
        ExecutionError::ExchangeInfoSymbolMissing(symbol) => ErrorClassification {
            error_kind: "exchange_info_symbol_missing".to_string(),
            reason: symbol.clone(),
            http_status: None,
            binance_code: None,
            path: None,
            body_snippet: None,
            request_id: None,
            retry_count: 0,
            explicit_error_reason: None,
            ws_disconnect: false,
        },
        ExecutionError::ExchangeInfoFilterMissing { symbol, filter } => ErrorClassification {
            error_kind: "exchange_info_filter_missing".to_string(),
            reason: format!("{} {}", symbol, filter),
            http_status: None,
            binance_code: None,
            path: None,
            body_snippet: None,
            request_id: None,
            retry_count: 0,
            explicit_error_reason: None,
            ws_disconnect: false,
        },
        ExecutionError::InvalidExchangeInfoFilter {
            symbol,
            field,
            value,
        } => ErrorClassification {
            error_kind: "invalid_exchange_info_filter".to_string(),
            reason: format!("{} {}={}", symbol, field, value),
            http_status: None,
            binance_code: None,
            path: None,
            body_snippet: None,
            request_id: None,
            retry_count: 0,
            explicit_error_reason: None,
            ws_disconnect: false,
        },
        ExecutionError::UserStreamParse(source) => ErrorClassification {
            error_kind: "user_stream_parse".to_string(),
            reason: source.to_string(),
            http_status: None,
            binance_code: None,
            path: None,
            body_snippet: None,
            request_id: None,
            retry_count: 0,
            explicit_error_reason: None,
            ws_disconnect: false,
        },
        ExecutionError::MissingListenKey => ErrorClassification {
            error_kind: "missing_listen_key".to_string(),
            reason: "missing listenKey".to_string(),
            http_status: None,
            binance_code: None,
            path: None,
            body_snippet: None,
            request_id: None,
            retry_count: 0,
            explicit_error_reason: None,
            ws_disconnect: false,
        },
    }
}

fn bump_error_counter<F>(
    total: &mut ErrorCounters,
    measured: &mut ErrorCounters,
    is_measured: bool,
    mut apply: F,
) where
    F: FnMut(&mut ErrorCounters),
{
    apply(total);
    if is_measured {
        apply(measured);
    }
}

fn transport_methods(transport: ExecutionTransport) -> (&'static str, &'static str) {
    match transport {
        ExecutionTransport::Rest => ("POST /api/v3/order", "DELETE /api/v3/order"),
        ExecutionTransport::WsApi => ("order.place", "order.cancel"),
    }
}

fn default_out_json_path(run_started_ms: u128) -> PathBuf {
    PathBuf::from(format!("runs/exec_smoke_{}.json", run_started_ms))
}

fn write_run_report(path: &Path, report: &RunReport) -> Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)
            .with_context(|| format!("failed to create output dir {}", parent.display()))?;
    }
    let file =
        File::create(path).with_context(|| format!("failed to create {}", path.display()))?;
    let mut writer = BufWriter::new(file);
    serde_json::to_writer_pretty(&mut writer, report)
        .with_context(|| format!("failed to serialize {}", path.display()))?;
    writer
        .write_all(b"\n")
        .with_context(|| format!("failed to finalize {}", path.display()))?;
    writer
        .flush()
        .with_context(|| format!("failed to flush {}", path.display()))?;
    Ok(())
}

#[cfg(feature = "pprof")]
fn maybe_start_profiler(enabled: bool) -> Result<Option<pprof::ProfilerGuard<'static>>> {
    if !enabled {
        return Ok(None);
    }
    let guard = pprof::ProfilerGuard::new(100).context("failed to start pprof profiler")?;
    Ok(Some(guard))
}

#[cfg(not(feature = "pprof"))]
fn maybe_start_profiler(enabled: bool) -> Result<Option<()>> {
    if enabled {
        warn!("--profile requested but binary built without 'pprof' feature");
    }
    Ok(None)
}

#[cfg(feature = "pprof")]
fn maybe_finish_profiler(
    guard: Option<pprof::ProfilerGuard<'static>>,
    out_json_path: &Path,
) -> Result<Option<PathBuf>> {
    let Some(guard) = guard else {
        return Ok(None);
    };
    let report = match guard.report().build() {
        Ok(report) => report,
        Err(error) => {
            warn!(error = %error, "failed to build pprof report");
            return Ok(None);
        }
    };
    let flamegraph_path = out_json_path.with_extension("flamegraph.svg");
    if let Some(parent) = flamegraph_path.parent() {
        fs::create_dir_all(parent)
            .with_context(|| format!("failed to create output dir {}", parent.display()))?;
    }
    let file = File::create(&flamegraph_path)
        .with_context(|| format!("failed to create {}", flamegraph_path.display()))?;
    let mut writer = BufWriter::new(file);
    report
        .flamegraph(&mut writer)
        .with_context(|| format!("failed to write {}", flamegraph_path.display()))?;
    Ok(Some(flamegraph_path))
}

#[cfg(not(feature = "pprof"))]
fn maybe_finish_profiler(_guard: Option<()>, _out_json_path: &Path) -> Result<Option<PathBuf>> {
    Ok(None)
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = parse_args()?;
    if let Some(level) = args.log_level.as_deref() {
        std::env::set_var("RUST_LOG", level);
    }
    let run_started_ms = now_millis();
    let out_json_path = args
        .out_json
        .clone()
        .unwrap_or_else(|| default_out_json_path(run_started_ms));

    telemetry::init("exec_smoke");
    let profiler_guard = maybe_start_profiler(args.profile)?;

    let cfg = AppConfig::load_from_path(&args.config_path)
        .with_context(|| format!("failed to load config from {}", args.config_path))?;

    let qty = args.qty.unwrap_or(cfg.strategy.order_qty);
    if qty <= 0.0 {
        bail!("qty must be > 0 (effective qty={})", qty);
    }
    let cancel_policy = resolve_cancel_policy(&args, &cfg)?;

    let mut md_rx = spawn_marketdata(&cfg.marketdata, cfg.general.symbol.clone())
        .context("failed to start marketdata for mid price")?;

    let (snapshot_tx, mut snapshot_rx) = watch::channel::<Option<MarketSnapshot>>(None);
    let symbol = cfg.general.symbol.to_uppercase();
    let md_symbol = symbol.clone();
    let md_task = tokio::spawn(async move {
        while let Some(event) = md_rx.recv().await {
            let rx_instant = Instant::now();
            let mid = (event.bid + event.ask) * 0.5;
            let spread = event.ask - event.bid;
            if mid > 0.0 && mid.is_finite() && spread.is_finite() {
                let spread_bps = (spread / mid) * 10_000.0;
                if spread_bps.is_finite() {
                    let _ = snapshot_tx.send(Some(MarketSnapshot {
                        ask: event.ask,
                        mid,
                        spread_bps,
                        rx_instant,
                    }));
                }
            }
        }
        warn!(symbol = %md_symbol, "marketdata stream closed in exec_smoke");
    });

    let first_snapshot = timeout(
        Duration::from_secs(INITIAL_MID_TIMEOUT_SECS),
        wait_for_market_snapshot(&mut snapshot_rx),
    )
    .await
    .context("timed out waiting for first mid price")?
    .context("marketdata did not provide a valid mid price")?;

    let transport = resolve_transport(&args, &cfg)?;
    let (place_method, cancel_method) = transport_methods(transport);

    match transport {
        ExecutionTransport::Rest => {
            info!(
                transport = transport.as_str(),
                place_method,
                cancel_method,
                rest_url = %cfg.execution.rest_url,
                "execution transport selected"
            );
        }
        ExecutionTransport::WsApi => {
            info!(
                transport = transport.as_str(),
                place_method,
                cancel_method,
                ws_api_url = %cfg.execution.ws_api_url,
                "execution transport selected"
            );
        }
    }

    let executor = match transport {
        ExecutionTransport::Rest => SmokeExecutor::Rest(
            BinanceTestnetExecutor::from_config(&cfg.execution).context(
                "failed to create Binance REST executor (check BINANCE_API_KEY/BINANCE_API_SECRET)",
            )?,
        ),
        ExecutionTransport::WsApi => SmokeExecutor::WsApi(
            BinanceWsExecutor::from_config(&cfg.execution).context(
                "failed to create Binance WS API executor (check BINANCE_API_KEY/BINANCE_API_SECRET and execution.ws_api_url)",
            )?,
        ),
    };

    let filters = executor
        .fetch_symbol_filters(&symbol)
        .await
        .with_context(|| format!("failed to fetch exchangeInfo filters for {}", symbol))?;

    let initial_discount_bps = args.discount_bps.value_for_iteration(0, args.iterations);
    let initial_computed_price = match args.liquidity_mode {
        LiquidityMode::Maker | LiquidityMode::PostOnlyMaker => {
            price_from_mid(first_snapshot.mid, initial_discount_bps)?
        }
        LiquidityMode::Taker => {
            aggressive_price_from_ask(first_snapshot.ask, initial_discount_bps)?
        }
    };
    let (
        initial_computed_price,
        initial_quantized_price,
        initial_computed_qty,
        initial_quantized_qty,
    ) = quantized_order_values(initial_computed_price, qty, &filters)?;

    info!(
        symbol = %filters.symbol,
        tick_size = filters.tick_size,
        step_size = filters.step_size,
        min_notional = ?filters.min_notional,
        computed_price = initial_computed_price,
        quantized_price = initial_quantized_price,
        computed_qty = initial_computed_qty,
        quantized_qty = initial_quantized_qty,
        "loaded exchange filters and quantized initial order values"
    );

    let total_iterations = args.warmup_iterations.saturating_add(args.iterations);
    info!(
        symbol = %symbol,
        qty,
        warmup_iterations = args.warmup_iterations,
        iterations = args.iterations,
        total_iterations,
        transport = transport.as_str(),
        place_method,
        cancel_method,
        liquidity_mode = args.liquidity_mode.as_str(),
        cancel_mode = cancel_policy.mode.as_str(),
        wait_cancel_ms = cancel_policy.wait_cancel_ms,
        adaptive_mid_move_bps = cancel_policy.adaptive_mid_move_bps,
        adaptive_spread_change_bps = cancel_policy.adaptive_spread_change_bps,
        benchmark_mode = cancel_policy.is_benchmark_mode(),
        discount_bps = %args.discount_bps.display(),
        summary_window = args.summary_window,
        summary_global = args.summary_global,
        "starting execution smoke loop"
    );

    let mut run_prefix = String::with_capacity(32);
    run_prefix.push_str("smk");
    push_u128_decimal(&mut run_prefix, run_started_ms);
    run_prefix.push('_');

    let log_every = if cancel_policy.is_benchmark_mode() {
        BENCHMARK_LOG_EVERY
    } else {
        1usize
    };

    let mut consecutive_failures = 0u32;
    let mut attempted_measured = 0usize;
    let mut succeeded_measured = 0usize;
    let mut placed_ok_measured = 0usize;
    let mut canceled_ok_measured = 0usize;
    let mut stats = BenchmarkStats::new(args.summary_window, args.iterations);
    let mut failed_iterations: Vec<FailedIteration> = Vec::new();
    let mut error_counts_total = ErrorCounters::default();
    let mut error_counts_measured = ErrorCounters::default();
    let mut error_reasons: BTreeMap<String, u64> = BTreeMap::new();
    let mut last_http_status: Option<u16> = None;
    let mut last_binance_code: Option<i64> = None;
    let mut last_error_body_snippet: Option<String> = None;
    let mut last_path: Option<String> = None;

    for iteration_index in 0..total_iterations {
        let is_warmup = iteration_index < args.warmup_iterations;
        let measured_iteration = if is_warmup {
            None
        } else {
            Some(iteration_index - args.warmup_iterations + 1)
        };
        let measured_index_zero_based = measured_iteration.map_or(0usize, |value| value - 1);
        let should_log_iteration = measured_iteration
            .map(|value| log_every <= 1 || value % log_every == 0)
            .unwrap_or(false);
        let client_order_id = build_client_order_id(&run_prefix, iteration_index + 1);

        let _iteration_span = if args.profile {
            Some(
                tracing::info_span!(
                    "exec_smoke_iteration",
                    iteration = iteration_index + 1,
                    measured_iteration = measured_iteration,
                    warmup = is_warmup
                )
                .entered(),
            )
        } else {
            None
        };

        let mut place_state = PlaceState::PlacedErr;
        let mut cancel_state = CancelState::CancelSkipped;
        let mut ack_state = AckState::AckNotApplicable;
        let mut iteration_metrics = IterationMetrics::default();
        let mut failure_reason: Option<String> = None;
        let mut failure_method = "local".to_string();
        let mut failure_http_status = None;
        let mut failure_binance_code = None;
        let mut failure_kind = "precheck".to_string();
        let mut place_error_counted = false;

        'attempt: {
            let market_snapshot = match latest_market_snapshot(&snapshot_rx) {
                Some(value) => value,
                None => {
                    match timeout(
                        Duration::from_secs(3),
                        wait_for_market_snapshot(&mut snapshot_rx),
                    )
                    .await
                    {
                        Ok(Ok(value)) => value,
                        Ok(Err(err)) => {
                            failure_reason = Some(format!("marketdata unavailable: {}", err));
                            break 'attempt;
                        }
                        Err(_) => {
                            failure_reason =
                                Some("timed out waiting for marketdata snapshot".to_string());
                            break 'attempt;
                        }
                    }
                }
            };

            let mid = market_snapshot.mid;
            let discount_bps = args
                .discount_bps
                .value_for_iteration(measured_index_zero_based, args.iterations);
            let computed_price = match args.liquidity_mode {
                LiquidityMode::Maker | LiquidityMode::PostOnlyMaker => {
                    price_from_mid(mid, discount_bps)
                }
                LiquidityMode::Taker => {
                    aggressive_price_from_ask(market_snapshot.ask, discount_bps)
                }
            };
            let (computed_price, quantized_price, _computed_qty, quantized_qty) =
                match computed_price.and_then(|price| quantized_order_values(price, qty, &filters))
                {
                    Ok(value) => value,
                    Err(err) => {
                        failure_reason = Some(format!("quantize failed: {}", err));
                        break 'attempt;
                    }
                };

            if let Some(min_notional) = filters.min_notional {
                let order_notional = quantized_price * quantized_qty;
                if order_notional < min_notional {
                    failure_reason = Some(format!(
                        "quantized notional={} below min_notional={} price={} qty={}",
                        order_notional, min_notional, quantized_price, quantized_qty
                    ));
                    break 'attempt;
                }
            }

            let request = OrderRequest {
                symbol: symbol.clone(),
                side: Side::Buy,
                qty: quantized_qty,
                price: quantized_price,
                order_type: order_type_for_mode(args.liquidity_mode),
            };
            let t_decide = Instant::now();
            let t_rx = market_snapshot.rx_instant;
            iteration_metrics.rx_to_decide_us =
                Some(t_decide.saturating_duration_since(t_rx).as_micros() as u64);

            let place_outcome = match executor
                .new_limit_order_with_metrics(&request, client_order_id.clone())
                .await
            {
                Ok(outcome) => outcome,
                Err(err) => {
                    if is_filter_reject_1013(&err) {
                        return Err(anyhow!(
                            "binance rejected order with -1013 (filter failure). this indicates an invalid quantized order: tick_size={} step_size={} min_notional={:?} computed_price={} quantized_price={} quantized_qty={}",
                            filters.tick_size,
                            filters.step_size,
                            filters.min_notional,
                            computed_price,
                            quantized_price,
                            quantized_qty
                        ));
                    }
                    let meta = classify_execution_error(&err);
                    failure_kind = meta.error_kind.clone();
                    failure_reason = Some(meta.reason.clone());
                    failure_http_status = meta.http_status;
                    failure_binance_code = meta.binance_code;
                    failure_method = place_method.to_string();
                    if let Some(explicit_reason) = meta.explicit_error_reason.as_ref() {
                        *error_reasons.entry(explicit_reason.clone()).or_insert(0) += 1;
                    }
                    if matches!(transport, ExecutionTransport::Rest)
                        && (meta.error_kind == "api_error" || meta.error_kind == "http_error")
                    {
                        last_http_status = meta.http_status;
                        last_binance_code = meta.binance_code;
                        last_error_body_snippet = meta
                            .body_snippet
                            .clone()
                            .or_else(|| Some(meta.reason.chars().take(300).collect()));
                        last_path = meta.path.clone();
                        warn!(
                            transport = transport.as_str(),
                            request_id = ?meta.request_id,
                            retry_count = meta.retry_count,
                            path = ?meta.path.as_deref(),
                            http_status = ?meta.http_status,
                            binance_code = ?meta.binance_code,
                            error_reason = ?meta.explicit_error_reason,
                            last_error_body_snippet = ?meta.body_snippet.as_deref(),
                            "exec_smoke rest http error"
                        );
                    }
                    error!(
                        iteration = iteration_index + 1,
                        measured_iteration = ?measured_iteration,
                        warmup = is_warmup,
                        client_order_id = %client_order_id,
                        transport = transport.as_str(),
                        method = place_method,
                        http_status = ?meta.http_status,
                        binance_code = ?meta.binance_code,
                        error_kind = %meta.error_kind,
                        reason = %meta.reason,
                        "place failed"
                    );
                    bump_error_counter(
                        &mut error_counts_total,
                        &mut error_counts_measured,
                        !is_warmup,
                        |counters| counters.place_errors = counters.place_errors.saturating_add(1),
                    );
                    place_error_counted = true;
                    if meta.ws_disconnect {
                        bump_error_counter(
                            &mut error_counts_total,
                            &mut error_counts_measured,
                            !is_warmup,
                            |counters| {
                                counters.ws_disconnects = counters.ws_disconnects.saturating_add(1)
                            },
                        );
                    }
                    if is_retriable_error(&err) {
                        consecutive_failures = consecutive_failures.saturating_add(1);
                        let backoff = backoff_duration(&err, consecutive_failures);
                        warn!(
                            iteration = iteration_index + 1,
                            backoff_ms = backoff.as_millis() as u64,
                            "place retriable error; backing off"
                        );
                        sleep(backoff).await;
                    }
                    break 'attempt;
                }
            };

            consecutive_failures = 0;
            place_state = PlaceState::PlacedOk;
            iteration_metrics.place_rtt_us = Some(place_outcome.place_http_us);
            if let Some(place_to_ack_us) = place_outcome.place_to_ack_us {
                iteration_metrics.place_to_ack_us = Some(place_to_ack_us);
                ack_state = AckState::AckOk;
            } else {
                ack_state = AckState::AckTimeout;
                bump_error_counter(
                    &mut error_counts_total,
                    &mut error_counts_measured,
                    !is_warmup,
                    |counters| counters.ack_timeouts = counters.ack_timeouts.saturating_add(1),
                );
            }
            if let Some(t_ack) = place_outcome.place_ack_at {
                iteration_metrics.decide_to_ack_us =
                    Some(t_ack.saturating_duration_since(t_decide).as_micros() as u64);
                iteration_metrics.rx_to_ack_us =
                    Some(t_ack.saturating_duration_since(t_rx).as_micros() as u64);
            }

            if should_log_iteration {
                info!(
                    iteration = iteration_index + 1,
                    measured_iteration = ?measured_iteration,
                    client_order_id = %place_outcome.order.client_order_id,
                    order_id = ?place_outcome.order.order_id,
                    status = ?place_outcome.order.status,
                    mid,
                    computed_price,
                    order_price = quantized_price,
                    order_qty = quantized_qty,
                    order_type = ?request.order_type,
                    liquidity_mode = args.liquidity_mode.as_str(),
                    discount_bps,
                    place_rtt_us = place_outcome.place_http_us,
                    place_to_ack_us = ?place_outcome.place_to_ack_us,
                    "order placed"
                );
            }

            if cancel_policy.wait_cancel_ms > 0 {
                sleep(Duration::from_millis(cancel_policy.wait_cancel_ms)).await;
            }

            let should_cancel = if matches!(args.liquidity_mode, LiquidityMode::Taker) {
                false
            } else {
                match cancel_policy.mode {
                    CancelMode::Always => true,
                    CancelMode::Never => false,
                    CancelMode::Adaptive => {
                        let latest_snapshot =
                            latest_market_snapshot(&snapshot_rx).unwrap_or(market_snapshot);
                        should_cancel_adaptive(
                            market_snapshot,
                            latest_snapshot,
                            cancel_policy.adaptive_mid_move_bps,
                            cancel_policy.adaptive_spread_change_bps,
                        )
                    }
                }
            };

            if !should_cancel {
                cancel_state = CancelState::CancelSkipped;
                break 'attempt;
            }

            match executor
                .cancel_order_with_metrics(&symbol, &place_outcome.order.client_order_id)
                .await
            {
                Ok(cancel_outcome) => {
                    cancel_state = CancelState::CancelOk;
                    iteration_metrics.cancel_rtt_us = Some(cancel_outcome.cancel_http_us);
                    if let Some(cancel_to_ack_us) = cancel_outcome.cancel_to_ack_us {
                        iteration_metrics.cancel_to_ack_us = Some(cancel_to_ack_us);
                    } else {
                        ack_state = AckState::AckTimeout;
                        bump_error_counter(
                            &mut error_counts_total,
                            &mut error_counts_measured,
                            !is_warmup,
                            |counters| {
                                counters.ack_timeouts = counters.ack_timeouts.saturating_add(1)
                            },
                        );
                    }
                    if should_log_iteration {
                        info!(
                            iteration = iteration_index + 1,
                            measured_iteration = ?measured_iteration,
                            client_order_id = %cancel_outcome.order.client_order_id,
                            order_id = ?cancel_outcome.order.order_id,
                            status = ?cancel_outcome.order.status,
                            cancel_rtt_us = cancel_outcome.cancel_http_us,
                            cancel_to_ack_us = ?cancel_outcome.cancel_to_ack_us,
                            "order canceled"
                        );
                    }
                }
                Err(err) => {
                    if is_filter_reject_1013(&err) {
                        return Err(anyhow!(
                            "binance returned -1013 during cancel path. this indicates quantize/filter mismatch: tick_size={} step_size={} min_notional={:?}",
                            filters.tick_size,
                            filters.step_size,
                            filters.min_notional
                        ));
                    }

                    cancel_state = CancelState::CancelErr;
                    let meta = classify_execution_error(&err);
                    failure_kind = meta.error_kind.clone();
                    failure_reason = Some(meta.reason.clone());
                    failure_http_status = meta.http_status;
                    failure_binance_code = meta.binance_code;
                    failure_method = cancel_method.to_string();
                    if let Some(explicit_reason) = meta.explicit_error_reason.as_ref() {
                        *error_reasons.entry(explicit_reason.clone()).or_insert(0) += 1;
                    }
                    if matches!(transport, ExecutionTransport::Rest)
                        && (meta.error_kind == "api_error" || meta.error_kind == "http_error")
                    {
                        last_http_status = meta.http_status;
                        last_binance_code = meta.binance_code;
                        last_error_body_snippet = meta
                            .body_snippet
                            .clone()
                            .or_else(|| Some(meta.reason.chars().take(300).collect()));
                        last_path = meta.path.clone();
                        warn!(
                            transport = transport.as_str(),
                            request_id = ?meta.request_id,
                            retry_count = meta.retry_count,
                            path = ?meta.path.as_deref(),
                            http_status = ?meta.http_status,
                            binance_code = ?meta.binance_code,
                            error_reason = ?meta.explicit_error_reason,
                            last_error_body_snippet = ?meta.body_snippet.as_deref(),
                            "exec_smoke rest http error"
                        );
                    }
                    error!(
                        iteration = iteration_index + 1,
                        measured_iteration = ?measured_iteration,
                        warmup = is_warmup,
                        client_order_id = %place_outcome.order.client_order_id,
                        transport = transport.as_str(),
                        method = cancel_method,
                        http_status = ?meta.http_status,
                        binance_code = ?meta.binance_code,
                        error_kind = %meta.error_kind,
                        reason = %meta.reason,
                        "cancel failed"
                    );
                    bump_error_counter(
                        &mut error_counts_total,
                        &mut error_counts_measured,
                        !is_warmup,
                        |counters| {
                            counters.cancel_errors = counters.cancel_errors.saturating_add(1)
                        },
                    );
                    if meta.ws_disconnect {
                        bump_error_counter(
                            &mut error_counts_total,
                            &mut error_counts_measured,
                            !is_warmup,
                            |counters| {
                                counters.ws_disconnects = counters.ws_disconnects.saturating_add(1)
                            },
                        );
                    }
                    if is_retriable_error(&err) {
                        consecutive_failures = consecutive_failures.saturating_add(1);
                        let backoff = backoff_duration(&err, consecutive_failures);
                        warn!(
                            iteration = iteration_index + 1,
                            backoff_ms = backoff.as_millis() as u64,
                            "cancel retriable error; backing off"
                        );
                        sleep(backoff).await;
                    }
                }
            }
        }

        if failure_reason.is_some()
            && !matches!(place_state, PlaceState::PlacedOk)
            && !place_error_counted
        {
            bump_error_counter(
                &mut error_counts_total,
                &mut error_counts_measured,
                !is_warmup,
                |counters| counters.place_errors = counters.place_errors.saturating_add(1),
            );
        }

        if let Some(reason) = failure_reason {
            *error_reasons.entry(reason.clone()).or_insert(0) += 1;
            failed_iterations.push(FailedIteration {
                iteration: iteration_index + 1,
                measured_iteration,
                warmup: is_warmup,
                client_order_id: client_order_id.clone(),
                place_state,
                cancel_state,
                ack_state,
                reason,
                transport: transport.as_str().to_string(),
                method: failure_method,
                http_status: failure_http_status,
                binance_code: failure_binance_code,
                error_kind: failure_kind,
            });
        }

        if is_warmup {
            continue;
        }

        attempted_measured = attempted_measured.saturating_add(1);
        stats.record(iteration_metrics);
        stats.maybe_log_window(attempted_measured, transport, place_method, cancel_method);

        let place_ok = matches!(place_state, PlaceState::PlacedOk);
        let cancel_ok_or_skipped = matches!(
            cancel_state,
            CancelState::CancelOk | CancelState::CancelSkipped
        );
        if place_ok {
            placed_ok_measured = placed_ok_measured.saturating_add(1);
        }
        if matches!(cancel_state, CancelState::CancelOk) {
            canceled_ok_measured = canceled_ok_measured.saturating_add(1);
        }
        if place_ok && cancel_ok_or_skipped {
            succeeded_measured = succeeded_measured.saturating_add(1);
        }
    }

    stats.flush_window(attempted_measured, transport, place_method, cancel_method);
    let global_stats = stats.take_global_report();
    if args.summary_global {
        log_latency_report(
            "global",
            attempted_measured,
            transport,
            place_method,
            cancel_method,
            &global_stats,
        );
    }

    md_task.abort();
    let _ = md_task.await;

    let finished_at_ms = now_millis();
    let run_report = RunReport {
        started_at_ms: run_started_ms,
        finished_at_ms,
        effective_config: EffectiveRunConfig {
            symbol: symbol.clone(),
            qty,
            warmup_iterations: args.warmup_iterations,
            measured_iterations: args.iterations,
            summary_window: args.summary_window,
            summary_global: args.summary_global,
            transport: transport.as_str().to_string(),
            place_method: place_method.to_string(),
            cancel_method: cancel_method.to_string(),
            discount_bps: args.discount_bps.display(),
            cancel_mode: cancel_policy.mode.as_str().to_string(),
            liquidity_mode: args.liquidity_mode.as_str().to_string(),
            execution_rest_url: cfg.execution.rest_url.clone(),
            execution_ws_api_url: cfg.execution.ws_api_url.clone(),
            execution_ws_url: cfg.execution.ws_url.clone(),
            marketdata_ws_url: cfg.marketdata.ws_url.clone(),
            marketdata_rest_url: cfg.marketdata.rest_url.clone(),
        },
        iteration_counts: IterationCounts {
            attempted_total: total_iterations,
            attempted_measured,
            succeeded_measured,
            placed_ok_measured,
            canceled_ok_measured,
        },
        stats_global: global_stats.clone(),
        last_http_status,
        last_binance_code,
        last_error_body_snippet: last_error_body_snippet.clone(),
        last_path: last_path.clone(),
        errors: ErrorSummary {
            total: error_counts_total.clone(),
            measured: error_counts_measured.clone(),
            by_reason: error_reasons,
        },
        failed_iterations: failed_iterations.clone(),
    };

    write_run_report(&out_json_path, &run_report)?;
    let flamegraph_path = maybe_finish_profiler(profiler_guard, &out_json_path)?;

    info!(
        symbol = %symbol,
        attempted_total = total_iterations,
        attempted_measured,
        succeeded_measured,
        placed_ok_measured,
        canceled_ok_measured,
        place_errors_total = error_counts_total.place_errors,
        cancel_errors_total = error_counts_total.cancel_errors,
        ack_timeouts_total = error_counts_total.ack_timeouts,
        ws_disconnects_total = error_counts_total.ws_disconnects,
        last_http_status = ?last_http_status,
        last_binance_code = ?last_binance_code,
        last_path = ?last_path,
        failed_iterations = failed_iterations.len(),
        output_json = %out_json_path.display(),
        flamegraph = ?flamegraph_path.as_ref().map(|path| path.display().to_string()),
        "exec_smoke finished"
    );

    if !failed_iterations.is_empty() {
        for failure in &failed_iterations {
            warn!(
                iteration = failure.iteration,
                measured_iteration = ?failure.measured_iteration,
                warmup = failure.warmup,
                client_order_id = %failure.client_order_id,
                place_state = ?failure.place_state,
                cancel_state = ?failure.cancel_state,
                ack_state = ?failure.ack_state,
                method = %failure.method,
                http_status = ?failure.http_status,
                binance_code = ?failure.binance_code,
                error_kind = %failure.error_kind,
                reason = %failure.reason,
                "failed iteration"
            );
        }
    }

    Ok(())
}
