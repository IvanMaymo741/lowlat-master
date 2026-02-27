use anyhow::{anyhow, bail, Context, Result};
use serde::Serialize;
use std::collections::{BTreeMap, VecDeque};
use std::fs::{self, File};
use std::io::{BufWriter, Write};
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};
use tokio::time::sleep;
use tracing::{error, info, warn};

use lowlat_bot::config::AppConfig;
use lowlat_bot::execution::{
    explicit_rest_error_reason, BinanceTestnetExecutor, BinanceWsExecutor, ExecutionError,
    OrderRequest, OrderSnapshot, OrderStatus, PlaceOrderResult, Side, SymbolFilters,
};
use lowlat_bot::marketdata::spawn_marketdata;
use lowlat_bot::orderbook::OrderBook;
use lowlat_bot::strategy::{
    classify_trigger, compute_signal_snapshot, RunnerAction, RunnerPhase, RunnerTransition,
    SignalDirection, SignalRunner, SignalRunnerState, SignalThresholds,
};
use lowlat_bot::telemetry;
use lowlat_bot::util::{now_millis, quantize_price_floor, quantize_qty_floor};

const DEFAULT_DISCOUNT_BPS: f64 = 1_000.0;
const DEFAULT_ADVERSE_MID_MOVE_BPS: f64 = 5.0;
const DEFAULT_MAX_ACTIONS_PER_MINUTE: usize = 30;
const DEFAULT_MAX_CONSECUTIVE_ERRORS: u32 = 5;
const DEFAULT_LATENCY_SPIKE_MS: u64 = 750;
const DEFAULT_MAX_LATENCY_SPIKES: u32 = 3;
const LATENCY_SPIKE_LAST_N: usize = 20;
const LATENCY_SPIKE_WARMUP_ACTIONS: u64 = 2;
const FILL_POLL_INTERVAL_MS: u128 = 200;
const MIN_FILL_QTY_EPS: f64 = 1e-9;
const FILL_POLL_ERROR_LOG_INTERVAL_MS: u128 = 5_000;

#[derive(Debug, Clone)]
struct LiveArgs {
    config_path: String,
    run_for_s: Option<u64>,
    warmup_s: u64,
    min_rest_ms: u64,
    adverse_mid_move_bps: f64,
    imbalance_enter: f64,
    tilt_enter_bps: f64,
    imbalance_exit: f64,
    tilt_exit_bps: f64,
    cooldown_ms: u64,
    max_actions_per_minute: usize,
    discount_bps: f64,
    out_json: Option<PathBuf>,
    max_consecutive_errors: u32,
    latency_spike_ms: u64,
    max_latency_spikes: u32,
    transport: Option<ExecutionTransport>,
}

impl Default for LiveArgs {
    fn default() -> Self {
        Self {
            config_path: "config/default.toml".to_string(),
            run_for_s: None,
            warmup_s: 5,
            min_rest_ms: 0,
            adverse_mid_move_bps: DEFAULT_ADVERSE_MID_MOVE_BPS,
            imbalance_enter: 0.20,
            tilt_enter_bps: 0.5,
            imbalance_exit: 0.10,
            tilt_exit_bps: 0.2,
            cooldown_ms: 1_500,
            max_actions_per_minute: DEFAULT_MAX_ACTIONS_PER_MINUTE,
            discount_bps: DEFAULT_DISCOUNT_BPS,
            out_json: None,
            max_consecutive_errors: DEFAULT_MAX_CONSECUTIVE_ERRORS,
            latency_spike_ms: DEFAULT_LATENCY_SPIKE_MS,
            max_latency_spikes: DEFAULT_MAX_LATENCY_SPIKES,
            transport: None,
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

#[derive(Clone)]
enum SignalExecutor {
    Rest(BinanceTestnetExecutor),
    WsApi(BinanceWsExecutor),
}

impl SignalExecutor {
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
    ) -> std::result::Result<lowlat_bot::execution::CancelOrderResult, ExecutionError> {
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

    async fn query_order_snapshot(
        &self,
        symbol: &str,
        client_order_id: &str,
    ) -> std::result::Result<OrderSnapshot, ExecutionError> {
        match self {
            Self::Rest(executor) => executor.query_order_snapshot(symbol, client_order_id).await,
            Self::WsApi(executor) => executor.query_order_snapshot(symbol, client_order_id).await,
        }
    }
}

#[derive(Debug, Clone, Serialize)]
struct SignalLiveConfig {
    symbol: String,
    qty: f64,
    transport: String,
    discount_bps: f64,
    min_rest_ms: u64,
    adverse_mid_move_bps: f64,
    thresholds: SignalThresholdsConfig,
    max_actions_per_minute: usize,
    max_consecutive_errors: u32,
    latency_spike_ms: u64,
    max_latency_spikes: u32,
    rest_url: String,
    ws_api_url: String,
}

#[derive(Debug, Clone, Serialize)]
struct SignalThresholdsConfig {
    imbalance_enter: f64,
    tilt_enter_bps: f64,
    imbalance_exit: f64,
    tilt_exit_bps: f64,
    cooldown_ms: u64,
}

#[derive(Debug, Clone, Copy, Serialize)]
struct U64Quantiles {
    samples: usize,
    p50: u64,
    p95: u64,
    p99: u64,
}

#[derive(Debug, Clone, Copy, Serialize)]
struct F64Quantiles {
    samples: usize,
    p50: f64,
    p95: f64,
    p99: f64,
}

#[derive(Debug, Default, Clone, Serialize)]
struct LiveEventCounters {
    order_ack: u64,
    cancel_ack: u64,
    trade_fill: u64,
}

#[derive(Debug, Default, Clone, Serialize)]
struct LiveCounters {
    triggers_long: u64,
    triggers_short: u64,
    entries: u64,
    cancels: u64,
    fills: u64,
    partial_fills: u64,
    filled_qty_total: f64,
    cooldown_skips: u64,
    rearm_wait_skips: u64,
    errors: u64,
    action_rate_limit_skips: u64,
}

#[derive(Debug, Default, Clone, Serialize)]
struct CancelsByReason {
    neutral: u64,
    adverse: u64,
    kill: u64,
    error: u64,
}

#[derive(Debug, Clone, Serialize)]
struct LiveRunSummary {
    started_at_ms: u128,
    finished_at_ms: u128,
    requested_run_for_s: u64,
    actual_elapsed_s: f64,
    exit_reason: ExitReason,
    warmup_s: u64,
    warmup_actions_excluded: u64,
    min_rest_ms: u64,
    adverse_mid_move_bps: f64,
    neutral_cancel_delayed_count: u64,
    effective_config: SignalLiveConfig,
    event_counters: LiveEventCounters,
    counters: LiveCounters,
    place_rtt_us: Option<U64Quantiles>,
    cancel_rtt_us: Option<U64Quantiles>,
    ack_latency_ms: Option<U64Quantiles>,
    effective_rest_ms: Option<U64Quantiles>,
    place_rtt_us_excluding_warmup: Option<U64Quantiles>,
    cancel_rtt_us_excluding_warmup: Option<U64Quantiles>,
    avg_time_to_fill_ms: Option<f64>,
    avg_rest_ms: Option<f64>,
    time_to_fill_ms: Option<U64Quantiles>,
    adverse_bps_1s: Option<F64Quantiles>,
    adverse_bps_5s: Option<F64Quantiles>,
    distance_to_best_bps: Option<F64Quantiles>,
    mid_move_bps_at_cancel: Option<F64Quantiles>,
    cancels_by_reason: CancelsByReason,
    latency_spike_stats: LatencySpikeStats,
    last_http_status: Option<u16>,
    last_binance_code: Option<i64>,
    last_error_body_snippet: Option<String>,
    last_path: Option<String>,
    minute_buckets: Vec<MinuteBucketSummary>,
    state_transitions: Vec<RunnerTransition>,
    error_reasons: BTreeMap<String, u64>,
    stopped_by_kill_switch: bool,
    kill_switch_reason: Option<String>,
}

#[derive(Debug, Clone, Copy, Serialize)]
#[serde(rename_all = "snake_case")]
enum ExitReason {
    Timeout,
    KillSwitch,
    Errors,
    Sigint,
}

#[derive(Debug, Clone, Serialize)]
struct LatencySpikeStats {
    threshold_ms: u64,
    threshold_us: u64,
    warmup_actions_ignored: u64,
    last_n_rtt_us: Vec<u64>,
    spikes_count_total: u64,
    max_rtt_us: u64,
    spike_streak_final: u32,
}

#[derive(Debug, Clone, Serialize)]
struct MinuteBucketSummary {
    minute_index: u64,
    triggers: u64,
    entries: u64,
    cancels: u64,
    neutral_cancels: u64,
    adverse_cancels: u64,
    delayed_neutral_cancels: u64,
    fills: u64,
    filled_qty: f64,
    rate_limit_skips: u64,
    avg_mid: Option<f64>,
    avg_spread_bps: Option<f64>,
    avg_time_in_trade_ms: Option<f64>,
    avg_time_to_fill_ms: Option<f64>,
    avg_rest_ms: Option<f64>,
    avg_effective_rest_ms: Option<f64>,
    distance_to_best_bps: Option<F64Quantiles>,
    place_rtt_us: Option<U64Quantiles>,
    cancel_rtt_us: Option<U64Quantiles>,
}

#[derive(Debug, Default)]
struct MinuteBucketAccumulator {
    triggers: u64,
    entries: u64,
    cancels: u64,
    neutral_cancels: u64,
    adverse_cancels: u64,
    delayed_neutral_cancels: u64,
    fills: u64,
    filled_qty: f64,
    rate_limit_skips: u64,
    mid_sum: f64,
    spread_bps_sum: f64,
    snapshot_samples: u64,
    time_in_trade_ms_sum: u128,
    time_in_trade_samples: u64,
    rest_ms_sum: u128,
    rest_samples: u64,
    effective_rest_ms_sum: u128,
    effective_rest_samples: u64,
    time_to_fill_ms_sum: u128,
    time_to_fill_samples: u64,
    distance_to_best_bps: Vec<f64>,
    place_rtt_us: Vec<u64>,
    cancel_rtt_us: Vec<u64>,
}

#[derive(Debug, Clone)]
struct ActiveOrder {
    direction: SignalDirection,
    client_order_id: String,
    entered_at_ts_ms: u128,
    place_ack_instant: Option<Instant>,
    entry_mid: f64,
    orig_qty: f64,
    executed_qty: f64,
}

#[derive(Debug, Clone)]
struct PendingAdverseSample {
    direction: SignalDirection,
    mid_at_fill: f64,
    target_1s_ts_ms: u128,
    target_5s_ts_ms: u128,
    recorded_1s: bool,
    recorded_5s: bool,
}

impl PendingAdverseSample {
    fn new(direction: SignalDirection, fill_ts_ms: u128, mid_at_fill: f64) -> Self {
        Self {
            direction,
            mid_at_fill,
            target_1s_ts_ms: fill_ts_ms.saturating_add(1_000),
            target_5s_ts_ms: fill_ts_ms.saturating_add(5_000),
            recorded_1s: false,
            recorded_5s: false,
        }
    }
}

#[derive(Debug)]
struct SpikeTracker {
    threshold_ms: u64,
    threshold_us: u64,
    warmup_actions: u64,
    actions_seen: u64,
    spike_streak: u32,
    spikes_count_total: u64,
    max_rtt_us: u64,
    last_n_rtt_us: VecDeque<u64>,
}

#[derive(Debug, Clone, Copy)]
struct SpikeUpdate {
    is_spike: bool,
    streak: u32,
}

impl SpikeTracker {
    fn new(threshold_ms: u64, warmup_actions: u64, last_n_capacity: usize) -> Self {
        let threshold_us = threshold_ms.saturating_mul(1_000);
        Self {
            threshold_ms,
            threshold_us,
            warmup_actions,
            actions_seen: 0,
            spike_streak: 0,
            spikes_count_total: 0,
            max_rtt_us: 0,
            last_n_rtt_us: VecDeque::with_capacity(last_n_capacity.max(1)),
        }
    }

    fn record_rtt(&mut self, rtt_us: u64) -> SpikeUpdate {
        self.actions_seen = self.actions_seen.saturating_add(1);
        self.max_rtt_us = self.max_rtt_us.max(rtt_us);
        if self.last_n_rtt_us.len() == self.last_n_rtt_us.capacity() {
            self.last_n_rtt_us.pop_front();
        }
        self.last_n_rtt_us.push_back(rtt_us);

        if self.actions_seen <= self.warmup_actions {
            self.spike_streak = 0;
            return SpikeUpdate {
                is_spike: false,
                streak: self.spike_streak,
            };
        }

        if rtt_us > self.threshold_us {
            self.spike_streak = self.spike_streak.saturating_add(1);
            self.spikes_count_total = self.spikes_count_total.saturating_add(1);
            SpikeUpdate {
                is_spike: true,
                streak: self.spike_streak,
            }
        } else {
            self.spike_streak = 0;
            SpikeUpdate {
                is_spike: false,
                streak: self.spike_streak,
            }
        }
    }

    fn spike_streak(&self) -> u32 {
        self.spike_streak
    }

    fn threshold_us(&self) -> u64 {
        self.threshold_us
    }

    fn summary(&self) -> LatencySpikeStats {
        LatencySpikeStats {
            threshold_ms: self.threshold_ms,
            threshold_us: self.threshold_us,
            warmup_actions_ignored: self.warmup_actions,
            last_n_rtt_us: self.last_n_rtt_us.iter().copied().collect(),
            spikes_count_total: self.spikes_count_total,
            max_rtt_us: self.max_rtt_us,
            spike_streak_final: self.spike_streak,
        }
    }
}

#[derive(Debug, Default)]
struct ActionRateLimiter {
    max_actions_per_minute: usize,
    recent_action_ts_ms: VecDeque<u128>,
}

impl ActionRateLimiter {
    fn new(max_actions_per_minute: usize) -> Self {
        Self {
            max_actions_per_minute: max_actions_per_minute.max(1),
            recent_action_ts_ms: VecDeque::new(),
        }
    }

    fn allow(&mut self, now_ts_ms: u128) -> bool {
        let window_ms = 60_000u128;
        while let Some(oldest) = self.recent_action_ts_ms.front().copied() {
            if now_ts_ms.saturating_sub(oldest) >= window_ms {
                self.recent_action_ts_ms.pop_front();
            } else {
                break;
            }
        }

        if self.recent_action_ts_ms.len() >= self.max_actions_per_minute {
            return false;
        }

        self.recent_action_ts_ms.push_back(now_ts_ms);
        true
    }
}

fn parse_args() -> Result<LiveArgs> {
    let raw_args: Vec<String> = std::env::args().skip(1).collect();
    let mut args = LiveArgs::default();
    let mut positional_config_set = false;

    let mut i = 0usize;
    while i < raw_args.len() {
        match raw_args[i].as_str() {
            "--config" => {
                i += 1;
                let Some(value) = raw_args.get(i) else {
                    bail!("missing value for --config");
                };
                args.config_path = value.clone();
            }
            "--run-for-s" | "--duration-s" => {
                i += 1;
                let Some(value) = raw_args.get(i) else {
                    bail!("missing value for --run-for-s");
                };
                args.run_for_s = Some(
                    value
                        .parse::<u64>()
                        .with_context(|| format!("invalid --run-for-s '{}'", value))?,
                );
            }
            "--warmup-s" => {
                i += 1;
                let Some(value) = raw_args.get(i) else {
                    bail!("missing value for --warmup-s");
                };
                args.warmup_s = value
                    .parse::<u64>()
                    .with_context(|| format!("invalid --warmup-s '{}'", value))?;
            }
            "--min-rest-ms" => {
                i += 1;
                let Some(value) = raw_args.get(i) else {
                    bail!("missing value for --min-rest-ms");
                };
                args.min_rest_ms = value
                    .parse::<u64>()
                    .with_context(|| format!("invalid --min-rest-ms '{}'", value))?;
            }
            "--adverse-mid-move-bps" => {
                i += 1;
                args.adverse_mid_move_bps = parse_f64_arg(&raw_args, i, "--adverse-mid-move-bps")?;
            }
            "--imbalance-enter" => {
                i += 1;
                args.imbalance_enter = parse_f64_arg(&raw_args, i, "--imbalance-enter")?;
            }
            "--tilt-enter-bps" => {
                i += 1;
                args.tilt_enter_bps = parse_f64_arg(&raw_args, i, "--tilt-enter-bps")?;
            }
            "--imbalance-exit" => {
                i += 1;
                args.imbalance_exit = parse_f64_arg(&raw_args, i, "--imbalance-exit")?;
            }
            "--tilt-exit-bps" => {
                i += 1;
                args.tilt_exit_bps = parse_f64_arg(&raw_args, i, "--tilt-exit-bps")?;
            }
            "--cooldown-ms" => {
                i += 1;
                let Some(value) = raw_args.get(i) else {
                    bail!("missing value for --cooldown-ms");
                };
                args.cooldown_ms = value
                    .parse::<u64>()
                    .with_context(|| format!("invalid --cooldown-ms '{}'", value))?;
            }
            "--max-actions-per-minute" => {
                i += 1;
                let Some(value) = raw_args.get(i) else {
                    bail!("missing value for --max-actions-per-minute");
                };
                args.max_actions_per_minute = value
                    .parse::<usize>()
                    .with_context(|| format!("invalid --max-actions-per-minute '{}'", value))?;
            }
            "--discount-bps" => {
                i += 1;
                args.discount_bps = parse_f64_arg(&raw_args, i, "--discount-bps")?;
            }
            "--out-json" => {
                i += 1;
                let Some(value) = raw_args.get(i) else {
                    bail!("missing value for --out-json");
                };
                args.out_json = Some(PathBuf::from(value));
            }
            "--max-consecutive-errors" => {
                i += 1;
                let Some(value) = raw_args.get(i) else {
                    bail!("missing value for --max-consecutive-errors");
                };
                args.max_consecutive_errors = value
                    .parse::<u32>()
                    .with_context(|| format!("invalid --max-consecutive-errors '{}'", value))?;
            }
            "--latency-spike-ms" => {
                i += 1;
                let Some(value) = raw_args.get(i) else {
                    bail!("missing value for --latency-spike-ms");
                };
                args.latency_spike_ms = value
                    .parse::<u64>()
                    .with_context(|| format!("invalid --latency-spike-ms '{}'", value))?;
            }
            "--max-latency-spikes" => {
                i += 1;
                let Some(value) = raw_args.get(i) else {
                    bail!("missing value for --max-latency-spikes");
                };
                args.max_latency_spikes = value
                    .parse::<u32>()
                    .with_context(|| format!("invalid --max-latency-spikes '{}'", value))?;
            }
            "--transport" => {
                i += 1;
                let Some(value) = raw_args.get(i) else {
                    bail!("missing value for --transport");
                };
                args.transport = Some(
                    ExecutionTransport::parse(value)
                        .ok_or_else(|| anyhow!("invalid --transport '{}'", value))?,
                );
            }
            "--help" | "-h" => {
                print_usage();
                std::process::exit(0);
            }
            value if !value.starts_with('-') && !positional_config_set => {
                args.config_path = value.to_string();
                positional_config_set = true;
            }
            value => bail!("unknown argument '{}'", value),
        }
        i += 1;
    }

    if args.imbalance_enter <= 0.0 || args.tilt_enter_bps <= 0.0 {
        bail!("entry thresholds must be > 0");
    }
    if args.imbalance_exit <= 0.0 || args.tilt_exit_bps <= 0.0 {
        bail!("exit thresholds must be > 0");
    }
    if args.cooldown_ms == 0 {
        bail!("--cooldown-ms must be > 0");
    }
    if args.max_actions_per_minute == 0 {
        bail!("--max-actions-per-minute must be > 0");
    }
    if args.discount_bps <= 0.0 {
        bail!("--discount-bps must be > 0");
    }
    if args.adverse_mid_move_bps <= 0.0 {
        bail!("--adverse-mid-move-bps must be > 0");
    }
    if args.max_consecutive_errors == 0 {
        bail!("--max-consecutive-errors must be > 0");
    }
    if args.latency_spike_ms == 0 {
        bail!("--latency-spike-ms must be > 0");
    }
    if args.max_latency_spikes == 0 {
        bail!("--max-latency-spikes must be > 0");
    }
    if let Some(run_for_s) = args.run_for_s {
        if run_for_s == 0 {
            bail!("--run-for-s must be > 0");
        }
    }

    Ok(args)
}

fn parse_f64_arg(raw_args: &[String], idx: usize, flag: &str) -> Result<f64> {
    let Some(value) = raw_args.get(idx) else {
        bail!("missing value for {}", flag);
    };
    value
        .parse::<f64>()
        .with_context(|| format!("invalid {} '{}'", flag, value))
}

fn print_usage() {
    eprintln!(
        "Usage:
  cargo run --bin signal_live -- [config/default.toml]
  cargo run --bin signal_live -- --config config/default.toml --run-for-s 120 --warmup-s 5 --min-rest-ms 0 --adverse-mid-move-bps 5.0 --imbalance-enter 0.20 --tilt-enter-bps 0.5 --imbalance-exit 0.10 --tilt-exit-bps 0.2 --cooldown-ms 1500 --max-actions-per-minute 30 --discount-bps 1000 --out-json runs/signal_live.json"
    );
}

fn resolve_transport(args: &LiveArgs, cfg: &AppConfig) -> Result<ExecutionTransport> {
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

fn transport_methods(transport: ExecutionTransport) -> (&'static str, &'static str) {
    match transport {
        ExecutionTransport::Rest => ("POST /api/v3/order", "DELETE /api/v3/order"),
        ExecutionTransport::WsApi => ("order.place", "order.cancel"),
    }
}

fn default_out_path(started_at_ms: u128) -> PathBuf {
    PathBuf::from(format!("runs/signal_live_{}.json", started_at_ms))
}

fn write_summary(path: &Path, summary: &LiveRunSummary) -> Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)
            .with_context(|| format!("failed to create output dir {}", parent.display()))?;
    }
    let file =
        File::create(path).with_context(|| format!("failed to create {}", path.display()))?;
    let mut writer = BufWriter::new(file);
    serde_json::to_writer_pretty(&mut writer, summary)
        .with_context(|| format!("failed to serialize {}", path.display()))?;
    writer
        .write_all(b"\n")
        .with_context(|| format!("failed to finalize {}", path.display()))?;
    writer
        .flush()
        .with_context(|| format!("failed to flush {}", path.display()))?;
    Ok(())
}

fn quantiles(values: &mut Vec<u64>) -> Option<U64Quantiles> {
    if values.is_empty() {
        return None;
    }
    values.sort_unstable();
    let samples = values.len();
    let p50 = percentile(values, 0.50);
    let p95 = percentile(values, 0.95);
    let p99 = percentile(values, 0.99);
    values.clear();

    Some(U64Quantiles {
        samples,
        p50,
        p95,
        p99,
    })
}

fn quantiles_f64(values: &mut Vec<f64>) -> Option<F64Quantiles> {
    values.retain(|value| value.is_finite());
    if values.is_empty() {
        return None;
    }
    values.sort_by(|left, right| left.total_cmp(right));
    let samples = values.len();
    let p50 = percentile_f64(values, 0.50);
    let p95 = percentile_f64(values, 0.95);
    let p99 = percentile_f64(values, 0.99);
    values.clear();

    Some(F64Quantiles {
        samples,
        p50,
        p95,
        p99,
    })
}

fn minute_bucket_mut(
    buckets: &mut BTreeMap<u64, MinuteBucketAccumulator>,
    minute_index: u64,
) -> &mut MinuteBucketAccumulator {
    buckets.entry(minute_index).or_default()
}

fn finalize_minute_buckets(
    minute_buckets: BTreeMap<u64, MinuteBucketAccumulator>,
) -> Vec<MinuteBucketSummary> {
    minute_buckets
        .into_iter()
        .map(|(minute_index, mut bucket)| MinuteBucketSummary {
            minute_index,
            triggers: bucket.triggers,
            entries: bucket.entries,
            cancels: bucket.cancels,
            neutral_cancels: bucket.neutral_cancels,
            adverse_cancels: bucket.adverse_cancels,
            delayed_neutral_cancels: bucket.delayed_neutral_cancels,
            fills: bucket.fills,
            filled_qty: bucket.filled_qty,
            rate_limit_skips: bucket.rate_limit_skips,
            avg_mid: average_f64(bucket.mid_sum, bucket.snapshot_samples),
            avg_spread_bps: average_f64(bucket.spread_bps_sum, bucket.snapshot_samples),
            avg_time_in_trade_ms: average_u128_to_f64(
                bucket.time_in_trade_ms_sum,
                bucket.time_in_trade_samples,
            ),
            avg_time_to_fill_ms: average_u128_to_f64(
                bucket.time_to_fill_ms_sum,
                bucket.time_to_fill_samples,
            ),
            avg_rest_ms: average_u128_to_f64(bucket.rest_ms_sum, bucket.rest_samples),
            avg_effective_rest_ms: average_u128_to_f64(
                bucket.effective_rest_ms_sum,
                bucket.effective_rest_samples,
            ),
            distance_to_best_bps: quantiles_f64(&mut bucket.distance_to_best_bps),
            place_rtt_us: quantiles(&mut bucket.place_rtt_us),
            cancel_rtt_us: quantiles(&mut bucket.cancel_rtt_us),
        })
        .collect()
}

fn average_f64(sum: f64, samples: u64) -> Option<f64> {
    if samples == 0 {
        None
    } else {
        Some(sum / samples as f64)
    }
}

fn average_u128_to_f64(sum: u128, samples: u64) -> Option<f64> {
    if samples == 0 {
        None
    } else {
        Some(sum as f64 / samples as f64)
    }
}

fn percentile(sorted_values: &[u64], pct: f64) -> u64 {
    if sorted_values.is_empty() {
        return 0;
    }
    let rank = ((sorted_values.len() as f64 - 1.0) * pct).round() as usize;
    sorted_values[rank.min(sorted_values.len() - 1)]
}

fn percentile_f64(sorted_values: &[f64], pct: f64) -> f64 {
    if sorted_values.is_empty() {
        return 0.0;
    }
    let rank = ((sorted_values.len() as f64 - 1.0) * pct).round() as usize;
    sorted_values[rank.min(sorted_values.len() - 1)]
}

fn duration_ms_u64(value: Duration) -> u64 {
    value.as_millis().min(u128::from(u64::MAX)) as u64
}

fn ack_latency_ms(entry_decision: Instant, ack_instant: Option<Instant>) -> Option<u64> {
    ack_instant.map(|ack| duration_ms_u64(ack.saturating_duration_since(entry_decision)))
}

fn effective_rest_ms(cancel_decision: Instant, ack_instant: Option<Instant>) -> Option<u64> {
    ack_instant.map(|ack| duration_ms_u64(cancel_decision.saturating_duration_since(ack)))
}

fn adverse_bps(direction: SignalDirection, mid_at_fill: f64, mid_after: f64) -> Option<f64> {
    if !mid_at_fill.is_finite() || !mid_after.is_finite() || mid_at_fill <= 0.0 {
        return None;
    }
    let delta_bps = ((mid_after - mid_at_fill) / mid_at_fill) * 10_000.0;
    let signed_by_direction = match direction {
        SignalDirection::Long => delta_bps,
        SignalDirection::Short => -delta_bps,
    };
    Some(-signed_by_direction)
}

fn distance_to_best_bps(
    direction: SignalDirection,
    best_bid: f64,
    best_ask: f64,
    mid: f64,
    order_price: f64,
) -> Option<f64> {
    if !best_bid.is_finite()
        || !best_ask.is_finite()
        || !mid.is_finite()
        || !order_price.is_finite()
        || mid <= 0.0
    {
        return None;
    }

    let value = match direction {
        SignalDirection::Long => (best_bid - order_price) / mid * 10_000.0,
        SignalDirection::Short => (order_price - best_ask) / mid * 10_000.0,
    };
    Some(value)
}

fn mid_move_bps(entry_mid: f64, current_mid: f64) -> Option<f64> {
    if !entry_mid.is_finite() || !current_mid.is_finite() || entry_mid <= 0.0 {
        return None;
    }
    Some(((current_mid - entry_mid) / entry_mid) * 10_000.0)
}

fn adverse_cancel_allowed(mid_move_bps_value: Option<f64>, adverse_mid_move_bps: f64) -> bool {
    mid_move_bps_value
        .map(|value| value.abs() >= adverse_mid_move_bps)
        .unwrap_or(false)
}

fn update_pending_adverse_samples(
    pending: &mut Vec<PendingAdverseSample>,
    snapshot_ts_ms: u128,
    mid: f64,
    adverse_bps_1s: &mut Vec<f64>,
    adverse_bps_5s: &mut Vec<f64>,
) {
    for sample in pending.iter_mut() {
        if !sample.recorded_1s && snapshot_ts_ms >= sample.target_1s_ts_ms {
            if let Some(value) = adverse_bps(sample.direction, sample.mid_at_fill, mid) {
                adverse_bps_1s.push(value);
            }
            sample.recorded_1s = true;
        }
        if !sample.recorded_5s && snapshot_ts_ms >= sample.target_5s_ts_ms {
            if let Some(value) = adverse_bps(sample.direction, sample.mid_at_fill, mid) {
                adverse_bps_5s.push(value);
            }
            sample.recorded_5s = true;
        }
    }

    pending.retain(|sample| !(sample.recorded_1s && sample.recorded_5s));
}

#[derive(Debug, Clone)]
struct ErrorMeta {
    kind: String,
    http_status: Option<u16>,
    binance_code: Option<i64>,
    message: String,
    body_snippet: Option<String>,
    path: Option<String>,
    request_id: Option<u64>,
    retry_count: u32,
}

fn error_meta(error: &ExecutionError) -> ErrorMeta {
    match error {
        ExecutionError::Api {
            status,
            code,
            msg,
            path,
            body_snippet,
            request_id,
            retry_count,
        } => ErrorMeta {
            kind: "api_error".to_string(),
            http_status: Some(*status),
            binance_code: *code,
            message: msg.clone(),
            body_snippet: body_snippet.clone(),
            path: path.clone(),
            request_id: *request_id,
            retry_count: *retry_count,
        },
        ExecutionError::RateLimited(msg) => ErrorMeta {
            kind: "rate_limited".to_string(),
            http_status: None,
            binance_code: None,
            message: msg.clone(),
            body_snippet: Some(msg.chars().take(300).collect()),
            path: None,
            request_id: None,
            retry_count: 0,
        },
        ExecutionError::Http {
            source,
            path,
            request_id,
            retry_count,
            body_snippet,
        } => ErrorMeta {
            kind: "http_error".to_string(),
            http_status: source.status().map(|status| status.as_u16()),
            binance_code: None,
            message: source.to_string(),
            body_snippet: body_snippet.clone(),
            path: path.clone(),
            request_id: *request_id,
            retry_count: *retry_count,
        },
        ExecutionError::WsApiSocket(source) => ErrorMeta {
            kind: "ws_api_socket".to_string(),
            http_status: None,
            binance_code: None,
            message: source.to_string(),
            body_snippet: None,
            path: None,
            request_id: None,
            retry_count: 0,
        },
        ExecutionError::WsApiProtocol(msg) => ErrorMeta {
            kind: "ws_api_protocol".to_string(),
            http_status: None,
            binance_code: None,
            message: msg.clone(),
            body_snippet: None,
            path: None,
            request_id: None,
            retry_count: 0,
        },
        other => ErrorMeta {
            kind: "other".to_string(),
            http_status: None,
            binance_code: None,
            message: other.to_string(),
            body_snippet: None,
            path: None,
            request_id: None,
            retry_count: 0,
        },
    }
}

fn side_from_direction(direction: SignalDirection) -> Side {
    match direction {
        SignalDirection::Long => Side::Buy,
        SignalDirection::Short => Side::Sell,
    }
}

fn price_for_direction(mid: f64, discount_bps: f64, direction: SignalDirection) -> Result<f64> {
    if !mid.is_finite() || mid <= 0.0 {
        bail!("invalid mid {}", mid);
    }
    let factor = match direction {
        SignalDirection::Long => 1.0 - discount_bps / 10_000.0,
        SignalDirection::Short => 1.0 + discount_bps / 10_000.0,
    };
    let price = mid * factor;
    if !price.is_finite() || price <= 0.0 {
        bail!("invalid derived price {}", price);
    }
    Ok(price)
}

fn quantized_order_values(
    computed_price: f64,
    qty: f64,
    filters: &SymbolFilters,
) -> Result<(f64, f64)> {
    let quantized_price =
        quantize_price_floor(computed_price, filters.tick_size).ok_or_else(|| {
            anyhow!(
                "failed to quantize price computed={} tick_size={}",
                computed_price,
                filters.tick_size
            )
        })?;
    let quantized_qty = quantize_qty_floor(qty, filters.step_size).ok_or_else(|| {
        anyhow!(
            "failed to quantize qty qty={} step_size={}",
            qty,
            filters.step_size
        )
    })?;
    if quantized_price <= 0.0 || quantized_qty <= 0.0 {
        bail!(
            "non-positive quantized values price={} qty={}",
            quantized_price,
            quantized_qty
        );
    }
    Ok((quantized_price, quantized_qty))
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

fn build_client_order_id(prefix: &str, iteration: usize) -> String {
    let mut out = String::with_capacity(prefix.len() + 20);
    out.push_str(prefix);
    push_usize_decimal(&mut out, iteration);
    out
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = parse_args()?;
    telemetry::init("signal_live");

    let started_at_ms = now_millis();
    let out_path = args
        .out_json
        .clone()
        .unwrap_or_else(|| default_out_path(started_at_ms));

    let cfg = AppConfig::load_from_path(&args.config_path)
        .with_context(|| format!("failed to load config from {}", args.config_path))?;
    let transport = resolve_transport(&args, &cfg)?;
    let requested_run_for_s = args.run_for_s.unwrap_or_else(|| cfg.general.duration_s());
    let (place_method, cancel_method) = transport_methods(transport);

    let mut md_rx = spawn_marketdata(&cfg.marketdata, cfg.general.symbol.clone())
        .context("failed to start marketdata source")?;
    let mut orderbook = OrderBook::new(cfg.general.symbol.clone());
    let qty = cfg.strategy.order_qty;
    if qty <= 0.0 {
        bail!("strategy.order_qty must be > 0");
    }

    let executor = match transport {
        ExecutionTransport::Rest => SignalExecutor::Rest(
            BinanceTestnetExecutor::from_config(&cfg.execution).context(
                "failed to create Binance REST executor (check BINANCE_API_KEY/BINANCE_API_SECRET)",
            )?,
        ),
        ExecutionTransport::WsApi => SignalExecutor::WsApi(
            BinanceWsExecutor::from_config(&cfg.execution).context(
                "failed to create Binance WS API executor (check BINANCE_API_KEY/BINANCE_API_SECRET)",
            )?,
        ),
    };

    let symbol = cfg.general.symbol.to_uppercase();
    let filters = executor
        .fetch_symbol_filters(&symbol)
        .await
        .with_context(|| format!("failed to fetch exchangeInfo filters for {}", symbol))?;

    let thresholds = SignalThresholds {
        imbalance_enter: args.imbalance_enter,
        tilt_enter_bps: args.tilt_enter_bps,
        imbalance_exit: args.imbalance_exit,
        tilt_exit_bps: args.tilt_exit_bps,
        cooldown_ms: args.cooldown_ms,
    };
    let mut runner = SignalRunner::new(thresholds);
    let mut event_counters = LiveEventCounters::default();
    let mut counters = LiveCounters::default();
    let mut cancels_by_reason = CancelsByReason::default();
    let mut neutral_cancel_delayed_count: u64 = 0;
    let mut neutral_rest_ms_sum: u128 = 0;
    let mut neutral_rest_samples: u64 = 0;
    let mut error_reasons: BTreeMap<String, u64> = BTreeMap::new();
    let mut last_http_status: Option<u16> = None;
    let mut last_binance_code: Option<i64> = None;
    let mut last_error_body_snippet: Option<String> = None;
    let mut last_path: Option<String> = None;
    let mut place_rtt_us_excluding_warmup: Vec<u64> = Vec::new();
    let mut cancel_rtt_us_excluding_warmup: Vec<u64> = Vec::new();
    let mut ack_latency_ms_samples: Vec<u64> = Vec::new();
    let mut effective_rest_ms_samples: Vec<u64> = Vec::new();
    let mut distance_to_best_bps_samples: Vec<f64> = Vec::new();
    let mut time_to_fill_ms_samples: Vec<u64> = Vec::new();
    let mut time_to_fill_ms_sum: u128 = 0;
    let mut adverse_bps_1s_samples: Vec<f64> = Vec::new();
    let mut adverse_bps_5s_samples: Vec<f64> = Vec::new();
    let mut mid_move_bps_at_cancel_samples: Vec<f64> = Vec::new();
    let mut pending_adverse_samples: Vec<PendingAdverseSample> = Vec::new();
    let mut minute_buckets: BTreeMap<u64, MinuteBucketAccumulator> = BTreeMap::new();
    let mut state_transitions: Vec<RunnerTransition> = Vec::new();
    let mut active_order: Option<ActiveOrder> = None;
    let mut action_limiter = ActionRateLimiter::new(args.max_actions_per_minute);
    let mut next_fill_poll_ts_ms: u128 = 0;
    let mut last_fill_poll_error_log_ms: u128 = 0;

    let mut run_prefix = String::with_capacity(32);
    run_prefix.push_str("sig");
    push_u128_decimal(&mut run_prefix, started_at_ms);
    run_prefix.push('_');
    let mut order_seq: usize = 0;

    let mut consecutive_errors: u32 = 0;
    let mut spike_tracker = SpikeTracker::new(
        args.latency_spike_ms,
        LATENCY_SPIKE_WARMUP_ACTIONS,
        LATENCY_SPIKE_LAST_N,
    );
    let mut kill_switch_reason: Option<String> = None;
    let warmup_s = args.warmup_s;
    let warmup_duration = Duration::from_secs(warmup_s);
    let warmup_start_ms = started_at_ms;
    let warmup_target_end_ms = warmup_start_ms.saturating_add(u128::from(warmup_s) * 1_000);
    let mut warmup_end_logged = false;
    let mut warmup_actions_excluded = 0u64;
    let run_started = tokio::time::Instant::now();
    let deadline = run_started + Duration::from_secs(requested_run_for_s);
    let run_timeout = tokio::time::sleep_until(deadline);
    tokio::pin!(run_timeout);
    let sigint = tokio::signal::ctrl_c();
    tokio::pin!(sigint);
    let mut exit_reason: Option<ExitReason> = None;

    info!(
        symbol = %symbol,
        transport = transport.as_str(),
        requested_run_for_s,
        warmup_s,
        min_rest_ms = args.min_rest_ms,
        adverse_mid_move_bps = args.adverse_mid_move_bps,
        place_method,
        cancel_method,
        discount_bps = args.discount_bps,
        max_actions_per_minute = args.max_actions_per_minute,
        max_consecutive_errors = args.max_consecutive_errors,
        latency_spike_ms = args.latency_spike_ms,
        latency_spike_us = spike_tracker.threshold_us(),
        latency_spike_warmup_actions = LATENCY_SPIKE_WARMUP_ACTIONS,
        max_latency_spikes = args.max_latency_spikes,
        "starting signal_live"
    );

    info!(
        warmup_start_ms,
        warmup_target_end_ms, warmup_s, "signal_live warmup start"
    );

    while exit_reason.is_none() {
        let maybe_event = tokio::select! {
            _ = &mut run_timeout => {
                exit_reason = Some(ExitReason::Timeout);
                None
            }
            _ = &mut sigint => {
                exit_reason = Some(ExitReason::Sigint);
                None
            }
            maybe_event = md_rx.recv() => maybe_event,
        };

        let Some(event) = maybe_event else {
            if exit_reason.is_some() {
                break;
            }

            counters.errors = counters.errors.saturating_add(1);
            *error_reasons
                .entry("marketdata_closed".to_string())
                .or_insert(0) += 1;
            kill_switch_reason.get_or_insert_with(|| "marketdata channel closed".to_string());
            exit_reason = Some(ExitReason::Errors);
            break;
        };

        orderbook.apply(&event);
        let Some(snapshot) = compute_signal_snapshot(
            event.ts_ms,
            orderbook.best_bid,
            orderbook.best_bid_qty,
            orderbook.best_ask,
            orderbook.best_ask_qty,
        ) else {
            continue;
        };
        let elapsed = run_started.elapsed();
        let in_warmup = elapsed < warmup_duration;
        if !warmup_end_logged && !in_warmup {
            warmup_end_logged = true;
            info!(
                warmup_start_ms,
                warmup_end_ms = now_millis(),
                warmup_actions_excluded,
                "signal_live warmup end"
            );
        }
        let minute_index = if in_warmup {
            None
        } else {
            Some(elapsed.saturating_sub(warmup_duration).as_secs() / 60)
        };
        if let Some(minute_index) = minute_index {
            let bucket = minute_bucket_mut(&mut minute_buckets, minute_index);
            bucket.snapshot_samples = bucket.snapshot_samples.saturating_add(1);
            bucket.mid_sum += snapshot.mid;
            bucket.spread_bps_sum += snapshot.spread_bps;
        }
        update_pending_adverse_samples(
            &mut pending_adverse_samples,
            snapshot.ts_ms,
            snapshot.mid,
            &mut adverse_bps_1s_samples,
            &mut adverse_bps_5s_samples,
        );

        if snapshot.ts_ms >= next_fill_poll_ts_ms {
            if let Some(active) = active_order.as_ref() {
                next_fill_poll_ts_ms = snapshot.ts_ms.saturating_add(FILL_POLL_INTERVAL_MS);
                let polled_client_order_id = active.client_order_id.clone();
                match executor
                    .query_order_snapshot(&symbol, &polled_client_order_id)
                    .await
                {
                    Ok(polled) => {
                        let mut clear_active_order = false;
                        let mut force_wait_rearm = false;
                        if let Some(current) = active_order.as_mut() {
                            if current.client_order_id == polled_client_order_id {
                                let previous_executed_qty = current.executed_qty;
                                let latest_executed_qty =
                                    previous_executed_qty.max(polled.executed_qty);
                                current.executed_qty = latest_executed_qty;
                                let filled_delta = latest_executed_qty - previous_executed_qty;
                                if filled_delta > MIN_FILL_QTY_EPS && !in_warmup {
                                    event_counters.trade_fill =
                                        event_counters.trade_fill.saturating_add(1);
                                    counters.fills = counters.fills.saturating_add(1);
                                    counters.filled_qty_total += filled_delta;
                                    let is_partial =
                                        matches!(polled.order.status, OrderStatus::PartiallyFilled)
                                            || latest_executed_qty + MIN_FILL_QTY_EPS
                                                < current.orig_qty;
                                    if is_partial {
                                        counters.partial_fills =
                                            counters.partial_fills.saturating_add(1);
                                    }

                                    let time_to_fill_ms = snapshot
                                        .ts_ms
                                        .saturating_sub(current.entered_at_ts_ms)
                                        .min(u128::from(u64::MAX))
                                        as u64;
                                    time_to_fill_ms_sum = time_to_fill_ms_sum
                                        .saturating_add(u128::from(time_to_fill_ms));
                                    time_to_fill_ms_samples.push(time_to_fill_ms);
                                    if let Some(minute_index) = minute_index {
                                        let bucket =
                                            minute_bucket_mut(&mut minute_buckets, minute_index);
                                        bucket.fills = bucket.fills.saturating_add(1);
                                        bucket.filled_qty += filled_delta;
                                        bucket.time_to_fill_ms_sum = bucket
                                            .time_to_fill_ms_sum
                                            .saturating_add(u128::from(time_to_fill_ms));
                                        bucket.time_to_fill_samples =
                                            bucket.time_to_fill_samples.saturating_add(1);
                                    }
                                    pending_adverse_samples.push(PendingAdverseSample::new(
                                        current.direction,
                                        snapshot.ts_ms,
                                        snapshot.mid,
                                    ));
                                }

                                if matches!(
                                    polled.order.status,
                                    OrderStatus::Filled
                                        | OrderStatus::Canceled
                                        | OrderStatus::Rejected
                                ) {
                                    clear_active_order = true;
                                    if matches!(polled.order.status, OrderStatus::Filled) {
                                        force_wait_rearm = true;
                                    }
                                }
                            }
                        }

                        if clear_active_order {
                            active_order = None;
                            if force_wait_rearm {
                                runner.restore_state(SignalRunnerState {
                                    last_trade_ts_ms: Some(snapshot.ts_ms),
                                    active_direction: None,
                                    phase: RunnerPhase::WaitRearm,
                                });
                            }
                        }
                    }
                    Err(error) => {
                        counters.errors = counters.errors.saturating_add(1);
                        let meta = error_meta(&error);
                        *error_reasons
                            .entry(format!("fill_poll_{}", meta.kind))
                            .or_insert(0) += 1;
                        if let Some(reason) =
                            explicit_rest_error_reason(meta.http_status, meta.binance_code)
                        {
                            *error_reasons
                                .entry(format!("fill_poll_{reason}"))
                                .or_insert(0) += 1;
                        }
                        if matches!(transport, ExecutionTransport::Rest)
                            && (meta.kind == "api_error" || meta.kind == "http_error")
                        {
                            last_http_status = meta.http_status;
                            last_binance_code = meta.binance_code;
                            last_error_body_snippet = meta
                                .body_snippet
                                .clone()
                                .or_else(|| Some(meta.message.chars().take(300).collect()));
                            last_path = meta.path.clone();
                            warn!(
                                transport = transport.as_str(),
                                request_id = ?meta.request_id,
                                retry_count = meta.retry_count,
                                path = ?meta.path.as_deref(),
                                http_status = ?meta.http_status,
                                binance_code = ?meta.binance_code,
                                error_reason = ?explicit_rest_error_reason(
                                    meta.http_status,
                                    meta.binance_code
                                ),
                                last_error_body_snippet = ?last_error_body_snippet.as_deref(),
                                "signal_live rest http error"
                            );
                        }
                        let now_ms = now_millis();
                        if now_ms.saturating_sub(last_fill_poll_error_log_ms)
                            >= FILL_POLL_ERROR_LOG_INTERVAL_MS
                        {
                            last_fill_poll_error_log_ms = now_ms;
                            warn!(
                                client_order_id = %polled_client_order_id,
                                error_kind = %meta.kind,
                                http_status = ?meta.http_status,
                                binance_code = ?meta.binance_code,
                                "fill poll failed"
                            );
                        }
                    }
                }
            }
        }

        match classify_trigger(&snapshot, runner.thresholds()) {
            Some(SignalDirection::Long) => {
                counters.triggers_long = counters.triggers_long.saturating_add(1);
                if let Some(minute_index) = minute_index {
                    let bucket = minute_bucket_mut(&mut minute_buckets, minute_index);
                    bucket.triggers = bucket.triggers.saturating_add(1);
                }
            }
            Some(SignalDirection::Short) => {
                counters.triggers_short = counters.triggers_short.saturating_add(1);
                if let Some(minute_index) = minute_index {
                    let bucket = minute_bucket_mut(&mut minute_buckets, minute_index);
                    bucket.triggers = bucket.triggers.saturating_add(1);
                }
            }
            None => {}
        }

        let prev_state = runner.state();
        let decision = runner.on_snapshot(&snapshot);
        match decision.action {
            RunnerAction::None => {}
            RunnerAction::CooldownSkip(direction) => {
                counters.cooldown_skips = counters.cooldown_skips.saturating_add(1);
                info!(
                    reason = "cooldown_skip",
                    direction = ?direction,
                    ts_ms = snapshot.ts_ms,
                    imbalance = snapshot.imbalance,
                    tilt_bps = snapshot.tilt_bps,
                    spread_bps = snapshot.spread_bps,
                    mid = snapshot.mid,
                    "signal decision"
                );
            }
            RunnerAction::RearmWaitSkip(direction) => {
                counters.rearm_wait_skips = counters.rearm_wait_skips.saturating_add(1);
                info!(
                    reason = "rearm_wait_skip",
                    direction = ?direction,
                    ts_ms = snapshot.ts_ms,
                    imbalance = snapshot.imbalance,
                    tilt_bps = snapshot.tilt_bps,
                    spread_bps = snapshot.spread_bps,
                    mid = snapshot.mid,
                    "signal decision"
                );
            }
            RunnerAction::Enter(direction) => {
                if !action_limiter.allow(snapshot.ts_ms) {
                    if !in_warmup {
                        counters.action_rate_limit_skips =
                            counters.action_rate_limit_skips.saturating_add(1);
                        if let Some(minute_index) = minute_index {
                            let bucket = minute_bucket_mut(&mut minute_buckets, minute_index);
                            bucket.rate_limit_skips = bucket.rate_limit_skips.saturating_add(1);
                        }
                    }
                    runner.restore_state(prev_state);
                    warn!(
                        reason = "max_actions_per_minute",
                        direction = ?direction,
                        max_actions_per_minute = args.max_actions_per_minute,
                        ts_ms = snapshot.ts_ms,
                        "entry skipped by rate limiter"
                    );
                    continue;
                }

                let computed_price =
                    match price_for_direction(snapshot.mid, args.discount_bps, direction) {
                        Ok(value) => value,
                        Err(error) => {
                            counters.errors = counters.errors.saturating_add(1);
                            runner.restore_state(prev_state);
                            *error_reasons
                                .entry("invalid_price".to_string())
                                .or_insert(0) += 1;
                            error!(error = %error, "failed to compute entry price");
                            continue;
                        }
                    };
                let (order_price, order_qty) =
                    match quantized_order_values(computed_price, qty, &filters) {
                        Ok(values) => values,
                        Err(error) => {
                            counters.errors = counters.errors.saturating_add(1);
                            runner.restore_state(prev_state);
                            *error_reasons
                                .entry("quantize_failed".to_string())
                                .or_insert(0) += 1;
                            error!(error = %error, "failed to quantize order");
                            continue;
                        }
                    };
                let best_bid = orderbook.best_bid;
                let best_ask = orderbook.best_ask;
                let distance_to_best_bps_value =
                    distance_to_best_bps(direction, best_bid, best_ask, snapshot.mid, order_price);
                if !in_warmup {
                    if let Some(distance_bps) = distance_to_best_bps_value {
                        distance_to_best_bps_samples.push(distance_bps);
                        if let Some(minute_index) = minute_index {
                            let bucket = minute_bucket_mut(&mut minute_buckets, minute_index);
                            bucket.distance_to_best_bps.push(distance_bps);
                        }
                    }
                }

                if let Some(min_notional) = filters.min_notional {
                    let notional = order_price * order_qty;
                    if notional < min_notional {
                        counters.errors = counters.errors.saturating_add(1);
                        runner.restore_state(prev_state);
                        *error_reasons.entry("min_notional".to_string()).or_insert(0) += 1;
                        error!(
                            notional,
                            min_notional, order_price, order_qty, "entry blocked by min_notional"
                        );
                        continue;
                    }
                }

                order_seq = order_seq.saturating_add(1);
                let client_order_id = build_client_order_id(&run_prefix, order_seq);
                let request = OrderRequest {
                    symbol: symbol.clone(),
                    side: side_from_direction(direction),
                    qty: order_qty,
                    price: order_price,
                    order_type: lowlat_bot::execution::OrderType::LimitMaker,
                };
                let entry_decision_instant = Instant::now();

                match executor
                    .new_limit_order_with_metrics(&request, client_order_id.clone())
                    .await
                {
                    Ok(outcome) => {
                        let ack_latency_ms_value =
                            ack_latency_ms(entry_decision_instant, outcome.place_ack_at);
                        if in_warmup {
                            warmup_actions_excluded = warmup_actions_excluded.saturating_add(1);
                        } else {
                            counters.entries = counters.entries.saturating_add(1);
                            if outcome.place_to_ack_us.is_some() {
                                event_counters.order_ack =
                                    event_counters.order_ack.saturating_add(1);
                            }
                            if let Some(ack_latency_ms_value) = ack_latency_ms_value {
                                ack_latency_ms_samples.push(ack_latency_ms_value);
                            }
                            place_rtt_us_excluding_warmup.push(outcome.place_http_us);
                            if let Some(minute_index) = minute_index {
                                let bucket = minute_bucket_mut(&mut minute_buckets, minute_index);
                                bucket.entries = bucket.entries.saturating_add(1);
                                bucket.place_rtt_us.push(outcome.place_http_us);
                            }
                        }
                        active_order = Some(ActiveOrder {
                            direction,
                            client_order_id: outcome.order.client_order_id.clone(),
                            entered_at_ts_ms: snapshot.ts_ms,
                            place_ack_instant: outcome.place_ack_at,
                            entry_mid: snapshot.mid,
                            orig_qty: order_qty,
                            executed_qty: 0.0,
                        });
                        next_fill_poll_ts_ms = snapshot.ts_ms;
                        consecutive_errors = 0;

                        let spike = spike_tracker.record_rtt(outcome.place_http_us);
                        if spike.is_spike {
                            warn!(
                                latency_rtt_us = outcome.place_http_us,
                                latency_spike_streak = spike.streak,
                                latency_spike_us = spike_tracker.threshold_us(),
                                latency_spike_ms = args.latency_spike_ms,
                                "place latency spike"
                            );
                        }

                        info!(
                            action = "entry",
                            reason = "trigger",
                            direction = ?direction,
                            client_order_id = %outcome.order.client_order_id,
                            ts_ms = snapshot.ts_ms,
                            imbalance = snapshot.imbalance,
                            tilt_bps = snapshot.tilt_bps,
                            spread_bps = snapshot.spread_bps,
                            mid = snapshot.mid,
                            best_bid,
                            best_ask,
                            order_price,
                            distance_to_best_bps = ?distance_to_best_bps_value,
                            order_qty,
                            place_rtt_us = outcome.place_http_us,
                            "signal action"
                        );
                    }
                    Err(error) => {
                        let meta = error_meta(&error);
                        counters.errors = counters.errors.saturating_add(1);
                        consecutive_errors = consecutive_errors.saturating_add(1);
                        runner.restore_state(prev_state);
                        *error_reasons.entry(meta.kind.clone()).or_insert(0) += 1;
                        if let Some(reason) =
                            explicit_rest_error_reason(meta.http_status, meta.binance_code)
                        {
                            *error_reasons.entry(reason.to_string()).or_insert(0) += 1;
                        }
                        if matches!(transport, ExecutionTransport::Rest)
                            && (meta.kind == "api_error" || meta.kind == "http_error")
                        {
                            last_http_status = meta.http_status;
                            last_binance_code = meta.binance_code;
                            last_error_body_snippet = meta
                                .body_snippet
                                .clone()
                                .or_else(|| Some(meta.message.chars().take(300).collect()));
                            last_path = meta.path.clone();
                            warn!(
                                transport = transport.as_str(),
                                request_id = ?meta.request_id,
                                retry_count = meta.retry_count,
                                path = ?meta.path.as_deref(),
                                http_status = ?meta.http_status,
                                binance_code = ?meta.binance_code,
                                error_reason = ?explicit_rest_error_reason(
                                    meta.http_status,
                                    meta.binance_code
                                ),
                                last_error_body_snippet = ?meta.body_snippet.as_deref(),
                                "signal_live rest http error"
                            );
                        }
                        error!(
                            action = "entry",
                            reason = "executor_error",
                            direction = ?direction,
                            client_order_id = %client_order_id,
                            ts_ms = snapshot.ts_ms,
                            best_bid,
                            best_ask,
                            mid = snapshot.mid,
                            order_price,
                            distance_to_best_bps = ?distance_to_best_bps_value,
                            http_status = ?meta.http_status,
                            binance_code = ?meta.binance_code,
                            error_kind = %meta.kind,
                            error = %meta.message,
                            "signal action failed"
                        );
                        sleep(Duration::from_millis(200)).await;
                    }
                }
            }
            RunnerAction::CancelNeutral(direction) | RunnerAction::CancelAdverse(direction) => {
                let (reason, is_neutral) = match decision.action {
                    RunnerAction::CancelNeutral(_) => ("neutral", true),
                    RunnerAction::CancelAdverse(_) => ("adverse", false),
                    _ => unreachable!("cancel branch expects cancel action"),
                };

                let Some(active) = active_order.clone() else {
                    runner.restore_state(prev_state);
                    continue;
                };
                if active.direction != direction {
                    runner.restore_state(prev_state);
                    continue;
                }
                let mid_move_bps_value = mid_move_bps(active.entry_mid, snapshot.mid);
                if !is_neutral
                    && !adverse_cancel_allowed(mid_move_bps_value, args.adverse_mid_move_bps)
                {
                    runner.restore_state(prev_state);
                    continue;
                }
                let time_in_trade_ms = snapshot.ts_ms.saturating_sub(active.entered_at_ts_ms);
                if is_neutral && time_in_trade_ms < u128::from(args.min_rest_ms) {
                    runner.restore_state(prev_state);
                    if !in_warmup {
                        neutral_cancel_delayed_count =
                            neutral_cancel_delayed_count.saturating_add(1);
                        if let Some(minute_index) = minute_index {
                            let bucket = minute_bucket_mut(&mut minute_buckets, minute_index);
                            bucket.delayed_neutral_cancels =
                                bucket.delayed_neutral_cancels.saturating_add(1);
                        }
                    }
                    continue;
                }
                if !action_limiter.allow(snapshot.ts_ms) {
                    if !in_warmup {
                        counters.action_rate_limit_skips =
                            counters.action_rate_limit_skips.saturating_add(1);
                        if let Some(minute_index) = minute_index {
                            let bucket = minute_bucket_mut(&mut minute_buckets, minute_index);
                            bucket.rate_limit_skips = bucket.rate_limit_skips.saturating_add(1);
                        }
                    }
                    runner.restore_state(prev_state);
                    warn!(
                        reason = "max_actions_per_minute",
                        direction = ?direction,
                        max_actions_per_minute = args.max_actions_per_minute,
                        ts_ms = snapshot.ts_ms,
                        "cancel skipped by rate limiter"
                    );
                    continue;
                }
                let cancel_decision_instant = Instant::now();
                let effective_rest_ms_value =
                    effective_rest_ms(cancel_decision_instant, active.place_ack_instant);

                match executor
                    .cancel_order_with_metrics(&symbol, &active.client_order_id)
                    .await
                {
                    Ok(outcome) => {
                        if in_warmup {
                            warmup_actions_excluded = warmup_actions_excluded.saturating_add(1);
                        } else {
                            counters.cancels = counters.cancels.saturating_add(1);
                            if is_neutral {
                                cancels_by_reason.neutral =
                                    cancels_by_reason.neutral.saturating_add(1);
                                neutral_rest_ms_sum =
                                    neutral_rest_ms_sum.saturating_add(time_in_trade_ms);
                                neutral_rest_samples = neutral_rest_samples.saturating_add(1);
                            } else {
                                cancels_by_reason.adverse =
                                    cancels_by_reason.adverse.saturating_add(1);
                            }
                            if let Some(mid_move_bps_value) = mid_move_bps_value {
                                mid_move_bps_at_cancel_samples.push(mid_move_bps_value);
                            }
                            if let Some(effective_rest_ms_value) = effective_rest_ms_value {
                                effective_rest_ms_samples.push(effective_rest_ms_value);
                            }
                            if outcome.cancel_to_ack_us.is_some() {
                                event_counters.cancel_ack =
                                    event_counters.cancel_ack.saturating_add(1);
                            }
                            cancel_rtt_us_excluding_warmup.push(outcome.cancel_http_us);
                            if let Some(minute_index) = minute_index {
                                let bucket = minute_bucket_mut(&mut minute_buckets, minute_index);
                                bucket.cancels = bucket.cancels.saturating_add(1);
                                if is_neutral {
                                    bucket.neutral_cancels =
                                        bucket.neutral_cancels.saturating_add(1);
                                    bucket.rest_ms_sum =
                                        bucket.rest_ms_sum.saturating_add(time_in_trade_ms);
                                    bucket.rest_samples = bucket.rest_samples.saturating_add(1);
                                } else {
                                    bucket.adverse_cancels =
                                        bucket.adverse_cancels.saturating_add(1);
                                }
                                bucket.time_in_trade_ms_sum =
                                    bucket.time_in_trade_ms_sum.saturating_add(time_in_trade_ms);
                                bucket.time_in_trade_samples =
                                    bucket.time_in_trade_samples.saturating_add(1);
                                if let Some(effective_rest_ms_value) = effective_rest_ms_value {
                                    bucket.effective_rest_ms_sum = bucket
                                        .effective_rest_ms_sum
                                        .saturating_add(u128::from(effective_rest_ms_value));
                                    bucket.effective_rest_samples =
                                        bucket.effective_rest_samples.saturating_add(1);
                                }
                                bucket.cancel_rtt_us.push(outcome.cancel_http_us);
                            }
                        }
                        if matches!(outcome.order.status, OrderStatus::Filled) {
                            let final_fill_qty = (active.orig_qty - active.executed_qty).max(0.0);
                            if final_fill_qty > MIN_FILL_QTY_EPS && !in_warmup {
                                event_counters.trade_fill =
                                    event_counters.trade_fill.saturating_add(1);
                                counters.fills = counters.fills.saturating_add(1);
                                counters.filled_qty_total += final_fill_qty;

                                let time_to_fill_ms = snapshot
                                    .ts_ms
                                    .saturating_sub(active.entered_at_ts_ms)
                                    .min(u128::from(u64::MAX))
                                    as u64;
                                time_to_fill_ms_sum =
                                    time_to_fill_ms_sum.saturating_add(u128::from(time_to_fill_ms));
                                time_to_fill_ms_samples.push(time_to_fill_ms);
                                if let Some(minute_index) = minute_index {
                                    let bucket =
                                        minute_bucket_mut(&mut minute_buckets, minute_index);
                                    bucket.fills = bucket.fills.saturating_add(1);
                                    bucket.filled_qty += final_fill_qty;
                                    bucket.time_to_fill_ms_sum = bucket
                                        .time_to_fill_ms_sum
                                        .saturating_add(u128::from(time_to_fill_ms));
                                    bucket.time_to_fill_samples =
                                        bucket.time_to_fill_samples.saturating_add(1);
                                }
                                pending_adverse_samples.push(PendingAdverseSample::new(
                                    active.direction,
                                    snapshot.ts_ms,
                                    snapshot.mid,
                                ));
                            }
                        }
                        active_order = None;
                        consecutive_errors = 0;

                        let spike = spike_tracker.record_rtt(outcome.cancel_http_us);
                        if spike.is_spike {
                            warn!(
                                latency_rtt_us = outcome.cancel_http_us,
                                latency_spike_streak = spike.streak,
                                latency_spike_us = spike_tracker.threshold_us(),
                                latency_spike_ms = args.latency_spike_ms,
                                "cancel latency spike"
                            );
                        }

                        info!(
                            action = "cancel",
                            reason,
                            direction = ?direction,
                            client_order_id = %active.client_order_id,
                            ts_ms = snapshot.ts_ms,
                            imbalance = snapshot.imbalance,
                            tilt_bps = snapshot.tilt_bps,
                            spread_bps = snapshot.spread_bps,
                            mid = snapshot.mid,
                            mid_move_bps = ?mid_move_bps_value,
                            adverse_mid_move_bps = args.adverse_mid_move_bps,
                            cancel_rtt_us = outcome.cancel_http_us,
                            "signal action"
                        );
                    }
                    Err(error) => {
                        let meta = error_meta(&error);
                        counters.errors = counters.errors.saturating_add(1);
                        consecutive_errors = consecutive_errors.saturating_add(1);
                        runner.restore_state(prev_state);
                        *error_reasons.entry(meta.kind.clone()).or_insert(0) += 1;
                        if let Some(reason) =
                            explicit_rest_error_reason(meta.http_status, meta.binance_code)
                        {
                            *error_reasons.entry(reason.to_string()).or_insert(0) += 1;
                        }
                        if matches!(transport, ExecutionTransport::Rest)
                            && (meta.kind == "api_error" || meta.kind == "http_error")
                        {
                            last_http_status = meta.http_status;
                            last_binance_code = meta.binance_code;
                            last_error_body_snippet = meta
                                .body_snippet
                                .clone()
                                .or_else(|| Some(meta.message.chars().take(300).collect()));
                            last_path = meta.path.clone();
                            warn!(
                                transport = transport.as_str(),
                                request_id = ?meta.request_id,
                                retry_count = meta.retry_count,
                                path = ?meta.path.as_deref(),
                                http_status = ?meta.http_status,
                                binance_code = ?meta.binance_code,
                                error_reason = ?explicit_rest_error_reason(
                                    meta.http_status,
                                    meta.binance_code
                                ),
                                last_error_body_snippet = ?meta.body_snippet.as_deref(),
                                "signal_live rest http error"
                            );
                        }
                        error!(
                            action = "cancel",
                            reason,
                            direction = ?direction,
                            client_order_id = %active.client_order_id,
                            ts_ms = snapshot.ts_ms,
                            http_status = ?meta.http_status,
                            binance_code = ?meta.binance_code,
                            error_kind = %meta.kind,
                            error = %meta.message,
                            "signal action failed"
                        );
                        sleep(Duration::from_millis(200)).await;
                    }
                }
            }
        }

        if let Some(transition) = decision.transition {
            if runner.state() != prev_state {
                info!(
                    ts_ms = transition.ts_ms,
                    from = ?transition.from,
                    to = ?transition.to,
                    reason = ?transition.reason,
                    "signal state transition"
                );
                state_transitions.push(transition);
            }
        }

        if consecutive_errors >= args.max_consecutive_errors {
            kill_switch_reason = Some(format!(
                "consecutive_errors={} reached limit={}",
                consecutive_errors, args.max_consecutive_errors
            ));
            exit_reason = Some(ExitReason::Errors);
            break;
        }

        if spike_tracker.spike_streak() > args.max_latency_spikes {
            kill_switch_reason = Some(format!(
                "latency_spike_streak={} exceeded limit={} (> {} ms / {} us)",
                spike_tracker.spike_streak(),
                args.max_latency_spikes,
                args.latency_spike_ms,
                spike_tracker.threshold_us()
            ));
            exit_reason = Some(ExitReason::KillSwitch);
            break;
        }
    }

    if !warmup_end_logged {
        info!(
            warmup_start_ms,
            warmup_end_ms = now_millis(),
            warmup_actions_excluded,
            "signal_live warmup end"
        );
    }

    let exit_reason = exit_reason.unwrap_or(ExitReason::Timeout);
    if matches!(exit_reason, ExitReason::KillSwitch | ExitReason::Errors) {
        if let Some(active) = active_order.take() {
            let force_reason = if matches!(exit_reason, ExitReason::KillSwitch) {
                "kill"
            } else {
                "error"
            };
            let force_cancel_decision_instant = Instant::now();
            let force_effective_rest_ms =
                effective_rest_ms(force_cancel_decision_instant, active.place_ack_instant);
            match executor
                .cancel_order_with_metrics(&symbol, &active.client_order_id)
                .await
            {
                Ok(outcome) => {
                    let elapsed_now = run_started.elapsed();
                    let in_warmup_now = elapsed_now < warmup_duration;
                    let minute_index_now = if in_warmup_now {
                        None
                    } else {
                        Some(elapsed_now.saturating_sub(warmup_duration).as_secs() / 60)
                    };
                    let force_cancel_ts_ms = now_millis();
                    let time_in_trade_ms =
                        force_cancel_ts_ms.saturating_sub(active.entered_at_ts_ms);

                    if in_warmup_now {
                        warmup_actions_excluded = warmup_actions_excluded.saturating_add(1);
                    } else {
                        counters.cancels = counters.cancels.saturating_add(1);
                        if force_reason == "kill" {
                            cancels_by_reason.kill = cancels_by_reason.kill.saturating_add(1);
                        } else {
                            cancels_by_reason.error = cancels_by_reason.error.saturating_add(1);
                        }
                        if outcome.cancel_to_ack_us.is_some() {
                            event_counters.cancel_ack = event_counters.cancel_ack.saturating_add(1);
                        }
                        if let Some(force_effective_rest_ms) = force_effective_rest_ms {
                            effective_rest_ms_samples.push(force_effective_rest_ms);
                        }
                        cancel_rtt_us_excluding_warmup.push(outcome.cancel_http_us);
                        if let Some(minute_index) = minute_index_now {
                            let bucket = minute_bucket_mut(&mut minute_buckets, minute_index);
                            bucket.cancels = bucket.cancels.saturating_add(1);
                            bucket.time_in_trade_ms_sum =
                                bucket.time_in_trade_ms_sum.saturating_add(time_in_trade_ms);
                            bucket.time_in_trade_samples =
                                bucket.time_in_trade_samples.saturating_add(1);
                            if let Some(force_effective_rest_ms) = force_effective_rest_ms {
                                bucket.effective_rest_ms_sum = bucket
                                    .effective_rest_ms_sum
                                    .saturating_add(u128::from(force_effective_rest_ms));
                                bucket.effective_rest_samples =
                                    bucket.effective_rest_samples.saturating_add(1);
                            }
                            bucket.cancel_rtt_us.push(outcome.cancel_http_us);
                        }
                    }
                    info!(
                        action = "cancel",
                        reason = force_reason,
                        direction = ?active.direction,
                        client_order_id = %active.client_order_id,
                        cancel_rtt_us = outcome.cancel_http_us,
                        "forced cancel on exit"
                    );
                }
                Err(error) => {
                    let meta = error_meta(&error);
                    counters.errors = counters.errors.saturating_add(1);
                    *error_reasons
                        .entry(format!("force_cancel_{}", meta.kind))
                        .or_insert(0) += 1;
                    if let Some(reason) =
                        explicit_rest_error_reason(meta.http_status, meta.binance_code)
                    {
                        *error_reasons
                            .entry(format!("force_cancel_{reason}"))
                            .or_insert(0) += 1;
                    }
                    if matches!(transport, ExecutionTransport::Rest)
                        && (meta.kind == "api_error" || meta.kind == "http_error")
                    {
                        last_http_status = meta.http_status;
                        last_binance_code = meta.binance_code;
                        last_error_body_snippet = meta
                            .body_snippet
                            .clone()
                            .or_else(|| Some(meta.message.chars().take(300).collect()));
                        last_path = meta.path.clone();
                        warn!(
                            transport = transport.as_str(),
                            request_id = ?meta.request_id,
                            retry_count = meta.retry_count,
                            path = ?meta.path.as_deref(),
                            http_status = ?meta.http_status,
                            binance_code = ?meta.binance_code,
                            error_reason =
                                ?explicit_rest_error_reason(meta.http_status, meta.binance_code),
                            last_error_body_snippet = ?meta.body_snippet.as_deref(),
                            "signal_live rest http error"
                        );
                    }
                    error!(
                        action = "cancel",
                        reason = force_reason,
                        direction = ?active.direction,
                        client_order_id = %active.client_order_id,
                        http_status = ?meta.http_status,
                        binance_code = ?meta.binance_code,
                        error_kind = %meta.kind,
                        error = %meta.message,
                        "forced cancel failed"
                    );
                }
            }
        }
    }
    let actual_elapsed_s = run_started.elapsed().as_secs_f64();
    let place_rtt_us_quantiles = quantiles(&mut place_rtt_us_excluding_warmup);
    let cancel_rtt_us_quantiles = quantiles(&mut cancel_rtt_us_excluding_warmup);
    let ack_latency_ms_quantiles = quantiles(&mut ack_latency_ms_samples);
    let effective_rest_ms_quantiles = quantiles(&mut effective_rest_ms_samples);
    let time_to_fill_ms_quantiles = quantiles(&mut time_to_fill_ms_samples);
    let avg_time_to_fill_ms = if counters.fills == 0 {
        None
    } else {
        Some(time_to_fill_ms_sum as f64 / counters.fills as f64)
    };
    let avg_rest_ms = if neutral_rest_samples == 0 {
        None
    } else {
        Some(neutral_rest_ms_sum as f64 / neutral_rest_samples as f64)
    };
    let distance_to_best_bps_quantiles = quantiles_f64(&mut distance_to_best_bps_samples);
    let adverse_bps_1s_quantiles = quantiles_f64(&mut adverse_bps_1s_samples);
    let adverse_bps_5s_quantiles = quantiles_f64(&mut adverse_bps_5s_samples);
    let mid_move_bps_at_cancel_quantiles = quantiles_f64(&mut mid_move_bps_at_cancel_samples);

    let finished_at_ms = now_millis();
    let summary = LiveRunSummary {
        started_at_ms,
        finished_at_ms,
        requested_run_for_s,
        actual_elapsed_s,
        exit_reason,
        warmup_s,
        warmup_actions_excluded,
        min_rest_ms: args.min_rest_ms,
        adverse_mid_move_bps: args.adverse_mid_move_bps,
        neutral_cancel_delayed_count,
        effective_config: SignalLiveConfig {
            symbol: symbol.clone(),
            qty,
            transport: transport.as_str().to_string(),
            discount_bps: args.discount_bps,
            min_rest_ms: args.min_rest_ms,
            adverse_mid_move_bps: args.adverse_mid_move_bps,
            thresholds: SignalThresholdsConfig {
                imbalance_enter: args.imbalance_enter,
                tilt_enter_bps: args.tilt_enter_bps,
                imbalance_exit: args.imbalance_exit,
                tilt_exit_bps: args.tilt_exit_bps,
                cooldown_ms: args.cooldown_ms,
            },
            max_actions_per_minute: args.max_actions_per_minute,
            max_consecutive_errors: args.max_consecutive_errors,
            latency_spike_ms: args.latency_spike_ms,
            max_latency_spikes: args.max_latency_spikes,
            rest_url: cfg.execution.rest_url.clone(),
            ws_api_url: cfg.execution.ws_api_url.clone(),
        },
        event_counters: event_counters.clone(),
        counters: counters.clone(),
        place_rtt_us: place_rtt_us_quantiles,
        cancel_rtt_us: cancel_rtt_us_quantiles,
        ack_latency_ms: ack_latency_ms_quantiles,
        effective_rest_ms: effective_rest_ms_quantiles,
        place_rtt_us_excluding_warmup: place_rtt_us_quantiles,
        cancel_rtt_us_excluding_warmup: cancel_rtt_us_quantiles,
        avg_time_to_fill_ms,
        avg_rest_ms,
        time_to_fill_ms: time_to_fill_ms_quantiles,
        adverse_bps_1s: adverse_bps_1s_quantiles,
        adverse_bps_5s: adverse_bps_5s_quantiles,
        distance_to_best_bps: distance_to_best_bps_quantiles,
        mid_move_bps_at_cancel: mid_move_bps_at_cancel_quantiles,
        cancels_by_reason: cancels_by_reason.clone(),
        latency_spike_stats: spike_tracker.summary(),
        last_http_status,
        last_binance_code,
        last_error_body_snippet,
        last_path,
        minute_buckets: finalize_minute_buckets(minute_buckets),
        state_transitions,
        error_reasons,
        stopped_by_kill_switch: matches!(exit_reason, ExitReason::KillSwitch),
        kill_switch_reason: kill_switch_reason.clone(),
    };

    write_summary(&out_path, &summary)?;

    info!(
        triggers_long = counters.triggers_long,
        triggers_short = counters.triggers_short,
        entries = counters.entries,
        cancels = counters.cancels,
        fills = counters.fills,
        partial_fills = counters.partial_fills,
        filled_qty_total = counters.filled_qty_total,
        order_ack_events = event_counters.order_ack,
        cancel_ack_events = event_counters.cancel_ack,
        trade_fill_events = event_counters.trade_fill,
        min_rest_ms = args.min_rest_ms,
        adverse_mid_move_bps = args.adverse_mid_move_bps,
        neutral_cancel_delayed_count,
        avg_rest_ms = ?avg_rest_ms,
        cancels_neutral = cancels_by_reason.neutral,
        cancels_adverse = cancels_by_reason.adverse,
        cancels_kill = cancels_by_reason.kill,
        cancels_error = cancels_by_reason.error,
        cooldown_skips = counters.cooldown_skips,
        rearm_wait_skips = counters.rearm_wait_skips,
        errors = counters.errors,
        action_rate_limit_skips = counters.action_rate_limit_skips,
        minute_buckets = summary.minute_buckets.len(),
        state_transitions = summary.state_transitions.len(),
        requested_run_for_s = summary.requested_run_for_s,
        warmup_s = summary.warmup_s,
        warmup_actions_excluded = summary.warmup_actions_excluded,
        actual_elapsed_s = summary.actual_elapsed_s,
        exit_reason = ?summary.exit_reason,
        stopped_by_kill_switch = summary.stopped_by_kill_switch,
        kill_switch_reason = ?kill_switch_reason,
        last_http_status = ?summary.last_http_status,
        last_binance_code = ?summary.last_binance_code,
        last_path = ?summary.last_path,
        out_json = %out_path.display(),
        "signal_live finished"
    );

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn spike_tracker_converts_ms_threshold_to_us() {
        let mut tracker = SpikeTracker::new(750, 0, 8);
        assert_eq!(tracker.threshold_us(), 750_000);

        let update = tracker.record_rtt(321_921);
        assert!(!update.is_spike);
        assert_eq!(update.streak, 0);

        let update = tracker.record_rtt(1_809_365);
        assert!(update.is_spike);
        assert_eq!(update.streak, 1);
    }

    #[test]
    fn spike_tracker_resets_streak_when_rtt_drops_below_threshold() {
        let mut tracker = SpikeTracker::new(750, 0, 8);

        let update = tracker.record_rtt(1_000_000);
        assert!(update.is_spike);
        assert_eq!(update.streak, 1);

        let update = tracker.record_rtt(350_000);
        assert!(!update.is_spike);
        assert_eq!(update.streak, 0);
        assert_eq!(tracker.spike_streak(), 0);
    }

    #[test]
    fn spike_tracker_ignores_warmup_actions() {
        let mut tracker = SpikeTracker::new(750, 2, 8);

        let update = tracker.record_rtt(1_200_000);
        assert!(!update.is_spike);
        assert_eq!(tracker.spike_streak(), 0);

        let update = tracker.record_rtt(1_300_000);
        assert!(!update.is_spike);
        assert_eq!(tracker.spike_streak(), 0);

        let update = tracker.record_rtt(1_400_000);
        assert!(update.is_spike);
        assert_eq!(update.streak, 1);
        assert_eq!(tracker.spike_streak(), 1);
    }

    #[test]
    fn streak_must_exceed_limit_to_trigger_kill_switch() {
        let mut tracker = SpikeTracker::new(750, 0, 8);
        let limit = 3u32;

        for _ in 0..limit {
            let update = tracker.record_rtt(900_000);
            assert!(update.is_spike);
        }

        assert_eq!(tracker.spike_streak(), limit);
        assert!(tracker.spike_streak() <= limit);

        let update = tracker.record_rtt(950_000);
        assert!(update.is_spike);
        assert_eq!(tracker.spike_streak(), limit + 1);
        assert!(tracker.spike_streak() > limit);
    }

    #[test]
    fn adverse_cancel_requires_mid_move_threshold() {
        assert!(!adverse_cancel_allowed(Some(4.9), 5.0));
        assert!(!adverse_cancel_allowed(Some(-4.9), 5.0));
        assert!(adverse_cancel_allowed(Some(5.0), 5.0));
        assert!(adverse_cancel_allowed(Some(-5.1), 5.0));
        assert!(!adverse_cancel_allowed(None, 5.0));
    }

    #[test]
    fn mid_move_bps_uses_entry_mid_as_baseline() {
        let up = mid_move_bps(100.0, 101.0).expect("mid move should be computed");
        let down = mid_move_bps(100.0, 99.5).expect("mid move should be computed");
        assert!((up - 100.0).abs() < f64::EPSILON);
        assert!((down + 50.0).abs() < f64::EPSILON);
    }

    #[test]
    fn effective_rest_is_non_negative() {
        let now = Instant::now();
        let later = now + Duration::from_millis(25);
        assert_eq!(effective_rest_ms(later, Some(now)), Some(25));
        assert_eq!(effective_rest_ms(now, Some(later)), Some(0));
    }

    #[test]
    fn ack_latency_is_non_negative() {
        let now = Instant::now();
        let later = now + Duration::from_millis(15);
        assert_eq!(ack_latency_ms(now, Some(later)), Some(15));
        assert_eq!(ack_latency_ms(later, Some(now)), Some(0));
        assert_eq!(ack_latency_ms(now, None), None);
    }
}
