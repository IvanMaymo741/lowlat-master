use anyhow::{anyhow, bail, Context, Result};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use std::fs::File;
use std::io::{BufRead, BufReader, Read};

use crate::marketdata::MarketEvent;
use crate::orderbook::OrderBook;
use crate::strategy::{
    classify_trigger, compute_signal_snapshot, RunnerAction, RunnerPhase, SignalDirection,
    SignalRunner, SignalRunnerState, SignalThresholds,
};

const DEFAULT_QTY: f64 = 1.0;
const EPS: f64 = 1e-12;

#[derive(Debug, Clone, Serialize)]
pub struct RecordedQuote {
    pub symbol: String,
    pub bid: f64,
    pub ask: f64,
    pub bid_qty: f64,
    pub ask_qty: f64,
    pub ts_ms: u128,
}

#[derive(Debug, Deserialize)]
struct NdjsonRecordedQuote {
    symbol: String,
    bid: f64,
    ask: f64,
    #[serde(default = "default_qty")]
    bid_qty: f64,
    #[serde(default = "default_qty")]
    ask_qty: f64,
    ts_ms: u128,
}

fn default_qty() -> f64 {
    DEFAULT_QTY
}

#[derive(Debug, Clone, Serialize)]
pub struct LoadedRecording {
    pub quotes: Vec<RecordedQuote>,
    pub rows_total: usize,
    pub rows_parsed: usize,
    pub parse_errors: usize,
}

#[derive(Debug, Clone, Serialize)]
pub struct BacktestStrategyParams {
    pub imbalance_enter: f64,
    pub tilt_enter_bps: f64,
    pub imbalance_exit: f64,
    pub tilt_exit_bps: f64,
    pub cooldown_ms: u64,
    pub rearm: bool,
    pub discount_bps: f64,
    pub min_rest_ms: u64,
    pub adverse_mid_move_bps: f64,
}

impl Default for BacktestStrategyParams {
    fn default() -> Self {
        Self {
            imbalance_enter: 0.20,
            tilt_enter_bps: 0.5,
            imbalance_exit: 0.10,
            tilt_exit_bps: 0.2,
            cooldown_ms: 1_500,
            rearm: true,
            discount_bps: 1_000.0,
            min_rest_ms: 0,
            adverse_mid_move_bps: 5.0,
        }
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct BacktestExecutionModelParams {
    pub qty: f64,
    pub ack_delay_ms: u64,
    pub cancel_delay_ms: u64,
    pub fees_bps: f64,
    pub slippage_bps: f64,
}

impl Default for BacktestExecutionModelParams {
    fn default() -> Self {
        Self {
            qty: 1.0,
            ack_delay_ms: 0,
            cancel_delay_ms: 0,
            fees_bps: 1.0,
            slippage_bps: 0.0,
        }
    }
}

#[derive(Debug, Clone, Serialize, Default)]
pub struct BacktestParams {
    pub strategy: BacktestStrategyParams,
    pub execution: BacktestExecutionModelParams,
}

impl BacktestParams {
    pub fn validate(&self) -> Result<()> {
        if self.strategy.imbalance_enter <= 0.0 || self.strategy.tilt_enter_bps <= 0.0 {
            bail!("entry thresholds must be > 0");
        }
        if self.strategy.imbalance_exit <= 0.0 || self.strategy.tilt_exit_bps <= 0.0 {
            bail!("exit thresholds must be > 0");
        }
        if self.strategy.cooldown_ms == 0 {
            bail!("cooldown_ms must be > 0");
        }
        if self.strategy.discount_bps <= 0.0 {
            bail!("discount_bps must be > 0");
        }
        if self.strategy.adverse_mid_move_bps <= 0.0 {
            bail!("adverse_mid_move_bps must be > 0");
        }
        if self.execution.qty <= 0.0 {
            bail!("qty must be > 0");
        }
        if self.execution.fees_bps < 0.0 {
            bail!("fees_bps must be >= 0");
        }
        if self.execution.slippage_bps < 0.0 {
            bail!("slippage_bps must be >= 0");
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, Serialize)]
pub struct U64Quantiles {
    pub samples: usize,
    pub p50: u64,
    pub p95: u64,
    pub p99: u64,
}

#[derive(Debug, Clone, Copy, Serialize)]
pub struct F64Quantiles {
    pub samples: usize,
    pub p50: f64,
    pub p95: f64,
    pub p99: f64,
}

#[derive(Debug, Clone, Serialize)]
pub struct BacktestPnlSummary {
    pub realized_pnl: f64,
    pub unrealized_pnl: f64,
    pub total_pnl: f64,
    pub pnl_bps_on_notional: f64,
}

#[derive(Debug, Clone, Serialize)]
pub struct BacktestMinuteBucket {
    pub minute_index: u64,
    pub pnl: f64,
    pub fills: u64,
    pub inventory: f64,
    pub drawdown: f64,
}

#[derive(Debug, Clone, Serialize)]
pub struct BacktestSummary {
    pub entries: u64,
    pub cancels: u64,
    pub fills: u64,
    pub fill_rate: f64,
    pub avg_time_to_fill_ms: Option<f64>,
    pub ack_latency_ms: Option<U64Quantiles>,
    pub effective_rest_ms: Option<U64Quantiles>,
    pub pnl: BacktestPnlSummary,
    pub fees_paid: f64,
    pub slippage_cost: f64,
    pub adverse_selection_bps_1s: Option<F64Quantiles>,
    pub adverse_selection_bps_5s: Option<F64Quantiles>,
    pub minute_buckets: Vec<BacktestMinuteBucket>,
    pub triggers_long: u64,
    pub triggers_short: u64,
    pub cooldown_skips: u64,
    pub rearm_wait_skips: u64,
}

#[derive(Debug, Clone)]
struct ActiveOrder {
    direction: SignalDirection,
    order_price: f64,
    qty: f64,
    entry_decision_ts_ms: u128,
    entry_mid: f64,
    ack_ts_ms: u128,
    cancel_ack_ts_ms: Option<u128>,
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

#[derive(Debug, Default)]
struct PortfolioState {
    inventory: f64,
    avg_entry_price: f64,
    realized_pnl: f64,
    fees_paid: f64,
    slippage_cost: f64,
    traded_notional: f64,
}

impl PortfolioState {
    fn apply_fill(
        &mut self,
        direction: SignalDirection,
        qty: f64,
        order_price: f64,
        slippage_bps: f64,
        fees_bps: f64,
    ) {
        let side_sign = match direction {
            SignalDirection::Long => 1.0,
            SignalDirection::Short => -1.0,
        };
        let exec_price = execution_price(order_price, direction, slippage_bps);
        let signed_qty = side_sign * qty;

        let fee = (exec_price * qty).abs() * (fees_bps / 10_000.0);
        self.fees_paid += fee;
        self.realized_pnl -= fee;

        let slippage = (exec_price - order_price).abs() * qty;
        self.slippage_cost += slippage;
        self.traded_notional += (exec_price * qty).abs();

        let current_inventory = self.inventory;
        if current_inventory.abs() < EPS || current_inventory.signum() == signed_qty.signum() {
            let new_inventory = current_inventory + signed_qty;
            let current_qty_abs = current_inventory.abs();
            let new_qty_abs = new_inventory.abs();
            self.avg_entry_price = if new_qty_abs < EPS {
                0.0
            } else {
                ((self.avg_entry_price * current_qty_abs) + (exec_price * qty)) / new_qty_abs
            };
            self.inventory = new_inventory;
            return;
        }

        let closing_qty = current_inventory.abs().min(qty);
        if current_inventory > 0.0 && signed_qty < 0.0 {
            self.realized_pnl += (exec_price - self.avg_entry_price) * closing_qty;
        } else if current_inventory < 0.0 && signed_qty > 0.0 {
            self.realized_pnl += (self.avg_entry_price - exec_price) * closing_qty;
        }

        let remaining_qty = qty - closing_qty;
        if remaining_qty <= EPS {
            self.inventory = current_inventory + signed_qty;
            if self.inventory.abs() < EPS {
                self.inventory = 0.0;
                self.avg_entry_price = 0.0;
            }
            return;
        }

        self.inventory = signed_qty.signum() * remaining_qty;
        self.avg_entry_price = exec_price;
    }

    fn unrealized_pnl(&self, mid: f64) -> f64 {
        if self.inventory.abs() < EPS || !mid.is_finite() || mid <= 0.0 {
            return 0.0;
        }
        (mid - self.avg_entry_price) * self.inventory
    }
}

#[derive(Debug, Default)]
struct MinuteBucketAccumulator {
    fills: u64,
    start_total_pnl: Option<f64>,
    end_total_pnl: f64,
    end_inventory: f64,
    max_drawdown: f64,
}

impl MinuteBucketAccumulator {
    fn observe(&mut self, total_pnl: f64, inventory: f64, drawdown: f64) {
        if self.start_total_pnl.is_none() {
            self.start_total_pnl = Some(total_pnl);
        }
        self.end_total_pnl = total_pnl;
        self.end_inventory = inventory;
        if drawdown > self.max_drawdown {
            self.max_drawdown = drawdown;
        }
    }
}

pub fn load_recording(path: &std::path::Path) -> Result<LoadedRecording> {
    let file = File::open(path).with_context(|| format!("failed to open {}", path.display()))?;
    let reader = BufReader::new(file);

    let mut rows_total = 0usize;
    let mut rows_parsed = 0usize;
    let mut parse_errors = 0usize;
    let mut quotes = Vec::new();

    for line in reader.lines() {
        rows_total = rows_total.saturating_add(1);
        let line = line.with_context(|| format!("failed to read line {}", rows_total))?;
        match parse_recorded_quote_line(&line) {
            Ok(Some(quote)) => {
                rows_parsed = rows_parsed.saturating_add(1);
                quotes.push(quote);
            }
            Ok(None) => {}
            Err(_) => {
                parse_errors = parse_errors.saturating_add(1);
            }
        }
    }

    if quotes.is_empty() {
        bail!("no valid quotes parsed from {}", path.display());
    }

    Ok(LoadedRecording {
        quotes,
        rows_total,
        rows_parsed,
        parse_errors,
    })
}

pub fn recording_sha256_hex(path: &std::path::Path) -> Result<String> {
    let mut file =
        File::open(path).with_context(|| format!("failed to open {}", path.display()))?;
    let mut hasher = Sha256::new();
    let mut buffer = [0u8; 8192];

    loop {
        let bytes = file
            .read(&mut buffer)
            .with_context(|| format!("failed reading {}", path.display()))?;
        if bytes == 0 {
            break;
        }
        hasher.update(&buffer[..bytes]);
    }

    let digest = hasher.finalize();
    Ok(format!("{digest:x}"))
}

pub fn run_backtest(quotes: &[RecordedQuote], params: &BacktestParams) -> Result<BacktestSummary> {
    params.validate()?;
    if quotes.is_empty() {
        bail!("quotes cannot be empty");
    }

    let thresholds = SignalThresholds {
        imbalance_enter: params.strategy.imbalance_enter,
        tilt_enter_bps: params.strategy.tilt_enter_bps,
        imbalance_exit: params.strategy.imbalance_exit,
        tilt_exit_bps: params.strategy.tilt_exit_bps,
        cooldown_ms: params.strategy.cooldown_ms,
    };
    let mut runner = SignalRunner::new(thresholds);
    let mut orderbook = OrderBook::new(quotes[0].symbol.clone());

    let run_start_ts_ms = quotes[0].ts_ms;
    let mut portfolio = PortfolioState::default();
    let mut active_order: Option<ActiveOrder> = None;
    let mut pending_adverse_samples: Vec<PendingAdverseSample> = Vec::new();

    let mut entries = 0u64;
    let mut cancels = 0u64;
    let mut fills = 0u64;
    let mut fill_time_ms_sum = 0u128;
    let mut fill_time_samples = 0u64;
    let mut triggers_long = 0u64;
    let mut triggers_short = 0u64;
    let mut cooldown_skips = 0u64;
    let mut rearm_wait_skips = 0u64;

    let mut ack_latency_ms_values: Vec<u64> = Vec::new();
    let mut effective_rest_ms_values: Vec<u64> = Vec::new();
    let mut adverse_bps_1s_values: Vec<f64> = Vec::new();
    let mut adverse_bps_5s_values: Vec<f64> = Vec::new();

    let mut minute_buckets: BTreeMap<u64, MinuteBucketAccumulator> = BTreeMap::new();
    let mut peak_total_pnl = 0.0f64;
    let mut last_mid: Option<f64> = None;

    for quote in quotes {
        let mut event = MarketEvent::new(quote.symbol.clone(), quote.bid, quote.ask, quote.ts_ms);
        event.bid_qty = quote.bid_qty;
        event.ask_qty = quote.ask_qty;
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

        last_mid = Some(snapshot.mid);
        update_pending_adverse_samples(
            &mut pending_adverse_samples,
            snapshot.ts_ms,
            snapshot.mid,
            &mut adverse_bps_1s_values,
            &mut adverse_bps_5s_values,
        );

        let mut filled_this_tick = false;
        if let Some(active) = active_order.as_ref() {
            let canceled_ack = active
                .cancel_ack_ts_ms
                .map(|cancel_ack_ts| snapshot.ts_ms >= cancel_ack_ts)
                .unwrap_or(false);
            if canceled_ack {
                active_order = None;
            } else {
                let order_live = snapshot.ts_ms >= active.ack_ts_ms
                    && active
                        .cancel_ack_ts_ms
                        .map(|cancel_ack_ts| snapshot.ts_ms < cancel_ack_ts)
                        .unwrap_or(true);

                if order_live
                    && maker_fill_hit(
                        active.direction,
                        active.order_price,
                        orderbook.best_bid,
                        orderbook.best_ask,
                    )
                {
                    let fill_ts_ms = snapshot.ts_ms;
                    let fill_time_ms = fill_ts_ms.saturating_sub(active.entry_decision_ts_ms);
                    fill_time_ms_sum = fill_time_ms_sum.saturating_add(fill_time_ms);
                    fill_time_samples = fill_time_samples.saturating_add(1);

                    fills = fills.saturating_add(1);
                    let minute_index = minute_index(run_start_ts_ms, snapshot.ts_ms);
                    let bucket = minute_buckets.entry(minute_index).or_default();
                    bucket.fills = bucket.fills.saturating_add(1);

                    portfolio.apply_fill(
                        active.direction,
                        active.qty,
                        active.order_price,
                        params.execution.slippage_bps,
                        params.execution.fees_bps,
                    );

                    pending_adverse_samples.push(PendingAdverseSample::new(
                        active.direction,
                        fill_ts_ms,
                        snapshot.mid,
                    ));

                    let next_phase = if params.strategy.rearm {
                        RunnerPhase::WaitRearm
                    } else {
                        RunnerPhase::Armed
                    };
                    runner.restore_state(SignalRunnerState {
                        last_trade_ts_ms: Some(snapshot.ts_ms),
                        active_direction: None,
                        phase: next_phase,
                    });

                    active_order = None;
                    filled_this_tick = true;
                }
            }
        }

        if !filled_this_tick {
            match classify_trigger(&snapshot, runner.thresholds()) {
                Some(SignalDirection::Long) => triggers_long = triggers_long.saturating_add(1),
                Some(SignalDirection::Short) => triggers_short = triggers_short.saturating_add(1),
                None => {}
            }

            let prev_state = runner.state();
            let decision = runner.on_snapshot(&snapshot);
            match decision.action {
                RunnerAction::Enter(direction) => {
                    if active_order.is_some() {
                        runner.restore_state(prev_state);
                    } else if let Some(order_price) =
                        maker_order_price(snapshot.mid, params.strategy.discount_bps, direction)
                    {
                        let entry_decision_ts_ms = snapshot.ts_ms;
                        let ack_ts_ms = entry_decision_ts_ms
                            .saturating_add(u128::from(params.execution.ack_delay_ms));
                        let ack_latency_ms = ack_ts_ms
                            .saturating_sub(entry_decision_ts_ms)
                            .min(u128::from(u64::MAX))
                            as u64;
                        ack_latency_ms_values.push(ack_latency_ms);

                        active_order = Some(ActiveOrder {
                            direction,
                            order_price,
                            qty: params.execution.qty,
                            entry_decision_ts_ms,
                            entry_mid: snapshot.mid,
                            ack_ts_ms,
                            cancel_ack_ts_ms: None,
                        });
                        entries = entries.saturating_add(1);
                    } else {
                        runner.restore_state(prev_state);
                    }
                }
                RunnerAction::CancelNeutral(direction) | RunnerAction::CancelAdverse(direction) => {
                    let is_neutral = matches!(decision.action, RunnerAction::CancelNeutral(_));
                    let Some(active) = active_order.clone() else {
                        runner.restore_state(prev_state);
                        continue;
                    };
                    if active.direction != direction {
                        runner.restore_state(prev_state);
                        continue;
                    }

                    let mid_move_bps =
                        ((snapshot.mid - active.entry_mid) / active.entry_mid) * 10_000.0;
                    if !is_neutral && mid_move_bps.abs() < params.strategy.adverse_mid_move_bps {
                        runner.restore_state(prev_state);
                        continue;
                    }

                    let rest_elapsed_ms =
                        snapshot.ts_ms.saturating_sub(active.entry_decision_ts_ms);
                    if is_neutral && rest_elapsed_ms < u128::from(params.strategy.min_rest_ms) {
                        runner.restore_state(prev_state);
                        continue;
                    }

                    let cancel_decision_ts_ms = snapshot.ts_ms;
                    let effective_rest = cancel_decision_ts_ms
                        .saturating_sub(active.ack_ts_ms)
                        .min(u128::from(u64::MAX)) as u64;
                    effective_rest_ms_values.push(effective_rest);

                    let cancel_ack_ts_ms = cancel_decision_ts_ms
                        .saturating_add(u128::from(params.execution.cancel_delay_ms));
                    cancels = cancels.saturating_add(1);

                    if params.execution.cancel_delay_ms == 0 {
                        active_order = None;
                    } else if let Some(order) = active_order.as_mut() {
                        order.cancel_ack_ts_ms = Some(cancel_ack_ts_ms);
                    }

                    if !params.strategy.rearm {
                        let mut state = runner.state();
                        state.phase = RunnerPhase::Armed;
                        runner.restore_state(state);
                    }
                }
                RunnerAction::CooldownSkip(_) => {
                    cooldown_skips = cooldown_skips.saturating_add(1);
                }
                RunnerAction::RearmWaitSkip(_) => {
                    rearm_wait_skips = rearm_wait_skips.saturating_add(1);
                }
                RunnerAction::None => {}
            }
        }

        let unrealized = portfolio.unrealized_pnl(snapshot.mid);
        let total_pnl = portfolio.realized_pnl + unrealized;
        if total_pnl > peak_total_pnl {
            peak_total_pnl = total_pnl;
        }
        let drawdown = (peak_total_pnl - total_pnl).max(0.0);

        let bucket = minute_buckets
            .entry(minute_index(run_start_ts_ms, snapshot.ts_ms))
            .or_default();
        bucket.observe(total_pnl, portfolio.inventory, drawdown);
    }

    let final_mid = last_mid.unwrap_or(0.0);
    let unrealized_pnl = portfolio.unrealized_pnl(final_mid);
    let total_pnl = portfolio.realized_pnl + unrealized_pnl;
    let pnl_bps_on_notional = if portfolio.traded_notional > 0.0 {
        (total_pnl / portfolio.traded_notional) * 10_000.0
    } else {
        0.0
    };

    let fill_rate = if entries == 0 {
        0.0
    } else {
        fills as f64 / entries as f64
    };
    let avg_time_to_fill_ms = if fill_time_samples == 0 {
        None
    } else {
        Some(fill_time_ms_sum as f64 / fill_time_samples as f64)
    };

    let ack_latency_ms = quantiles_u64(&mut ack_latency_ms_values);
    let effective_rest_ms = quantiles_u64(&mut effective_rest_ms_values);
    let adverse_selection_bps_1s = quantiles_f64(&mut adverse_bps_1s_values);
    let adverse_selection_bps_5s = quantiles_f64(&mut adverse_bps_5s_values);

    let minute_buckets = finalize_minute_buckets(minute_buckets);

    Ok(BacktestSummary {
        entries,
        cancels,
        fills,
        fill_rate,
        avg_time_to_fill_ms,
        ack_latency_ms,
        effective_rest_ms,
        pnl: BacktestPnlSummary {
            realized_pnl: portfolio.realized_pnl,
            unrealized_pnl,
            total_pnl,
            pnl_bps_on_notional,
        },
        fees_paid: portfolio.fees_paid,
        slippage_cost: portfolio.slippage_cost,
        adverse_selection_bps_1s,
        adverse_selection_bps_5s,
        minute_buckets,
        triggers_long,
        triggers_short,
        cooldown_skips,
        rearm_wait_skips,
    })
}

pub fn sharpe_like_from_minute_buckets(buckets: &[BacktestMinuteBucket]) -> f64 {
    if buckets.len() < 2 {
        return 0.0;
    }
    let samples: Vec<f64> = buckets.iter().map(|bucket| bucket.pnl).collect();
    let mean = samples.iter().sum::<f64>() / samples.len() as f64;
    let variance = samples
        .iter()
        .map(|value| {
            let delta = *value - mean;
            delta * delta
        })
        .sum::<f64>()
        / samples.len() as f64;
    let stddev = variance.sqrt();
    if stddev <= EPS {
        0.0
    } else {
        (mean / stddev) * (samples.len() as f64).sqrt()
    }
}

fn parse_recorded_quote_line(line: &str) -> Result<Option<RecordedQuote>> {
    let trimmed = line.trim();
    if trimmed.is_empty() {
        return Ok(None);
    }

    if trimmed.starts_with('{') {
        let ndjson = serde_json::from_str::<NdjsonRecordedQuote>(trimmed)
            .with_context(|| "invalid ndjson marketdata line")?;
        return Ok(Some(RecordedQuote {
            symbol: ndjson.symbol,
            bid: ndjson.bid,
            ask: ndjson.ask,
            bid_qty: ndjson.bid_qty,
            ask_qty: ndjson.ask_qty,
            ts_ms: ndjson.ts_ms,
        }));
    }

    let cols: Vec<&str> = trimmed.split(',').collect();
    if cols.is_empty() {
        return Ok(None);
    }
    if cols[0].eq_ignore_ascii_case("symbol") {
        return Ok(None);
    }

    if cols.len() >= 11 {
        let symbol = cols[0].to_string();
        let bid = cols[1]
            .parse::<f64>()
            .with_context(|| format!("invalid bid '{}'", cols[1]))?;
        let ask = cols[2]
            .parse::<f64>()
            .with_context(|| format!("invalid ask '{}'", cols[2]))?;
        let bid_qty = cols[3]
            .parse::<f64>()
            .with_context(|| format!("invalid bid_qty '{}'", cols[3]))?;
        let ask_qty = cols[4]
            .parse::<f64>()
            .with_context(|| format!("invalid ask_qty '{}'", cols[4]))?;
        let ts_ms = cols[5]
            .parse::<u128>()
            .with_context(|| format!("invalid ts_ms '{}'", cols[5]))?;
        return Ok(Some(RecordedQuote {
            symbol,
            bid,
            ask,
            bid_qty,
            ask_qty,
            ts_ms,
        }));
    }

    if cols.len() == 9 {
        let symbol = cols[0].to_string();
        let bid = cols[1]
            .parse::<f64>()
            .with_context(|| format!("invalid bid '{}'", cols[1]))?;
        let ask = cols[2]
            .parse::<f64>()
            .with_context(|| format!("invalid ask '{}'", cols[2]))?;
        let ts_ms = cols[3]
            .parse::<u128>()
            .with_context(|| format!("invalid ts_ms '{}'", cols[3]))?;
        return Ok(Some(RecordedQuote {
            symbol,
            bid,
            ask,
            bid_qty: 1.0,
            ask_qty: 1.0,
            ts_ms,
        }));
    }

    Err(anyhow!(
        "unsupported recording line format with {} columns",
        cols.len()
    ))
}

fn maker_order_price(mid: f64, discount_bps: f64, direction: SignalDirection) -> Option<f64> {
    if !mid.is_finite() || mid <= 0.0 || !discount_bps.is_finite() || discount_bps <= 0.0 {
        return None;
    }
    let factor = match direction {
        SignalDirection::Long => 1.0 - discount_bps / 10_000.0,
        SignalDirection::Short => 1.0 + discount_bps / 10_000.0,
    };
    let price = mid * factor;
    if !price.is_finite() || price <= 0.0 {
        return None;
    }
    Some(price)
}

fn execution_price(order_price: f64, direction: SignalDirection, slippage_bps: f64) -> f64 {
    let factor = match direction {
        SignalDirection::Long => 1.0 + slippage_bps / 10_000.0,
        SignalDirection::Short => 1.0 - slippage_bps / 10_000.0,
    };
    let price = order_price * factor;
    if price.is_finite() && price > 0.0 {
        price
    } else {
        order_price
    }
}

fn maker_fill_hit(
    direction: SignalDirection,
    order_price: f64,
    best_bid: f64,
    best_ask: f64,
) -> bool {
    match direction {
        SignalDirection::Long => best_ask <= order_price,
        SignalDirection::Short => best_bid >= order_price,
    }
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

fn minute_index(start_ts_ms: u128, current_ts_ms: u128) -> u64 {
    current_ts_ms
        .saturating_sub(start_ts_ms)
        .saturating_div(60_000)
        .min(u128::from(u64::MAX)) as u64
}

fn finalize_minute_buckets(
    minute_buckets: BTreeMap<u64, MinuteBucketAccumulator>,
) -> Vec<BacktestMinuteBucket> {
    minute_buckets
        .into_iter()
        .map(|(minute_index, bucket)| {
            let start = bucket.start_total_pnl.unwrap_or(bucket.end_total_pnl);
            BacktestMinuteBucket {
                minute_index,
                pnl: bucket.end_total_pnl - start,
                fills: bucket.fills,
                inventory: bucket.end_inventory,
                drawdown: bucket.max_drawdown,
            }
        })
        .collect()
}

fn quantiles_u64(values: &mut Vec<u64>) -> Option<U64Quantiles> {
    if values.is_empty() {
        return None;
    }
    values.sort_unstable();
    let samples = values.len();
    let p50 = percentile_u64(values, 0.50);
    let p95 = percentile_u64(values, 0.95);
    let p99 = percentile_u64(values, 0.99);
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

fn percentile_u64(sorted_values: &[u64], pct: f64) -> u64 {
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

#[cfg(test)]
mod tests {
    use super::*;

    fn q(ts_ms: u128, bid: f64, ask: f64, bid_qty: f64, ask_qty: f64) -> RecordedQuote {
        RecordedQuote {
            symbol: "BTCUSDT".to_string(),
            bid,
            ask,
            bid_qty,
            ask_qty,
            ts_ms,
        }
    }

    #[test]
    fn maker_buy_fills_when_ask_crosses_order_price() {
        let quotes = vec![
            q(0, 100.0, 101.0, 10.0, 1.0),
            q(100, 100.0, 100.4, 10.0, 1.0),
        ];
        let mut params = BacktestParams::default();
        params.strategy.discount_bps = 0.1;
        params.execution.qty = 1.0;
        params.execution.fees_bps = 0.0;
        params.execution.slippage_bps = 0.0;

        let summary = run_backtest(&quotes, &params).expect("backtest should run");
        assert!(summary.entries >= 1);
        assert_eq!(summary.fills, 1);
        assert!(summary.fill_rate > 0.0);
    }

    #[test]
    fn pending_cancel_still_allows_fill_before_cancel_ack() {
        let quotes = vec![
            q(0, 100.0, 101.0, 10.0, 1.0),
            q(50, 100.0, 101.0, 1.0, 1.0),
            q(100, 100.0, 100.4, 1.0, 1.0),
            q(200, 100.0, 100.4, 1.0, 1.0),
        ];
        let mut params = BacktestParams::default();
        params.strategy.discount_bps = 0.1;
        params.strategy.min_rest_ms = 0;
        params.execution.cancel_delay_ms = 100;
        params.execution.fees_bps = 0.0;
        params.execution.slippage_bps = 0.0;

        let summary = run_backtest(&quotes, &params).expect("backtest should run");
        assert!(summary.cancels >= 1);
        assert_eq!(summary.fills, 1);
    }
}
