use anyhow::{anyhow, bail, Context, Result};
use serde::{Deserialize, Serialize};
use std::fs::{self, File};
use std::io::{BufRead, BufReader, BufWriter, Write};
use std::path::{Path, PathBuf};

use lowlat_bot::marketdata::MarketEvent;
use lowlat_bot::orderbook::OrderBook;
use lowlat_bot::strategy::{
    classify_trigger, compute_signal_snapshot, RunnerAction, RunnerTransition, SignalDirection,
    SignalRunner, SignalThresholds,
};
use lowlat_bot::telemetry;
use lowlat_bot::util::now_millis;

#[derive(Debug, Clone)]
struct ReplayArgs {
    input_path: PathBuf,
    out_json: Option<PathBuf>,
    imbalance_enter: f64,
    tilt_enter_bps: f64,
    imbalance_exit: f64,
    tilt_exit_bps: f64,
    cooldown_ms: u64,
}

impl Default for ReplayArgs {
    fn default() -> Self {
        Self {
            input_path: PathBuf::new(),
            out_json: None,
            imbalance_enter: 0.20,
            tilt_enter_bps: 0.5,
            imbalance_exit: 0.10,
            tilt_exit_bps: 0.2,
            cooldown_ms: 1_500,
        }
    }
}

#[derive(Debug, Clone)]
struct RecordedQuote {
    symbol: String,
    bid: f64,
    ask: f64,
    bid_qty: f64,
    ask_qty: f64,
    ts_ms: u128,
}

#[derive(Debug, Clone, Deserialize)]
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

#[derive(Debug, Clone, Copy, Serialize)]
struct F64Quantiles {
    samples: usize,
    p50: f64,
    p95: f64,
    p99: f64,
}

#[derive(Debug, Clone, Serialize)]
struct HistogramBucket {
    bucket: String,
    count: u64,
}

#[derive(Debug, Clone, Serialize)]
struct ReplaySummary {
    started_at_ms: u128,
    finished_at_ms: u128,
    input_path: String,
    rows_total: usize,
    rows_parsed: usize,
    parse_errors: usize,
    thresholds: ReplayThresholds,
    imbalance_stats: Option<F64Quantiles>,
    tilt_bps_stats: Option<F64Quantiles>,
    imbalance_histogram: Vec<HistogramBucket>,
    tilt_bps_histogram: Vec<HistogramBucket>,
    triggers_long: u64,
    triggers_short: u64,
    entries: u64,
    cancels: u64,
    cooldown_skips: u64,
    rearm_wait_skips: u64,
    state_transitions: Vec<RunnerTransition>,
}

#[derive(Debug, Clone, Serialize)]
struct ReplayThresholds {
    imbalance_enter: f64,
    tilt_enter_bps: f64,
    imbalance_exit: f64,
    tilt_exit_bps: f64,
    cooldown_ms: u64,
}

fn parse_args() -> Result<ReplayArgs> {
    let raw_args: Vec<String> = std::env::args().skip(1).collect();
    let mut args = ReplayArgs::default();

    let mut i = 0usize;
    let mut positional_input_set = false;
    while i < raw_args.len() {
        match raw_args[i].as_str() {
            "--input" => {
                i += 1;
                let Some(value) = raw_args.get(i) else {
                    bail!("missing value for --input");
                };
                args.input_path = PathBuf::from(value);
                positional_input_set = true;
            }
            "--out-json" => {
                i += 1;
                let Some(value) = raw_args.get(i) else {
                    bail!("missing value for --out-json");
                };
                args.out_json = Some(PathBuf::from(value));
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
            "--help" | "-h" => {
                print_usage();
                std::process::exit(0);
            }
            value if !value.starts_with('-') && !positional_input_set => {
                args.input_path = PathBuf::from(value);
                positional_input_set = true;
            }
            value => bail!("unknown argument '{}'", value),
        }
        i += 1;
    }

    if args.input_path.as_os_str().is_empty() {
        bail!("missing input file: use --input <path> or positional path");
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
  cargo run --bin signal_replay -- <recording.csv|recording.ndjson>
  cargo run --bin signal_replay -- --input data/md_ticks.csv --out-json runs/signal_replay.json --imbalance-enter 0.20 --tilt-enter-bps 0.5 --imbalance-exit 0.10 --tilt-exit-bps 0.2 --cooldown-ms 1500"
    );
}

fn default_out_path(started_at_ms: u128) -> PathBuf {
    PathBuf::from(format!("runs/signal_replay_{}.json", started_at_ms))
}

fn default_qty() -> f64 {
    1.0
}

fn parse_recorded_quote(line: &str) -> Result<Option<RecordedQuote>> {
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

    Err(anyhow!(
        "unsupported CSV format with {} columns",
        cols.len()
    ))
}

fn quantiles(values: &mut Vec<f64>) -> Option<F64Quantiles> {
    if values.is_empty() {
        return None;
    }
    values.sort_by(|a, b| a.total_cmp(b));
    let samples = values.len();
    let p50 = percentile(values, 0.50);
    let p95 = percentile(values, 0.95);
    let p99 = percentile(values, 0.99);
    values.clear();

    Some(F64Quantiles {
        samples,
        p50,
        p95,
        p99,
    })
}

fn percentile(sorted_values: &[f64], pct: f64) -> f64 {
    if sorted_values.is_empty() {
        return 0.0;
    }
    let rank = ((sorted_values.len() as f64 - 1.0) * pct).round() as usize;
    sorted_values[rank.min(sorted_values.len() - 1)]
}

fn build_histogram(values: &[f64], edges: &[f64], unit: &str) -> Vec<HistogramBucket> {
    if edges.len() < 2 {
        return Vec::new();
    }

    let mut buckets = vec![0u64; edges.len() + 1];
    for value in values {
        let mut placed = false;
        for (idx, edge) in edges.iter().enumerate() {
            if *value < *edge {
                buckets[idx] = buckets[idx].saturating_add(1);
                placed = true;
                break;
            }
        }
        if !placed {
            let last = buckets.len() - 1;
            buckets[last] = buckets[last].saturating_add(1);
        }
    }

    let mut out = Vec::with_capacity(buckets.len());
    out.push(HistogramBucket {
        bucket: format!("< {:.4}{}", edges[0], unit),
        count: buckets[0],
    });
    for idx in 0..(edges.len() - 1) {
        out.push(HistogramBucket {
            bucket: format!("[{:.4}{}, {:.4}{})", edges[idx], unit, edges[idx + 1], unit),
            count: buckets[idx + 1],
        });
    }
    out.push(HistogramBucket {
        bucket: format!(">= {:.4}{}", edges[edges.len() - 1], unit),
        count: buckets[buckets.len() - 1],
    });
    out
}

fn write_summary(path: &Path, summary: &ReplaySummary) -> Result<()> {
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

#[tokio::main]
async fn main() -> Result<()> {
    let args = parse_args()?;
    telemetry::init("signal_replay");

    let started_at_ms = now_millis();
    let out_path = args
        .out_json
        .clone()
        .unwrap_or_else(|| default_out_path(started_at_ms));

    let input_file = File::open(&args.input_path)
        .with_context(|| format!("failed to open input {}", args.input_path.display()))?;
    let reader = BufReader::new(input_file);

    let thresholds = SignalThresholds {
        imbalance_enter: args.imbalance_enter,
        tilt_enter_bps: args.tilt_enter_bps,
        imbalance_exit: args.imbalance_exit,
        tilt_exit_bps: args.tilt_exit_bps,
        cooldown_ms: args.cooldown_ms,
    };
    let mut runner = SignalRunner::new(thresholds);

    let mut rows_total = 0usize;
    let mut rows_parsed = 0usize;
    let mut parse_errors = 0usize;

    let mut imbalance_values: Vec<f64> = Vec::new();
    let mut tilt_values: Vec<f64> = Vec::new();

    let mut triggers_long = 0u64;
    let mut triggers_short = 0u64;
    let mut entries = 0u64;
    let mut cancels = 0u64;
    let mut cooldown_skips = 0u64;
    let mut rearm_wait_skips = 0u64;
    let mut state_transitions: Vec<RunnerTransition> = Vec::new();

    let mut book: Option<OrderBook> = None;

    for line in reader.lines() {
        rows_total = rows_total.saturating_add(1);
        let line = line.with_context(|| format!("failed to read line {}", rows_total))?;
        let parsed = match parse_recorded_quote(&line) {
            Ok(Some(value)) => value,
            Ok(None) => continue,
            Err(error) => {
                parse_errors = parse_errors.saturating_add(1);
                eprintln!("parse error at line {}: {}", rows_total, error);
                continue;
            }
        };

        rows_parsed = rows_parsed.saturating_add(1);
        if book.is_none() {
            book = Some(OrderBook::new(parsed.symbol.clone()));
        }

        let mut event =
            MarketEvent::new(parsed.symbol.clone(), parsed.bid, parsed.ask, parsed.ts_ms);
        event.bid_qty = parsed.bid_qty;
        event.ask_qty = parsed.ask_qty;

        let orderbook = book.as_mut().expect("orderbook initialized");
        orderbook.apply(&event);

        let Some(signal) = compute_signal_snapshot(
            event.ts_ms,
            orderbook.best_bid,
            orderbook.best_bid_qty,
            orderbook.best_ask,
            orderbook.best_ask_qty,
        ) else {
            continue;
        };

        imbalance_values.push(signal.imbalance);
        tilt_values.push(signal.tilt_bps);

        match classify_trigger(&signal, runner.thresholds()) {
            Some(SignalDirection::Long) => triggers_long = triggers_long.saturating_add(1),
            Some(SignalDirection::Short) => triggers_short = triggers_short.saturating_add(1),
            None => {}
        }

        let decision = runner.on_snapshot(&signal);
        if let Some(transition) = decision.transition {
            println!(
                "state transition ts={} from={:?} to={:?} reason={:?}",
                transition.ts_ms, transition.from, transition.to, transition.reason
            );
            state_transitions.push(transition);
        }
        match decision.action {
            RunnerAction::Enter(_) => entries = entries.saturating_add(1),
            RunnerAction::CancelNeutral(_) | RunnerAction::CancelAdverse(_) => {
                cancels = cancels.saturating_add(1)
            }
            RunnerAction::CooldownSkip(_) => cooldown_skips = cooldown_skips.saturating_add(1),
            RunnerAction::RearmWaitSkip(_) => rearm_wait_skips = rearm_wait_skips.saturating_add(1),
            RunnerAction::None => {}
        }
    }

    let imbalance_histogram = build_histogram(
        &imbalance_values,
        &[-0.8, -0.4, -0.2, -0.1, 0.0, 0.1, 0.2, 0.4, 0.8],
        "",
    );
    let tilt_histogram = build_histogram(
        &tilt_values,
        &[-5.0, -2.0, -1.0, -0.5, 0.0, 0.5, 1.0, 2.0, 5.0],
        "bps",
    );
    let imbalance_stats = quantiles(&mut imbalance_values);
    let tilt_bps_stats = quantiles(&mut tilt_values);

    let finished_at_ms = now_millis();
    let summary = ReplaySummary {
        started_at_ms,
        finished_at_ms,
        input_path: args.input_path.display().to_string(),
        rows_total,
        rows_parsed,
        parse_errors,
        thresholds: ReplayThresholds {
            imbalance_enter: args.imbalance_enter,
            tilt_enter_bps: args.tilt_enter_bps,
            imbalance_exit: args.imbalance_exit,
            tilt_exit_bps: args.tilt_exit_bps,
            cooldown_ms: args.cooldown_ms,
        },
        imbalance_stats,
        tilt_bps_stats,
        imbalance_histogram,
        tilt_bps_histogram: tilt_histogram,
        triggers_long,
        triggers_short,
        entries,
        cancels,
        cooldown_skips,
        rearm_wait_skips,
        state_transitions,
    };

    write_summary(&out_path, &summary)?;

    println!("signal_replay summary");
    println!("  input: {}", summary.input_path);
    println!("  rows_total: {}", summary.rows_total);
    println!("  rows_parsed: {}", summary.rows_parsed);
    println!("  parse_errors: {}", summary.parse_errors);
    println!("  triggers_long: {}", summary.triggers_long);
    println!("  triggers_short: {}", summary.triggers_short);
    println!("  entries: {}", summary.entries);
    println!("  cancels: {}", summary.cancels);
    println!("  cooldown_skips: {}", summary.cooldown_skips);
    println!("  rearm_wait_skips: {}", summary.rearm_wait_skips);
    println!("  state_transitions: {}", summary.state_transitions.len());
    if let Some(stats) = summary.imbalance_stats {
        println!(
            "  imbalance p50/p95/p99: {:.6}/{:.6}/{:.6}",
            stats.p50, stats.p95, stats.p99
        );
    }
    if let Some(stats) = summary.tilt_bps_stats {
        println!(
            "  tilt_bps p50/p95/p99: {:.6}/{:.6}/{:.6}",
            stats.p50, stats.p95, stats.p99
        );
    }
    println!("  out_json: {}", out_path.display());

    Ok(())
}
