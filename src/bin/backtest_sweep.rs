use anyhow::{bail, Context, Result};
use serde::Serialize;
use std::fs::{self, File};
use std::io::{BufWriter, Write};
use std::path::{Path, PathBuf};

use lowlat_bot::backtest::{
    load_recording, recording_sha256_hex, run_backtest, sharpe_like_from_minute_buckets,
    BacktestExecutionModelParams, BacktestParams, BacktestStrategyParams,
};
use lowlat_bot::telemetry;
use lowlat_bot::util::now_millis;

#[derive(Debug, Clone)]
struct SweepArgs {
    input_path: PathBuf,
    out_json: Option<PathBuf>,
    out_csv: Option<PathBuf>,
    strategy: BacktestStrategyParams,
    execution: BacktestExecutionModelParams,
    discount_bps_grid: Vec<f64>,
    min_rest_ms_grid: Vec<u64>,
    top_k: usize,
}

impl Default for SweepArgs {
    fn default() -> Self {
        Self {
            input_path: PathBuf::new(),
            out_json: None,
            out_csv: None,
            strategy: BacktestStrategyParams::default(),
            execution: BacktestExecutionModelParams::default(),
            discount_bps_grid: Vec::new(),
            min_rest_ms_grid: Vec::new(),
            top_k: 10,
        }
    }
}

#[derive(Debug, Clone, Serialize)]
struct SweepRunRow {
    discount_bps: f64,
    min_rest_ms: u64,
    entries: u64,
    fills: u64,
    fill_rate: f64,
    total_pnl: f64,
    realized_pnl: f64,
    unrealized_pnl: f64,
    pnl_bps_on_notional: f64,
    fees_paid: f64,
    slippage_cost: f64,
    avg_time_to_fill_ms: Option<f64>,
    sharpe_like: f64,
}

#[derive(Debug, Clone, Serialize)]
struct SweepOutput {
    started_at_ms: u128,
    finished_at_ms: u128,
    recording_path: String,
    recording_hash_sha256: String,
    rows_total: usize,
    rows_parsed: usize,
    parse_errors: usize,
    base_params: BacktestParams,
    discount_bps_grid: Vec<f64>,
    min_rest_ms_grid: Vec<u64>,
    runs: Vec<SweepRunRow>,
    top_by_total_pnl: Vec<SweepRunRow>,
    top_by_sharpe_like: Vec<SweepRunRow>,
}

fn parse_args() -> Result<SweepArgs> {
    let raw_args: Vec<String> = std::env::args().skip(1).collect();
    let mut args = SweepArgs::default();
    let mut positional_input_set = false;
    let mut i = 0usize;

    while i < raw_args.len() {
        match raw_args[i].as_str() {
            "--out-json" => {
                i += 1;
                let Some(value) = raw_args.get(i) else {
                    bail!("missing value for --out-json");
                };
                args.out_json = Some(PathBuf::from(value));
            }
            "--out-csv" => {
                i += 1;
                let Some(value) = raw_args.get(i) else {
                    bail!("missing value for --out-csv");
                };
                args.out_csv = Some(PathBuf::from(value));
            }
            "--discount-bps-grid" => {
                i += 1;
                let Some(value) = raw_args.get(i) else {
                    bail!("missing value for --discount-bps-grid");
                };
                args.discount_bps_grid = parse_f64_list(value, "--discount-bps-grid")?;
            }
            "--min-rest-ms-grid" => {
                i += 1;
                let Some(value) = raw_args.get(i) else {
                    bail!("missing value for --min-rest-ms-grid");
                };
                args.min_rest_ms_grid = parse_u64_list(value, "--min-rest-ms-grid")?;
            }
            "--top-k" => {
                i += 1;
                args.top_k = parse_usize_arg(&raw_args, i, "--top-k")?;
            }
            "--imbalance-enter" => {
                i += 1;
                args.strategy.imbalance_enter = parse_f64_arg(&raw_args, i, "--imbalance-enter")?;
            }
            "--tilt-enter-bps" => {
                i += 1;
                args.strategy.tilt_enter_bps = parse_f64_arg(&raw_args, i, "--tilt-enter-bps")?;
            }
            "--imbalance-exit" => {
                i += 1;
                args.strategy.imbalance_exit = parse_f64_arg(&raw_args, i, "--imbalance-exit")?;
            }
            "--tilt-exit-bps" => {
                i += 1;
                args.strategy.tilt_exit_bps = parse_f64_arg(&raw_args, i, "--tilt-exit-bps")?;
            }
            "--cooldown-ms" => {
                i += 1;
                args.strategy.cooldown_ms = parse_u64_arg(&raw_args, i, "--cooldown-ms")?;
            }
            "--rearm" => {
                i += 1;
                args.strategy.rearm = parse_bool_arg(&raw_args, i, "--rearm")?;
            }
            "--discount-bps" => {
                i += 1;
                args.strategy.discount_bps = parse_f64_arg(&raw_args, i, "--discount-bps")?;
            }
            "--min-rest-ms" => {
                i += 1;
                args.strategy.min_rest_ms = parse_u64_arg(&raw_args, i, "--min-rest-ms")?;
            }
            "--adverse-mid-move-bps" => {
                i += 1;
                args.strategy.adverse_mid_move_bps =
                    parse_f64_arg(&raw_args, i, "--adverse-mid-move-bps")?;
            }
            "--qty" => {
                i += 1;
                args.execution.qty = parse_f64_arg(&raw_args, i, "--qty")?;
            }
            "--ack-delay-ms" => {
                i += 1;
                args.execution.ack_delay_ms = parse_u64_arg(&raw_args, i, "--ack-delay-ms")?;
            }
            "--cancel-delay-ms" => {
                i += 1;
                args.execution.cancel_delay_ms = parse_u64_arg(&raw_args, i, "--cancel-delay-ms")?;
            }
            "--fees-bps" => {
                i += 1;
                args.execution.fees_bps = parse_f64_arg(&raw_args, i, "--fees-bps")?;
            }
            "--slippage-bps" => {
                i += 1;
                args.execution.slippage_bps = parse_f64_arg(&raw_args, i, "--slippage-bps")?;
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
        bail!("missing recording path");
    }
    if args.top_k == 0 {
        bail!("--top-k must be > 0");
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

fn parse_u64_arg(raw_args: &[String], idx: usize, flag: &str) -> Result<u64> {
    let Some(value) = raw_args.get(idx) else {
        bail!("missing value for {}", flag);
    };
    value
        .parse::<u64>()
        .with_context(|| format!("invalid {} '{}'", flag, value))
}

fn parse_usize_arg(raw_args: &[String], idx: usize, flag: &str) -> Result<usize> {
    let Some(value) = raw_args.get(idx) else {
        bail!("missing value for {}", flag);
    };
    value
        .parse::<usize>()
        .with_context(|| format!("invalid {} '{}'", flag, value))
}

fn parse_bool_arg(raw_args: &[String], idx: usize, flag: &str) -> Result<bool> {
    let Some(value) = raw_args.get(idx) else {
        bail!("missing value for {}", flag);
    };
    match value.trim().to_ascii_lowercase().as_str() {
        "true" | "1" | "yes" => Ok(true),
        "false" | "0" | "no" => Ok(false),
        _ => bail!("invalid {} '{}' (expected true|false)", flag, value),
    }
}

fn parse_f64_list(raw: &str, flag: &str) -> Result<Vec<f64>> {
    let mut out = Vec::new();
    for token in raw.split(',') {
        let trimmed = token.trim();
        if trimmed.is_empty() {
            continue;
        }
        let value = trimmed
            .parse::<f64>()
            .with_context(|| format!("invalid {} token '{}'", flag, trimmed))?;
        out.push(value);
    }
    if out.is_empty() {
        bail!("{} requires at least one value", flag);
    }
    Ok(out)
}

fn parse_u64_list(raw: &str, flag: &str) -> Result<Vec<u64>> {
    let mut out = Vec::new();
    for token in raw.split(',') {
        let trimmed = token.trim();
        if trimmed.is_empty() {
            continue;
        }
        let value = trimmed
            .parse::<u64>()
            .with_context(|| format!("invalid {} token '{}'", flag, trimmed))?;
        out.push(value);
    }
    if out.is_empty() {
        bail!("{} requires at least one value", flag);
    }
    Ok(out)
}

fn print_usage() {
    eprintln!(
        "Usage:
  cargo run --bin backtest_sweep -- <recording.ndjson> --discount-bps-grid 1,3,5 --min-rest-ms-grid 0,250,500 --out-json runs/backtest_sweep.json --out-csv runs/backtest_sweep.csv
  cargo run --bin backtest_sweep -- recordings/btcusdt.ndjson --discount-bps-grid 2,4,6 --min-rest-ms-grid 0,100,300 --rearm true --fees-bps 1.0 --slippage-bps 0.0"
    );
}

fn default_out_json_path(started_at_ms: u128) -> PathBuf {
    PathBuf::from(format!("runs/backtest_sweep_{}.json", started_at_ms))
}

fn write_json(path: &Path, output: &SweepOutput) -> Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)
            .with_context(|| format!("failed to create output dir {}", parent.display()))?;
    }
    let file =
        File::create(path).with_context(|| format!("failed to create {}", path.display()))?;
    let mut writer = BufWriter::new(file);
    serde_json::to_writer_pretty(&mut writer, output)
        .with_context(|| format!("failed to serialize {}", path.display()))?;
    writer
        .write_all(b"\n")
        .with_context(|| format!("failed to finalize {}", path.display()))?;
    writer
        .flush()
        .with_context(|| format!("failed to flush {}", path.display()))?;
    Ok(())
}

fn write_csv(path: &Path, rows: &[SweepRunRow]) -> Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)
            .with_context(|| format!("failed to create output dir {}", parent.display()))?;
    }
    let file =
        File::create(path).with_context(|| format!("failed to create {}", path.display()))?;
    let mut writer = BufWriter::new(file);
    writer.write_all(
        b"discount_bps,min_rest_ms,entries,fills,fill_rate,total_pnl,realized_pnl,unrealized_pnl,pnl_bps_on_notional,fees_paid,slippage_cost,avg_time_to_fill_ms,sharpe_like\n",
    )?;
    for row in rows {
        writeln!(
            writer,
            "{:.6},{},{},{},{:.8},{:.10},{:.10},{:.10},{:.8},{:.10},{:.10},{},{:.8}",
            row.discount_bps,
            row.min_rest_ms,
            row.entries,
            row.fills,
            row.fill_rate,
            row.total_pnl,
            row.realized_pnl,
            row.unrealized_pnl,
            row.pnl_bps_on_notional,
            row.fees_paid,
            row.slippage_cost,
            row.avg_time_to_fill_ms
                .map(|value| format!("{value:.6}"))
                .unwrap_or_else(|| "".to_string()),
            row.sharpe_like,
        )?;
    }
    writer.flush()?;
    Ok(())
}

fn top_by_total_pnl(rows: &[SweepRunRow], top_k: usize) -> Vec<SweepRunRow> {
    let mut sorted = rows.to_vec();
    sorted.sort_by(|left, right| right.total_pnl.total_cmp(&left.total_pnl));
    sorted.truncate(top_k.min(sorted.len()));
    sorted
}

fn top_by_sharpe_like(rows: &[SweepRunRow], top_k: usize) -> Vec<SweepRunRow> {
    let mut sorted = rows.to_vec();
    sorted.sort_by(|left, right| right.sharpe_like.total_cmp(&left.sharpe_like));
    sorted.truncate(top_k.min(sorted.len()));
    sorted
}

fn main() -> Result<()> {
    let args = parse_args()?;
    telemetry::init("backtest_sweep");

    let started_at_ms = now_millis();
    let out_json = args
        .out_json
        .clone()
        .unwrap_or_else(|| default_out_json_path(started_at_ms));
    let out_csv = args
        .out_csv
        .clone()
        .unwrap_or_else(|| out_json.with_extension("csv"));

    let recording = load_recording(&args.input_path)?;
    let recording_hash = recording_sha256_hex(&args.input_path)?;

    let discount_grid = if args.discount_bps_grid.is_empty() {
        vec![args.strategy.discount_bps]
    } else {
        args.discount_bps_grid.clone()
    };
    let min_rest_grid = if args.min_rest_ms_grid.is_empty() {
        vec![args.strategy.min_rest_ms]
    } else {
        args.min_rest_ms_grid.clone()
    };

    let mut rows: Vec<SweepRunRow> = Vec::new();
    for discount_bps in &discount_grid {
        for min_rest_ms in &min_rest_grid {
            let mut strategy = args.strategy.clone();
            strategy.discount_bps = *discount_bps;
            strategy.min_rest_ms = *min_rest_ms;

            let params = BacktestParams {
                strategy,
                execution: args.execution.clone(),
            };
            params.validate()?;

            let summary = run_backtest(&recording.quotes, &params)?;
            let sharpe_like = sharpe_like_from_minute_buckets(&summary.minute_buckets);
            rows.push(SweepRunRow {
                discount_bps: *discount_bps,
                min_rest_ms: *min_rest_ms,
                entries: summary.entries,
                fills: summary.fills,
                fill_rate: summary.fill_rate,
                total_pnl: summary.pnl.total_pnl,
                realized_pnl: summary.pnl.realized_pnl,
                unrealized_pnl: summary.pnl.unrealized_pnl,
                pnl_bps_on_notional: summary.pnl.pnl_bps_on_notional,
                fees_paid: summary.fees_paid,
                slippage_cost: summary.slippage_cost,
                avg_time_to_fill_ms: summary.avg_time_to_fill_ms,
                sharpe_like,
            });
        }
    }

    let output = SweepOutput {
        started_at_ms,
        finished_at_ms: now_millis(),
        recording_path: args.input_path.display().to_string(),
        recording_hash_sha256: recording_hash,
        rows_total: recording.rows_total,
        rows_parsed: recording.rows_parsed,
        parse_errors: recording.parse_errors,
        base_params: BacktestParams {
            strategy: args.strategy.clone(),
            execution: args.execution.clone(),
        },
        discount_bps_grid: discount_grid,
        min_rest_ms_grid: min_rest_grid,
        top_by_total_pnl: top_by_total_pnl(&rows, args.top_k),
        top_by_sharpe_like: top_by_sharpe_like(&rows, args.top_k),
        runs: rows.clone(),
    };

    write_json(&out_json, &output)?;
    write_csv(&out_csv, &rows)?;

    println!("backtest_sweep summary");
    println!("  recording: {}", output.recording_path);
    println!("  rows_parsed: {}", output.rows_parsed);
    println!("  runs: {}", output.runs.len());
    println!("  out_json: {}", out_json.display());
    println!("  out_csv: {}", out_csv.display());

    Ok(())
}
