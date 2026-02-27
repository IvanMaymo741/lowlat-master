use anyhow::{bail, Context, Result};
use serde::Serialize;
use std::fs::{self, File};
use std::io::{BufWriter, Write};
use std::path::{Path, PathBuf};

use lowlat_bot::backtest::{
    load_recording, recording_sha256_hex, run_backtest, BacktestExecutionModelParams,
    BacktestParams, BacktestStrategyParams, BacktestSummary,
};
use lowlat_bot::telemetry;
use lowlat_bot::util::now_millis;

#[derive(Debug, Clone)]
struct RunnerArgs {
    input_path: PathBuf,
    out_json: Option<PathBuf>,
    strategy: BacktestStrategyParams,
    execution: BacktestExecutionModelParams,
}

impl Default for RunnerArgs {
    fn default() -> Self {
        Self {
            input_path: PathBuf::new(),
            out_json: None,
            strategy: BacktestStrategyParams::default(),
            execution: BacktestExecutionModelParams::default(),
        }
    }
}

#[derive(Debug, Clone, Serialize)]
struct BacktestRunOutput {
    started_at_ms: u128,
    finished_at_ms: u128,
    recording_path: String,
    recording_hash_sha256: String,
    rows_total: usize,
    rows_parsed: usize,
    parse_errors: usize,
    params: BacktestParams,
    summary: BacktestSummary,
}

fn parse_args() -> Result<RunnerArgs> {
    let raw_args: Vec<String> = std::env::args().skip(1).collect();
    let mut args = RunnerArgs::default();
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

fn print_usage() {
    eprintln!(
        "Usage:
  cargo run --bin backtest_runner -- <recording.ndjson> --out-json runs/backtest.json
  cargo run --bin backtest_runner -- recordings/btcusdt.ndjson --discount-bps 5 --min-rest-ms 250 --adverse-mid-move-bps 5 --ack-delay-ms 0 --cancel-delay-ms 0 --fees-bps 1.0 --slippage-bps 0.0"
    );
}

fn default_out_path(started_at_ms: u128) -> PathBuf {
    PathBuf::from(format!("runs/backtest_{}.json", started_at_ms))
}

fn write_json(path: &Path, output: &BacktestRunOutput) -> Result<()> {
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

fn main() -> Result<()> {
    let args = parse_args()?;
    telemetry::init("backtest_runner");

    let started_at_ms = now_millis();
    let out_json = args
        .out_json
        .clone()
        .unwrap_or_else(|| default_out_path(started_at_ms));

    let params = BacktestParams {
        strategy: args.strategy.clone(),
        execution: args.execution.clone(),
    };
    params.validate()?;

    let recording = load_recording(&args.input_path)?;
    let recording_hash = recording_sha256_hex(&args.input_path)?;
    let summary = run_backtest(&recording.quotes, &params)?;

    let finished_at_ms = now_millis();
    let output = BacktestRunOutput {
        started_at_ms,
        finished_at_ms,
        recording_path: args.input_path.display().to_string(),
        recording_hash_sha256: recording_hash,
        rows_total: recording.rows_total,
        rows_parsed: recording.rows_parsed,
        parse_errors: recording.parse_errors,
        params,
        summary,
    };

    write_json(&out_json, &output)?;

    println!("backtest_runner summary");
    println!("  recording: {}", output.recording_path);
    println!("  rows_parsed: {}", output.rows_parsed);
    println!("  entries: {}", output.summary.entries);
    println!("  fills: {}", output.summary.fills);
    println!("  fill_rate: {:.4}", output.summary.fill_rate);
    println!("  total_pnl: {:.8}", output.summary.pnl.total_pnl);
    println!(
        "  pnl_bps_on_notional: {:.4}",
        output.summary.pnl.pnl_bps_on_notional
    );
    println!("  out_json: {}", out_json.display());

    Ok(())
}
