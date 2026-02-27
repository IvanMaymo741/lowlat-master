use anyhow::{anyhow, bail, Context, Result};
use serde::Serialize;
use std::path::{Path, PathBuf};
use std::time::Duration;
use tokio::fs::OpenOptions;
use tokio::io::{AsyncWriteExt, BufWriter};
use tokio::sync::mpsc;
use tokio::sync::mpsc::error::TrySendError;
use tokio::time;
use tracing::info;

use lowlat_bot::config::AppConfig;
use lowlat_bot::marketdata::{spawn_marketdata, MarketEvent};
use lowlat_bot::telemetry;
use lowlat_bot::util::now_millis;

const WRITER_FLUSH_LINES: usize = 500;
const WRITER_FLUSH_INTERVAL_SECS: u64 = 1;
const DEFAULT_CONFIG_PATH: &str = "config/default.toml";

#[derive(Debug, Clone)]
struct RecorderArgs {
    config_path: String,
    duration_s: Option<u64>,
    out: Option<String>,
    dry_run: bool,
}

impl Default for RecorderArgs {
    fn default() -> Self {
        Self {
            config_path: DEFAULT_CONFIG_PATH.to_string(),
            duration_s: None,
            out: None,
            dry_run: false,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum OutputPathKind {
    File,
    Directory,
}

impl OutputPathKind {
    fn as_str(self) -> &'static str {
        match self {
            Self::File => "file",
            Self::Directory => "directory",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum OutputFormat {
    Csv,
    Ndjson,
}

impl OutputFormat {
    fn as_str(self) -> &'static str {
        match self {
            Self::Csv => "csv",
            Self::Ndjson => "ndjson",
        }
    }
}

#[derive(Debug, Clone)]
struct OutputResolution {
    requested_path: String,
    requested_kind: OutputPathKind,
    written_path: PathBuf,
    format: OutputFormat,
}

#[derive(Debug, Default)]
struct WriterStats {
    lines_written: u64,
    bytes_written: u64,
    flushed: bool,
    fsynced: bool,
}

#[derive(Debug, Default)]
struct MetricsWindow {
    inter_msg_us: Vec<u64>,
    parse_us: Vec<u64>,
    dropped_events: u64,
    dropped_writer_lines: u64,
    latest_quote: Option<QuoteSnapshot>,
}

impl MetricsWindow {
    fn record(&mut self, event: &MarketEvent) {
        if let Some(inter_msg_us) = event.inter_msg_us {
            self.inter_msg_us.push(inter_msg_us);
        }
        self.parse_us.push(event.parse_latency_us);
        self.dropped_events = self.dropped_events.saturating_add(event.dropped_events);

        let mid_price = (event.bid + event.ask) * 0.5;
        if event.bid > 0.0 && event.ask > 0.0 && mid_price > 0.0 {
            self.latest_quote = Some(QuoteSnapshot {
                best_bid: event.bid,
                best_ask: event.ask,
                mid_price,
                spread_bps: ((event.ask - event.bid) / mid_price) * 10_000.0,
            });
        }
    }

    fn record_writer_drop(&mut self) {
        self.dropped_writer_lines = self.dropped_writer_lines.saturating_add(1);
    }

    fn log_and_reset(&mut self) {
        let inter_stats = percentiles(&mut self.inter_msg_us);
        let parse_stats = percentiles(&mut self.parse_us);
        let dropped_events = std::mem::take(&mut self.dropped_events);
        let dropped_writer_lines = std::mem::take(&mut self.dropped_writer_lines);
        let quote = self.latest_quote;

        match (inter_stats, parse_stats) {
            (Some(inter), Some(parse)) => {
                if let Some(quote) = quote {
                    info!(
                        inter_samples = inter.samples,
                        inter_p50_us = inter.p50,
                        inter_p95_us = inter.p95,
                        inter_p99_us = inter.p99,
                        parse_samples = parse.samples,
                        parse_p50_us = parse.p50,
                        parse_p95_us = parse.p95,
                        parse_p99_us = parse.p99,
                        dropped_events,
                        dropped_writer_lines,
                        best_bid = quote.best_bid,
                        best_ask = quote.best_ask,
                        mid_price = quote.mid_price,
                        spread_bps = quote.spread_bps,
                        "marketdata latency stats (last 5s)"
                    );
                } else {
                    info!(
                        inter_samples = inter.samples,
                        inter_p50_us = inter.p50,
                        inter_p95_us = inter.p95,
                        inter_p99_us = inter.p99,
                        parse_samples = parse.samples,
                        parse_p50_us = parse.p50,
                        parse_p95_us = parse.p95,
                        parse_p99_us = parse.p99,
                        dropped_events,
                        dropped_writer_lines,
                        "marketdata latency stats (last 5s)"
                    );
                }
            }
            (None, Some(parse)) => {
                if let Some(quote) = quote {
                    info!(
                        parse_samples = parse.samples,
                        parse_p50_us = parse.p50,
                        parse_p95_us = parse.p95,
                        parse_p99_us = parse.p99,
                        dropped_events,
                        dropped_writer_lines,
                        best_bid = quote.best_bid,
                        best_ask = quote.best_ask,
                        mid_price = quote.mid_price,
                        spread_bps = quote.spread_bps,
                        "marketdata parse latency stats (last 5s)"
                    );
                } else {
                    info!(
                        parse_samples = parse.samples,
                        parse_p50_us = parse.p50,
                        parse_p95_us = parse.p95,
                        parse_p99_us = parse.p99,
                        dropped_events,
                        dropped_writer_lines,
                        "marketdata parse latency stats (last 5s)"
                    );
                }
            }
            _ => {
                if dropped_events > 0 || dropped_writer_lines > 0 {
                    if let Some(quote) = quote {
                        info!(
                            dropped_events,
                            dropped_writer_lines,
                            best_bid = quote.best_bid,
                            best_ask = quote.best_ask,
                            mid_price = quote.mid_price,
                            spread_bps = quote.spread_bps,
                            "marketdata drop stats (last 5s)"
                        );
                    } else {
                        info!(
                            dropped_events,
                            dropped_writer_lines, "marketdata drop stats (last 5s)"
                        );
                    }
                }
            }
        }
    }
}

#[derive(Debug, Clone, Copy)]
struct QuoteSnapshot {
    best_bid: f64,
    best_ask: f64,
    mid_price: f64,
    spread_bps: f64,
}

#[derive(Debug, Clone, Copy)]
struct Quantiles {
    samples: usize,
    p50: u64,
    p95: u64,
    p99: u64,
}

#[derive(Debug, Serialize)]
struct RecordedMarketEvent<'a> {
    symbol: &'a str,
    bid: f64,
    ask: f64,
    bid_qty: f64,
    ask_qty: f64,
    ts_ms: u128,
    recv_ts_ms: u128,
    parsed_ts_ms: u128,
    inter_msg_us: Option<u64>,
    parse_latency_us: u64,
    dropped_events: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    yes_no_imbalance: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    spread_pct: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    depth_ratio: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    yes_best_bid: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    yes_best_ask: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    no_best_bid: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    no_best_ask: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    yes_depth: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    no_depth: Option<f64>,
}

fn parse_args() -> Result<RecorderArgs> {
    let raw_args: Vec<String> = std::env::args().skip(1).collect();
    parse_args_from(&raw_args)
}

fn parse_args_from(raw_args: &[String]) -> Result<RecorderArgs> {
    let mut args = RecorderArgs::default();
    let mut i = 0usize;
    let mut positional_config_set = false;

    while i < raw_args.len() {
        match raw_args[i].as_str() {
            "--config" => {
                i += 1;
                let Some(value) = raw_args.get(i) else {
                    bail!("missing value for --config");
                };
                args.config_path = value.clone();
            }
            "--duration-s" => {
                i += 1;
                let Some(value) = raw_args.get(i) else {
                    bail!("missing value for --duration-s");
                };
                let parsed = value
                    .parse::<u64>()
                    .with_context(|| format!("invalid --duration-s '{}'", value))?;
                if parsed == 0 {
                    bail!("--duration-s must be > 0");
                }
                args.duration_s = Some(parsed);
            }
            "--out" => {
                i += 1;
                let Some(value) = raw_args.get(i) else {
                    bail!("missing value for --out");
                };
                if value.trim().is_empty() {
                    bail!("--out cannot be empty");
                }
                args.out = Some(value.clone());
            }
            "--dry-run" => {
                args.dry_run = true;
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

    Ok(args)
}

fn print_usage() {
    eprintln!(
        "Usage:
  cargo run --bin md_recorder -- [config_path]
  cargo run --bin md_recorder -- --config config/default.toml --duration-s 120 --out recordings/ --dry-run"
    );
}

fn percentiles(values: &mut Vec<u64>) -> Option<Quantiles> {
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
        samples,
        p50,
        p95,
        p99,
    })
}

fn percentile(sorted_values: &[u64], pct: f64) -> u64 {
    let len = sorted_values.len();
    if len == 0 {
        return 0;
    }

    let rank = ((len as f64 - 1.0) * pct).round() as usize;
    sorted_values[rank.min(len - 1)]
}

fn infer_output_path_kind(path: &Path, raw: &str) -> OutputPathKind {
    if path.exists() {
        if path.is_dir() {
            return OutputPathKind::Directory;
        }
        return OutputPathKind::File;
    }

    if raw.ends_with('/') || raw.ends_with('\\') {
        return OutputPathKind::Directory;
    }

    if path.extension().is_some() {
        return OutputPathKind::File;
    }

    OutputPathKind::Directory
}

fn output_format_for_path(path: &Path) -> OutputFormat {
    match path
        .extension()
        .and_then(|ext| ext.to_str())
        .map(|ext| ext.to_ascii_lowercase())
    {
        Some(ext) if ext == "ndjson" || ext == "jsonl" => OutputFormat::Ndjson,
        _ => OutputFormat::Csv,
    }
}

fn sanitize_symbol_for_filename(symbol: &str) -> String {
    let mut out = String::with_capacity(symbol.len());
    for ch in symbol.chars() {
        if ch.is_ascii_alphanumeric() {
            out.push(ch.to_ascii_lowercase());
        } else {
            out.push('_');
        }
    }

    if out.is_empty() {
        "symbol".to_string()
    } else {
        out
    }
}

async fn resolve_output_path(
    raw_output: &str,
    symbol: &str,
    started_at_ms: u128,
) -> Result<OutputResolution> {
    if raw_output.trim().is_empty() {
        bail!("output path cannot be empty");
    }

    let requested = PathBuf::from(raw_output);
    let requested_kind = infer_output_path_kind(&requested, raw_output);

    let written_path = match requested_kind {
        OutputPathKind::File => {
            if let Some(parent) = requested.parent() {
                if !parent.as_os_str().is_empty() {
                    tokio::fs::create_dir_all(parent).await.with_context(|| {
                        format!("failed to create parent directory {}", parent.display())
                    })?;
                }
            }
            requested
        }
        OutputPathKind::Directory => {
            tokio::fs::create_dir_all(&requested)
                .await
                .with_context(|| {
                    format!("failed to create output directory {}", requested.display())
                })?;
            let symbol = sanitize_symbol_for_filename(symbol);
            requested.join(format!("{}_{}.ndjson", symbol, started_at_ms))
        }
    };

    let format = output_format_for_path(&written_path);

    Ok(OutputResolution {
        requested_path: raw_output.to_string(),
        requested_kind,
        written_path,
        format,
    })
}

fn encode_event_line(event: &MarketEvent, output_format: OutputFormat) -> Result<String> {
    match output_format {
        OutputFormat::Csv => Ok(format!(
            "{},{:.6},{:.6},{:.8},{:.8},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{}\n",
            event.symbol,
            event.bid,
            event.ask,
            event.bid_qty,
            event.ask_qty,
            event.ts_ms,
            event.recv_ts_ms,
            event.parsed_ts_ms,
            event.inter_msg_us.unwrap_or(0),
            event.parse_latency_us,
            event.dropped_events,
            csv_opt_f64(event.yes_no_imbalance),
            csv_opt_f64(event.spread_pct),
            csv_opt_f64(event.depth_ratio),
            csv_opt_f64(event.yes_best_bid),
            csv_opt_f64(event.yes_best_ask),
            csv_opt_f64(event.no_best_bid),
            csv_opt_f64(event.no_best_ask),
            csv_opt_f64(event.yes_depth),
            csv_opt_f64(event.no_depth),
        )),
        OutputFormat::Ndjson => {
            let record = RecordedMarketEvent {
                symbol: &event.symbol,
                bid: event.bid,
                ask: event.ask,
                bid_qty: event.bid_qty,
                ask_qty: event.ask_qty,
                ts_ms: event.ts_ms,
                recv_ts_ms: event.recv_ts_ms,
                parsed_ts_ms: event.parsed_ts_ms,
                inter_msg_us: event.inter_msg_us,
                parse_latency_us: event.parse_latency_us,
                dropped_events: event.dropped_events,
                yes_no_imbalance: event.yes_no_imbalance,
                spread_pct: event.spread_pct,
                depth_ratio: event.depth_ratio,
                yes_best_bid: event.yes_best_bid,
                yes_best_ask: event.yes_best_ask,
                no_best_bid: event.no_best_bid,
                no_best_ask: event.no_best_ask,
                yes_depth: event.yes_depth,
                no_depth: event.no_depth,
            };
            let mut line =
                serde_json::to_string(&record).context("failed to serialize market event")?;
            line.push('\n');
            Ok(line)
        }
    }
}

fn csv_opt_f64(value: Option<f64>) -> String {
    value.map(|v| format!("{v:.8}")).unwrap_or_default()
}

async fn writer_task(path: PathBuf, mut rx: mpsc::Receiver<String>) -> Result<WriterStats> {
    let display_path = path.display().to_string();
    let file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(&path)
        .await
        .with_context(|| format!("failed to open market data output file {}", display_path))?;
    let mut writer = BufWriter::new(file);

    let mut stats = WriterStats::default();
    let mut pending_lines = 0usize;
    let mut flush_interval = time::interval(Duration::from_secs(WRITER_FLUSH_INTERVAL_SECS));
    flush_interval.set_missed_tick_behavior(time::MissedTickBehavior::Skip);

    loop {
        tokio::select! {
            _ = flush_interval.tick() => {
                if pending_lines > 0 {
                    writer
                        .flush()
                        .await
                        .with_context(|| format!("failed to flush output file {}", display_path))?;
                    stats.flushed = true;
                    pending_lines = 0;
                }
            }
            maybe_line = rx.recv() => {
                let Some(line) = maybe_line else {
                    break;
                };

                writer
                    .write_all(line.as_bytes())
                    .await
                    .with_context(|| format!("failed to write output file {}", display_path))?;
                stats.lines_written = stats.lines_written.saturating_add(1);
                stats.bytes_written = stats.bytes_written.saturating_add(line.len() as u64);
                pending_lines += 1;

                if pending_lines >= WRITER_FLUSH_LINES {
                    writer
                        .flush()
                        .await
                        .with_context(|| format!("failed to flush output file {}", display_path))?;
                    stats.flushed = true;
                    pending_lines = 0;
                }
            }
        }
    }

    writer
        .flush()
        .await
        .with_context(|| format!("failed to flush output file {}", display_path))?;
    stats.flushed = true;

    let file = writer.into_inner();
    file.sync_all()
        .await
        .with_context(|| format!("failed to fsync output file {}", display_path))?;
    stats.fsynced = true;

    Ok(stats)
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = parse_args()?;

    let cfg = AppConfig::load_from_path(&args.config_path)
        .with_context(|| format!("failed to load config from {}", args.config_path))?;

    telemetry::init("md_recorder");

    let requested_duration_s = args.duration_s.unwrap_or_else(|| cfg.general.duration_s());
    let raw_output = args
        .out
        .as_deref()
        .unwrap_or(&cfg.general.md_output_path)
        .to_string();

    let started_at_ms = now_millis();
    let output = resolve_output_path(&raw_output, &cfg.general.symbol, started_at_ms).await?;

    info!(
        symbol = %cfg.general.symbol,
        duration_s = requested_duration_s,
        out_path = %output.written_path.display(),
        out_input = %output.requested_path,
        out_interpretation = output.requested_kind.as_str(),
        output_format = output.format.as_str(),
        dry_run = args.dry_run,
        mode = %cfg.marketdata.mode,
        "md_recorder effective config"
    );

    let mut rx = spawn_marketdata(&cfg.marketdata, cfg.general.symbol.clone())
        .context("failed to start market data source")?;

    let mut line_tx: Option<mpsc::Sender<String>> = None;
    let mut writer_handle = None;

    if !args.dry_run {
        let (tx, writer_rx) = mpsc::channel::<String>(cfg.marketdata.channel_buffer());
        line_tx = Some(tx);
        let output_path = output.written_path.clone();
        writer_handle = Some(tokio::spawn(async move {
            writer_task(output_path, writer_rx).await
        }));
    }

    let mut metrics = MetricsWindow::default();
    let mut report_interval = time::interval(Duration::from_secs(5));
    report_interval.set_missed_tick_behavior(time::MissedTickBehavior::Skip);

    let mut dry_run_stats = WriterStats::default();
    let run_start = time::Instant::now();
    let deadline = run_start + Duration::from_secs(requested_duration_s);
    let stop_at = time::sleep_until(deadline);
    tokio::pin!(stop_at);

    loop {
        tokio::select! {
            _ = &mut stop_at => {
                break;
            }
            _ = report_interval.tick() => {
                metrics.log_and_reset();
            }
            maybe_event = rx.recv() => {
                let Some(event) = maybe_event else {
                    break;
                };

                metrics.record(&event);
                let line = encode_event_line(&event, output.format)?;

                if args.dry_run {
                    dry_run_stats.lines_written = dry_run_stats.lines_written.saturating_add(1);
                    dry_run_stats.bytes_written = dry_run_stats.bytes_written.saturating_add(line.len() as u64);
                    continue;
                }

                if let Some(tx) = line_tx.as_ref() {
                    match tx.try_send(line) {
                        Ok(()) => {}
                        Err(TrySendError::Full(_)) => metrics.record_writer_drop(),
                        Err(TrySendError::Closed(_)) => {
                            return Err(anyhow!("writer task channel closed unexpectedly"));
                        }
                    }
                }
            }
        }
    }

    metrics.log_and_reset();

    let writer_stats = if args.dry_run {
        dry_run_stats
    } else {
        drop(line_tx.take());

        writer_handle
            .ok_or_else(|| anyhow!("writer task handle missing"))?
            .await
            .context("writer task join failure")?
            .context("writer task failed")?
    };

    let actual_elapsed_s = run_start.elapsed().as_secs_f64();

    info!(
        requested_duration_s,
        actual_elapsed_s,
        written_path = %output.written_path.display(),
        lines_written = writer_stats.lines_written,
        bytes_written = writer_stats.bytes_written,
        flushed = writer_stats.flushed,
        fsynced = writer_stats.fsynced,
        dry_run = args.dry_run,
        "md_recorder finished"
    );

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_args_applies_cli_values() {
        let raw = vec![
            "--config".to_string(),
            "config/alt.toml".to_string(),
            "--duration-s".to_string(),
            "42".to_string(),
            "--out".to_string(),
            "recordings".to_string(),
            "--dry-run".to_string(),
        ];

        let args = parse_args_from(&raw).expect("args should parse");

        assert_eq!(args.config_path, "config/alt.toml");
        assert_eq!(args.duration_s, Some(42));
        assert_eq!(args.out.as_deref(), Some("recordings"));
        assert!(args.dry_run);
    }

    #[test]
    fn infer_directory_for_missing_path_without_extension() {
        let path = Path::new("recordings");
        let kind = infer_output_path_kind(path, "recordings");
        assert_eq!(kind, OutputPathKind::Directory);
    }

    #[test]
    fn infer_file_when_extension_present() {
        let path = Path::new("recordings/run.csv");
        let kind = infer_output_path_kind(path, "recordings/run.csv");
        assert_eq!(kind, OutputPathKind::File);
    }

    #[test]
    fn ndjson_selected_for_jsonl_extensions() {
        assert_eq!(
            output_format_for_path(Path::new("recordings/a.ndjson")),
            OutputFormat::Ndjson
        );
        assert_eq!(
            output_format_for_path(Path::new("recordings/a.jsonl")),
            OutputFormat::Ndjson
        );
        assert_eq!(
            output_format_for_path(Path::new("recordings/a.csv")),
            OutputFormat::Csv
        );
    }
}
