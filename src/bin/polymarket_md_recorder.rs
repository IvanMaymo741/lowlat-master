use anyhow::{anyhow, bail, Context, Result};
use serde::Serialize;
use serde_json::{json, Map as JsonMap, Value as JsonValue};
use std::collections::HashSet;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};
use tokio::fs::OpenOptions;
use tokio::io::{AsyncWriteExt, BufWriter};
use tokio::sync::mpsc;
use tokio::sync::mpsc::error::TrySendError;
use tokio::time;
use tracing::{info, warn};

use lowlat_bot::config::{AppConfig, MarketDataConfig};
use lowlat_bot::marketdata::{spawn_marketdata, MarketEvent};
use lowlat_bot::telemetry;
use lowlat_bot::util::now_millis;

const DEFAULT_CONFIG_PATH: &str = "config/default.toml";
const DEFAULT_OUTPUT_PATH: &str = "data/polymarket/md.ndjson";
const DEFAULT_HEARTBEAT_SECS: u64 = 30;
const DEFAULT_FLUSH_EVERY: usize = 10;
const WRITER_FLUSH_INTERVAL_SECS: u64 = 1;
const MAX_UNBOUNDED_RUNTIME_SECS: u64 = 365 * 24 * 60 * 60;

#[derive(Debug, Clone)]
struct RecorderArgs {
    config_path: String,
    out: String,
    markets: Vec<String>,
    markets_file: Option<String>,
    ws_url: Option<String>,
    heartbeat_secs: u64,
    flush_every: usize,
    max_runtime_secs: Option<u64>,
}

impl Default for RecorderArgs {
    fn default() -> Self {
        Self {
            config_path: DEFAULT_CONFIG_PATH.to_string(),
            out: DEFAULT_OUTPUT_PATH.to_string(),
            markets: Vec::new(),
            markets_file: None,
            ws_url: None,
            heartbeat_secs: DEFAULT_HEARTBEAT_SECS,
            flush_every: DEFAULT_FLUSH_EVERY,
            max_runtime_secs: None,
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

#[derive(Debug, Clone)]
struct OutputResolution {
    requested_path: String,
    requested_kind: OutputPathKind,
    written_path: PathBuf,
}

#[derive(Debug, Clone)]
struct MarketSelection {
    market_id: String,
    yes_asset_id: String,
    no_asset_id: String,
}

#[derive(Debug)]
enum StreamMessage {
    Market(Box<MarketEvent>),
    SourceClosed { market: String },
}

#[derive(Debug, Default)]
struct WriterStats {
    lines_written: u64,
    bytes_written: u64,
    flushed: bool,
    fsynced: bool,
}

#[derive(Debug, Serialize)]
struct RecorderRecord {
    ts_ms: u64,
    source: &'static str,
    market: String,
    event: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    seq: Option<u64>,
    data: JsonValue,
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
            "--out" => {
                i += 1;
                let Some(value) = raw_args.get(i) else {
                    bail!("missing value for --out");
                };
                if value.trim().is_empty() {
                    bail!("--out cannot be empty");
                }
                args.out = value.clone();
            }
            "--market" => {
                i += 1;
                let Some(value) = raw_args.get(i) else {
                    bail!("missing value for --market");
                };
                if value.trim().is_empty() {
                    bail!("--market cannot be empty");
                }
                args.markets.push(value.clone());
            }
            "--markets-file" => {
                i += 1;
                let Some(value) = raw_args.get(i) else {
                    bail!("missing value for --markets-file");
                };
                if value.trim().is_empty() {
                    bail!("--markets-file cannot be empty");
                }
                args.markets_file = Some(value.clone());
            }
            "--url" | "--ws-url" => {
                i += 1;
                let Some(value) = raw_args.get(i) else {
                    bail!("missing value for --ws-url");
                };
                if value.trim().is_empty() {
                    bail!("--ws-url cannot be empty");
                }
                args.ws_url = Some(value.clone());
            }
            "--heartbeat-secs" => {
                i += 1;
                let Some(value) = raw_args.get(i) else {
                    bail!("missing value for --heartbeat-secs");
                };
                let parsed = parse_non_zero_u64(value, "--heartbeat-secs")?;
                args.heartbeat_secs = parsed;
            }
            "--flush-every" => {
                i += 1;
                let Some(value) = raw_args.get(i) else {
                    bail!("missing value for --flush-every");
                };
                let parsed = parse_non_zero_usize(value, "--flush-every")?;
                args.flush_every = parsed;
            }
            "--max-runtime-secs" => {
                i += 1;
                let Some(value) = raw_args.get(i) else {
                    bail!("missing value for --max-runtime-secs");
                };
                let parsed = parse_non_zero_u64(value, "--max-runtime-secs")?;
                args.max_runtime_secs = Some(parsed);
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

fn parse_non_zero_u64(value: &str, flag: &str) -> Result<u64> {
    let parsed = value
        .parse::<u64>()
        .with_context(|| format!("invalid {} '{}'", flag, value))?;
    if parsed == 0 {
        bail!("{} must be > 0", flag);
    }
    Ok(parsed)
}

fn parse_non_zero_usize(value: &str, flag: &str) -> Result<usize> {
    let parsed = value
        .parse::<usize>()
        .with_context(|| format!("invalid {} '{}'", flag, value))?;
    if parsed == 0 {
        bail!("{} must be > 0", flag);
    }
    Ok(parsed)
}

fn print_usage() {
    eprintln!(
        "Usage:
  cargo run --bin polymarket_md_recorder -- --config config/default.toml --market <market:yes_asset:no_asset>
  cargo run --bin polymarket_md_recorder -- --markets-file config/polymarket_markets.txt --out data/polymarket/ --flush-every 10

Flags:
  --out <path>                 Output file or directory (default data/polymarket/md.ndjson)
  --market <id>                Repeatable. Accepts '<market_id>' or '<market_id>:<yes_asset_id>:<no_asset_id>'
  --markets-file <path>        One market per line (same format as --market)
  --url, --ws-url <url>        Override Polymarket WS URL
  --heartbeat-secs <N>         Heartbeat event interval in seconds (default 30)
  --flush-every <N>            Flush writer every N lines (default 10)
  --max-runtime-secs <N>       Optional max runtime
"
    );
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

async fn resolve_output_path(raw_output: &str, started_at_ms: u64) -> Result<OutputResolution> {
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
            requested.join(format!("polymarket_md_{}.ndjson", started_at_ms))
        }
    };

    Ok(OutputResolution {
        requested_path: raw_output.to_string(),
        requested_kind,
        written_path,
    })
}

fn parse_market_selection(
    raw: &str,
    default_yes: &str,
    default_no: &str,
) -> Result<MarketSelection> {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        bail!("market selection cannot be empty");
    }

    for delimiter in [':', ','] {
        let mut parts = trimmed.split(delimiter);
        let first = parts.next();
        let second = parts.next();
        let third = parts.next();
        let extra = parts.next();

        if let (Some(market_id), Some(yes_asset_id), Some(no_asset_id), None) =
            (first, second, third, extra)
        {
            return build_market_selection(market_id, yes_asset_id, no_asset_id, raw);
        }
    }

    if default_yes.is_empty() || default_no.is_empty() {
        bail!(
            "market '{}' requires yes/no asset ids; use '<market:yes_asset:no_asset>' or set marketdata.polymarket_yes_asset_id/no_asset_id",
            raw
        );
    }

    build_market_selection(trimmed, default_yes, default_no, raw)
}

fn build_market_selection(
    market_id: &str,
    yes_asset_id: &str,
    no_asset_id: &str,
    raw: &str,
) -> Result<MarketSelection> {
    let market_id = market_id.trim();
    let yes_asset_id = yes_asset_id.trim();
    let no_asset_id = no_asset_id.trim();

    if market_id.is_empty() {
        bail!(
            "invalid market selection '{}': market_id cannot be empty",
            raw
        );
    }
    if yes_asset_id.is_empty() || no_asset_id.is_empty() {
        bail!(
            "invalid market selection '{}': yes/no asset ids cannot be empty",
            raw
        );
    }
    if yes_asset_id == no_asset_id {
        bail!(
            "invalid market selection '{}': yes/no asset ids must be different",
            raw
        );
    }

    Ok(MarketSelection {
        market_id: market_id.to_string(),
        yes_asset_id: yes_asset_id.to_string(),
        no_asset_id: no_asset_id.to_string(),
    })
}

fn load_markets_file(path: &str) -> Result<Vec<String>> {
    let content = std::fs::read_to_string(path)
        .with_context(|| format!("failed to read markets file {}", path))?;
    let mut tokens = Vec::new();

    for line in content.lines() {
        let trimmed = line.trim();
        if trimmed.is_empty() || trimmed.starts_with('#') {
            continue;
        }
        tokens.push(trimmed.to_string());
    }

    Ok(tokens)
}

fn resolve_market_selections(args: &RecorderArgs, cfg: &AppConfig) -> Result<Vec<MarketSelection>> {
    let mut tokens = args.markets.clone();
    if let Some(path) = args.markets_file.as_deref() {
        tokens.extend(load_markets_file(path)?);
    }

    if tokens.is_empty() {
        let from_cfg = cfg.marketdata.polymarket_market_id.trim();
        if from_cfg.is_empty() {
            bail!(
                "no market provided; use --market/--markets-file or set marketdata.polymarket_market_id"
            );
        }
        tokens.push(from_cfg.to_string());
    }

    let default_yes = cfg.marketdata.polymarket_yes_asset_id.trim();
    let default_no = cfg.marketdata.polymarket_no_asset_id.trim();

    let mut selections = Vec::with_capacity(tokens.len());
    for token in tokens {
        selections.push(parse_market_selection(&token, default_yes, default_no)?);
    }

    Ok(selections)
}

fn build_marketdata_cfg(
    base_cfg: &MarketDataConfig,
    selection: &MarketSelection,
    ws_url_override: Option<&str>,
) -> MarketDataConfig {
    let mut market_cfg = base_cfg.clone();
    market_cfg.mode = "polymarket_ws".to_string();
    market_cfg.polymarket_market_id = selection.market_id.clone();
    market_cfg.polymarket_yes_asset_id = selection.yes_asset_id.clone();
    market_cfg.polymarket_no_asset_id = selection.no_asset_id.clone();
    if let Some(url) = ws_url_override {
        market_cfg.polymarket_ws_url = url.to_string();
    }
    market_cfg
}

fn now_ms_u64() -> u64 {
    u64::try_from(now_millis()).unwrap_or(u64::MAX)
}

fn as_u64(value: u128) -> u64 {
    u64::try_from(value).unwrap_or(u64::MAX)
}

fn append_f64(data: &mut JsonMap<String, JsonValue>, key: &str, value: Option<f64>) {
    if let Some(v) = value {
        data.insert(key.to_string(), json!(v));
    }
}

fn append_u64(data: &mut JsonMap<String, JsonValue>, key: &str, value: Option<u64>) {
    if let Some(v) = value {
        data.insert(key.to_string(), json!(v));
    }
}

fn l2_record(event: &MarketEvent, event_type: &str) -> RecorderRecord {
    let mut data = JsonMap::new();

    data.insert(
        "yes".to_string(),
        json!({
            "bids": [[event.bid, event.bid_qty]],
            "asks": [[event.ask, event.ask_qty]],
            "depth": event.yes_depth
        }),
    );

    let no_bids = event
        .no_best_bid
        .map(|price| json!([[price, 0.0]]))
        .unwrap_or_else(|| json!([]));
    let no_asks = event
        .no_best_ask
        .map(|price| json!([[price, 0.0]]))
        .unwrap_or_else(|| json!([]));
    data.insert(
        "no".to_string(),
        json!({
            "bids": no_bids,
            "asks": no_asks,
            "depth": event.no_depth
        }),
    );

    let mid = (event.bid + event.ask) * 0.5;
    data.insert("mid".to_string(), json!(mid));
    data.insert("dropped_events".to_string(), json!(event.dropped_events));
    data.insert("recv_ts_ms".to_string(), json!(as_u64(event.recv_ts_ms)));
    data.insert(
        "parsed_ts_ms".to_string(),
        json!(as_u64(event.parsed_ts_ms)),
    );
    data.insert(
        "parse_latency_us".to_string(),
        json!(event.parse_latency_us),
    );

    append_u64(&mut data, "inter_msg_us", event.inter_msg_us);
    append_f64(&mut data, "yes_no_imbalance", event.yes_no_imbalance);
    append_f64(&mut data, "spread_pct", event.spread_pct);
    append_f64(&mut data, "depth_ratio", event.depth_ratio);

    RecorderRecord {
        ts_ms: as_u64(event.ts_ms),
        source: "polymarket",
        market: event.symbol.clone(),
        event: event_type.to_string(),
        seq: None,
        data: JsonValue::Object(data),
    }
}

fn heartbeat_record(active_sources: usize, l2_events: u64, error_events: u64) -> RecorderRecord {
    RecorderRecord {
        ts_ms: now_ms_u64(),
        source: "polymarket",
        market: "all".to_string(),
        event: "heartbeat".to_string(),
        seq: None,
        data: json!({
            "active_sources": active_sources,
            "l2_events": l2_events,
            "error_events": error_events,
        }),
    }
}

fn error_record(market: &str, message: impl Into<String>, context: JsonValue) -> RecorderRecord {
    RecorderRecord {
        ts_ms: now_ms_u64(),
        source: "polymarket",
        market: market.to_string(),
        event: "error".to_string(),
        seq: None,
        data: json!({
            "message": message.into(),
            "context": context,
        }),
    }
}

fn enqueue_record(
    tx: &mpsc::Sender<String>,
    record: RecorderRecord,
    dropped_total: &mut u64,
    dropped_since_log: &mut u64,
    last_drop_log: &mut Instant,
) -> Result<bool> {
    let mut line = serde_json::to_string(&record).context("failed to serialize recorder event")?;
    line.push('\n');

    match tx.try_send(line) {
        Ok(()) => Ok(true),
        Err(TrySendError::Full(_)) => {
            *dropped_total = dropped_total.saturating_add(1);
            *dropped_since_log = dropped_since_log.saturating_add(1);
            if last_drop_log.elapsed() >= Duration::from_secs(5) {
                warn!(
                    dropped_writer_lines = *dropped_since_log,
                    dropped_writer_lines_total = *dropped_total,
                    "polymarket recorder writer channel full; dropping lines"
                );
                *dropped_since_log = 0;
                *last_drop_log = Instant::now();
            }
            Ok(false)
        }
        Err(TrySendError::Closed(_)) => Err(anyhow!("writer task channel closed unexpectedly")),
    }
}

async fn writer_task(
    path: PathBuf,
    flush_every: usize,
    mut rx: mpsc::Receiver<String>,
) -> Result<WriterStats> {
    let flush_every = flush_every.max(1);
    let display_path = path.display().to_string();
    let file = OpenOptions::new()
        .create(true)
        .truncate(true)
        .write(true)
        .open(&path)
        .await
        .with_context(|| format!("failed to open output file {}", display_path))?;
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

                if pending_lines >= flush_every {
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

    telemetry::init("polymarket_md_recorder");

    let started_at_ms = now_ms_u64();
    let output = resolve_output_path(&args.out, started_at_ms).await?;
    let market_selections = resolve_market_selections(&args, &cfg)?;
    let market_ids: Vec<String> = market_selections
        .iter()
        .map(|selection| selection.market_id.clone())
        .collect();

    let has_api_key = std::env::var("POLYMARKET_API_KEY").ok().is_some();
    let has_private_key = std::env::var("POLYMARKET_PRIVATE_KEY").ok().is_some();
    let has_passphrase = std::env::var("POLYMARKET_PASSPHRASE").ok().is_some();

    info!(
        out_path = %output.written_path.display(),
        out_input = %output.requested_path,
        out_interpretation = output.requested_kind.as_str(),
        markets = %market_ids.join(","),
        market_count = market_ids.len(),
        ws_url = %args.ws_url.as_deref().unwrap_or(&cfg.marketdata.polymarket_ws_url),
        heartbeat_secs = args.heartbeat_secs,
        flush_every = args.flush_every,
        max_runtime_secs = args.max_runtime_secs.unwrap_or(0),
        polymarket_api_key_set = has_api_key,
        polymarket_private_key_set = has_private_key,
        polymarket_passphrase_set = has_passphrase,
        "polymarket_md_recorder effective config"
    );

    let channel_capacity = cfg.marketdata.channel_buffer().max(1_024);
    let (line_tx, line_rx) = mpsc::channel::<String>(channel_capacity);
    let writer_path = output.written_path.clone();
    let flush_every = args.flush_every;
    let writer_handle =
        tokio::spawn(async move { writer_task(writer_path, flush_every, line_rx).await });

    let (stream_tx, mut stream_rx) = mpsc::channel::<StreamMessage>(channel_capacity);
    let mut active_sources: HashSet<String> = HashSet::new();
    let mut seen_markets: HashSet<String> = HashSet::new();
    let mut dropped_writer_lines_total = 0u64;
    let mut dropped_writer_lines_since_log = 0u64;
    let mut last_drop_log = Instant::now();

    let mut l2_events = 0u64;
    let mut heartbeat_events = 0u64;
    let mut error_events = 0u64;

    for selection in market_selections {
        let market = selection.market_id.clone();
        let market_cfg = build_marketdata_cfg(&cfg.marketdata, &selection, args.ws_url.as_deref());
        match spawn_marketdata(&market_cfg, market.clone()) {
            Ok(mut rx) => {
                active_sources.insert(market.clone());
                let tx = stream_tx.clone();
                tokio::spawn(async move {
                    while let Some(event) = rx.recv().await {
                        if tx
                            .send(StreamMessage::Market(Box::new(event)))
                            .await
                            .is_err()
                        {
                            return;
                        }
                    }
                    let _ = tx.send(StreamMessage::SourceClosed { market }).await;
                });
            }
            Err(error) => {
                let record = error_record(
                    &market,
                    format!("failed to spawn market data stream: {error}"),
                    json!({"stage": "spawn"}),
                );
                if enqueue_record(
                    &line_tx,
                    record,
                    &mut dropped_writer_lines_total,
                    &mut dropped_writer_lines_since_log,
                    &mut last_drop_log,
                )? {
                    error_events = error_events.saturating_add(1);
                }
            }
        }
    }
    drop(stream_tx);

    if active_sources.is_empty() {
        let record = error_record(
            "all",
            "no active market data streams",
            json!({"stage": "bootstrap"}),
        );
        let _ = enqueue_record(
            &line_tx,
            record,
            &mut dropped_writer_lines_total,
            &mut dropped_writer_lines_since_log,
            &mut last_drop_log,
        )?;
        drop(line_tx);
        let _ = writer_handle.await;
        bail!("failed to start any polymarket market data stream");
    }

    let mut heartbeat_interval = time::interval(Duration::from_secs(args.heartbeat_secs));
    heartbeat_interval.set_missed_tick_behavior(time::MissedTickBehavior::Skip);
    let runtime_limit_secs = args.max_runtime_secs.unwrap_or(MAX_UNBOUNDED_RUNTIME_SECS);
    let max_runtime_sleep = time::sleep(Duration::from_secs(runtime_limit_secs));
    tokio::pin!(max_runtime_sleep);
    let ctrl_c = tokio::signal::ctrl_c();
    tokio::pin!(ctrl_c);
    let run_start = Instant::now();
    let exit_reason = loop {
        tokio::select! {
            _ = &mut ctrl_c => {
                break "ctrl_c".to_string();
            }
            _ = &mut max_runtime_sleep, if args.max_runtime_secs.is_some() => {
                break "max_runtime".to_string();
            }
            _ = heartbeat_interval.tick() => {
                let record = heartbeat_record(active_sources.len(), l2_events, error_events);
                if enqueue_record(
                    &line_tx,
                    record,
                    &mut dropped_writer_lines_total,
                    &mut dropped_writer_lines_since_log,
                    &mut last_drop_log,
                )? {
                    heartbeat_events = heartbeat_events.saturating_add(1);
                }
            }
            maybe_msg = stream_rx.recv() => {
                let Some(msg) = maybe_msg else {
                    break "stream_channel_closed".to_string();
                };

                match msg {
                    StreamMessage::Market(event) => {
                        let event = *event;
                        let event_type = if seen_markets.insert(event.symbol.clone()) {
                            "l2_snapshot"
                        } else {
                            "l2_update"
                        };
                        let record = l2_record(&event, event_type);
                        if enqueue_record(
                            &line_tx,
                            record,
                            &mut dropped_writer_lines_total,
                            &mut dropped_writer_lines_since_log,
                            &mut last_drop_log,
                        )? {
                            l2_events = l2_events.saturating_add(1);
                        }
                    }
                    StreamMessage::SourceClosed { market } => {
                        if active_sources.remove(&market) {
                            let record = error_record(
                                &market,
                                "market data source closed",
                                json!({"stage": "source_closed"}),
                            );
                            if enqueue_record(
                                &line_tx,
                                record,
                                &mut dropped_writer_lines_total,
                                &mut dropped_writer_lines_since_log,
                                &mut last_drop_log,
                            )? {
                                error_events = error_events.saturating_add(1);
                            }
                        }

                        if active_sources.is_empty() {
                            break "all_sources_closed".to_string();
                        }
                    }
                }
            }
        }
    };

    if dropped_writer_lines_since_log > 0 {
        warn!(
            dropped_writer_lines = dropped_writer_lines_since_log,
            dropped_writer_lines_total, "polymarket recorder finished with dropped writer lines"
        );
    }

    drop(line_tx);
    let writer_stats = writer_handle
        .await
        .context("writer task join failure")?
        .context("writer task failed")?;

    let actual_elapsed_s = run_start.elapsed().as_secs_f64();
    info!(
        exit_reason,
        actual_elapsed_s,
        written_path = %output.written_path.display(),
        lines_written = writer_stats.lines_written,
        bytes_written = writer_stats.bytes_written,
        flushed = writer_stats.flushed,
        fsynced = writer_stats.fsynced,
        l2_events,
        heartbeat_events,
        error_events,
        dropped_writer_lines_total,
        "polymarket_md_recorder finished"
    );

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_market_selection_from_triplet() {
        let parsed = parse_market_selection("mkt1:yes1:no1", "", "").expect("parse should pass");
        assert_eq!(parsed.market_id, "mkt1");
        assert_eq!(parsed.yes_asset_id, "yes1");
        assert_eq!(parsed.no_asset_id, "no1");
    }

    #[test]
    fn parse_market_selection_uses_defaults() {
        let parsed =
            parse_market_selection("mkt1", "yes_default", "no_default").expect("parse should pass");
        assert_eq!(parsed.market_id, "mkt1");
        assert_eq!(parsed.yes_asset_id, "yes_default");
        assert_eq!(parsed.no_asset_id, "no_default");
    }

    #[test]
    fn parse_args_parses_polymarket_flags() {
        let raw = vec![
            "--config".to_string(),
            "config/custom.toml".to_string(),
            "--out".to_string(),
            "recordings/polymarket/".to_string(),
            "--market".to_string(),
            "m1:y1:n1".to_string(),
            "--heartbeat-secs".to_string(),
            "20".to_string(),
            "--flush-every".to_string(),
            "7".to_string(),
            "--max-runtime-secs".to_string(),
            "120".to_string(),
            "--ws-url".to_string(),
            "wss://example.com/ws".to_string(),
        ];

        let args = parse_args_from(&raw).expect("args should parse");
        assert_eq!(args.config_path, "config/custom.toml");
        assert_eq!(args.out, "recordings/polymarket/");
        assert_eq!(args.markets, vec!["m1:y1:n1"]);
        assert_eq!(args.heartbeat_secs, 20);
        assert_eq!(args.flush_every, 7);
        assert_eq!(args.max_runtime_secs, Some(120));
        assert_eq!(args.ws_url.as_deref(), Some("wss://example.com/ws"));
    }
}
