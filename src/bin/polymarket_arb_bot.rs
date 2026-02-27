use std::collections::VecDeque;
use std::env;
use std::path::PathBuf;
use std::time::{Duration, Instant};

use anyhow::Context;
use lowlat_bot::config::AppConfig;
use lowlat_bot::marketdata::spawn_marketdata;
use lowlat_bot::strategy::{detect_polymarket_arb, PolymarketArbThresholds};
use lowlat_bot::telemetry;
use tracing::{info, warn};

#[derive(Debug)]
struct Args {
    config: PathBuf,
    min_edge_bps: f64,
    min_depth: f64,
    cooldown_ms: u64,
    top_k: usize,
}

fn parse_args() -> anyhow::Result<Args> {
    let mut config = PathBuf::from("config/default.toml");
    let mut min_edge_bps = 8.0;
    let mut min_depth = 0.0;
    let mut cooldown_ms = 75_u64;
    let mut top_k = 20_usize;

    let mut args = env::args().skip(1);
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--config" => {
                let value = args.next().context("missing value for --config")?;
                config = PathBuf::from(value);
            }
            "--min-edge-bps" => {
                let value = args.next().context("missing value for --min-edge-bps")?;
                min_edge_bps = value.parse().with_context(|| {
                    format!("invalid --min-edge-bps '{}': expected number", value)
                })?;
            }
            "--min-depth" => {
                let value = args.next().context("missing value for --min-depth")?;
                min_depth = value
                    .parse()
                    .with_context(|| format!("invalid --min-depth '{}': expected number", value))?;
            }
            "--cooldown-ms" => {
                let value = args.next().context("missing value for --cooldown-ms")?;
                cooldown_ms = value.parse().with_context(|| {
                    format!("invalid --cooldown-ms '{}': expected integer", value)
                })?;
            }
            "--top-k" => {
                let value = args.next().context("missing value for --top-k")?;
                top_k = value
                    .parse()
                    .with_context(|| format!("invalid --top-k '{}': expected integer", value))?;
            }
            "-h" | "--help" => {
                print_help();
                std::process::exit(0);
            }
            other => anyhow::bail!("unknown arg '{}'; use --help", other),
        }
    }

    Ok(Args {
        config,
        min_edge_bps,
        min_depth,
        cooldown_ms,
        top_k: top_k.max(1),
    })
}

fn print_help() {
    println!(
        "polymarket_arb_bot\n\nUSAGE:\n  cargo run --release --bin polymarket_arb_bot -- [options]\n\nOPTIONS:\n  --config <path>          Ruta TOML (default config/default.toml)\n  --min-edge-bps <bps>     Edge mínimo para emitir señal (default 8.0)\n  --min-depth <qty>        Profundidad mínima yes/no para filtrar (default 0)\n  --cooldown-ms <ms>       Cooldown para no spamear misma señal (default 75)\n  --top-k <n>              Mantener ranking de mejores señales (default 20)\n"
    );
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    telemetry::init("polymarket_arb_bot");
    let args = parse_args()?;

    let cfg = AppConfig::load_from_path(&args.config)
        .with_context(|| format!("failed to load config {}", args.config.display()))?;

    if !cfg.marketdata.mode.eq_ignore_ascii_case("polymarket_ws") {
        anyhow::bail!(
            "marketdata.mode must be 'polymarket_ws' for this binary (current '{}')",
            cfg.marketdata.mode
        );
    }

    let thresholds = PolymarketArbThresholds {
        min_edge_bps: args.min_edge_bps,
        min_depth: args.min_depth,
    };

    info!(
        symbol = %cfg.general.symbol,
        market_id = %cfg.marketdata.polymarket_market_id,
        min_edge_bps = thresholds.min_edge_bps,
        min_depth = thresholds.min_depth,
        cooldown_ms = args.cooldown_ms,
        "starting polymarket arb scanner"
    );

    let mut rx = spawn_marketdata(&cfg.marketdata, cfg.general.symbol.clone())
        .context("failed to spawn marketdata")?;

    let cooldown = Duration::from_millis(args.cooldown_ms.max(1));
    let mut last_emit = Instant::now() - cooldown;
    let mut best_edges: VecDeque<f64> = VecDeque::with_capacity(args.top_k);

    while let Some(event) = rx.recv().await {
        let Some(opp) = detect_polymarket_arb(&event, &thresholds) else {
            continue;
        };

        let now = Instant::now();
        if now.duration_since(last_emit) < cooldown {
            continue;
        }
        last_emit = now;

        best_edges.push_back(opp.edge_bps);
        while best_edges.len() > args.top_k {
            best_edges.pop_front();
        }

        let best_seen_bps = best_edges
            .iter()
            .copied()
            .fold(f64::NEG_INFINITY, f64::max)
            .max(opp.edge_bps);

        info!(
            ts_ms = event.ts_ms,
            side = ?opp.side,
            edge_bps = opp.edge_bps,
            edge_pct = opp.edge_pct,
            yes_price = opp.yes_price,
            no_price = opp.no_price,
            bundle_price = opp.bundle_price,
            executable_depth = ?opp.executable_depth,
            recv_to_parse_us = (event.parsed_ts_ms.saturating_sub(event.recv_ts_ms)) * 1_000,
            parse_latency_us = event.parse_latency_us,
            inter_msg_us = ?event.inter_msg_us,
            dropped_events = event.dropped_events,
            best_seen_bps,
            "arb opportunity"
        );

        if event.dropped_events > 0 {
            warn!(
                dropped_events = event.dropped_events,
                "upstream drops detected"
            );
        }
    }

    Ok(())
}
