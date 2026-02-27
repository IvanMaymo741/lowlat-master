use anyhow::{Context, Result};
use tracing::{info, warn};

use lowlat_bot::config::AppConfig;
use lowlat_bot::execution::{LiveExecutor, OrderRequest, OrderType, Side};
use lowlat_bot::marketdata::spawn_marketdata;
use lowlat_bot::orderbook::OrderBook;
use lowlat_bot::risk::{RiskDecision, RiskManager};
use lowlat_bot::strategy::{MidReversionStrategy, Strategy};
use lowlat_bot::telemetry;

#[tokio::main]
async fn main() -> Result<()> {
    let config_path = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "config/default.toml".to_string());

    let cfg = AppConfig::load_from_path(&config_path)
        .with_context(|| format!("failed to load config from {}", config_path))?;

    telemetry::init("live_bot");
    info!(testnet = cfg.execution.testnet, venue = %cfg.execution.venue, "starting live_bot");

    let executor = LiveExecutor::new(cfg.execution.testnet, cfg.execution.venue.clone());
    let mut rx = spawn_marketdata(&cfg.marketdata, cfg.general.symbol.clone())
        .context("failed to start market data source")?;
    let mut book = OrderBook::new(cfg.general.symbol.clone());
    let mut strategy = MidReversionStrategy::new(cfg.strategy.threshold_bps);
    let risk = RiskManager::new(
        cfg.risk.max_position,
        cfg.risk.max_order_qty,
        cfg.risk.max_notional,
    );

    let mut position = 0.0;
    for _ in 0..cfg.general.max_events {
        let Some(event) = rx.recv().await else { break };
        book.apply(&event);

        let signal = strategy.on_book(&book);
        let Some(side) = Side::from_signal(signal) else {
            continue;
        };

        let Some(mid) = book.mid_price() else {
            continue;
        };
        let qty = cfg.strategy.order_qty;

        match risk.validate(side, qty, mid, position) {
            RiskDecision::Allow => {}
            RiskDecision::Reject(reason) => {
                warn!(%reason, "order blocked by risk");
                continue;
            }
        }

        let request = OrderRequest {
            symbol: cfg.general.symbol.clone(),
            side,
            qty,
            price: mid,
            order_type: OrderType::Limit,
        };

        let fill = executor.execute(&request).await?;
        position += fill.side.signed_qty(fill.qty);

        info!(
            symbol = %fill.symbol,
            px = fill.price,
            pos = position,
            "live(testnet) fill"
        );
    }

    info!(final_position = position, "live_bot finished");
    Ok(())
}
