use serde::Deserialize;
use std::fs;
use std::path::Path;
use thiserror::Error;

#[derive(Debug, Clone, Deserialize)]
pub struct AppConfig {
    pub general: GeneralConfig,
    pub marketdata: MarketDataConfig,
    pub strategy: StrategyConfig,
    pub risk: RiskConfig,
    pub execution: ExecutionConfig,
}

#[derive(Debug, Clone, Deserialize)]
pub struct GeneralConfig {
    pub symbol: String,
    #[serde(default = "default_general_duration_s")]
    pub duration_s: u64,
    pub max_events: usize,
    pub md_output_path: String,
}

impl GeneralConfig {
    pub fn duration_s(&self) -> u64 {
        self.duration_s.max(1)
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct MarketDataConfig {
    #[serde(default = "default_marketdata_mode", alias = "source")]
    pub mode: String,
    #[serde(default = "default_marketdata_interval_ms")]
    pub interval_ms: u64,
    #[serde(default = "default_binance_ws_url")]
    pub ws_url: String,
    #[serde(default = "default_binance_rest_url")]
    pub rest_url: String,
    #[serde(default = "default_depth_stream")]
    pub depth_stream: String,
    #[serde(default = "default_snapshot_limit")]
    pub snapshot_limit: u32,
    #[serde(default = "default_reconnect_backoff_ms")]
    pub reconnect_backoff_ms: u64,
    #[serde(default = "default_ping_interval_s")]
    pub ping_interval_s: u64,
    #[serde(default = "default_channel_buffer")]
    pub channel_buffer: usize,
    #[serde(default = "default_polymarket_ws_url")]
    pub polymarket_ws_url: String,
    #[serde(default)]
    pub polymarket_market_id: String,
    #[serde(default)]
    pub polymarket_yes_asset_id: String,
    #[serde(default)]
    pub polymarket_no_asset_id: String,
    #[serde(default = "default_polymarket_depth_levels")]
    pub polymarket_depth_levels: usize,
}

impl MarketDataConfig {
    pub fn channel_buffer(&self) -> usize {
        self.channel_buffer.max(1)
    }

    pub fn reconnect_backoff_ms(&self) -> u64 {
        self.reconnect_backoff_ms.max(1)
    }

    pub fn ping_interval_s(&self) -> u64 {
        self.ping_interval_s.max(1)
    }

    pub fn snapshot_limit(&self) -> u32 {
        self.snapshot_limit.clamp(1, 5_000)
    }

    pub fn polymarket_depth_levels(&self) -> usize {
        self.polymarket_depth_levels.clamp(1, 100)
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct StrategyConfig {
    pub threshold_bps: f64,
    pub order_qty: f64,
}

#[derive(Debug, Clone, Deserialize)]
pub struct RiskConfig {
    pub max_position: f64,
    pub max_order_qty: f64,
    pub max_notional: f64,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ExecutionConfig {
    #[serde(default = "default_execution_testnet")]
    pub testnet: bool,
    #[serde(default = "default_execution_venue")]
    pub venue: String,
    #[serde(default = "default_execution_enable_user_stream")]
    pub enable_user_stream: bool,
    #[serde(default = "default_execution_transport")]
    pub transport: String,
    #[serde(default = "default_execution_rest_url")]
    pub rest_url: String,
    #[serde(default = "default_execution_ws_url")]
    pub ws_url: String,
    #[serde(default = "default_execution_ws_api_url")]
    pub ws_api_url: String,
    #[serde(default = "default_execution_timeout_ms")]
    pub timeout_ms: u64,
    #[serde(default = "default_execution_recv_window_ms")]
    pub recv_window_ms: u64,
    #[serde(default = "default_execution_rate_limit_per_s")]
    pub rate_limit_per_s: u32,
    #[serde(default = "default_execution_cancel_mode")]
    pub cancel_mode: String,
    #[serde(default = "default_execution_wait_cancel_ms")]
    pub wait_cancel_ms: u64,
    #[serde(default = "default_execution_adaptive_mid_move_bps")]
    pub adaptive_mid_move_bps: f64,
    #[serde(default = "default_execution_adaptive_spread_change_bps")]
    pub adaptive_spread_change_bps: f64,
}

impl ExecutionConfig {
    pub fn timeout_ms(&self) -> u64 {
        self.timeout_ms.max(100)
    }

    pub fn recv_window_ms(&self) -> u64 {
        self.recv_window_ms.max(1)
    }

    pub fn rate_limit_per_s(&self) -> u32 {
        self.rate_limit_per_s.max(1)
    }

    pub fn wait_cancel_ms(&self) -> u64 {
        self.wait_cancel_ms
    }

    pub fn adaptive_mid_move_bps(&self) -> f64 {
        self.adaptive_mid_move_bps.max(0.0)
    }

    pub fn adaptive_spread_change_bps(&self) -> f64 {
        self.adaptive_spread_change_bps.max(0.0)
    }
}

#[derive(Debug, Error)]
pub enum ConfigError {
    #[error("could not read config file: {0}")]
    Io(#[from] std::io::Error),
    #[error("could not parse config TOML: {0}")]
    Parse(#[from] toml::de::Error),
}

impl AppConfig {
    pub fn load_from_path(path: impl AsRef<Path>) -> Result<Self, ConfigError> {
        let raw = fs::read_to_string(path)?;
        let cfg = toml::from_str(&raw)?;
        Ok(cfg)
    }
}

fn default_marketdata_mode() -> String {
    "mock".to_string()
}

fn default_general_duration_s() -> u64 {
    120
}

fn default_marketdata_interval_ms() -> u64 {
    25
}

fn default_binance_ws_url() -> String {
    "wss://stream.binance.com:9443/ws".to_string()
}

fn default_binance_rest_url() -> String {
    "https://api.binance.com".to_string()
}

fn default_depth_stream() -> String {
    "depth@100ms".to_string()
}

fn default_snapshot_limit() -> u32 {
    1_000
}

fn default_reconnect_backoff_ms() -> u64 {
    500
}

fn default_ping_interval_s() -> u64 {
    15
}

fn default_channel_buffer() -> usize {
    4_096
}

fn default_polymarket_ws_url() -> String {
    "wss://ws-subscriptions-clob.polymarket.com/ws/market".to_string()
}

fn default_polymarket_depth_levels() -> usize {
    5
}

fn default_execution_testnet() -> bool {
    true
}

fn default_execution_venue() -> String {
    "binance_testnet".to_string()
}

fn default_execution_transport() -> String {
    "rest".to_string()
}

fn default_execution_enable_user_stream() -> bool {
    true
}

fn default_execution_rest_url() -> String {
    "https://testnet.binance.vision".to_string()
}

fn default_execution_ws_url() -> String {
    "wss://stream.testnet.binance.vision/ws".to_string()
}

fn default_execution_ws_api_url() -> String {
    "wss://ws-api.testnet.binance.vision/ws-api/v3".to_string()
}

fn default_execution_timeout_ms() -> u64 {
    2_000
}

fn default_execution_recv_window_ms() -> u64 {
    5_000
}

fn default_execution_rate_limit_per_s() -> u32 {
    8
}

fn default_execution_cancel_mode() -> String {
    "always".to_string()
}

fn default_execution_wait_cancel_ms() -> u64 {
    200
}

fn default_execution_adaptive_mid_move_bps() -> f64 {
    3.0
}

fn default_execution_adaptive_spread_change_bps() -> f64 {
    2.0
}
