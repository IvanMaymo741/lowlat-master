# lowlat_bot

Scaffold inicial de un bot de trading modular en Rust con 9 binarios:

- `md_recorder`: solo captura/guarda market data.
- `paper_bot`: simula estrategia + ejecución paper.
- `live_bot`: loop de estrategia con ejecución testnet (stub).
- `exec_smoke`: smoke test de ejecución real sobre Binance Spot testnet.
- `signal_replay`: replay offline de señales sobre recording CSV.
- `signal_live`: runner de señales live en testnet (modo seguro).
- `backtest_runner`: backtest offline con modelo maker-fill + PnL sobre recording NDJSON/CSV.
- `backtest_sweep`: grid search offline (discount/min_rest) con ranking por PnL y sharpe-like.
- `polymarket_md_recorder`: recorder dedicado para Polymarket (NDJSON, multi-market, heartbeat).
- `polymarket_arb_bot`: scanner ultra-liviano de ineficiencias YES/NO (edge por bundle y ranking).

## Stack

- Tokio async
- tracing + tracing-subscriber
- serde + toml
- anyhow + thiserror

## Estructura

- `src/lib.rs`: expone módulos
- `src/config`: carga/parseo de TOML
- `src/marketdata`: fuente `mock`, `binance_ws` o `polymarket_ws` con reconexión automática
- `src/orderbook`: estado local de libro (best bid/ask)
- `src/strategy`: estrategias (mid reversion + signal runner por imbalance/microprice)
- `src/execution`: paper executor + cliente Binance testnet firmado (HMAC)
- `src/risk`: validaciones de riesgo pre-trade
- `src/telemetry`: inicialización de tracing
- `src/util`: utilidades (timestamps)
- `src/bin/*`: binarios `md_recorder`, `paper_bot`, `live_bot`, `exec_smoke`, `signal_replay`, `signal_live`, `backtest_runner`, `backtest_sweep`, `polymarket_md_recorder`

## Config

Archivo ejemplo: `config/default.toml`

Campos relevantes para `general`:

- `symbol`: símbolo a consumir (ej. `BTCUSDT`)
- `duration_s`: duración default para `md_recorder` (overrideable por `--duration-s`)
- `md_output_path`: salida default para `md_recorder` (overrideable por `--out`)

Campos relevantes para `marketdata`:

- `mode`: `"mock"`, `"binance_ws"` o `"polymarket_ws"`
- `ws_url`: URL base de websocket de Binance
- `rest_url`: URL base REST para snapshot de profundidad
- `depth_stream`: sufijo stream de profundidad (ej. `depth@100ms`)
- `snapshot_limit`: profundidad pedida al endpoint `/api/v3/depth`
- `reconnect_backoff_ms`: backoff fijo de reconexión
- `ping_interval_s`: intervalo de ping websocket
- `channel_buffer`: tamaño de `tokio::sync::mpsc`
- `polymarket_ws_url`: URL websocket de Polymarket CLOB
- `polymarket_market_id`: mercado (opcional, se envía en subscribe si está seteado)
- `polymarket_yes_asset_id`: asset id YES (requerido en `polymarket_ws`)
- `polymarket_no_asset_id`: asset id NO (requerido en `polymarket_ws`)
- `polymarket_depth_levels`: profundidad agregada para métricas YES/NO

En `binance_ws`, el bot conecta a: `{symbol.to_lowercase()}@{depth_stream}`.
En `polymarket_ws`, el bot se suscribe al canal `Market` y normaliza ticks en lados YES/NO con métricas `yes_no_imbalance`, `spread_pct` y `depth_ratio`.

Campos relevantes para `execution`:

- `enable_user_stream`: `true`/`false` (default `true`, pero con `execution.testnet=true` se fuerza `false`)
- `transport`: `"rest"` o `"ws_api"`
- `rest_url`: base REST (`https://testnet.binance.vision`)
- `ws_url`: base WS de user-data stream (`wss://stream.testnet.binance.vision/ws`)
- `ws_api_url`: base WS-API de trading (`wss://ws-api.testnet.binance.vision/ws-api/v3`)

## Cómo correr

Requisitos: Rust toolchain estable (`cargo`, `rustc`).

```bash
# solo market data (120s por default.toml)
cargo run --bin md_recorder -- config/default.toml

# override por CLI: duración real + out dir (genera .ndjson)
cargo run --bin md_recorder -- --config config/default.toml --duration-s 120 --out recordings/

# ejemplo Polymarket (setear mode + asset ids en TOML)
cargo run --bin md_recorder -- --config config/default.toml --duration-s 120 --out recordings/polymarket/

# recorder dedicado Polymarket (mercado + asset ids por flag)
cargo run --bin polymarket_md_recorder -- \
  --config config/default.toml \
  --market "<market_id>:<yes_asset_id>:<no_asset_id>" \
  --out data/polymarket/ \
  --heartbeat-secs 30 \
  --flush-every 10


# scanner de arbitraje YES/NO (solo detección, sin ejecución automática)
cargo run --release --bin polymarket_arb_bot -- \
  --config config/default.toml \
  --min-edge-bps 8 \
  --min-depth 100 \
  --cooldown-ms 75 \
  --top-k 20

# debug de ingest sin I/O a disco
cargo run --bin md_recorder -- --config config/default.toml --duration-s 30 --out recordings/ --dry-run

# simulación paper
cargo run --bin paper_bot -- config/default.toml

# testnet (stub de integración)
cargo run --bin live_bot -- config/default.toml

# smoke de execution testnet (real)
BINANCE_API_KEY=... BINANCE_API_SECRET=... \
  cargo run --bin exec_smoke -- config/default.toml

# smoke por WS-API (poner execution.transport="ws_api" en TOML)
BINANCE_API_KEY=... BINANCE_API_SECRET=... \
  cargo run --bin exec_smoke -- config/default.toml

# replay offline de señales (acepta CSV o NDJSON)
cargo run --bin signal_replay -- --input data/md_ticks.csv

# runner de señales en testnet (safe mode)
BINANCE_API_KEY=... BINANCE_API_SECRET=... \
  cargo run --bin signal_live -- --config config/default.toml

# backtest offline con fills + PnL sobre recording NDJSON
cargo run --bin backtest_runner -- \
  recordings/btcusdt_123.ndjson \
  --out-json runs/backtest.json \
  --discount-bps 5 \
  --min-rest-ms 250 \
  --adverse-mid-move-bps 5 \
  --ack-delay-ms 0 \
  --cancel-delay-ms 0 \
  --fees-bps 1.0 \
  --slippage-bps 0.0

# sweep de parámetros (grid) y ranking por total_pnl / sharpe-like
cargo run --bin backtest_sweep -- \
  recordings/btcusdt_123.ndjson \
  --discount-bps-grid 1,3,5,7 \
  --min-rest-ms-grid 0,250,500 \
  --out-json runs/backtest_sweep.json \
  --out-csv runs/backtest_sweep.csv
```

`exec_smoke` también acepta flags:

- `--qty` (qty por orden, default `strategy.order_qty`)
- `--iterations` / `--iters` (default `200`)
- `--warmup-iterations` (default `10`, no cuenta en stats globales)
- `--cancel-mode` (`always` | `never` | `adaptive`, default `execution.cancel_mode`)
- `--wait-cancel-ms` (default `execution.wait_cancel_ms`)
- `--discount-bps` (acepta valor único, ej. `3`, o rango, ej. `1..5`; default `1000`)
- `--liquidity-mode` (`maker` | `post_only_maker` | `taker`, default `maker`)
- `--transport` (`rest` | `ws_api`, default `execution.transport`)
- `--summary-window` (default `10`)
- `--summary-global` (`true|false`, default `true`)
- `--out-json` (default `runs/exec_smoke_<timestamp>.json`)
- `--log-level` (`trace|debug|info|warn|error`)
- `--profile` (activa spans de profiling; con `--features pprof` genera flamegraph SVG)

Métricas de `exec_smoke`:

- Ventana + global: `place_rtt_us`, `cancel_rtt_us`, `place_to_ack_us`, `cancel_to_ack_us`, `rx_to_decide_us`, `decide_to_ack_us`, `rx_to_ack_us`
- Conteos de error: `place_errors`, `cancel_errors`, `ack_timeouts`, `ws_disconnects`
- Iteraciones fallidas (con `reason`, `http_status`, `binance_code`, `error_kind`) en JSON final

## Benchmark local reproducible

REST baseline:

```bash
BINANCE_API_KEY=... BINANCE_API_SECRET=... \
cargo run --release --bin exec_smoke -- \
  --config config/default.toml \
  --transport rest \
  --iterations 200 \
  --warmup-iterations 10 \
  --discount-bps 1..5 \
  --liquidity-mode maker \
  --out-json runs/rest_baseline.json
```

WS-API:

```bash
BINANCE_API_KEY=... BINANCE_API_SECRET=... \
cargo run --release --bin exec_smoke -- \
  --config config/default.toml \
  --transport ws_api \
  --iterations 200 \
  --warmup-iterations 10 \
  --discount-bps 1..5 \
  --liquidity-mode maker \
  --out-json runs/ws_api.json
```

Comparar dos runs JSON (ejemplo con `jq`):

```bash
jq '{run: input_filename, place_p95: .stats_global.place_rtt_us.p95, cancel_p95: .stats_global.cancel_rtt_us.p95, ack_p95: .stats_global.place_to_ack_us.p95, success: .iteration_counts.succeeded_measured, place_errors: .errors.measured.place_errors, cancel_errors: .errors.measured.cancel_errors, ack_timeouts: .errors.measured.ack_timeouts}' runs/rest_baseline.json runs/ws_api.json
```

`md_recorder` toma `general.duration_s` y `general.md_output_path` como defaults, pero CLI tiene precedencia:

- `--duration-s`: override de duración en segundos (deadline monotónico, tiempo real)
- `--out`: override de salida
- `--dry-run`: no escribe a disco, solo consume/contabiliza eventos

Semántica de `--out`:

- Si apunta a archivo: escribe exactamente ahí (creando parent dirs si faltan)
- Si apunta a directorio: crea un archivo `{symbol}_{timestamp}.ndjson` dentro de ese directorio

Al inicio loguea la config efectiva (`symbol`, `duration_s`, `out_path` final y si `out` se interpretó como file/dir).  
Al finalizar loguea: `written_path`, `lines_written`, `bytes_written`, `flush`, `fsync`, `requested_duration_s`, `actual_elapsed_s`.

`md_recorder` reporta cada 5s:

- p50/p95/p99 de tiempo entre mensajes
- p50/p95/p99 de tiempo de parseo
- `dropped_events` por backpressure de marketdata/canal de escritura
- `best_bid`/`best_ask`/`mid_price`/`spread_bps`

La escritura de salida (CSV o NDJSON según extensión) se hace con `BufWriter` en una task dedicada (flush cada 1s o 500 líneas).

## Signal Runner

`strategy` incluye un runner simple por señal en cada update:

- `imbalance = (bid_qty - ask_qty) / (bid_qty + ask_qty)`
- `microprice = (bid*ask_qty + ask*bid_qty) / (bid_qty + ask_qty)`
- `tilt_bps = ((microprice - mid) / mid) * 10_000`
- snapshot: `SignalSnapshot { ts, imbalance, tilt_bps, spread_bps, mid }`

Reglas por defecto:

- long trigger: `imbalance > 0.20` y `tilt_bps > 0.5`
- short trigger: `imbalance < -0.20` y `tilt_bps < -0.5`
- cooldown: `1500ms`
- cancel: cuando la señal deja de favorecer (neutral/adversa)
- hysteresis/re-arm: `ARMED -> IN_TRADE -> WAIT_REARM -> ARMED`; tras entrar, no permite nueva entrada hasta observar reset (`abs(imbalance) < imbalance_exit` o `abs(tilt_bps) < tilt_exit_bps`)

Flags principales (`signal_replay` y `signal_live`):

- `--imbalance-enter 0.20 --tilt-enter-bps 0.5`
- `--imbalance-exit 0.10 --tilt-exit-bps 0.2`
- `--cooldown-ms 1500`
- `--out-json <path>`

Flags extra de `signal_live`:

- `--run-for-s 120` (alias `--duration-s`; si no se pasa usa `general.duration_s`)
- `--warmup-s 5` (fase de warmup: conecta y permite acciones, pero excluye sus acciones/RTT de métricas)
- `--min-rest-ms 0` (resting mínimo antes de cancelar por señal neutral; adverse/kill/error cancelan inmediato)
- `--max-actions-per-minute 30`
- `--discount-bps 1000` (maker lejos para minimizar fills)
- métricas extra: `rearm_wait_skips` y `state_transitions` en JSON de `signal_replay`/`signal_live`
- runtime summary en JSON: `requested_run_for_s`, `actual_elapsed_s`, `exit_reason` (`timeout|kill_switch|errors|sigint`)
- warmup summary en JSON: `warmup_s`, `warmup_actions_excluded`, `place_rtt_us_excluding_warmup`, `cancel_rtt_us_excluding_warmup`
- rollup por minuto en `signal_live`: `minute_buckets[]` con `triggers`, `entries`, `cancels`, `neutral_cancels`, `adverse_cancels`, `delayed_neutral_cancels`, `fills`, `filled_qty`, `rate_limit_skips`, `avg_mid`, `avg_spread_bps`, `avg_time_in_trade_ms`, `avg_time_to_fill_ms`, `avg_rest_ms`, `distance_to_best_bps(p50/p95/p99)`, `place_rtt_us(p50/p95/p99)`, `cancel_rtt_us(p50/p95/p99)`
- fill/adverse/cancel summary en JSON (`signal_live`): `event_counters(order_ack,cancel_ack,trade_fill)`, `counters(fills,partial_fills,filled_qty_total)`, `min_rest_ms`, `neutral_cancel_delayed_count`, `avg_rest_ms`, `cancels_by_reason(neutral,adverse,kill,error)`, `avg_time_to_fill_ms`, `time_to_fill_ms(p50/p95)`, `distance_to_best_bps(p50/p95/p99)`, `adverse_bps_1s(p50/p95/p99)`, `adverse_bps_5s(p50/p95/p99)`

Flujo recomendado para validar lógica:

1. Grabar 2 minutos de market data:

```bash
timeout 120s cargo run --release --bin md_recorder -- config/default.toml
```

2. Replay offline sobre ese recording (CSV o NDJSON):

```bash
cargo run --release --bin signal_replay -- \
  --input data/md_ticks.csv \
  --out-json runs/signal_replay.json
```

3. Ejecutar live en testnet (modo seguro):

```bash
BINANCE_API_KEY=... BINANCE_API_SECRET=... \
cargo run --release --bin signal_live -- \
  --config config/default.toml \
  --min-rest-ms 0 \
  --max-actions-per-minute 30 \
  --discount-bps 1000 \
  --out-json runs/signal_live.json
```

## Execution testnet

`execution` soporta Binance Spot testnet (`https://testnet.binance.vision`) con:

- Auth HMAC SHA256 en querystring (`timestamp`, `recvWindow`, `signature`)
- `GET /api/v3/exchangeInfo?symbol=...` para leer `tickSize`/`stepSize` y `minNotional`
- User Data Stream:
  - `POST /api/v3/userDataStream` para `listenKey`
  - `PUT /api/v3/userDataStream` keepalive cada 30 minutos
  - WS `wss://stream.testnet.binance.vision/ws/<listenKey>` (o `execution.ws_url`)
  - si `execution.testnet=true`, el bot lo deshabilita al iniciar y loguea una vez: `testnet user stream not supported; disabled`
  - Si Binance devuelve `404/410` (o respuesta HTML), el bot deshabilita user stream una sola vez y sigue operando sin reintentos de listenKey.
- Endpoints firmados:
  - `POST /api/v3/order` (new limit)
  - `DELETE /api/v3/order` (cancel)
  - `GET /api/v3/order` (query)
- WS-API de trading (Spot Testnet soportado oficialmente):
  - endpoint: `wss://ws-api.testnet.binance.vision/ws-api/v3`
  - métodos: `order.place`, `order.cancel`, `order.status`
  - `execution.transport = "ws_api"` usa `BinanceWsExecutor` con la misma interfaz pública que `BinanceTestnetExecutor`
- Credenciales desde env vars:
  - `BINANCE_API_KEY`
  - `BINANCE_API_SECRET`
- Estado mínimo en `OrderManager` por `clientOrderId`:
  - `PendingNew`, `New`, `PartiallyFilled`, `Filled`, `Canceled`, `Rejected`
- Quantize de órdenes en floor por filtros (`tickSize`, `stepSize`) antes de enviar
- Política de cancel configurable:
  - `execution.cancel_mode = always|never|adaptive`
  - `execution.wait_cancel_ms`
  - `execution.adaptive_mid_move_bps`
  - `execution.adaptive_spread_change_bps`
- Benchmark mode: `execution.cancel_mode=always` + `execution.wait_cancel_ms=0`
- Latencia de execution (p50/p95/p99 cada 10 ops):
  - `before-send` (armado/query/firma/url)
  - `after-send` (roundtrip HTTP o roundtrip WS-API según `execution.transport`)
  - en spikes (`>500ms`) se loguea: `http_status`, `binance_code`, `retry_count`, `rate_limiter_delay_ms`, `dns_cached_inferred`, `connection_reused_inferred`
  - se reporta también cuántas veces `wait_rate_limit()` durmió y cuántos ms acumuló (window + total)
- `place_to_ack_us` / `cancel_to_ack_us`:
  - se reportan cuando user stream está activo
  - con user stream deshabilitado, se usa fallback por polling `GET /api/v3/order` por `origClientOrderId` (timeout corto)
  - pueden quedar en `None` si el fallback no logra observar el estado a tiempo

## CI

Workflow básico en `.github/workflows/ci.yml`:

- `cargo fmt --all -- --check`
- `cargo clippy --all-targets --all-features`
- `cargo test --all-targets --all-features`
