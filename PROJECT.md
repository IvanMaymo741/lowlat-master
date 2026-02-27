# Project: Market Microstructure Trading Engine
## Focus: Polymarket (Prediction Markets)

---

## 0.4 Chat Management & Context Preservation (CRITICAL)

This project is developed using:
- Codex (or similar) for code generation
- ChatGPT (web) for planning, analysis, and decision-making

To avoid performance issues, context loss, and degraded reasoning quality, the following rules are mandatory:

### 0.4.1 Chat Rotation Rule
- The ChatGPT conversation MUST be restarted every 2–3 major exchanges or whenever the chat becomes long/heavy.
- Long-running chats are NOT allowed to be the primary context holder.

### 0.4.2 New Chat Bootstrap Requirement
Whenever a new chat is started, the FIRST message must include:
1. The full content of PROJECT.md (pasted entirely).
2. A short recap of the current active step (1–3 bullets max).
3. The exact prompt to continue from the previous chat.

### 0.4.3 Assistant Responsibility
The assistant MUST:
- Explicitly warn when the chat is getting long and suggest starting a new chat.
- Provide a ready-to-copy bootstrap prompt for the new chat, including:
  - Reference to PROJECT.md
  - Current objective
  - Immediate next action

### 0.4.4 Context Escalation Rule
If any new decision, constraint, failure mode, architectural insight, or workflow rule appears that would cause confusion if lost:
- It MUST be added to PROJECT.md under the appropriate section
- Especially under:
  - "Critical Context Registry"
  - "Decisions & Rejections"
  - "Current Constraints"

The chat must NEVER be the only place where important context exists.

### 0.4.5 Language Policy (MANDATORY)
- All ChatGPT conversations for this project MUST be conducted in Spanish.
- This applies regardless of:
  - The language of PROJECT.md
  - The language of prompts sent to Codex
  - The language used in code comments or commit messages
- If a new chat starts responding in a different language, the assistant must immediately switch back to Spanish without being asked.
- Spanish is the canonical language for:
  - Planning
  - Analysis
  - Explanations
  - Decisions

### 0.4.6 PROJECT.md Update Delivery Rule (NEW — MANDATORY)
Whenever the assistant proposes **any modification** to `PROJECT.md`:
- The assistant MUST provide the **ENTIRE updated PROJECT.md file**
- It must be sent **fully pasted in chat**, ready to:
  - Copy
  - Paste
  - Replace the previous version
- Partial patches, diffs, or “add this block” instructions are **not allowed**

This rule exists to:
- Prevent merge or copy errors
- Ensure PROJECT.md remains the single canonical source of truth
- Allow clean bootstrapping of new chats without ambiguity

---

## 1. High-level Goal

Build a **monetizable trading bot** that exploits **structural inefficiencies** in prediction markets (Polymarket), using orderbook imbalance, belief drift, and slow human-driven liquidity.

The project prioritizes:
- Correct reasoning over speed
- Data-driven decisions
- Reusable architecture
- Avoiding premature infrastructure investment

---

## 2. Project Principles

- Hypotheses must be validated or closed using data
- No optimization without proven edge
- Separate **signal quality** from **execution quality**
- Use backtests and replays before risking capital
- Favor slow, inefficient markets over ultra-competitive ones

---

## 2.1 Working Agreement (Chat & Project Memory)

This project follows a strict separation between:
- **Chat sessions** → reasoning, analysis, interpretation, planning
- **PROJECT.md** → single source of truth and long-term memory

### Rule:
Whenever a hypothesis is closed, a decision is made, or the project direction changes,
**the chat must produce an updated version of `PROJECT.md` (full file, not a patch).**

Chat conversations are not considered authoritative memory.  
`PROJECT.md` is the canonical reference for project state.

If there is a mismatch between chat and `PROJECT.md`,
**`PROJECT.md` always wins**.

---

## 2.2 Critical Context Registry

The project maintains an explicit registry of **critical context**.

Critical context is defined as any information that, if lost, could:
- Lead to incorrect strategic decisions
- Cause repetition of already-closed hypotheses
- Invalidate conclusions drawn from past experiments
- Create false expectations about feasibility or monetization

### Registered Critical Context

#### Polymarket Ingestion Architecture Decision
- Decision: Market data ingestion for Polymarket will use a **dedicated binary**
  `polymarket_md_recorder`
- Rationale:
  - Avoid overloading generic recorders
  - Keep adapter responsibilities clean
  - Enable focused debugging and replay
- Constraint:
  - **No trading or execution logic** in this phase
  - Only streaming ingestion + `.ndjson` persistence

---

## 2.3 Current Project State

- Current focus: **Polymarket market data ingestion**
- Dedicated Polymarket recorder binary planned and required
- Binance spot market exploration is concluded
- No production capital is deployed
- All trading so far is experimental / research-only
- Infrastructure is local; no cloud optimization in progress
- Priority is edge discovery, not speed optimization

---

## 2.4 Optimization Rule

No infrastructure, latency, or micro-parameter optimization
should be pursued unless a clear and testable edge
has already been identified.

Speed amplifies edges.  
Speed does not create edges.

---

## 3. Current Status (TL;DR)

- Core trading engine implemented and stable
- Binance BTCUSDT maker strategy **evaluated and rejected**
- Strategic pivot to **Polymarket**
- Polymarket integration active
- Current task: **build dedicated Polymarket market data recorder**

---

## 4. Closed Hypotheses (Very Important)

### ❌ Binance BTCUSDT maker passive strategy is monetizable from local
**Status:** CLOSED  
**Reason:**
- 0 fills in backtest and live tests
- Market too efficient
- Fees + queue position + latency eliminate edge

This is a confirmed data-driven conclusion.

---

## 5. Active Hypotheses

### 🟢 Polymarket belief drift + orderbook imbalance is exploitable
**Status:** ACTIVE  
**Rationale:**
- Markets are slower and less efficient
- Orderbooks are sparse
- Participants are mostly humans
- Belief updates are delayed and discrete
- Maker liquidity provision is viable

---

## 6. Architecture Overview

### 6.1 Core Engine (Reusable)
- Event-driven signal evaluation
- Cooldown / rearm logic
- Risk controls
- Order lifecycle management
- Metrics + JSON summaries
- Backtesting and parameter sweeps

### 6.2 Market Adapters
- `binance/` (kept for research, not production)
- `polymarket/` (current focus)

Each adapter is responsible ONLY for:
- Market data ingestion
- Orderbook normalization
- Market-specific metrics

---

## 7. Polymarket Strategy (Initial Design)

### Strategy Type
**Liquidity-aware belief capture (maker-oriented)**

### Signal Components
- YES / NO orderbook imbalance
- Spread percentage
- Depth asymmetry
- Liquidity vacuum detection

### Time Horizon
- Minutes to hours (not seconds)

### Execution Style
- Maker orders
- Limited aggressiveness
- Cancel when belief signal weakens
- Exit on target, belief reversal, or timeout

---

## 8. Metrics That Matter

- Fill rate
- Time-to-fill
- Adverse belief move after fill
- PnL per market
- Drawdown per market
- Inventory exposure
- Capital at risk per market

Latency micro-optimizations are **explicitly not a focus**.

---

## 9. Roadmap

### Phase 1 — Polymarket Data
- Market data adapter
- Orderbook normalization
- Recorder (.ndjson)
  - Dedicated binary: `polymarket_md_recorder`
- Replay offline

### Phase 2 — Signal Validation
- Adapt signal to Polymarket
- Backtest belief drift strategies
- Parameter sweeps

### Phase 3 — Paper Trading
- Simulated execution
- Risk limits per market
- Kill switches

### Phase 4 — Real Capital (Small)
- Gradual rollout
- Many markets, small size
- Scale breadth, not size

---

## 10. Non-Goals (Explicit)

- Competing with HFT firms
- Ultra-low latency trading
- Scalping BTC/ETH spot
- Complex ML models (for now)

---

## 11. Open Questions

- Best Polymarket market types to target?
- Optimal holding duration?
- How to model belief drift quantitatively?
- Exit strategy design (fixed target vs belief-based)?

---

## 12. Last Updated
- Date: 2026-02-27
- Context: Polymarket ingestion — dedicated recorder + PROJECT.md full-file rule enforced