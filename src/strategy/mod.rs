use serde::Serialize;

use crate::orderbook::OrderBook;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Signal {
    Buy,
    Sell,
    Hold,
}

pub trait Strategy {
    fn on_book(&mut self, book: &OrderBook) -> Signal;
}

#[derive(Debug, Clone)]
pub struct MidReversionStrategy {
    threshold_bps: f64,
    anchor: Option<f64>,
}

impl MidReversionStrategy {
    pub fn new(threshold_bps: f64) -> Self {
        Self {
            threshold_bps,
            anchor: None,
        }
    }
}

impl Strategy for MidReversionStrategy {
    fn on_book(&mut self, book: &OrderBook) -> Signal {
        let mid = match book.mid_price() {
            Some(value) => value,
            None => return Signal::Hold,
        };

        let anchor = *self.anchor.get_or_insert(mid);
        if anchor == 0.0 {
            return Signal::Hold;
        }

        let move_bps = ((mid - anchor) / anchor) * 10_000.0;
        if move_bps >= self.threshold_bps {
            Signal::Sell
        } else if move_bps <= -self.threshold_bps {
            Signal::Buy
        } else {
            Signal::Hold
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Serialize)]
pub struct SignalSnapshot {
    pub ts_ms: u128,
    pub imbalance: f64,
    pub tilt_bps: f64,
    pub spread_bps: f64,
    pub mid: f64,
}

pub fn compute_signal_snapshot(
    ts_ms: u128,
    best_bid_price: f64,
    best_bid_qty: f64,
    best_ask_price: f64,
    best_ask_qty: f64,
) -> Option<SignalSnapshot> {
    if !best_bid_price.is_finite()
        || !best_ask_price.is_finite()
        || !best_bid_qty.is_finite()
        || !best_ask_qty.is_finite()
        || best_bid_price <= 0.0
        || best_ask_price <= 0.0
        || best_bid_qty <= 0.0
        || best_ask_qty <= 0.0
    {
        return None;
    }

    let mid = (best_bid_price + best_ask_price) * 0.5;
    if mid <= 0.0 || !mid.is_finite() {
        return None;
    }

    let qty_sum = best_bid_qty + best_ask_qty;
    if qty_sum <= 0.0 || !qty_sum.is_finite() {
        return None;
    }

    let imbalance = (best_bid_qty - best_ask_qty) / qty_sum;
    let microprice = ((best_bid_price * best_ask_qty) + (best_ask_price * best_bid_qty)) / qty_sum;
    let tilt_bps = ((microprice - mid) / mid) * 10_000.0;
    let spread_bps = ((best_ask_price - best_bid_price) / mid) * 10_000.0;

    if !imbalance.is_finite() || !tilt_bps.is_finite() || !spread_bps.is_finite() {
        return None;
    }

    Some(SignalSnapshot {
        ts_ms,
        imbalance,
        tilt_bps,
        spread_bps,
        mid,
    })
}

#[derive(Debug, Clone, Copy)]
pub struct SignalThresholds {
    pub imbalance_enter: f64,
    pub tilt_enter_bps: f64,
    pub imbalance_exit: f64,
    pub tilt_exit_bps: f64,
    pub cooldown_ms: u64,
}

impl Default for SignalThresholds {
    fn default() -> Self {
        Self {
            imbalance_enter: 0.20,
            tilt_enter_bps: 0.5,
            imbalance_exit: 0.10,
            tilt_exit_bps: 0.2,
            cooldown_ms: 1_500,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum SignalDirection {
    Long,
    Short,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum PolymarketArbSide {
    BuyBundle,
    SellBundle,
}

#[derive(Debug, Clone, Copy, PartialEq, Serialize)]
pub struct PolymarketArbOpportunity {
    pub side: PolymarketArbSide,
    pub edge_bps: f64,
    pub edge_pct: f64,
    pub yes_price: f64,
    pub no_price: f64,
    pub bundle_price: f64,
    pub executable_depth: Option<f64>,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct PolymarketArbThresholds {
    pub min_edge_bps: f64,
    pub min_depth: f64,
}

impl Default for PolymarketArbThresholds {
    fn default() -> Self {
        Self {
            min_edge_bps: 5.0,
            min_depth: 0.0,
        }
    }
}

pub fn detect_polymarket_arb(
    snapshot: &crate::marketdata::MarketEvent,
    thresholds: &PolymarketArbThresholds,
) -> Option<PolymarketArbOpportunity> {
    let yes_ask = snapshot.yes_best_ask?;
    let no_ask = snapshot.no_best_ask?;
    let yes_bid = snapshot.yes_best_bid?;
    let no_bid = snapshot.no_best_bid?;

    let min_edge_fraction = thresholds.min_edge_bps.max(0.0) / 10_000.0;

    let executable_depth = snapshot
        .yes_depth
        .zip(snapshot.no_depth)
        .map(|(yes, no)| yes.min(no));

    if let Some(depth) = executable_depth {
        if depth < thresholds.min_depth.max(0.0) {
            return None;
        }
    }

    let buy_bundle_price = yes_ask + no_ask;
    let buy_edge = 1.0 - buy_bundle_price;
    if buy_edge >= min_edge_fraction {
        return Some(PolymarketArbOpportunity {
            side: PolymarketArbSide::BuyBundle,
            edge_bps: buy_edge * 10_000.0,
            edge_pct: buy_edge * 100.0,
            yes_price: yes_ask,
            no_price: no_ask,
            bundle_price: buy_bundle_price,
            executable_depth,
        });
    }

    let sell_bundle_price = yes_bid + no_bid;
    let sell_edge = sell_bundle_price - 1.0;
    if sell_edge >= min_edge_fraction {
        return Some(PolymarketArbOpportunity {
            side: PolymarketArbSide::SellBundle,
            edge_bps: sell_edge * 10_000.0,
            edge_pct: sell_edge * 100.0,
            yes_price: yes_bid,
            no_price: no_bid,
            bundle_price: sell_bundle_price,
            executable_depth,
        });
    }

    None
}

pub fn long_trigger(snapshot: &SignalSnapshot, thresholds: &SignalThresholds) -> bool {
    snapshot.imbalance > thresholds.imbalance_enter && snapshot.tilt_bps > thresholds.tilt_enter_bps
}

pub fn short_trigger(snapshot: &SignalSnapshot, thresholds: &SignalThresholds) -> bool {
    snapshot.imbalance < -thresholds.imbalance_enter
        && snapshot.tilt_bps < -thresholds.tilt_enter_bps
}

pub fn classify_trigger(
    snapshot: &SignalSnapshot,
    thresholds: &SignalThresholds,
) -> Option<SignalDirection> {
    if long_trigger(snapshot, thresholds) {
        return Some(SignalDirection::Long);
    }
    if short_trigger(snapshot, thresholds) {
        return Some(SignalDirection::Short);
    }
    None
}

pub fn neutral_signal(snapshot: &SignalSnapshot, thresholds: &SignalThresholds) -> bool {
    snapshot.imbalance.abs() < thresholds.imbalance_exit
        || snapshot.tilt_bps.abs() < thresholds.tilt_exit_bps
}

fn favors_direction(direction: SignalDirection, snapshot: &SignalSnapshot) -> bool {
    match direction {
        SignalDirection::Long => snapshot.imbalance > 0.0 && snapshot.tilt_bps > 0.0,
        SignalDirection::Short => snapshot.imbalance < 0.0 && snapshot.tilt_bps < 0.0,
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum RunnerAction {
    None,
    Enter(SignalDirection),
    CancelNeutral(SignalDirection),
    CancelAdverse(SignalDirection),
    CooldownSkip(SignalDirection),
    RearmWaitSkip(SignalDirection),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum RunnerPhase {
    Armed,
    InTrade,
    WaitRearm,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum RunnerTransitionReason {
    EnteredTrade,
    CanceledNeutral,
    CanceledAdverse,
    Rearmed,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub struct RunnerTransition {
    pub ts_ms: u128,
    pub from: RunnerPhase,
    pub to: RunnerPhase,
    pub reason: RunnerTransitionReason,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub struct RunnerDecision {
    pub action: RunnerAction,
    pub active_direction: Option<SignalDirection>,
    pub transition: Option<RunnerTransition>,
}

#[derive(Debug, Clone)]
pub struct SignalRunner {
    thresholds: SignalThresholds,
    last_trade_ts_ms: Option<u128>,
    active_direction: Option<SignalDirection>,
    phase: RunnerPhase,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub struct SignalRunnerState {
    pub last_trade_ts_ms: Option<u128>,
    pub active_direction: Option<SignalDirection>,
    pub phase: RunnerPhase,
}

impl SignalRunner {
    pub fn new(thresholds: SignalThresholds) -> Self {
        Self {
            thresholds,
            last_trade_ts_ms: None,
            active_direction: None,
            phase: RunnerPhase::Armed,
        }
    }

    pub fn thresholds(&self) -> &SignalThresholds {
        &self.thresholds
    }

    pub fn active_direction(&self) -> Option<SignalDirection> {
        self.active_direction
    }

    pub fn phase(&self) -> RunnerPhase {
        self.phase
    }

    pub fn last_trade_ts_ms(&self) -> Option<u128> {
        self.last_trade_ts_ms
    }

    pub fn state(&self) -> SignalRunnerState {
        SignalRunnerState {
            last_trade_ts_ms: self.last_trade_ts_ms,
            active_direction: self.active_direction,
            phase: self.phase,
        }
    }

    pub fn restore_state(&mut self, state: SignalRunnerState) {
        self.last_trade_ts_ms = state.last_trade_ts_ms;
        self.active_direction = state.active_direction;
        self.phase = state.phase;
    }

    pub fn on_snapshot(&mut self, snapshot: &SignalSnapshot) -> RunnerDecision {
        match self.phase {
            RunnerPhase::InTrade => self.on_in_trade(snapshot),
            RunnerPhase::WaitRearm => self.on_wait_rearm(snapshot),
            RunnerPhase::Armed => self.on_armed(snapshot),
        }
    }

    fn in_cooldown(&self, now_ts_ms: u128) -> bool {
        let Some(last_trade_ts_ms) = self.last_trade_ts_ms else {
            return false;
        };

        let cooldown_ms = u128::from(self.thresholds.cooldown_ms);
        now_ts_ms.saturating_sub(last_trade_ts_ms) < cooldown_ms
    }

    fn on_in_trade(&mut self, snapshot: &SignalSnapshot) -> RunnerDecision {
        let direction = self
            .active_direction
            .expect("runner invariant: active_direction must exist in InTrade");

        if neutral_signal(snapshot, &self.thresholds) {
            self.active_direction = None;
            self.last_trade_ts_ms = Some(snapshot.ts_ms);
            let transition = self.transition(
                snapshot.ts_ms,
                RunnerPhase::WaitRearm,
                RunnerTransitionReason::CanceledNeutral,
            );
            return RunnerDecision {
                action: RunnerAction::CancelNeutral(direction),
                active_direction: self.active_direction,
                transition: Some(transition),
            };
        }

        if favors_direction(direction, snapshot) {
            return RunnerDecision {
                action: RunnerAction::None,
                active_direction: self.active_direction,
                transition: None,
            };
        }

        self.active_direction = None;
        self.last_trade_ts_ms = Some(snapshot.ts_ms);
        let transition = self.transition(
            snapshot.ts_ms,
            RunnerPhase::WaitRearm,
            RunnerTransitionReason::CanceledAdverse,
        );
        RunnerDecision {
            action: RunnerAction::CancelAdverse(direction),
            active_direction: self.active_direction,
            transition: Some(transition),
        }
    }

    fn on_wait_rearm(&mut self, snapshot: &SignalSnapshot) -> RunnerDecision {
        if neutral_signal(snapshot, &self.thresholds) {
            let transition = self.transition(
                snapshot.ts_ms,
                RunnerPhase::Armed,
                RunnerTransitionReason::Rearmed,
            );
            return RunnerDecision {
                action: RunnerAction::None,
                active_direction: self.active_direction,
                transition: Some(transition),
            };
        }

        let action = match classify_trigger(snapshot, &self.thresholds) {
            Some(direction) => RunnerAction::RearmWaitSkip(direction),
            None => RunnerAction::None,
        };

        RunnerDecision {
            action,
            active_direction: self.active_direction,
            transition: None,
        }
    }

    fn on_armed(&mut self, snapshot: &SignalSnapshot) -> RunnerDecision {
        let Some(trigger_direction) = classify_trigger(snapshot, &self.thresholds) else {
            return RunnerDecision {
                action: RunnerAction::None,
                active_direction: self.active_direction,
                transition: None,
            };
        };

        if self.in_cooldown(snapshot.ts_ms) {
            return RunnerDecision {
                action: RunnerAction::CooldownSkip(trigger_direction),
                active_direction: self.active_direction,
                transition: None,
            };
        }

        self.active_direction = Some(trigger_direction);
        self.last_trade_ts_ms = Some(snapshot.ts_ms);
        let transition = self.transition(
            snapshot.ts_ms,
            RunnerPhase::InTrade,
            RunnerTransitionReason::EnteredTrade,
        );

        RunnerDecision {
            action: RunnerAction::Enter(trigger_direction),
            active_direction: self.active_direction,
            transition: Some(transition),
        }
    }

    fn transition(
        &mut self,
        ts_ms: u128,
        to: RunnerPhase,
        reason: RunnerTransitionReason,
    ) -> RunnerTransition {
        let from = self.phase;
        self.phase = to;
        RunnerTransition {
            ts_ms,
            from,
            to,
            reason,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{
        compute_signal_snapshot, detect_polymarket_arb, MidReversionStrategy, PolymarketArbSide,
        PolymarketArbThresholds, RunnerAction, RunnerPhase, Signal, SignalRunner, SignalSnapshot,
        SignalThresholds, Strategy,
    };
    use crate::marketdata::MarketEvent;
    use crate::orderbook::OrderBook;

    fn snapshot(ts_ms: u128, imbalance: f64, tilt_bps: f64) -> SignalSnapshot {
        SignalSnapshot {
            ts_ms,
            imbalance,
            tilt_bps,
            spread_bps: 2.0,
            mid: 100.0,
        }
    }

    #[test]
    fn emits_hold_before_threshold() {
        let mut strategy = MidReversionStrategy::new(10.0);
        let mut book = OrderBook::new("BTCUSDT");

        book.apply(&MarketEvent::new("BTCUSDT", 99.99, 100.01, 1));
        assert_eq!(strategy.on_book(&book), Signal::Hold);

        book.apply(&MarketEvent::new("BTCUSDT", 100.04, 100.06, 2));
        assert_eq!(strategy.on_book(&book), Signal::Hold);
    }

    #[test]
    fn computes_signal_snapshot_metrics() {
        let signal = compute_signal_snapshot(10, 99.0, 2.0, 101.0, 1.0).expect("valid snapshot");

        assert_eq!(signal.ts_ms, 10);
        assert!((signal.mid - 100.0).abs() < 1e-9);
        assert!((signal.imbalance - (1.0 / 3.0)).abs() < 1e-9);
        assert!((signal.tilt_bps - 33.3333333333).abs() < 1e-6);
    }

    #[test]
    fn signal_runner_applies_entry_exit_and_cooldown() {
        let mut runner = SignalRunner::new(SignalThresholds::default());

        let enter = runner.on_snapshot(&snapshot(1_000, 0.25, 1.0));
        assert_eq!(
            enter.action,
            RunnerAction::Enter(super::SignalDirection::Long)
        );
        assert_eq!(
            runner.active_direction(),
            Some(super::SignalDirection::Long)
        );

        let keep = runner.on_snapshot(&snapshot(1_200, 0.15, 0.3));
        assert_eq!(keep.action, RunnerAction::None);
        assert_eq!(
            runner.active_direction(),
            Some(super::SignalDirection::Long)
        );

        let cancel = runner.on_snapshot(&snapshot(1_600, 0.05, 0.05));
        assert_eq!(
            cancel.action,
            RunnerAction::CancelNeutral(super::SignalDirection::Long)
        );
        assert_eq!(runner.active_direction(), None);
        assert_eq!(runner.phase(), RunnerPhase::WaitRearm);

        let rearm_wait = runner.on_snapshot(&snapshot(2_000, 0.30, 1.2));
        assert_eq!(
            rearm_wait.action,
            RunnerAction::RearmWaitSkip(super::SignalDirection::Long)
        );
        assert_eq!(runner.active_direction(), None);
        assert_eq!(runner.phase(), RunnerPhase::WaitRearm);

        let rearmed = runner.on_snapshot(&snapshot(2_100, 0.02, 0.01));
        assert_eq!(rearmed.action, RunnerAction::None);
        assert_eq!(runner.phase(), RunnerPhase::Armed);

        let cooldown = runner.on_snapshot(&snapshot(2_200, 0.30, 1.2));
        assert_eq!(
            cooldown.action,
            RunnerAction::CooldownSkip(super::SignalDirection::Long)
        );

        let reenter = runner.on_snapshot(&snapshot(3_200, 0.30, 1.2));
        assert_eq!(
            reenter.action,
            RunnerAction::Enter(super::SignalDirection::Long)
        );
        assert_eq!(
            runner.active_direction(),
            Some(super::SignalDirection::Long)
        );
    }

    #[test]
    fn signal_runner_cancels_on_adverse_signal() {
        let mut runner = SignalRunner::new(SignalThresholds::default());

        let enter = runner.on_snapshot(&snapshot(1_000, 0.25, 1.0));
        assert_eq!(
            enter.action,
            RunnerAction::Enter(super::SignalDirection::Long)
        );

        let adverse = runner.on_snapshot(&snapshot(1_100, -0.25, -1.0));
        assert_eq!(
            adverse.action,
            RunnerAction::CancelAdverse(super::SignalDirection::Long)
        );
        assert_eq!(runner.active_direction(), None);
        assert_eq!(runner.phase(), RunnerPhase::WaitRearm);

        let blocked = runner.on_snapshot(&snapshot(1_200, 0.30, 1.5));
        assert_eq!(
            blocked.action,
            RunnerAction::RearmWaitSkip(super::SignalDirection::Long)
        );

        let rearmed = runner.on_snapshot(&snapshot(1_300, 0.01, 0.01));
        assert_eq!(rearmed.action, RunnerAction::None);
        assert_eq!(runner.phase(), RunnerPhase::Armed);
    }

    #[test]
    fn detects_buy_bundle_arb() {
        let mut event = MarketEvent::new("pm", 0.48, 0.49, 1);
        event.yes_best_bid = Some(0.48);
        event.yes_best_ask = Some(0.49);
        event.no_best_bid = Some(0.49);
        event.no_best_ask = Some(0.50);
        event.yes_depth = Some(120.0);
        event.no_depth = Some(90.0);

        let thresholds = PolymarketArbThresholds {
            min_edge_bps: 5.0,
            min_depth: 50.0,
        };
        let opportunity = detect_polymarket_arb(&event, &thresholds).expect("arb expected");
        assert_eq!(opportunity.side, PolymarketArbSide::BuyBundle);
        assert!((opportunity.edge_bps - 100.0).abs() < 1e-9);
        assert_eq!(opportunity.executable_depth, Some(90.0));
    }

    #[test]
    fn detects_sell_bundle_arb() {
        let mut event = MarketEvent::new("pm", 0.50, 0.51, 1);
        event.yes_best_bid = Some(0.53);
        event.yes_best_ask = Some(0.54);
        event.no_best_bid = Some(0.48);
        event.no_best_ask = Some(0.49);

        let thresholds = PolymarketArbThresholds {
            min_edge_bps: 5.0,
            min_depth: 0.0,
        };
        let opportunity = detect_polymarket_arb(&event, &thresholds).expect("arb expected");
        assert_eq!(opportunity.side, PolymarketArbSide::SellBundle);
        assert!((opportunity.bundle_price - 1.01).abs() < 1e-9);
    }
}
