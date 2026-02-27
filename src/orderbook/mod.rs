use std::collections::BTreeMap;

use ordered_float::OrderedFloat;
use thiserror::Error;

use crate::marketdata::MarketEvent;

#[derive(Debug, Clone)]
pub struct OrderBook {
    pub symbol: String,
    pub best_bid: f64,
    pub best_bid_qty: f64,
    pub best_ask: f64,
    pub best_ask_qty: f64,
    pub last_ts_ms: u128,
    initialized: bool,
}

impl OrderBook {
    pub fn new(symbol: impl Into<String>) -> Self {
        Self {
            symbol: symbol.into(),
            best_bid: 0.0,
            best_bid_qty: 0.0,
            best_ask: 0.0,
            best_ask_qty: 0.0,
            last_ts_ms: 0,
            initialized: false,
        }
    }

    pub fn apply(&mut self, event: &MarketEvent) {
        self.best_bid = event.bid;
        self.best_bid_qty = event.bid_qty;
        self.best_ask = event.ask;
        self.best_ask_qty = event.ask_qty;
        self.last_ts_ms = event.ts_ms;
        self.initialized = true;
    }

    pub fn mid_price(&self) -> Option<f64> {
        if !self.initialized {
            return None;
        }
        Some((self.best_bid + self.best_ask) * 0.5)
    }
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct LevelUpdate {
    pub price: f64,
    pub qty: f64,
}

#[derive(Debug, Clone)]
pub struct DepthSnapshot {
    pub last_update_id: u64,
    pub bids: Vec<LevelUpdate>,
    pub asks: Vec<LevelUpdate>,
}

#[derive(Debug, Clone)]
pub struct DepthDelta {
    pub first_update_id: u64,
    pub final_update_id: u64,
    pub bids: Vec<LevelUpdate>,
    pub asks: Vec<LevelUpdate>,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct TopOfBook {
    pub best_bid: f64,
    pub best_bid_qty: f64,
    pub best_ask: f64,
    pub best_ask_qty: f64,
    pub mid_price: f64,
    pub spread_bps: f64,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum DepthApplyResult {
    DiscardedOld,
    Applied(TopOfBook),
    GapDetected,
}

#[derive(Debug, Error)]
pub enum OrderBookError {
    #[error("invalid {side} level (price={price}, qty={qty})")]
    InvalidLevel {
        side: &'static str,
        price: f64,
        qty: f64,
    },
    #[error("missing top of book after update")]
    MissingTopOfBook,
}

#[derive(Debug, Clone)]
pub struct BinanceSpotOrderBook {
    symbol: String,
    bids: BTreeMap<OrderedFloat<f64>, f64>,
    asks: BTreeMap<OrderedFloat<f64>, f64>,
    last_update_id: u64,
}

impl BinanceSpotOrderBook {
    pub fn new(symbol: impl Into<String>) -> Self {
        Self {
            symbol: symbol.into(),
            bids: BTreeMap::new(),
            asks: BTreeMap::new(),
            last_update_id: 0,
        }
    }

    pub fn symbol(&self) -> &str {
        &self.symbol
    }

    pub fn last_update_id(&self) -> u64 {
        self.last_update_id
    }

    pub fn apply_snapshot(&mut self, snapshot: DepthSnapshot) -> Result<TopOfBook, OrderBookError> {
        self.bids.clear();
        self.asks.clear();
        self.apply_levels(&snapshot.bids, Side::Bid)?;
        self.apply_levels(&snapshot.asks, Side::Ask)?;
        self.last_update_id = snapshot.last_update_id;

        self.top_of_book().ok_or(OrderBookError::MissingTopOfBook)
    }

    pub fn apply_delta(&mut self, delta: &DepthDelta) -> Result<DepthApplyResult, OrderBookError> {
        if delta.final_update_id <= self.last_update_id {
            return Ok(DepthApplyResult::DiscardedOld);
        }

        let next_update_id = self.last_update_id.saturating_add(1);
        if !(delta.first_update_id <= next_update_id && next_update_id <= delta.final_update_id) {
            return Ok(DepthApplyResult::GapDetected);
        }

        self.apply_levels(&delta.bids, Side::Bid)?;
        self.apply_levels(&delta.asks, Side::Ask)?;
        self.last_update_id = delta.final_update_id;

        let top = self.top_of_book().ok_or(OrderBookError::MissingTopOfBook)?;
        Ok(DepthApplyResult::Applied(top))
    }

    pub fn best_bid(&self) -> Option<f64> {
        self.bids.iter().next_back().map(|(price, _)| price.0)
    }

    pub fn best_bid_qty(&self) -> Option<f64> {
        self.bids.iter().next_back().map(|(_, qty)| *qty)
    }

    pub fn best_ask(&self) -> Option<f64> {
        self.asks.iter().next().map(|(price, _)| price.0)
    }

    pub fn best_ask_qty(&self) -> Option<f64> {
        self.asks.iter().next().map(|(_, qty)| *qty)
    }

    pub fn mid_price(&self) -> Option<f64> {
        let best_bid = self.best_bid()?;
        let best_ask = self.best_ask()?;
        Some((best_bid + best_ask) * 0.5)
    }

    pub fn spread_bps(&self) -> Option<f64> {
        let mid = self.mid_price()?;
        if mid <= 0.0 {
            return None;
        }

        let spread = self.best_ask()? - self.best_bid()?;
        Some((spread / mid) * 10_000.0)
    }

    pub fn top_of_book(&self) -> Option<TopOfBook> {
        let best_bid = self.best_bid()?;
        let best_bid_qty = self.best_bid_qty()?;
        let best_ask = self.best_ask()?;
        let best_ask_qty = self.best_ask_qty()?;
        let mid_price = (best_bid + best_ask) * 0.5;
        if mid_price <= 0.0 {
            return None;
        }

        Some(TopOfBook {
            best_bid,
            best_bid_qty,
            best_ask,
            best_ask_qty,
            mid_price,
            spread_bps: ((best_ask - best_bid) / mid_price) * 10_000.0,
        })
    }

    fn apply_levels(&mut self, levels: &[LevelUpdate], side: Side) -> Result<(), OrderBookError> {
        let side_label = side.label();

        for level in levels {
            if !level.price.is_finite()
                || level.price <= 0.0
                || !level.qty.is_finite()
                || level.qty < 0.0
            {
                return Err(OrderBookError::InvalidLevel {
                    side: side_label,
                    price: level.price,
                    qty: level.qty,
                });
            }

            let map = match side {
                Side::Bid => &mut self.bids,
                Side::Ask => &mut self.asks,
            };

            let key = OrderedFloat(level.price);
            if level.qty == 0.0 {
                map.remove(&key);
            } else {
                map.insert(key, level.qty);
            }
        }

        Ok(())
    }
}

#[derive(Debug, Clone, Copy)]
enum Side {
    Bid,
    Ask,
}

impl Side {
    fn label(self) -> &'static str {
        match self {
            Self::Bid => "bid",
            Self::Ask => "ask",
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{BinanceSpotOrderBook, DepthApplyResult, DepthDelta, DepthSnapshot, LevelUpdate};

    fn level(price: f64, qty: f64) -> LevelUpdate {
        LevelUpdate { price, qty }
    }

    #[test]
    fn applies_snapshot_and_valid_delta_sequence() {
        let mut book = BinanceSpotOrderBook::new("BTCUSDT");
        let snapshot = DepthSnapshot {
            last_update_id: 100,
            bids: vec![level(100.0, 1.0), level(99.5, 2.0)],
            asks: vec![level(100.5, 1.2), level(101.0, 3.0)],
        };

        let top = book
            .apply_snapshot(snapshot)
            .expect("snapshot should apply");
        assert_eq!(top.best_bid, 100.0);
        assert_eq!(top.best_ask, 100.5);
        assert_eq!(book.last_update_id(), 100);

        let first_delta = DepthDelta {
            first_update_id: 101,
            final_update_id: 101,
            bids: vec![level(100.2, 0.8)],
            asks: vec![level(100.5, 0.0), level(100.7, 1.1)],
        };

        let first = book
            .apply_delta(&first_delta)
            .expect("first delta should apply");
        match first {
            DepthApplyResult::Applied(top) => {
                assert_eq!(top.best_bid, 100.2);
                assert_eq!(top.best_ask, 100.7);
            }
            _ => panic!("expected applied"),
        }
        assert_eq!(book.last_update_id(), 101);

        let second_delta = DepthDelta {
            first_update_id: 102,
            final_update_id: 103,
            bids: vec![level(100.3, 0.5)],
            asks: vec![],
        };

        let second = book
            .apply_delta(&second_delta)
            .expect("second delta should apply");
        assert!(matches!(second, DepthApplyResult::Applied(_)));
        assert_eq!(book.last_update_id(), 103);
    }

    #[test]
    fn discards_old_events() {
        let mut book = BinanceSpotOrderBook::new("BTCUSDT");
        let snapshot = DepthSnapshot {
            last_update_id: 500,
            bids: vec![level(100.0, 1.0)],
            asks: vec![level(100.5, 1.0)],
        };
        book.apply_snapshot(snapshot)
            .expect("snapshot should apply");

        let old_delta = DepthDelta {
            first_update_id: 498,
            final_update_id: 500,
            bids: vec![level(101.0, 1.0)],
            asks: vec![],
        };

        let outcome = book
            .apply_delta(&old_delta)
            .expect("old delta should not fail");
        assert_eq!(outcome, DepthApplyResult::DiscardedOld);
        assert_eq!(book.last_update_id(), 500);
    }

    #[test]
    fn detects_gap_and_requires_resync() {
        let mut book = BinanceSpotOrderBook::new("BTCUSDT");
        let snapshot = DepthSnapshot {
            last_update_id: 1000,
            bids: vec![level(200.0, 1.0)],
            asks: vec![level(200.5, 1.0)],
        };
        book.apply_snapshot(snapshot)
            .expect("snapshot should apply");

        let gap_delta = DepthDelta {
            first_update_id: 1005,
            final_update_id: 1006,
            bids: vec![level(201.0, 1.0)],
            asks: vec![],
        };
        let gap = book
            .apply_delta(&gap_delta)
            .expect("gap delta should return outcome");
        assert_eq!(gap, DepthApplyResult::GapDetected);

        let resynced_snapshot = DepthSnapshot {
            last_update_id: 2000,
            bids: vec![level(300.0, 1.0)],
            asks: vec![level(300.5, 1.0)],
        };
        book.apply_snapshot(resynced_snapshot)
            .expect("resync snapshot should apply");

        let next_delta = DepthDelta {
            first_update_id: 2001,
            final_update_id: 2001,
            bids: vec![level(300.1, 1.0)],
            asks: vec![],
        };
        let outcome = book
            .apply_delta(&next_delta)
            .expect("post-resync delta should apply");
        assert!(matches!(outcome, DepthApplyResult::Applied(_)));
        assert_eq!(book.last_update_id(), 2001);
    }
}
