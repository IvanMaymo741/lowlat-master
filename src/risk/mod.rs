use crate::execution::Side;

#[derive(Debug, Clone)]
pub struct RiskManager {
    max_position: f64,
    max_order_qty: f64,
    max_notional: f64,
}

#[derive(Debug, Clone, PartialEq)]
pub enum RiskDecision {
    Allow,
    Reject(String),
}

impl RiskManager {
    pub fn new(max_position: f64, max_order_qty: f64, max_notional: f64) -> Self {
        Self {
            max_position,
            max_order_qty,
            max_notional,
        }
    }

    pub fn validate(
        &self,
        side: Side,
        qty: f64,
        price: f64,
        current_position: f64,
    ) -> RiskDecision {
        if qty <= 0.0 {
            return RiskDecision::Reject("qty must be > 0".to_string());
        }

        if qty > self.max_order_qty {
            return RiskDecision::Reject(format!(
                "qty {} exceeds max_order_qty {}",
                qty, self.max_order_qty
            ));
        }

        let notional = qty * price;
        if notional > self.max_notional {
            return RiskDecision::Reject(format!(
                "notional {} exceeds max_notional {}",
                notional, self.max_notional
            ));
        }

        let projected_position = current_position + side.signed_qty(qty);
        if projected_position.abs() > self.max_position {
            return RiskDecision::Reject(format!(
                "projected position {} exceeds max_position {}",
                projected_position, self.max_position
            ));
        }

        RiskDecision::Allow
    }
}

#[cfg(test)]
mod tests {
    use super::{RiskDecision, RiskManager};
    use crate::execution::Side;

    #[test]
    fn blocks_qty_over_limit() {
        let risk = RiskManager::new(10.0, 1.0, 1_000.0);
        let decision = risk.validate(Side::Buy, 2.0, 100.0, 0.0);
        assert!(matches!(decision, RiskDecision::Reject(_)));
    }
}
