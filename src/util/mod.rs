use std::time::{SystemTime, UNIX_EPOCH};

pub fn now_millis() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis())
        .unwrap_or(0)
}

pub fn quantize_price_floor(price: f64, tick_size: f64) -> Option<f64> {
    quantize_floor(price, tick_size)
}

pub fn quantize_qty_floor(qty: f64, step_size: f64) -> Option<f64> {
    quantize_floor(qty, step_size)
}

fn quantize_floor(value: f64, step: f64) -> Option<f64> {
    if !value.is_finite() || !step.is_finite() || value < 0.0 || step <= 0.0 {
        return None;
    }

    let scale = infer_decimal_scale(step);
    let factor = 10f64.powi(scale as i32);

    let value_units = (value * factor + 1e-9).floor();
    let step_units = (step * factor + 1e-9).round();
    if value_units < 0.0 || step_units <= 0.0 {
        return None;
    }

    let quantized_units = (value_units / step_units).floor() * step_units;
    if !quantized_units.is_finite() {
        return None;
    }

    let quantized = quantized_units / factor;
    if !quantized.is_finite() {
        return None;
    }

    // Re-snap to inferred decimal scale to avoid fp tails.
    Some((quantized * factor + 1e-9).floor() / factor)
}

fn infer_decimal_scale(step: f64) -> usize {
    let formatted = format!("{step:.16}");
    match formatted.split_once('.') {
        Some((_, frac)) => frac.trim_end_matches('0').len(),
        None => 0,
    }
}

#[cfg(test)]
mod tests {
    use super::{quantize_price_floor, quantize_qty_floor};

    fn approx_eq(left: f64, right: f64) {
        let diff = (left - right).abs();
        assert!(diff < 1e-12, "left={} right={} diff={}", left, right, diff);
    }

    #[test]
    fn quantizes_price_floor() {
        let got = quantize_price_floor(123.4567, 0.01).expect("should quantize");
        approx_eq(got, 123.45);
    }

    #[test]
    fn quantizes_qty_floor_with_float_edge() {
        let got = quantize_qty_floor(0.3, 0.1).expect("should quantize");
        approx_eq(got, 0.3);
    }
}
