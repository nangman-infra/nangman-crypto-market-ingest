pub(super) fn regime_missing_reasons(
    btc_return: Option<f64>,
    eth_return: Option<f64>,
    sector_return: Option<f64>,
    correlation_to_btc: Option<f64>,
) -> Vec<String> {
    let mut missing_reasons = Vec::new();
    push_missing_if_none(&mut missing_reasons, btc_return, "btc_return_missing");
    push_missing_if_none(&mut missing_reasons, eth_return, "eth_return_missing");
    push_missing_if_none(&mut missing_reasons, sector_return, "sector_return_missing");
    push_missing_if_none(
        &mut missing_reasons,
        correlation_to_btc,
        "correlation_to_btc_insufficient_samples",
    );
    missing_reasons
}

pub(super) fn regime_quality_status(
    missing_reasons: &[String],
    sector_return: Option<f64>,
) -> &'static str {
    if missing_reasons.is_empty() {
        "complete"
    } else if sector_return.is_some() {
        "partial"
    } else {
        "insufficient"
    }
}

pub(super) fn volatility_regime(volatility: Option<f64>) -> String {
    match volatility {
        Some(value) if value < 0.5 => "low".to_owned(),
        Some(value) if value < 2.0 => "medium".to_owned(),
        Some(_) => "high".to_owned(),
        None => "unknown".to_owned(),
    }
}

fn push_missing_if_none<T>(missing_reasons: &mut Vec<String>, value: Option<T>, reason: &str) {
    if value.is_none() {
        missing_reasons.push(reason.to_owned());
    }
}
