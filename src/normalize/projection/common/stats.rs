use std::cmp::Ordering;

pub(in crate::normalize::projection) fn percent_change(
    now: Option<f64>,
    previous: Option<f64>,
) -> Option<f64> {
    let now = now?;
    let previous = previous?;
    if !now.is_finite() || !previous.is_finite() || previous.abs() <= f64::EPSILON {
        return None;
    }
    Some(((now - previous) / previous) * 100.0)
}

pub(in crate::normalize::projection) fn mean(values: impl Iterator<Item = f64>) -> Option<f64> {
    let mut count = 0usize;
    let mut sum = 0.0;
    for value in values.filter(|value| value.is_finite()) {
        count += 1;
        sum += value;
    }
    (count > 0).then(|| sum / count as f64)
}

/// Population standard deviation (divisor = N).
///
/// volatility_regime treats the observed window as the entity to describe,
/// not as a sample drawn from a larger population. If a caller ever needs a
/// sample estimator, add `sample_stddev` (divisor = N - 1) explicitly so the
/// distinction stays in the call site, not the helper name.
pub(in crate::normalize::projection) fn population_stddev(
    values: impl Iterator<Item = f64>,
) -> Option<f64> {
    let values = values.filter(|value| value.is_finite()).collect::<Vec<_>>();
    if values.len() < 2 {
        return None;
    }
    let mean = values.iter().sum::<f64>() / values.len() as f64;
    let variance = values
        .iter()
        .map(|value| (value - mean).powi(2))
        .sum::<f64>()
        / values.len() as f64;
    Some(variance.sqrt())
}

pub(in crate::normalize::projection) fn correlation(left: &[f64], right: &[f64]) -> Option<f64> {
    if left.len() != right.len() || left.len() < 3 {
        return None;
    }
    let left_mean = left.iter().sum::<f64>() / left.len() as f64;
    let right_mean = right.iter().sum::<f64>() / right.len() as f64;
    let mut numerator = 0.0;
    let mut left_denominator = 0.0;
    let mut right_denominator = 0.0;
    for (left_value, right_value) in left.iter().zip(right.iter()) {
        let left_delta = left_value - left_mean;
        let right_delta = right_value - right_mean;
        numerator += left_delta * right_delta;
        left_denominator += left_delta.powi(2);
        right_denominator += right_delta.powi(2);
    }
    if left_denominator <= f64::EPSILON || right_denominator <= f64::EPSILON {
        return None;
    }
    Some(numerator / (left_denominator.sqrt() * right_denominator.sqrt()))
}

pub(in crate::normalize::projection) fn median(mut values: Vec<f64>) -> Option<f64> {
    values.retain(|value| value.is_finite());
    if values.is_empty() {
        return None;
    }
    values.sort_by(|left, right| left.partial_cmp(right).unwrap_or(Ordering::Equal));
    let middle = values.len() / 2;
    if values.len().is_multiple_of(2) {
        Some((values[middle - 1] + values[middle]) / 2.0)
    } else {
        Some(values[middle])
    }
}
