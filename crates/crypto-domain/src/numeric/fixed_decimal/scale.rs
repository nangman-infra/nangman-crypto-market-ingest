use super::FixedDecimal;
use crate::DomainError;

pub(super) fn align_value(decimal: &FixedDecimal, target_scale: u32) -> Result<i128, DomainError> {
    let scale_delta = target_scale
        .checked_sub(decimal.scale)
        .ok_or(DomainError::ScaleOverflow)?;
    decimal
        .value
        .checked_mul(pow10(scale_delta)?)
        .ok_or(DomainError::ScaleOverflow)
}

pub(super) fn pow10(scale: u32) -> Result<i128, DomainError> {
    let mut value = 1_i128;
    for _ in 0..scale {
        value = value.checked_mul(10).ok_or(DomainError::ScaleOverflow)?;
    }
    Ok(value)
}

pub(super) fn saturating_align(value: i128, scale_delta: u32) -> i128 {
    let mut acc = value;
    for _ in 0..scale_delta {
        acc = match acc.checked_mul(10) {
            Some(next) => next,
            None => {
                return if acc >= 0 { i128::MAX } else { i128::MIN };
            }
        };
    }
    acc
}
