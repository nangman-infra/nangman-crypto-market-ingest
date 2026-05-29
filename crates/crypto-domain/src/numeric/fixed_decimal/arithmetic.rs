use super::FixedDecimal;
use super::scale::{align_value, pow10};
use crate::DomainError;
use crate::numeric::Bps;

impl FixedDecimal {
    pub fn checked_add(self, rhs: Self) -> Result<Self, DomainError> {
        let scale = self.scale.max(rhs.scale);
        let left = align_value(&self, scale)?;
        let right = align_value(&rhs, scale)?;
        Ok(Self::new(left + right, scale))
    }

    pub fn checked_sub(self, rhs: Self) -> Result<Self, DomainError> {
        let scale = self.scale.max(rhs.scale);
        let left = align_value(&self, scale)?;
        let right = align_value(&rhs, scale)?;
        Ok(Self::new(left - right, scale))
    }

    pub fn div_to_scale(self, rhs: Self, output_scale: u32) -> Result<Self, DomainError> {
        if rhs.value == 0 {
            return Err(DomainError::DivideByZero);
        }
        let numerator = self
            .value
            .checked_mul(pow10(rhs.scale)?)
            .and_then(|value| value.checked_mul(pow10(output_scale).ok()?))
            .ok_or(DomainError::ScaleOverflow)?;
        let denominator = rhs
            .value
            .checked_mul(pow10(self.scale)?)
            .ok_or(DomainError::ScaleOverflow)?;
        Ok(Self::new(numerator / denominator, output_scale))
    }

    pub fn mul_bps(self, bps: Bps) -> Result<Self, DomainError> {
        let value = self
            .value
            .checked_mul(bps.value as i128)
            .ok_or(DomainError::ScaleOverflow)?
            / 10_000;
        Ok(Self::new(value, self.scale))
    }

    pub fn mul_to_scale(self, rhs: Self, output_scale: u32) -> Result<Self, DomainError> {
        let product = self
            .value
            .checked_mul(rhs.value)
            .ok_or(DomainError::ScaleOverflow)?;
        let input_scale = self
            .scale
            .checked_add(rhs.scale)
            .ok_or(DomainError::ScaleOverflow)?;
        let value = if output_scale >= input_scale {
            product
                .checked_mul(pow10(output_scale - input_scale)?)
                .ok_or(DomainError::ScaleOverflow)?
        } else {
            product / pow10(input_scale - output_scale)?
        };
        Ok(Self::new(value, output_scale))
    }

    pub fn abs(self) -> Result<Self, DomainError> {
        Ok(Self::new(
            self.value.checked_abs().ok_or(DomainError::ScaleOverflow)?,
            self.scale,
        ))
    }
}
