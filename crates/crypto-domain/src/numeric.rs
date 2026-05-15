use crate::DomainError;
use serde::ser::SerializeStruct;
use serde::{Deserialize, Serialize};
use std::cmp::Ordering;
use std::str::FromStr;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct FixedDecimal {
    pub value: i128,
    pub scale: u32,
}

impl FixedDecimal {
    pub const fn new(value: i128, scale: u32) -> Self {
        Self { value, scale }
    }

    pub const fn zero() -> Self {
        Self { value: 0, scale: 0 }
    }

    pub const fn one() -> Self {
        Self { value: 1, scale: 0 }
    }

    pub fn parse_unsigned(input: &str) -> Result<Self, DomainError> {
        let parsed = Self::from_str(input)?;
        if parsed.value < 0 {
            return Err(DomainError::NegativeUnsignedDecimal(input.to_owned()));
        }
        Ok(parsed)
    }

    pub fn is_positive(self) -> bool {
        self.value > 0
    }

    pub fn is_non_negative(self) -> bool {
        self.value >= 0
    }

    pub fn checked_add(self, rhs: Self) -> Result<Self, DomainError> {
        let scale = self.scale.max(rhs.scale);
        let left = self.align_value(scale)?;
        let right = rhs.align_value(scale)?;
        Ok(Self::new(left + right, scale))
    }

    pub fn checked_sub(self, rhs: Self) -> Result<Self, DomainError> {
        let scale = self.scale.max(rhs.scale);
        let left = self.align_value(scale)?;
        let right = rhs.align_value(scale)?;
        Ok(Self::new(left - right, scale))
    }

    pub fn checked_min(self, rhs: Self) -> Result<Self, DomainError> {
        Ok(if self.cmp_scaled(&rhs)? == Ordering::Greater {
            rhs
        } else {
            self
        })
    }

    pub fn checked_gt(self, rhs: Self) -> Result<bool, DomainError> {
        Ok(self.cmp_scaled(&rhs)? == Ordering::Greater)
    }

    pub fn checked_lt(self, rhs: Self) -> Result<bool, DomainError> {
        Ok(self.cmp_scaled(&rhs)? == Ordering::Less)
    }

    pub fn checked_eq(self, rhs: Self) -> Result<bool, DomainError> {
        Ok(self.cmp_scaled(&rhs)? == Ordering::Equal)
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

    fn cmp_scaled(&self, rhs: &Self) -> Result<Ordering, DomainError> {
        let scale = self.scale.max(rhs.scale);
        Ok(self.align_value(scale)?.cmp(&rhs.align_value(scale)?))
    }

    fn align_value(&self, target_scale: u32) -> Result<i128, DomainError> {
        let scale_delta = target_scale
            .checked_sub(self.scale)
            .ok_or(DomainError::ScaleOverflow)?;
        self.value
            .checked_mul(pow10(scale_delta)?)
            .ok_or(DomainError::ScaleOverflow)
    }
}

impl Default for FixedDecimal {
    fn default() -> Self {
        Self::zero()
    }
}

impl Serialize for FixedDecimal {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut state = serializer.serialize_struct("FixedDecimal", 2)?;
        state.serialize_field("value", &self.value.to_string())?;
        state.serialize_field("scale", &self.scale)?;
        state.end()
    }
}

impl<'de> Deserialize<'de> for FixedDecimal {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        #[derive(Deserialize)]
        struct FixedDecimalWire {
            value: String,
            scale: u32,
        }

        let wire = FixedDecimalWire::deserialize(deserializer)?;
        let value = wire
            .value
            .parse::<i128>()
            .map_err(serde::de::Error::custom)?;
        Ok(Self {
            value,
            scale: wire.scale,
        })
    }
}

impl FromStr for FixedDecimal {
    type Err = DomainError;

    fn from_str(input: &str) -> Result<Self, Self::Err> {
        let trimmed = input.trim();
        if trimmed.is_empty() {
            return Err(DomainError::EmptyDecimal);
        }

        let negative = trimmed.starts_with('-');
        let digits = if negative { &trimmed[1..] } else { trimmed };
        if digits.is_empty() {
            return Err(DomainError::InvalidDecimal(input.to_owned()));
        }

        let mut parts = digits.split('.');
        let whole = parts
            .next()
            .ok_or_else(|| DomainError::InvalidDecimal(input.to_owned()))?;
        let fraction = parts.next().unwrap_or("");
        if parts.next().is_some()
            || whole.is_empty()
            || !whole.chars().all(|ch| ch.is_ascii_digit())
            || !fraction.chars().all(|ch| ch.is_ascii_digit())
        {
            return Err(DomainError::InvalidDecimal(input.to_owned()));
        }

        let scale = u32::try_from(fraction.len()).map_err(|_| DomainError::ScaleOverflow)?;
        let combined = format!("{whole}{fraction}");
        let mut value = combined
            .parse::<i128>()
            .map_err(|_| DomainError::InvalidDecimal(input.to_owned()))?;
        if negative {
            value = -value;
        }
        Ok(Self { value, scale })
    }
}

fn pow10(scale: u32) -> Result<i128, DomainError> {
    let mut value = 1_i128;
    for _ in 0..scale {
        value = value.checked_mul(10).ok_or(DomainError::ScaleOverflow)?;
    }
    Ok(value)
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Bps {
    pub value: i64,
}

impl Bps {
    pub const fn new(value: i64) -> Self {
        Self { value }
    }
}

impl Default for Bps {
    fn default() -> Self {
        Self::new(0)
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct MicroBps {
    pub value: i64,
}

impl MicroBps {
    pub const fn new(value: i64) -> Self {
        Self { value }
    }
}

impl Default for MicroBps {
    fn default() -> Self {
        Self::new(0)
    }
}

pub type Price = FixedDecimal;
pub type Quantity = FixedDecimal;
pub type Notional = FixedDecimal;
pub type Ratio = FixedDecimal;
