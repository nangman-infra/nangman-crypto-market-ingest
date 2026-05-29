use super::FixedDecimal;
use crate::DomainError;
use std::str::FromStr;

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
