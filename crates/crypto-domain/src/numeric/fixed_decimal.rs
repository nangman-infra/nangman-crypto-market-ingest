use crate::DomainError;

mod arithmetic;
mod comparison;
mod ordering;
mod parse;
mod scale;
mod wire;

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
        let parsed = input.parse::<Self>()?;
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
}

impl Default for FixedDecimal {
    fn default() -> Self {
        Self::zero()
    }
}
