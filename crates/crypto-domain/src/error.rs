use std::fmt;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DomainError {
    EmptyDecimal,
    InvalidDecimal(String),
    NegativeUnsignedDecimal(String),
    ScaleOverflow,
    DivideByZero,
    InvalidSymbol(String),
    InvalidMarketValue(String),
}

impl fmt::Display for DomainError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::EmptyDecimal => write!(f, "decimal value is empty"),
            Self::InvalidDecimal(value) => write!(f, "invalid decimal value: {value}"),
            Self::NegativeUnsignedDecimal(value) => {
                write!(f, "unsigned decimal cannot be negative: {value}")
            }
            Self::ScaleOverflow => write!(f, "decimal scale overflow"),
            Self::DivideByZero => write!(f, "cannot divide by zero"),
            Self::InvalidSymbol(value) => write!(f, "invalid symbol: {value}"),
            Self::InvalidMarketValue(value) => write!(f, "invalid market value: {value}"),
        }
    }
}

impl std::error::Error for DomainError {}
