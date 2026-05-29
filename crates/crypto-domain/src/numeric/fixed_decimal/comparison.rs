use super::FixedDecimal;
use super::scale::align_value;
use crate::DomainError;
use std::cmp::Ordering;

impl FixedDecimal {
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

    fn cmp_scaled(&self, rhs: &Self) -> Result<Ordering, DomainError> {
        let scale = self.scale.max(rhs.scale);
        Ok(align_value(self, scale)?.cmp(&align_value(rhs, scale)?))
    }
}
