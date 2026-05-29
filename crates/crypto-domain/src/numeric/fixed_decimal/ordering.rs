use super::FixedDecimal;
use super::scale::saturating_align;
use std::cmp::Ordering;

/// Total ordering over `FixedDecimal` after aligning to the larger scale.
///
/// Checked comparison returns a `Result` because aligning can overflow `i128`
/// for extreme inputs; for ordering purposes we fall back to the saturating
/// behavior of the underlying integer representation when alignment overflows
/// so that `Ord` remains total and BTreeMap key usage is safe. In practice
/// market data never produces values close to `i128::MAX`.
impl PartialOrd for FixedDecimal {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for FixedDecimal {
    fn cmp(&self, other: &Self) -> Ordering {
        let scale = self.scale.max(other.scale);
        let left = saturating_align(self.value, scale.saturating_sub(self.scale));
        let right = saturating_align(other.value, scale.saturating_sub(other.scale));
        left.cmp(&right)
    }
}
