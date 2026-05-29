use crate::storage::record::sha256_hex;

pub(in crate::normalize::projection) fn stable_id(parts: &[&str]) -> String {
    sha256_hex(parts.join("|").as_bytes())
}
