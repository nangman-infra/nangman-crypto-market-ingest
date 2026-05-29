use std::path::PathBuf;

#[derive(Debug, Clone)]
pub(super) struct EvictableFile {
    pub(super) parquet_path: PathBuf,
    pub(super) sealed_path: PathBuf,
    pub(super) sealed_at_ms: i64,
    pub(super) parquet_size: u64,
}
