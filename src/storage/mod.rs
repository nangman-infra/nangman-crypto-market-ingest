pub mod disk;
pub mod eviction;
pub mod gap;
pub mod health;
pub mod orphan;
mod parquet_file;
mod partition;
pub mod record;
pub mod retention;
pub mod s3_upload;
mod sink;
pub mod symbol_health;

use std::fmt;

pub use disk::disk_used_pct;
pub use eviction::{EvictionConfig, EvictionStats, evict_once, sealed_marker_path};
pub use orphan::{UnsealedOrphanConfig, UnsealedOrphanStats, cleanup_invalid_unsealed_once};
pub use retention::{
    S3RetentionConfig, S3RetentionStats, default_l0_retention_prefixes,
    default_l1_retention_prefixes, run_s3_retention_once,
};
pub use sink::{L0StorageConfig, L0StorageSink, StorageReport};

#[derive(Debug)]
pub enum StorageError {
    Io(std::io::Error),
    Arrow(arrow_schema::ArrowError),
    Parquet(parquet::errors::ParquetError),
    Aws(String),
    Json(serde_json::Error),
    InvalidConfig(String),
}

impl fmt::Display for StorageError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io(error) => write!(f, "storage io error: {error}"),
            Self::Arrow(error) => write!(f, "storage arrow error: {error}"),
            Self::Parquet(error) => write!(f, "storage parquet error: {error}"),
            Self::Aws(error) => write!(f, "storage aws error: {error}"),
            Self::Json(error) => write!(f, "storage json error: {error}"),
            Self::InvalidConfig(error) => write!(f, "storage invalid config: {error}"),
        }
    }
}

impl std::error::Error for StorageError {}

impl From<std::io::Error> for StorageError {
    fn from(value: std::io::Error) -> Self {
        Self::Io(value)
    }
}

impl From<arrow_schema::ArrowError> for StorageError {
    fn from(value: arrow_schema::ArrowError) -> Self {
        Self::Arrow(value)
    }
}

impl From<parquet::errors::ParquetError> for StorageError {
    fn from(value: parquet::errors::ParquetError) -> Self {
        Self::Parquet(value)
    }
}

impl From<serde_json::Error> for StorageError {
    fn from(value: serde_json::Error) -> Self {
        Self::Json(value)
    }
}
