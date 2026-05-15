use super::admissibility::{
    L1AdmissibilityError, L1IndexPointer, L1ReadPlan, L1ReadRequest, build_read_plan,
};
use super::args::InputRange;
use super::model::{L1Manifest, NormalizationReport};
use super::write::index_pointer_key;
use crate::storage::StorageError;
use crate::storage::s3_upload::S3Uploader;
use std::error::Error;
use std::fmt;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

#[derive(Debug, Clone)]
pub struct L1ReaderConfig {
    pub l1_s3_bucket: String,
    pub aws_region: String,
    pub aws_profile: Option<String>,
    pub spool_root: PathBuf,
    pub window_ms: i64,
}

pub struct L1Reader {
    s3: S3Uploader,
    spool_root: PathBuf,
    window_ms: i64,
}

impl L1Reader {
    pub async fn new(config: L1ReaderConfig) -> Result<Self, L1ReaderError> {
        Ok(Self {
            s3: S3Uploader::new(config.l1_s3_bucket, config.aws_region, config.aws_profile).await?,
            spool_root: config.spool_root,
            window_ms: config.window_ms,
        })
    }

    pub async fn read_plan(&self, input_range: InputRange) -> Result<L1ReadPlan, L1ReaderError> {
        let request = L1ReadRequest::normalized_market_slice(input_range);
        let pointer_key = index_pointer_key(self.window_ms, input_range.start_ms);
        let pointer = self.download_json::<L1IndexPointer>(&pointer_key).await?;
        let manifest = self
            .download_json::<L1Manifest>(&pointer.canonical_manifest_key)
            .await?;
        let report = self
            .download_json::<NormalizationReport>(&manifest.report_key)
            .await?;
        Ok(build_read_plan(
            &pointer,
            &manifest,
            &report,
            &pointer.canonical_manifest_key,
            &request,
        )?)
    }

    async fn download_json<T: serde::de::DeserializeOwned>(
        &self,
        key: &str,
    ) -> Result<T, L1ReaderError> {
        let path = temp_json_path(&self.spool_root, key);
        self.s3.download_file(key, &path).await?;
        let bytes = tokio::fs::read(&path).await?;
        remove_file_best_effort(&path).await;
        Ok(serde_json::from_slice(&bytes)?)
    }
}

#[derive(Debug)]
pub enum L1ReaderError {
    Storage(StorageError),
    Io(std::io::Error),
    Json(serde_json::Error),
    Admissibility(L1AdmissibilityError),
}

impl fmt::Display for L1ReaderError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Storage(error) => write!(f, "{error}"),
            Self::Io(error) => write!(f, "l1 reader io error: {error}"),
            Self::Json(error) => write!(f, "l1 reader json error: {error}"),
            Self::Admissibility(error) => write!(f, "{error}"),
        }
    }
}

impl Error for L1ReaderError {}

impl From<StorageError> for L1ReaderError {
    fn from(value: StorageError) -> Self {
        Self::Storage(value)
    }
}

impl From<std::io::Error> for L1ReaderError {
    fn from(value: std::io::Error) -> Self {
        Self::Io(value)
    }
}

impl From<serde_json::Error> for L1ReaderError {
    fn from(value: serde_json::Error) -> Self {
        Self::Json(value)
    }
}

impl From<L1AdmissibilityError> for L1ReaderError {
    fn from(value: L1AdmissibilityError) -> Self {
        Self::Admissibility(value)
    }
}

fn temp_json_path(spool_root: &Path, key: &str) -> PathBuf {
    let sanitized = key.replace(['/', '='], "_");
    spool_root.join("reader-tmp").join(format!(
        "{}-{}-{sanitized}.json",
        std::process::id(),
        nanos_now()
    ))
}

fn nanos_now() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_nanos())
        .unwrap_or(0)
}

async fn remove_file_best_effort(path: &Path) {
    let _ = tokio::fs::remove_file(path).await;
}
