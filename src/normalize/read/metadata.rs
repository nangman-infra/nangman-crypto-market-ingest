use super::super::build::BuildInputMetadata;
use super::super::input_keys::{InputEntry, InputEntrySource};
use super::super::mode::RunMode;
use super::super::model::NormalizeInputs;
use super::DOWNLOAD_CONCURRENCY;
use crate::log_stream;
use serde_json::json;

pub(super) struct ReadInputMetadata {
    input_object_keys: Vec<String>,
    input_local_object_count: usize,
    input_s3_object_count: usize,
    fallback_alert: bool,
}

impl ReadInputMetadata {
    pub(super) fn from_entries(entries: &[InputEntry], run_mode: RunMode) -> Self {
        let input_object_keys = entries
            .iter()
            .map(|entry| entry.key.clone())
            .collect::<Vec<_>>();
        let input_local_object_count = entries
            .iter()
            .filter(|entry| matches!(entry.source, InputEntrySource::Local))
            .count();
        let input_s3_object_count = entries
            .iter()
            .filter(|entry| matches!(entry.source, InputEntrySource::S3))
            .count();
        // LIVE mode is supposed to read local only. Any S3 hit means L0 ingest is missing data
        // for this range and we had to recover via fallback — flag it for control-plane.
        let fallback_alert = matches!(run_mode, RunMode::Live) && input_s3_object_count > 0;

        Self {
            input_object_keys,
            input_local_object_count,
            input_s3_object_count,
            fallback_alert,
        }
    }

    pub(super) fn input_s3_object_count(&self) -> usize {
        self.input_s3_object_count
    }

    pub(super) fn log_download_start(&self) {
        if self.input_s3_object_count == 0 {
            return;
        }

        let _ = log_stream::debug(
            "market_normalize_downloading",
            json!({
                "s3_object_count": self.input_s3_object_count,
                "local_object_count": self.input_local_object_count,
                "total_object_count": self.input_object_keys.len(),
                "download_concurrency": DOWNLOAD_CONCURRENCY
            }),
        );
    }

    pub(super) fn into_normalize_inputs(self, run_mode: RunMode) -> NormalizeInputs {
        NormalizeInputs {
            raw_events: Vec::new(),
            symbol_health: Vec::new(),
            source_health: Vec::new(),
            gap_alerts: Vec::new(),
            run_mode: run_mode.as_str().to_owned(),
            fallback_alert: self.fallback_alert,
            input_local_object_count: self.input_local_object_count,
            input_s3_object_count: self.input_s3_object_count,
            input_object_keys: self.input_object_keys,
        }
    }

    pub(super) fn into_build_metadata(self, run_mode: RunMode) -> BuildInputMetadata {
        BuildInputMetadata {
            run_mode: run_mode.as_str().to_owned(),
            fallback_alert: self.fallback_alert,
            input_local_object_count: self.input_local_object_count,
            input_s3_object_count: self.input_s3_object_count,
            input_object_keys: self.input_object_keys,
        }
    }
}
