use crate::normalize::args::InputRange;
use crate::normalize::model::SLICE_SCHEMA_VERSION;
use serde::{Deserialize, Serialize};

pub const POINTER_SCHEMA_VERSION: &str = "l1_index_pointer_v1";

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
pub struct L1IndexPointer {
    pub schema_version: String,
    pub canonical_manifest_key: String,
    pub l1_run_id: String,
    pub status: String,
    pub finished_at_ms: i64,
    pub input_time_range_start_ms: i64,
    pub input_time_range_end_ms: i64,
    #[serde(default)]
    pub indexed_window_start_ms: Option<i64>,
    #[serde(default)]
    pub indexed_window_end_ms: Option<i64>,
    pub schema_version_emitted: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct L1ReadRequest {
    pub input_range: InputRange,
    pub schema_version_emitted: String,
}

impl L1ReadRequest {
    pub fn normalized_market_slice(input_range: InputRange) -> Self {
        Self {
            input_range,
            schema_version_emitted: SLICE_SCHEMA_VERSION.to_owned(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct L1ReadPlan {
    pub l1_run_id: String,
    pub manifest_key: String,
    pub report_key: String,
    pub output_object_keys: Vec<String>,
    pub market_data_quality_summary_key: Option<String>,
    pub market_feature_delta_key: Option<String>,
    pub market_feature_delta_summary_key: Option<String>,
    pub market_regime_context_key: Option<String>,
    pub symbol_universe_snapshot_key: Option<String>,
}
