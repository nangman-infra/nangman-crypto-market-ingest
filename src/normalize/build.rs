use super::args::{InputRange, NormalizeArgs};
use super::model::NormalizeInputs;

mod accumulator;
mod finish;
mod ingest;
mod result;
mod slices;
#[cfg(test)]
mod tests;

pub use accumulator::BuildAccumulator;
pub use result::{BuildInputMetadata, BuildResult};

pub fn build_slices(
    args: &NormalizeArgs,
    input_range: InputRange,
    scan_range: InputRange,
    inputs: NormalizeInputs,
    _started_at_ms: i64,
) -> BuildResult {
    let metadata = BuildInputMetadata {
        run_mode: inputs.run_mode,
        fallback_alert: inputs.fallback_alert,
        input_local_object_count: inputs.input_local_object_count,
        input_s3_object_count: inputs.input_s3_object_count,
        input_object_keys: inputs.input_object_keys,
    };
    let mut accumulator = BuildAccumulator::new(args, input_range, scan_range, metadata);
    for event in inputs.raw_events {
        accumulator.ingest_raw_event(args, event);
    }
    for row in inputs.symbol_health {
        accumulator.ingest_symbol_health(row);
    }
    for row in inputs.source_health {
        accumulator.ingest_source_health(row);
    }
    for row in inputs.gap_alerts {
        accumulator.ingest_gap_alert(row);
    }
    accumulator.finish()
}
