use super::super::SupervisorArgs;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::supervisor) struct BootstrapChunk {
    pub(in crate::supervisor) start_ms: i64,
    pub(in crate::supervisor) end_ms: i64,
}

pub(in crate::supervisor) fn bootstrap_chunks(
    args: &SupervisorArgs,
    now_ms: i64,
) -> Vec<BootstrapChunk> {
    let lookback_ms = args
        .bootstrap_lookback_days
        .saturating_mul(24)
        .saturating_mul(3_600_000);
    let chunk_ms = args.bootstrap_chunk_hours.saturating_mul(3_600_000);
    if chunk_ms <= 0 {
        return Vec::new();
    }
    let end_bound = align_down_to_chunk(now_ms.saturating_sub(3_600_000), chunk_ms);
    let start_bound = align_down_to_chunk(end_bound.saturating_sub(lookback_ms), chunk_ms);
    let mut chunks = Vec::new();
    let mut cursor = start_bound;
    while cursor < end_bound {
        let end_ms = cursor.saturating_add(chunk_ms).min(end_bound);
        if end_ms > cursor {
            chunks.push(BootstrapChunk {
                start_ms: cursor,
                end_ms,
            });
        }
        cursor = end_ms;
    }
    chunks
}

pub(in crate::supervisor) fn normalize_subchunks(
    args: &SupervisorArgs,
    chunk: BootstrapChunk,
) -> Vec<BootstrapChunk> {
    let interval_ms = args.normalize_schedule_interval_ms;
    if interval_ms <= 0 || chunk.end_ms <= chunk.start_ms {
        return Vec::new();
    }
    let mut chunks = Vec::new();
    let mut cursor = chunk.start_ms;
    while cursor < chunk.end_ms {
        let end_ms = cursor.saturating_add(interval_ms).min(chunk.end_ms);
        if end_ms <= cursor {
            break;
        }
        chunks.push(BootstrapChunk {
            start_ms: cursor,
            end_ms,
        });
        cursor = end_ms;
    }
    chunks
}

fn align_down_to_chunk(value: i64, chunk_ms: i64) -> i64 {
    value.div_euclid(chunk_ms) * chunk_ms
}
