use super::super::s3_upload::S3ObjectSummary;
use super::config::{S3RetentionConfig, S3RetentionStats};
#[cfg(test)]
use std::collections::BTreeSet;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) enum RetentionDecision {
    Keep,
    MissingLastModified,
    Protected,
    Delete { age_secs: i64 },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct RetentionDeleteCandidate {
    pub(super) key: String,
    pub(super) size_bytes: u64,
    pub(super) age_secs: i64,
}

#[cfg(test)]
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct RetentionDeletePlan {
    pub(super) stats: S3RetentionStats,
    pub(super) delete_candidates: Vec<RetentionDeleteCandidate>,
}

#[cfg(test)]
pub(super) fn plan_retention_deletes<I>(
    config: &S3RetentionConfig,
    now_ms: i64,
    objects: I,
) -> RetentionDeletePlan
where
    I: IntoIterator<Item = S3ObjectSummary>,
{
    let mut stats = S3RetentionStats::default();
    let mut delete_candidates = Vec::new();
    let mut seen = BTreeSet::new();

    for object in objects {
        if !seen.insert(object.key.clone()) {
            continue;
        }
        observe_retention_object(config, now_ms, object, &mut stats, &mut delete_candidates);
    }

    RetentionDeletePlan {
        stats,
        delete_candidates,
    }
}

pub(super) fn observe_retention_object(
    config: &S3RetentionConfig,
    now_ms: i64,
    object: S3ObjectSummary,
    stats: &mut S3RetentionStats,
    delete_candidates: &mut Vec<RetentionDeleteCandidate>,
) {
    stats.scanned_object_count += 1;
    match retention_decision(config, now_ms, &object) {
        RetentionDecision::Keep => {}
        RetentionDecision::MissingLastModified => stats.missing_last_modified_count += 1,
        RetentionDecision::Protected => stats.protected_object_count += 1,
        RetentionDecision::Delete { age_secs } => {
            stats.expired_object_count += 1;
            if delete_candidates.len() >= config.max_deletes_per_run {
                stats.stopped_at_delete_limit = true;
                return;
            }
            delete_candidates.push(RetentionDeleteCandidate {
                key: object.key,
                size_bytes: object.size_bytes,
                age_secs,
            });
        }
    }
}

pub(super) fn retention_decision(
    config: &S3RetentionConfig,
    now_ms: i64,
    object: &S3ObjectSummary,
) -> RetentionDecision {
    if is_protected_key(&object.key, &config.protected_prefixes) {
        return RetentionDecision::Protected;
    }
    let Some(last_modified_ms) = object.last_modified_ms else {
        return RetentionDecision::MissingLastModified;
    };
    let age_ms = now_ms.saturating_sub(last_modified_ms);
    let retention_ms = config.retention_secs.saturating_mul(1000);
    if age_ms < retention_ms {
        return RetentionDecision::Keep;
    }
    RetentionDecision::Delete {
        age_secs: age_ms / 1000,
    }
}

fn is_protected_key(key: &str, protected_prefixes: &[String]) -> bool {
    protected_prefixes
        .iter()
        .any(|prefix| !prefix.is_empty() && key.starts_with(prefix))
}
