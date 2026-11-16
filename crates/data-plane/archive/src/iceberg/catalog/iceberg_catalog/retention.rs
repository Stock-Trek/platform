use iceberg::spec::SnapshotRetention;
use std::collections::HashMap;

const MIN_SNAPSHOTS_TO_KEEP: &str = "history.expire.min-snapshots-to-keep";
const MAX_SNAPSHOT_AGE_MS: &str = "history.expire.max-snapshot-age-ms";
const COMMIT_RETRIES: &str = "commit.retry.num-retries";
const INDEX_MIN_SNAPSHOTS_TO_KEEP: i32 = 10;
const INDEX_MAX_SNAPSHOT_AGE_MS: i64 = 30 * 24 * 60 * 60 * 1_000;

pub(super) struct ClaimRetention;

impl ClaimRetention {
    pub(super) fn properties(index_authoritative: bool) -> HashMap<String, String> {
        let (minimum, maximum) = if index_authoritative {
            (INDEX_MIN_SNAPSHOTS_TO_KEEP, INDEX_MAX_SNAPSHOT_AGE_MS)
        } else {
            (i32::MAX, i64::MAX)
        };
        HashMap::from([
            (MIN_SNAPSHOTS_TO_KEEP.to_string(), minimum.to_string()),
            (MAX_SNAPSHOT_AGE_MS.to_string(), maximum.to_string()),
            (COMMIT_RETRIES.to_string(), "0".to_string()),
        ])
    }

    pub(super) fn branch(index_authoritative: bool) -> SnapshotRetention {
        if index_authoritative {
            SnapshotRetention::branch(
                Some(INDEX_MIN_SNAPSHOTS_TO_KEEP),
                Some(INDEX_MAX_SNAPSHOT_AGE_MS),
                None,
            )
        } else {
            SnapshotRetention::branch(Some(i32::MAX), Some(i64::MAX), None)
        }
    }
}
