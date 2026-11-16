use iceberg::spec::SnapshotRetention;
use std::collections::HashMap;

const MIN_SNAPSHOTS_TO_KEEP: &str = "history.expire.min-snapshots-to-keep";
const MAX_SNAPSHOT_AGE_MS: &str = "history.expire.max-snapshot-age-ms";
const COMMIT_RETRIES: &str = "commit.retry.num-retries";

pub(super) struct ClaimRetention;

impl ClaimRetention {
    pub(super) fn properties() -> HashMap<String, String> {
        HashMap::from([
            (MIN_SNAPSHOTS_TO_KEEP.to_string(), i32::MAX.to_string()),
            (MAX_SNAPSHOT_AGE_MS.to_string(), i64::MAX.to_string()),
            (COMMIT_RETRIES.to_string(), "0".to_string()),
        ])
    }

    pub(super) fn branch() -> SnapshotRetention {
        SnapshotRetention::branch(Some(i32::MAX), Some(i64::MAX), None)
    }
}
