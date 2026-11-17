use crate::{
    ArchiveKind, MaintenanceCandidate, MaintenanceFileKind, MaintenancePlan, MaintenanceStage,
};
use platform_types::TimestampMilliseconds;
use std::time::Duration;

fn candidate(path: &str, modified: i64) -> MaintenanceCandidate {
    MaintenanceCandidate::new(
        path.to_string(),
        ArchiveKind::Events,
        MaintenanceFileKind::Metadata,
        TimestampMilliseconds(modified),
    )
}

#[test]
fn plans_select_only_candidates_older_than_grace() {
    let now = TimestampMilliseconds(1_000_000);
    let plan = MaintenancePlan::dry_run(
        MaintenanceStage::OrphanCleanup,
        vec![candidate("s3://bucket/new", 990_000)],
    );

    assert!(
        plan.deletion_candidates(now, Duration::from_secs(60), Duration::from_secs(7))
            .unwrap()
            .is_empty()
    );
    let selected = plan
        .deletion_candidates(now, Duration::from_secs(1), Duration::from_secs(7))
        .unwrap();
    assert_eq!(selected.len(), 1);
}

#[test]
fn plans_fail_closed_below_the_retention_floor() {
    let now = TimestampMilliseconds(1_000_000);
    let plan = MaintenancePlan::dry_run(
        MaintenanceStage::OrphanCleanup,
        vec![candidate("s3://bucket/new", 990_000)],
    );

    assert!(
        plan.deletion_candidates(now, Duration::from_secs(1), Duration::from_secs(7))
            .is_ok()
    );
    assert!(
        plan.deletion_candidates(now, Duration::from_secs(1), Duration::from_secs(60))
            .is_err()
    );
}

#[test]
fn plans_report_age_bounds() {
    let now = TimestampMilliseconds(1_000_000);
    let plan = MaintenancePlan::dry_run(
        MaintenanceStage::OrphanCleanup,
        vec![
            candidate("s3://bucket/a", 900_000),
            candidate("s3://bucket/b", 800_000),
        ],
    );

    assert_eq!(plan.oldest_age(now), Some(Duration::from_secs(200)));
    assert_eq!(plan.youngest_age(now), Some(Duration::from_secs(100)));
    assert!(!plan.is_empty());
    assert!(MaintenancePlan::dry_run(MaintenanceStage::RawDeletion, Vec::new()).is_empty());
}
