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
fn plans_refuse_to_delete_inside_the_grace_period() {
    let now = TimestampMilliseconds(1_000_000);
    let plan = MaintenancePlan::dry_run(
        MaintenanceStage::OrphanCleanup,
        vec![candidate("s3://bucket/new", 990_000)],
    );

    assert!(
        plan.deletion_candidates(now, Duration::from_secs(60))
            .is_err()
    );
    assert!(
        plan.deletion_candidates(now, Duration::from_secs(1))
            .is_ok()
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
