use crate::{
    ArchiveKind, MIN_COMPACTED_SNAPSHOTS, MIN_ORPHAN_GRACE, MIN_RAW_RETENTION,
    MIN_SNAPSHOT_RETENTION, MaintenanceConfig, MaintenanceStage, PartitionTarget,
};
use std::time::Duration;

#[test]
fn defaults_validate_and_satisfy_retention_floors() {
    let config = MaintenanceConfig::default();

    config.validate().unwrap();
    assert!(config.raw_retention_for(ArchiveKind::Events).unwrap() >= MIN_RAW_RETENTION);
    assert!(config.raw_retention_for(ArchiveKind::Candles).unwrap() >= MIN_RAW_RETENTION);
    assert!(config.snapshot_retention >= MIN_SNAPSHOT_RETENTION);
    assert!(config.snapshots_to_keep >= MIN_COMPACTED_SNAPSHOTS);
    assert!(config.orphan_grace >= MIN_ORPHAN_GRACE);
    assert!(config.orphan_grace > config.longest_lease);
}

#[test]
fn retention_floors_are_enforced() {
    assert!(
        MaintenanceConfig::new()
            .with_raw_retention(ArchiveKind::Events, Duration::from_secs(1))
            .is_err()
    );
    assert!(
        MaintenanceConfig::new()
            .with_snapshot_retention(Duration::from_secs(1))
            .is_err()
    );
    assert!(
        MaintenanceConfig::new()
            .with_orphan_grace(Duration::from_secs(1))
            .is_err()
    );
}

#[test]
fn stages_are_strictly_ordered() {
    let stages = MaintenanceStage::all();
    assert_eq!(
        stages.map(MaintenanceStage::label),
        [
            "expire_snapshots",
            "orphan_cleanup",
            "compacted_rewrite",
            "raw_deletion"
        ]
    );
    assert!(
        stages
            .windows(2)
            .all(|pair| pair[0].order() < pair[1].order())
    );
    assert_eq!(
        MaintenanceStage::ExpireSnapshots.next(),
        Some(MaintenanceStage::OrphanCleanup)
    );
    assert_eq!(MaintenanceStage::RawDeletion.next(), None);
}

#[test]
fn partition_targets_must_be_positive() {
    assert!(PartitionTarget::default().exceeded_by(101, 0));
    assert!(!PartitionTarget::default().exceeded_by(1, 1));
    assert!(
        PartitionTarget {
            max_data_files: 0,
            max_bytes: 1,
        }
        .validate()
        .is_err()
    );
}
