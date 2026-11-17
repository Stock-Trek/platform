use crate::{ArchiveKind, IcebergFilePartition, SourceBatch};
use platform_lease::LeaseScope;

/// The partition-level coordination unit shared by compaction and maintenance.
///
/// A partition is one archive kind and hour bucket. Compaction holds the lease
/// for every partition it writes, and the maintenance rewrite stage holds it
/// for every partition it rewrites, so the two can never race on the same
/// partition. Both sides must derive the scope through this type.
pub struct MaintenancePartition {
    pub kind: ArchiveKind,
    pub hour_millis: i64,
}

impl MaintenancePartition {
    pub fn new(kind: ArchiveKind, hour_millis: i64) -> Self {
        Self { kind, hour_millis }
    }

    pub fn for_batch(batch: &SourceBatch) -> Self {
        Self::new(batch.kind, batch.bucket_start.0)
    }

    pub fn from_file_partition(partition: IcebergFilePartition) -> Self {
        Self::new(partition.kind, partition.hour_millis)
    }

    pub fn lease_scope(&self, bucket: &str, output_prefix: &str) -> LeaseScope {
        LeaseScope::new(format!(
            "archive-partition:{}:{}:{}:{}",
            bucket,
            output_prefix,
            self.kind.label(),
            self.hour_millis
        ))
    }
}
