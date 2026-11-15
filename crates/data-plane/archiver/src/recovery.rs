use crate::sink::S3ParquetSink;
use platform_archive::{
    ArchiveIcebergTable, ArchiveKind, ArchivePath, ArchiveRegistration, IcebergDataFile, ObjectKey,
    ParquetReader, RegisterOutcome, S3,
};
use platform_errors::PlatformResult;
use platform_lease::{Lease, LeaseGuard, LeaseRequest, LeaseScope};
use tracing::{info, warn};

const RECOVERY_MAX_ATTEMPTS: usize = 4;

impl S3ParquetSink {
    pub async fn recover_uncommitted(&self) -> PlatformResult<()> {
        let prefix = self.prefix.trim_matches('/');
        let list_prefix = if prefix.is_empty() {
            String::new()
        } else {
            format!("{prefix}/")
        };
        for kind in [ArchiveKind::Events, ArchiveKind::Candles] {
            let table = ArchiveIcebergTable::raw(kind);
            let mut guard = self.acquire_recovery_guard(table).await?;
            let recovery = async {
                let objects = S3::list_objects(
                    &self.s3_client,
                    &self.bucket,
                    &ObjectKey::new(list_prefix.clone()),
                )
                .await?;
                self.recover_table(table, &list_prefix, &objects, &mut guard)
                    .await
            }
            .await;
            let release = guard.release().await;
            recovery?;
            release?;
        }
        Ok(())
    }

    async fn acquire_recovery_guard(
        &self,
        table: ArchiveIcebergTable,
    ) -> PlatformResult<LeaseGuard> {
        let scope = LeaseScope::new(format!("archive-recovery:{}", table.name()));
        let owner_id = Lease::new_owner_id();
        let request = LeaseRequest::new(scope, &owner_id, self.lease_ttl)
            .with_initial_token(i64::try_from(self.lease_initial_token).unwrap_or(i64::MAX))?;
        loop {
            match LeaseGuard::acquire(self.leases.clone(), request.clone(), self.max_work).await? {
                Some(guard) => return Ok(guard),
                None => {
                    info!(
                        table = table.name(),
                        "archive recovery waiting for the current lease holder"
                    );
                    Lease::wait_for_lease(self.leases.as_ref(), &request).await?;
                }
            }
        }
    }

    async fn recover_table(
        &self,
        table: ArchiveIcebergTable,
        list_prefix: &str,
        objects: &[platform_archive::S3Object],
        guard: &mut LeaseGuard,
    ) -> PlatformResult<()> {
        let registered = self.catalog.registered_data_files(table).await?;
        let mut pending: Vec<IcebergDataFile> = Vec::new();
        for object in objects {
            let key = object.key.as_str();
            if !key.ends_with(".parquet") {
                continue;
            }
            let Some(relative) = ArchivePath::relative_key(key, list_prefix) else {
                continue;
            };
            let Some((kind_segment, _)) = relative.split_once('/') else {
                continue;
            };
            if kind_segment != table.kind().label()
                || ArchivePath::bucket_start_from_key(relative).is_none()
            {
                continue;
            }
            let location = format!("s3://{}/{}", self.bucket, key);
            if registered.contains(&location)
                || pending.iter().any(|file| file.file_path == location)
            {
                continue;
            }
            let Ok(file_size) = u64::try_from(object.size) else {
                warn!(
                    key,
                    size = object.size,
                    "skipping uncommitted archive object"
                );
                continue;
            };
            let reader = ParquetReader::new(&self.s3_client, &self.bucket, &object.key, file_size);
            let record_count = match reader.row_count().await {
                Ok(record_count) => record_count,
                Err(error) => {
                    warn!(%error, key, "skipping unreadable uncommitted archive object");
                    continue;
                }
            };
            pending.push(IcebergDataFile::new(location, object.size, record_count));
        }

        for attempt in 1..=RECOVERY_MAX_ATTEMPTS {
            if pending.is_empty() {
                return Ok(());
            }
            guard.ensure_writable().await?;
            let registration = ArchiveRegistration::new(pending.clone(), None::<Vec<String>>)?;
            match self.catalog.register_files(table, &registration).await? {
                RegisterOutcome::Applied | RegisterOutcome::AlreadyApplied => {
                    info!(
                        table = table.name(),
                        files = pending.len(),
                        "recovered uncommitted archive files"
                    );
                    return Ok(());
                }
                RegisterOutcome::Conflict(conflict) => {
                    warn!(
                        table = table.name(),
                        attempt,
                        live_outputs = ?conflict.live_outputs,
                        "archive recovery conflict; reloading live files"
                    );
                    let live = self.catalog.registered_data_files(table).await?;
                    pending.retain(|file| !live.contains(&file.file_path));
                }
            }
        }
        if pending.is_empty() {
            Ok(())
        } else {
            Err(platform_errors::PlatformError::external(
                std::io::Error::other(format!(
                    "archive recovery left {} unregistered files",
                    pending.len()
                )),
            ))
        }
    }
}
