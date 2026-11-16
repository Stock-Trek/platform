use crate::sink::S3ParquetSink;
use platform_archive::{
    ArchiveIcebergTable, ArchiveKind, ArchivePath, IcebergDataFile, ObjectKey, ParquetReader, S3,
    S3Object,
};
use platform_errors::PlatformResult;
use platform_lease::LeaseGuard;
use platform_metrics::MarketDataMetrics;
use tracing::warn;

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
            let recovery: PlatformResult<()> = async {
                let prefix = ObjectKey::new(list_prefix.clone());
                let mut continuation = None;
                loop {
                    guard.ensure_writable().await?;
                    let page = S3::list_objects_page(
                        &self.s3_client,
                        &self.bucket,
                        &prefix,
                        continuation.as_deref(),
                        None,
                        i32::try_from(self.recovery_page_files.min(1_000)).unwrap_or(1_000),
                    )
                    .await?;
                    self.recover_table(table, &list_prefix, &page.objects, &mut guard)
                        .await?;
                    match page.next_continuation_token {
                        Some(next) => continuation = Some(next),
                        None => break,
                    }
                }
                Ok(())
            }
            .await;
            let release = guard.release().await;
            recovery?;
            release?;
        }
        Ok(())
    }

    async fn recover_table(
        &self,
        table: ArchiveIcebergTable,
        list_prefix: &str,
        objects: &[S3Object],
        guard: &mut LeaseGuard,
    ) -> PlatformResult<()> {
        let registered = self.catalog.registered_data_files(table).await?;
        let mut already_registered = 0_u64;
        let mut duplicates = 0_u64;
        let mut unreadable = 0_u64;
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
            if registered.contains(&location) {
                already_registered += 1;
                continue;
            }
            if pending.iter().any(|file| file.file_path == location) {
                duplicates += 1;
                continue;
            }
            let Ok(file_size) = u64::try_from(object.size) else {
                unreadable += 1;
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
                    unreadable += 1;
                    warn!(%error, key, "skipping unreadable uncommitted archive object");
                    continue;
                }
            };
            pending.push(IcebergDataFile::new(location, object.size, record_count));
        }
        MarketDataMetrics::record_archive_recovery(
            table.name(),
            "already_live",
            already_registered,
        );
        MarketDataMetrics::record_archive_recovery(table.name(), "duplicate", duplicates);
        MarketDataMetrics::record_archive_recovery(table.name(), "unreadable", unreadable);
        MarketDataMetrics::record_archive_recovery(table.name(), "candidate", pending.len() as u64);
        self.register_pending_pages(table, pending, guard).await
    }
}
