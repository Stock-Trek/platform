use crate::sink::S3ParquetSink;
use platform_archive::{
    ArchiveIcebergTable, ArchiveRegistration, CommitContext, IcebergDataFile, RegisterOutcome,
};
use platform_errors::{PlatformError, PlatformResult};
use tracing::{info, warn};

const FLUSH_REGISTRATION_MAX_ATTEMPTS: usize = 4;

impl S3ParquetSink {
    pub(crate) async fn register_flush(
        &self,
        table: ArchiveIcebergTable,
        mut files: Vec<IcebergDataFile>,
    ) -> PlatformResult<()> {
        for attempt in 1..=FLUSH_REGISTRATION_MAX_ATTEMPTS {
            if files.is_empty() {
                return Ok(());
            }
            let registration = ArchiveRegistration::new(files.clone(), None::<Vec<String>>)?;
            let digest = registration.batch_digest();
            match self
                .catalog
                .register_files(table, &registration, &CommitContext::default())
                .await?
            {
                RegisterOutcome::Applied => {
                    info!(
                        table = table.name(),
                        files = files.len(),
                        source_batch_digest = %digest,
                        "registered archive flush"
                    );
                    return Ok(());
                }
                RegisterOutcome::AlreadyApplied => {
                    info!(
                        table = table.name(),
                        files = files.len(),
                        source_batch_digest = %digest,
                        "archive flush was already registered"
                    );
                    return Ok(());
                }
                RegisterOutcome::Conflict(conflict) => {
                    warn!(
                        table = table.name(),
                        attempt,
                        source_batch_digest = %digest,
                        live_outputs = ?conflict.live_outputs,
                        "archive flush registration conflicted; registering the remainder"
                    );
                    let live = self.catalog.registered_data_files(table).await?;
                    files.retain(|file| !live.contains(&file.file_path));
                }
            }
        }

        Err(PlatformError::external(std::io::Error::other(format!(
            "archive flush registration for {} remained conflicted with {} files",
            table.name(),
            files.len()
        ))))
    }
}
