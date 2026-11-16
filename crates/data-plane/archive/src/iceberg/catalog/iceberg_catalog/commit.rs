use crate::iceberg::catalog::iceberg_catalog::IcebergCatalog;
use crate::iceberg::catalog::iceberg_catalog::snapshot::SnapshotCommit;
use crate::iceberg::{ArchiveIcebergTable, ArchiveRegistration, ConflictDetails, RegisterOutcome};
use iceberg::table::Table;
use platform_errors::{PlatformError, PlatformResult};
use platform_metrics::MarketDataMetrics;
use platform_time::RetryBackoff;
use std::collections::HashSet;
use std::time::Duration;
use tracing::{info, warn};

const MAX_COMMIT_ATTEMPTS: usize = 4;
const COMMIT_BACKOFF: Duration = Duration::from_millis(250);
const COMMIT_MAX_BACKOFF: Duration = Duration::from_secs(1);

enum Evaluation {
    Apply,
    AlreadyApplied,
    Conflict(ConflictDetails),
}

impl IcebergCatalog {
    pub async fn commit_files(
        &self,
        table: ArchiveIcebergTable,
        registration: &ArchiveRegistration,
    ) -> PlatformResult<RegisterOutcome> {
        let mut backoff = RetryBackoff::linear(COMMIT_BACKOFF, COMMIT_MAX_BACKOFF).with_jitter();
        let mut last_error = None;
        for attempt in 1..=MAX_COMMIT_ATTEMPTS {
            let current = self.ensure_table(table).await?;
            match self.evaluate(&current, registration).await? {
                Evaluation::Apply => {}
                Evaluation::AlreadyApplied => {
                    MarketDataMetrics::record_archive_commit(table.name(), "already_applied");
                    info!(
                        table = table.name(),
                        "archive registration was already applied"
                    );
                    return Ok(RegisterOutcome::AlreadyApplied);
                }
                Evaluation::Conflict(conflict) => {
                    MarketDataMetrics::record_archive_commit(table.name(), "conflict");
                    return Ok(RegisterOutcome::Conflict(conflict));
                }
            }

            let commit = SnapshotCommit::build(
                &current,
                registration.files(),
                Self::snapshot_properties(registration.files(), registration.source_files()),
            )
            .await?;
            let request = commit.to_request(&self.table_ident(table));
            match self.send_commit(table, &request).await {
                Ok(()) => {
                    MarketDataMetrics::record_archive_commit(table.name(), "applied");
                    info!(
                        table = table.name(),
                        files = registration.files().len(),
                        "registered archive files in Iceberg"
                    );
                    return Ok(RegisterOutcome::Applied);
                }
                Err(error) => {
                    warn!(%error, table = table.name(), attempt, "conditional Iceberg commit did not succeed");
                    last_error = Some(error);
                }
            }

            let reloaded = self.ensure_table(table).await?;
            match self.evaluate(&reloaded, registration).await? {
                Evaluation::AlreadyApplied => {
                    MarketDataMetrics::record_archive_commit(table.name(), "already_applied");
                    return Ok(RegisterOutcome::AlreadyApplied);
                }
                Evaluation::Conflict(conflict) => {
                    MarketDataMetrics::record_archive_commit(table.name(), "conflict");
                    return Ok(RegisterOutcome::Conflict(conflict));
                }
                Evaluation::Apply => {
                    if attempt < MAX_COMMIT_ATTEMPTS {
                        tokio::time::sleep(backoff.next_delay()).await;
                    }
                }
            }
        }
        MarketDataMetrics::record_archive_commit(table.name(), "error");
        Err(last_error.unwrap_or_else(|| {
            PlatformError::external(std::io::Error::other(
                "conditional Iceberg commit remained ambiguous",
            ))
        }))
    }

    async fn evaluate(
        &self,
        table: &Table,
        registration: &ArchiveRegistration,
    ) -> PlatformResult<Evaluation> {
        let live_outputs = self.registered_paths(table).await?;
        let expected_outputs = registration.expected_outputs();
        let present_outputs = expected_outputs
            .intersection(&live_outputs)
            .cloned()
            .collect::<Vec<_>>();
        let all_outputs_present =
            !expected_outputs.is_empty() && present_outputs.len() == expected_outputs.len();

        let Some(sources) = registration.source_files() else {
            if expected_outputs.is_empty() || present_outputs.is_empty() {
                return Ok(Evaluation::Apply);
            }
            if all_outputs_present {
                return Ok(Evaluation::AlreadyApplied);
            }
            return Ok(Evaluation::Conflict(ConflictDetails {
                claimed_sources: Vec::new(),
                live_outputs: present_outputs,
            }));
        };

        let claimed = Self::source_files_from_metadata(table);
        let mut claimed_sources = sources
            .iter()
            .filter(|source| claimed.contains(*source))
            .cloned()
            .collect::<Vec<_>>();
        claimed_sources.sort();
        claimed_sources.dedup();
        let all_sources_present = claimed_sources.len() == sources.len();

        if all_sources_present && (expected_outputs.is_empty() || all_outputs_present) {
            return Ok(Evaluation::AlreadyApplied);
        }
        if !claimed_sources.is_empty() || !present_outputs.is_empty() {
            return Ok(Evaluation::Conflict(ConflictDetails {
                claimed_sources,
                live_outputs: present_outputs,
            }));
        }
        Ok(Evaluation::Apply)
    }

    async fn send_commit(
        &self,
        table: ArchiveIcebergTable,
        request: &serde_json::Value,
    ) -> PlatformResult<()> {
        let namespace = self.namespace.as_ref().join("\u{1f}");
        let prefix = self
            .prefix
            .as_ref()
            .map(|prefix| format!("/{prefix}"))
            .unwrap_or_default();
        let url = format!(
            "{}/v1{prefix}/namespaces/{namespace}/tables/{}",
            self.catalog_uri.trim_end_matches('/'),
            table.name()
        );
        let mut request_builder = reqwest::Client::new().post(url.clone()).json(request);
        if let Some(token) = &self.token {
            request_builder = request_builder.bearer_auth(token);
        }
        let response = request_builder.send().await.map_err(|error| {
            PlatformError::external(std::io::Error::other(format!(
                "conditional Iceberg commit to {url} failed: {error}"
            )))
        })?;
        let status = response.status();
        let body = response.text().await.unwrap_or_default();
        if status.is_success() {
            return Ok(());
        }
        Err(PlatformError::external(std::io::Error::other(format!(
            "conditional Iceberg commit rejected with {status}: {body}"
        ))))
    }

    pub fn expected_outputs(files: &[crate::iceberg::catalog::IcebergDataFile]) -> HashSet<String> {
        files.iter().map(|file| file.file_path.clone()).collect()
    }
}
