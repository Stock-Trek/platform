use crate::iceberg::{
    ArchiveIcebergTable, ArchiveRegistration, CommitContext, ConflictDetails, RegisterOutcome,
};
use async_trait::async_trait;
use platform_errors::{PlatformError, PlatformResult};
use platform_metrics::MarketDataMetrics;
use platform_time::RetryBackoff;
use std::collections::HashSet;
use std::time::Duration;
use tracing::{info, warn};

pub(super) enum Evaluation {
    Apply,
    AlreadyApplied,
    Conflict(ConflictDetails),
}

impl Evaluation {
    pub(super) fn resolve(
        registration: &ArchiveRegistration,
        claimed_sources: &HashSet<String>,
        live_outputs: &HashSet<String>,
    ) -> Self {
        let expected_outputs = registration.expected_outputs();
        let mut present_outputs = expected_outputs
            .intersection(live_outputs)
            .cloned()
            .collect::<Vec<_>>();
        present_outputs.sort();
        let all_outputs_present =
            !expected_outputs.is_empty() && present_outputs.len() == expected_outputs.len();

        let Some(sources) = registration.source_files() else {
            if expected_outputs.is_empty() || present_outputs.is_empty() {
                return Self::Apply;
            }
            if all_outputs_present {
                return Self::AlreadyApplied;
            }
            return Self::Conflict(ConflictDetails {
                claimed_sources: Vec::new(),
                live_outputs: present_outputs,
            });
        };

        let mut claimed_sources = sources
            .iter()
            .filter(|source| claimed_sources.contains(*source))
            .cloned()
            .collect::<Vec<_>>();
        claimed_sources.sort();
        claimed_sources.dedup();
        let all_sources_present = claimed_sources.len() == sources.len();

        if all_sources_present && (expected_outputs.is_empty() || all_outputs_present) {
            return Self::AlreadyApplied;
        }
        if !claimed_sources.is_empty() || !present_outputs.is_empty() {
            return Self::Conflict(ConflictDetails {
                claimed_sources,
                live_outputs: present_outputs,
            });
        }
        Self::Apply
    }
}

#[async_trait]
pub(super) trait ConditionalCommitTarget {
    type State;

    fn table(&self) -> ArchiveIcebergTable;

    async fn load(&self) -> PlatformResult<Self::State>;

    async fn evaluate(
        &self,
        state: &Self::State,
        registration: &ArchiveRegistration,
    ) -> PlatformResult<Evaluation>;

    async fn submit(
        &self,
        state: &Self::State,
        registration: &ArchiveRegistration,
    ) -> PlatformResult<()>;
}

pub(super) struct CommitRetry {
    pub max_attempts: usize,
    pub backoff: Duration,
    pub max_backoff: Duration,
}

impl Default for CommitRetry {
    fn default() -> Self {
        Self {
            max_attempts: 4,
            backoff: Duration::from_millis(250),
            max_backoff: Duration::from_secs(1),
        }
    }
}

pub(super) struct ConditionalCommit<B> {
    target: B,
    retry: CommitRetry,
}

impl<B> ConditionalCommit<B>
where
    B: ConditionalCommitTarget,
{
    pub(super) fn new(target: B, retry: CommitRetry) -> Self {
        Self { target, retry }
    }

    pub(super) async fn run(
        &self,
        registration: &ArchiveRegistration,
        context: &CommitContext,
    ) -> PlatformResult<RegisterOutcome> {
        let table = self.target.table();
        let digest = registration.batch_digest();
        let mut backoff =
            RetryBackoff::linear(self.retry.backoff, self.retry.max_backoff).with_jitter();
        let mut last_error = None;

        for attempt in 1..=self.retry.max_attempts {
            let current = self.target.load().await?;
            match self.target.evaluate(&current, registration).await? {
                Evaluation::Apply => {}
                Evaluation::AlreadyApplied => {
                    return Ok(Self::already_applied(table, &digest, context));
                }
                Evaluation::Conflict(conflict) => {
                    return Ok(Self::conflict(table, &digest, context, conflict));
                }
            }

            match self.target.submit(&current, registration).await {
                Ok(()) => {
                    MarketDataMetrics::record_archive_commit(table.name(), "applied");
                    info!(
                        table = table.name(),
                        scope = context.scope(),
                        owner = context.owner(),
                        token = context.fencing_token(),
                        source_batch_digest = %digest,
                        files = registration.files().len(),
                        "registered archive files in Iceberg"
                    );
                    return Ok(RegisterOutcome::Applied);
                }
                Err(error) => {
                    warn!(
                        %error,
                        table = table.name(),
                        scope = context.scope(),
                        owner = context.owner(),
                        token = context.fencing_token(),
                        source_batch_digest = %digest,
                        attempt,
                        "conditional Iceberg commit did not succeed"
                    );
                    last_error = Some(error);
                }
            }

            let reloaded = self.target.load().await?;
            match self.target.evaluate(&reloaded, registration).await? {
                Evaluation::AlreadyApplied => {
                    return Ok(Self::already_applied(table, &digest, context));
                }
                Evaluation::Conflict(conflict) => {
                    return Ok(Self::conflict(table, &digest, context, conflict));
                }
                Evaluation::Apply => {
                    if attempt < self.retry.max_attempts {
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

    fn already_applied(
        table: ArchiveIcebergTable,
        digest: &str,
        context: &CommitContext,
    ) -> RegisterOutcome {
        MarketDataMetrics::record_archive_commit(table.name(), "already_applied");
        info!(
            table = table.name(),
            scope = context.scope(),
            owner = context.owner(),
            token = context.fencing_token(),
            source_batch_digest = %digest,
            "archive registration was already applied"
        );
        RegisterOutcome::AlreadyApplied
    }

    fn conflict(
        table: ArchiveIcebergTable,
        digest: &str,
        context: &CommitContext,
        conflict: ConflictDetails,
    ) -> RegisterOutcome {
        MarketDataMetrics::record_archive_commit(table.name(), "conflict");
        warn!(
            table = table.name(),
            scope = context.scope(),
            owner = context.owner(),
            token = context.fencing_token(),
            source_batch_digest = %digest,
            claimed_sources = ?conflict.claimed_sources,
            live_outputs = ?conflict.live_outputs,
            "archive registration conflicted"
        );
        RegisterOutcome::Conflict(conflict)
    }
}
