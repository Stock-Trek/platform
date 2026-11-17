use super::commit_algorithm::{CommitRetry, ConditionalCommitTarget, Evaluation};
use crate::iceberg::{ArchiveIcebergTable, ArchiveRegistration, CommitContext, IcebergDataFile};
use async_trait::async_trait;
use parking_lot::Mutex;
use platform_errors::{PlatformError, PlatformResult};
use std::collections::HashSet;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::Duration;
use tokio::sync::{Barrier, Notify};

#[derive(Clone, Default)]
pub(super) struct FakeState {
    pub(super) version: u64,
    pub(super) claimed_sources: HashSet<String>,
    pub(super) live_outputs: HashSet<String>,
}

#[derive(Default)]
pub(super) struct FakeBackend {
    state: Mutex<FakeState>,
    pub(super) after_apply_error: AtomicBool,
    pub(super) fail_before_apply: AtomicBool,
    pub(super) commits: AtomicUsize,
    first_commit: Notify,
}

impl FakeBackend {
    pub(super) fn claim(&self, sources: &[&str], outputs: &[&str]) {
        let mut state = self.state.lock();
        state
            .claimed_sources
            .extend(sources.iter().map(|source| source.to_string()));
        state
            .live_outputs
            .extend(outputs.iter().map(|output| output.to_string()));
        state.version += 1;
    }

    fn state(&self) -> FakeState {
        self.state.lock().clone()
    }

    fn apply(&self, expected_version: u64, registration: &ArchiveRegistration) -> bool {
        let mut state = self.state.lock();
        if state.version != expected_version {
            return false;
        }
        if let Some(sources) = registration.source_files() {
            state.claimed_sources.extend(sources.iter().cloned());
        }
        state.live_outputs.extend(registration.expected_outputs());
        state.version += 1;
        self.commits.fetch_add(1, Ordering::SeqCst);
        drop(state);
        self.first_commit.notify_one();
        true
    }
}

pub(super) struct FakeTarget {
    backend: Arc<FakeBackend>,
    barrier: Option<Arc<Barrier>>,
    wait_for_first_commit: bool,
}

impl FakeTarget {
    pub(super) fn new(backend: Arc<FakeBackend>) -> Self {
        Self {
            backend,
            barrier: None,
            wait_for_first_commit: false,
        }
    }

    pub(super) fn with_barrier(backend: Arc<FakeBackend>, barrier: Arc<Barrier>) -> Self {
        Self {
            backend,
            barrier: Some(barrier),
            wait_for_first_commit: false,
        }
    }

    pub(super) fn waiting_for_first_commit(
        backend: Arc<FakeBackend>,
        barrier: Arc<Barrier>,
    ) -> Self {
        Self {
            backend,
            barrier: Some(barrier),
            wait_for_first_commit: true,
        }
    }

    pub(super) fn retry() -> CommitRetry {
        CommitRetry {
            max_attempts: 3,
            backoff: Duration::ZERO,
            max_backoff: Duration::ZERO,
        }
    }

    pub(super) fn registration(sources: Option<&[&str]>, outputs: &[&str]) -> ArchiveRegistration {
        ArchiveRegistration::new(
            outputs
                .iter()
                .map(|output| IcebergDataFile::new(*output, 10, 2)),
            sources.map(|sources| {
                sources
                    .iter()
                    .map(|source| source.to_string())
                    .collect::<Vec<_>>()
            }),
        )
        .unwrap()
    }
}

#[async_trait]
impl ConditionalCommitTarget for FakeTarget {
    type State = FakeState;

    fn table(&self) -> ArchiveIcebergTable {
        ArchiveIcebergTable::CompactedEvents
    }

    async fn load(&self, _context: &CommitContext) -> PlatformResult<Self::State> {
        Ok(self.backend.state())
    }

    async fn evaluate(
        &self,
        state: &Self::State,
        registration: &ArchiveRegistration,
    ) -> PlatformResult<Evaluation> {
        let evaluation =
            Evaluation::resolve(registration, &state.claimed_sources, &state.live_outputs);
        if matches!(evaluation, Evaluation::Apply)
            && let Some(barrier) = &self.barrier
        {
            barrier.wait().await;
        }
        Ok(evaluation)
    }

    async fn submit(
        &self,
        state: &Self::State,
        registration: &ArchiveRegistration,
        _context: &CommitContext,
    ) -> PlatformResult<()> {
        if self.wait_for_first_commit {
            self.backend.first_commit.notified().await;
        }
        if self.backend.fail_before_apply.swap(false, Ordering::SeqCst) {
            return Err(PlatformError::external("transport failed"));
        }
        if !self.backend.apply(state.version, registration) {
            return Err(PlatformError::external("snapshot requirement failed"));
        }
        if self.backend.after_apply_error.swap(false, Ordering::SeqCst) {
            return Err(PlatformError::external("commit response was lost"));
        }
        Ok(())
    }
}
