use crate::algorithms::commit_resolution::CommitError;
use crate::algorithms::{BuildState, BuildSummary, Strategy};
use crate::api::Id;
use crate::streaming::channel_registry::ChannelRegistry;
use crate::streaming::stream_types::Frame;
use chrono::{DateTime, Duration, Utc};
use parking_lot::Mutex;
use serde_json::Value;
use std::sync::Arc;

const LOG_TTL: Duration = Duration::hours(1);

fn frame_payload(build: &BuildSummary) -> Value {
    let mut payload = serde_json::to_value(build).expect("build serializes");
    payload
        .as_object_mut()
        .expect("object")
        .remove("streamSequence");
    payload
}

pub(super) struct BuildRow {
    pub(super) owner: String,
    pub(super) build: BuildSummary,
    pub(super) strategies: Vec<Strategy>,
    pub(super) log: Option<String>,
}

pub(crate) struct BuildStore {
    pub(super) algorithms: Mutex<Vec<super::algorithm_store::AlgorithmRow>>,
    pub(super) builds: Mutex<Vec<BuildRow>>,
    registry: Arc<ChannelRegistry>,
}

impl BuildStore {
    pub(crate) fn new(registry: Arc<ChannelRegistry>) -> Self {
        Self {
            algorithms: Mutex::new(Vec::new()),
            builds: Mutex::new(Vec::new()),
            registry,
        }
    }

    pub(crate) fn create_build(
        &self,
        owner: &str,
        algorithm_id: &str,
        git_ref: &str,
        commit: Option<&str>,
        now: DateTime<Utc>,
    ) -> Result<BuildSummary, CommitError> {
        let full_commit = self.resolve_commit(algorithm_id, git_ref, commit)?;
        if let Some(build) = self
            .find_build(owner, algorithm_id, git_ref, &full_commit)
            .filter(|build| build.state != BuildState::Failed)
        {
            return Ok(build);
        }
        let mut build = BuildSummary {
            build_id: Id::mint("build"),
            algorithm_id: algorithm_id.to_string(),
            git_ref: git_ref.to_string(),
            commit: full_commit,
            state: BuildState::Queued,
            error: None,
            log_available: false,
            created_at: now,
            completed_at: None,
            stream_sequence: 0,
        };
        let mut builds = self.builds.lock();
        builds.push(BuildRow {
            owner: owner.to_string(),
            build: build.clone(),
            strategies: Vec::new(),
            log: None,
        });
        let channel = Self::channel(algorithm_id);
        let frame = Frame::data(&channel, 0, frame_payload(&build));
        build.stream_sequence = self.registry.publish(&channel, frame);
        if let Some(row) = builds
            .iter_mut()
            .find(|row| row.build.build_id == build.build_id)
        {
            row.build.stream_sequence = build.stream_sequence;
        }
        Ok(build)
    }

    fn channel(algorithm_id: &str) -> String {
        format!("builds.{algorithm_id}")
    }

    fn resolve_commit(
        &self,
        algorithm_id: &str,
        git_ref: &str,
        commit: Option<&str>,
    ) -> Result<String, CommitError> {
        let mut rows = self.algorithms.lock();
        let row = rows
            .iter_mut()
            .find(|row| row.summary.algorithm_id == algorithm_id)
            .ok_or(CommitError::Unresolved)?;
        row.known_commits.resolve(commit, algorithm_id, git_ref)
    }

    fn find_build(
        &self,
        owner: &str,
        algorithm_id: &str,
        git_ref: &str,
        commit: &str,
    ) -> Option<BuildSummary> {
        self.builds
            .lock()
            .iter()
            .filter(|row| row.owner == owner && row.build.algorithm_id == algorithm_id)
            .map(|row| &row.build)
            .find(|build| build.git_ref == git_ref && build.commit == commit)
            .cloned()
    }

    pub(crate) fn builds(
        &self,
        owner: &str,
        algorithm_id: &str,
        git_ref: Option<&str>,
        state: Option<BuildState>,
    ) -> Vec<BuildSummary> {
        let rows = self.builds.lock();
        let mut listed: Vec<BuildSummary> = rows
            .iter()
            .filter(|row| row.owner == owner && row.build.algorithm_id == algorithm_id)
            .map(|row| row.build.clone())
            .filter(|build| git_ref.is_none_or(|git_ref| build.git_ref == git_ref))
            .filter(|build| state.is_none_or(|state| build.state == state))
            .collect();
        listed.sort_by_key(|row| std::cmp::Reverse(row.created_at));
        listed
    }

    pub(crate) fn build(
        &self,
        owner: &str,
        algorithm_id: &str,
        build_id: &str,
    ) -> Option<BuildSummary> {
        self.builds
            .lock()
            .iter()
            .find(|row| {
                row.owner == owner
                    && row.build.algorithm_id == algorithm_id
                    && row.build.build_id == build_id
            })
            .map(|row| row.build.clone())
    }

    pub(crate) fn ready_build(
        &self,
        owner: &str,
        algorithm_id: &str,
        build_id: &str,
    ) -> Option<BuildSummary> {
        let build = self.build(owner, algorithm_id, build_id)?;
        (build.state == BuildState::Ready).then_some(build)
    }

    #[allow(dead_code)]
    pub(crate) fn mark_ready(
        &self,
        owner: &str,
        algorithm_id: &str,
        build_id: &str,
        strategies: Vec<Strategy>,
        now: DateTime<Utc>,
    ) -> Option<BuildSummary> {
        let mut rows = self.builds.lock();
        let row = rows.iter_mut().find(|row| {
            row.owner == owner
                && row.build.algorithm_id == algorithm_id
                && row.build.build_id == build_id
        })?;
        row.build.state = BuildState::Ready;
        row.build.completed_at = Some(now);
        row.strategies = strategies;
        let mut ready = row.build.clone();
        drop(rows);
        let channel = Self::channel(algorithm_id);
        let frame = Frame::data(&channel, 0, frame_payload(&ready));
        ready.stream_sequence = self.registry.publish(&channel, frame);
        let mut builds = self.builds.lock();
        if let Some(row) = builds.iter_mut().find(|row| row.build.build_id == build_id) {
            row.build.stream_sequence = ready.stream_sequence;
        }
        drop(builds);
        self.record_ready_build(algorithm_id, &ready);
        Some(ready)
    }

    fn record_ready_build(&self, algorithm_id: &str, build: &BuildSummary) {
        let mut rows = self.algorithms.lock();
        if let Some(row) = rows
            .iter_mut()
            .find(|row| row.summary.algorithm_id == algorithm_id)
        {
            row.summary.latest_ready_build_id = Some(build.build_id.clone());
        }
    }

    pub(crate) fn strategies(
        &self,
        owner: &str,
        algorithm_id: &str,
        build_id: &str,
    ) -> Option<Vec<Strategy>> {
        self.build(owner, algorithm_id, build_id)?;
        self.builds
            .lock()
            .iter()
            .find(|row| {
                row.owner == owner
                    && row.build.algorithm_id == algorithm_id
                    && row.build.build_id == build_id
            })
            .map(|row| row.strategies.clone())
    }

    pub(crate) fn build_log(
        &self,
        owner: &str,
        algorithm_id: &str,
        build_id: &str,
        now: DateTime<Utc>,
    ) -> Option<crate::algorithms::BuildLogLink> {
        self.build(owner, algorithm_id, build_id)?;
        let builds = self.builds.lock();
        let row = builds.iter().find(|row| {
            row.owner == owner
                && row.build.algorithm_id == algorithm_id
                && row.build.build_id == build_id
        })?;
        let log = row.log.as_ref()?;
        Some(crate::algorithms::BuildLogLink {
            url: log.clone(),
            expires_at: now + LOG_TTL,
        })
    }

    #[allow(dead_code)]
    pub(crate) fn attach_log(&self, owner: &str, algorithm_id: &str, build_id: &str, url: &str) {
        let mut builds = self.builds.lock();
        if let Some(row) = builds.iter_mut().find(|row| {
            row.owner == owner
                && row.build.algorithm_id == algorithm_id
                && row.build.build_id == build_id
        }) {
            row.log = Some(url.to_string());
            row.build.log_available = true;
        }
    }

    pub(crate) fn delete_build(&self, owner: &str, algorithm_id: &str, build_id: &str) -> bool {
        let mut builds = self.builds.lock();
        let before = builds.len();
        builds.retain(|row| {
            !(row.owner == owner
                && row.build.algorithm_id == algorithm_id
                && row.build.build_id == build_id)
        });
        let deleted = builds.len() != before;
        let latest_ready = deleted.then(|| {
            builds
                .iter()
                .filter(|row| {
                    row.owner == owner
                        && row.build.algorithm_id == algorithm_id
                        && row.build.state == BuildState::Ready
                })
                .max_by_key(|row| row.build.created_at)
                .map(|row| row.build.build_id.clone())
        });
        drop(builds);
        if let Some(latest_ready) = latest_ready {
            let mut rows = self.algorithms.lock();
            if let Some(row) = rows
                .iter_mut()
                .find(|row| row.summary.algorithm_id == algorithm_id)
            {
                row.summary.latest_ready_build_id = latest_ready;
            }
        }
        deleted
    }
}
