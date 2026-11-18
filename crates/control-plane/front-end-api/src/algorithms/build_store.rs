use crate::algorithms::{
    AlgorithmState, AlgorithmSummary, BuildState, BuildSummary, Repository, Strategy,
};
use crate::api::Id;
use chrono::{DateTime, Duration, Utc};
use parking_lot::Mutex;

const LOG_TTL: Duration = Duration::hours(1);

struct AlgorithmRow {
    owner: String,
    summary: AlgorithmSummary,
}

struct BuildRow {
    owner: String,
    build: BuildSummary,
    strategies: Vec<Strategy>,
    log: Option<String>,
}

pub struct BuildStore {
    algorithms: Mutex<Vec<AlgorithmRow>>,
    builds: Mutex<Vec<BuildRow>>,
}

impl BuildStore {
    pub fn new() -> Self {
        Self {
            algorithms: Mutex::new(Vec::new()),
            builds: Mutex::new(Vec::new()),
        }
    }

    pub fn register_algorithm(
        &self,
        owner: &str,
        name: &str,
        repo: &str,
        default_ref: &str,
    ) -> AlgorithmSummary {
        // TODO: algorithms appear when the GitHub App connection is registered through the build plane.
        let summary = AlgorithmSummary {
            algorithm_id: Id::mint("algorithm"),
            name: name.to_string(),
            repository: Repository {
                provider: "github".to_string(),
                host: "github.com".to_string(),
                repo: repo.to_string(),
            },
            default_ref: default_ref.to_string(),
            state: AlgorithmState::Active,
            version: 1,
            latest_ready_build_id: None,
        };
        self.algorithms.lock().push(AlgorithmRow {
            owner: owner.to_string(),
            summary: summary.clone(),
        });
        summary
    }

    pub fn list_algorithms(&self, owner: &str) -> Vec<AlgorithmSummary> {
        let rows = self.algorithms.lock();
        rows.iter()
            .filter(|row| row.owner == owner)
            .map(|row| row.summary.clone())
            .collect()
    }

    pub fn algorithm(&self, owner: &str, algorithm_id: &str) -> Option<AlgorithmSummary> {
        self.algorithms
            .lock()
            .iter()
            .find(|row| row.owner == owner && row.summary.algorithm_id == algorithm_id)
            .map(|row| row.summary.clone())
    }

    pub fn archive_algorithm(&self, owner: &str, algorithm_id: &str) -> Option<AlgorithmSummary> {
        let mut rows = self.algorithms.lock();
        let row = rows
            .iter_mut()
            .find(|row| row.owner == owner && row.summary.algorithm_id == algorithm_id)?;
        row.summary.state = AlgorithmState::Archived;
        row.summary.version += 1;
        Some(row.summary.clone())
    }

    pub fn create_build(
        &self,
        owner: &str,
        algorithm_id: &str,
        git_ref: &str,
        commit: &str,
        now: DateTime<Utc>,
    ) -> BuildSummary {
        let existing = self.find_build(owner, algorithm_id, git_ref, commit);
        if let Some(build) = existing.filter(|build| build.state != BuildState::Failed) {
            return build;
        }
        let build = BuildSummary {
            build_id: Id::mint("build"),
            algorithm_id: algorithm_id.to_string(),
            git_ref: git_ref.to_string(),
            commit: commit.to_string(),
            state: BuildState::Queued,
            error: None,
            log_available: false,
            created_at: now,
            completed_at: None,
            stream_sequence: 0,
        };
        self.builds.lock().push(BuildRow {
            owner: owner.to_string(),
            build: build.clone(),
            strategies: Vec::new(),
            log: None,
        });
        build
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

    pub fn builds(
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

    pub fn build(&self, owner: &str, algorithm_id: &str, build_id: &str) -> Option<BuildSummary> {
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

    pub fn ready_build(
        &self,
        owner: &str,
        algorithm_id: &str,
        build_id: &str,
    ) -> Option<BuildSummary> {
        let build = self.build(owner, algorithm_id, build_id)?;
        (build.state == BuildState::Ready).then_some(build)
    }

    pub fn mark_ready(
        &self,
        owner: &str,
        algorithm_id: &str,
        build_id: &str,
        strategies: Vec<Strategy>,
        now: DateTime<Utc>,
    ) -> Option<BuildSummary> {
        // TODO: the build agent drives this transition through the build plane.
        let mut rows = self.builds.lock();
        let row = rows.iter_mut().find(|row| {
            row.owner == owner
                && row.build.algorithm_id == algorithm_id
                && row.build.build_id == build_id
        })?;
        row.build.state = BuildState::Ready;
        row.build.completed_at = Some(now);
        row.build.stream_sequence += 1;
        row.strategies = strategies;
        let ready = row.build.clone();
        drop(rows);
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

    pub fn strategies(
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

    pub fn build_log(
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

    pub fn attach_log(&self, owner: &str, algorithm_id: &str, build_id: &str, url: &str) {
        // TODO: build logs are uploaded by the build agent to object storage.
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

    pub fn delete_build(&self, owner: &str, algorithm_id: &str, build_id: &str) -> bool {
        let mut builds = self.builds.lock();
        let before = builds.len();
        builds.retain(|row| {
            !(row.owner == owner
                && row.build.algorithm_id == algorithm_id
                && row.build.build_id == build_id)
        });
        builds.len() != before
    }
}
