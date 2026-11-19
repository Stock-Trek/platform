use crate::algorithms::build_store::BuildStore;
use crate::algorithms::{AlgorithmState, AlgorithmSummary, Repository};
use crate::api::Id;

pub(super) struct AlgorithmRow {
    pub(super) owner: String,
    pub(super) summary: AlgorithmSummary,
    pub(super) known_commits: Vec<String>,
}

impl BuildStore {
    #[allow(dead_code)]
    pub fn register_algorithm(
        &self,
        owner: &str,
        name: &str,
        repo: &str,
        default_ref: &str,
    ) -> AlgorithmSummary {
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
            known_commits: Vec::new(),
        });
        summary
    }

    pub fn list_algorithms(&self, owner: &str) -> Vec<AlgorithmSummary> {
        let rows = self.algorithms.lock();
        rows.iter()
            .rev()
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

    pub fn owns_algorithm(&self, owner: &str, algorithm_id: &str) -> bool {
        self.algorithms
            .lock()
            .iter()
            .any(|row| row.owner == owner && row.summary.algorithm_id == algorithm_id)
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
}
