use crate::backtests::{BacktestResultItem, BacktestState, BacktestStatus};
use parking_lot::Mutex;

struct BacktestRow {
    owner: String,
    build_id: String,
    status: BacktestStatus,
    results: Vec<BacktestResultItem>,
}

pub struct BacktestStore {
    rows: Mutex<Vec<BacktestRow>>,
}

impl BacktestStore {
    pub fn new() -> Self {
        Self {
            rows: Mutex::new(Vec::new()),
        }
    }

    pub fn insert(&self, owner: &str, status: BacktestStatus) {
        self.rows.lock().push(BacktestRow {
            owner: owner.to_string(),
            build_id: status.build_id.clone(),
            status,
            results: Vec::new(),
        });
    }

    pub fn list(&self, owner: &str, state: Option<BacktestState>) -> Vec<BacktestStatus> {
        let rows = self.rows.lock();
        let mut listed: Vec<BacktestStatus> = rows
            .iter()
            .filter(|row| row.owner == owner)
            .map(|row| row.status.clone())
            .filter(|status| state.is_none_or(|state| status.state == state))
            .collect();
        listed.sort_by_key(|row| std::cmp::Reverse(row.created_at));
        listed
    }

    pub fn status(&self, owner: &str, backtest_id: &str) -> Option<BacktestStatus> {
        self.rows
            .lock()
            .iter()
            .find(|row| row.owner == owner && row.status.backtest_id == backtest_id)
            .map(|row| row.status.clone())
    }

    pub fn update(
        &self,
        owner: &str,
        backtest_id: &str,
        update: impl FnOnce(BacktestStatus) -> BacktestStatus,
    ) -> Option<BacktestStatus> {
        let mut rows = self.rows.lock();
        let row = rows
            .iter_mut()
            .find(|row| row.owner == owner && row.status.backtest_id == backtest_id)?;
        row.status = update(row.status.clone());
        Some(row.status.clone())
    }

    pub fn results(
        &self,
        owner: &str,
        backtest_id: &str,
        offset: usize,
        limit: usize,
    ) -> Option<(Vec<BacktestResultItem>, BacktestState, bool)> {
        let rows = self.rows.lock();
        let row = rows
            .iter()
            .find(|row| row.owner == owner && row.status.backtest_id == backtest_id)?;
        let data = row
            .results
            .get(offset..(offset + limit).min(row.results.len()))
            .unwrap_or_default()
            .to_vec();
        let has_more = offset + data.len() < row.results.len();
        Some((data, row.status.state, has_more))
    }

    #[allow(dead_code)]
    pub fn append_result(&self, owner: &str, backtest_id: &str, item: BacktestResultItem) {
        let mut rows = self.rows.lock();
        if let Some(row) = rows
            .iter_mut()
            .find(|row| row.owner == owner && row.status.backtest_id == backtest_id)
        {
            row.results.push(item);
            row.status.progress.completed = row.results.len() as u64;
            row.status.stream_sequence += 1;
        }
    }

    pub fn count_active(&self, owner: &str) -> usize {
        self.rows
            .lock()
            .iter()
            .filter(|row| row.owner == owner && row.status.state.is_active())
            .count()
    }

    pub fn delete_for_build(&self, build_id: &str) {
        self.rows.lock().retain(|row| row.build_id != build_id);
    }
}
