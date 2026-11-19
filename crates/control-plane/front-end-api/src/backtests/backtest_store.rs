use crate::backtests::{BacktestError, BacktestResultItem, BacktestState, BacktestStatus};
use crate::limits::MAX_RESULT_BYTES;
use crate::streaming::channel_registry::ChannelRegistry;
use crate::streaming::stream_types::Frame;
use chrono::Utc;
use parking_lot::{Mutex, MutexGuard};
use serde_json::Value;
use std::sync::Arc;

const RESULT_TOO_LARGE_CODE: &str = "result_too_large";

struct BacktestRow {
    owner: String,
    build_id: String,
    status: BacktestStatus,
    results: Vec<BacktestResultItem>,
}

pub struct BacktestStore {
    rows: Mutex<Vec<BacktestRow>>,
    registry: Arc<ChannelRegistry>,
}

impl BacktestStore {
    pub fn new(registry: Arc<ChannelRegistry>) -> Self {
        Self {
            rows: Mutex::new(Vec::new()),
            registry,
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
        let index = rows
            .iter()
            .position(|row| row.owner == owner && row.status.backtest_id == backtest_id)?;
        let was_terminal = rows[index].status.state.is_terminal();
        rows[index].status = update(rows[index].status.clone());
        let became_terminal = !was_terminal && rows[index].status.state.is_terminal();
        if became_terminal {
            Self::publish_final(&mut rows, &self.registry, backtest_id);
        }
        Some(rows[index].status.clone())
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
    pub fn append_result(&self, owner: &str, backtest_id: &str, simulation: u64, result: Value) {
        let oversized =
            serde_json::to_vec(&result).is_ok_and(|bytes| bytes.len() as u64 > MAX_RESULT_BYTES);
        let mut rows = self.rows.lock();
        let Some(row) = rows
            .iter_mut()
            .find(|row| row.owner == owner && row.status.backtest_id == backtest_id)
        else {
            return;
        };
        if oversized {
            row.status.state = BacktestState::Failed;
            row.status.error = Some(BacktestError {
                code: RESULT_TOO_LARGE_CODE.to_string(),
                message: format!("a simulation result exceeds the {MAX_RESULT_BYTES} byte limit"),
            });
            row.status.completed_at = Some(Utc::now());
            Self::publish_final(&mut rows, &self.registry, backtest_id);
            return;
        }
        let channel = Self::channel(backtest_id);
        let frame = Frame::backtest_result(&channel, 0, simulation, result.clone());
        let sequence = self.registry.publish(&channel, frame);
        row.results.push(BacktestResultItem {
            sequence,
            simulation,
            result,
        });
        row.status.progress.completed = row.results.len() as u64;
        row.status.stream_sequence = sequence;
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

    fn channel(backtest_id: &str) -> String {
        format!("backtests.{backtest_id}")
    }

    fn publish_final(
        rows: &mut MutexGuard<'_, Vec<BacktestRow>>,
        registry: &ChannelRegistry,
        backtest_id: &str,
    ) {
        let channel = Self::channel(backtest_id);
        let state = rows
            .iter()
            .find(|row| row.status.backtest_id == backtest_id)
            .map(|row| row.status.state);
        let Some(state) = state else {
            return;
        };
        let frame = Frame::backtest_final(&channel, 0, state.as_str());
        let sequence = registry.publish(&channel, frame);
        if let Some(row) = rows
            .iter_mut()
            .find(|row| row.status.backtest_id == backtest_id)
        {
            row.status.stream_sequence = sequence;
        }
    }
}
