use crate::bots::{Run, RunFilter, RunLogLink, RunState};
use chrono::{DateTime, Utc};
use parking_lot::Mutex;
use std::collections::HashMap;

struct RunRow {
    owner: String,
    run: Run,
    log: Option<RunLog>,
}

struct RunLog {
    url: String,
    expires_at: DateTime<Utc>,
}

pub struct RunStore {
    rows: Mutex<Vec<RunRow>>,
    sequences: Mutex<HashMap<String, u64>>,
}

impl RunStore {
    pub fn new() -> Self {
        Self {
            rows: Mutex::new(Vec::new()),
            sequences: Mutex::new(HashMap::new()),
        }
    }

    pub fn insert(&self, owner: &str, run: Run) {
        let bot_id = run.bot_id.clone();
        self.rows.lock().push(RunRow {
            owner: owner.to_string(),
            run,
            log: None,
        });
        Self::bump(&self.sequences, &bot_id);
    }

    pub fn get(&self, owner: &str, run_id: &str) -> Option<Run> {
        self.rows
            .lock()
            .iter()
            .find(|row| row.owner == owner && row.run.run_id == run_id)
            .map(|row| row.run.clone())
    }

    pub fn list(&self, owner: &str, filter: &RunFilter) -> Vec<Run> {
        let rows = self.rows.lock();
        let mut listed: Vec<&Run> = rows
            .iter()
            .filter(|row| row.owner == owner)
            .map(|row| &row.run)
            .filter(|run| Self::matches(run, filter))
            .collect();
        listed.sort_by_key(|row| std::cmp::Reverse(row.scheduled_at));
        listed.into_iter().cloned().collect()
    }

    pub fn active(&self, bot_id: &str) -> Option<Run> {
        self.rows
            .lock()
            .iter()
            .map(|row| &row.run)
            .find(|run| {
                run.bot_id == bot_id && matches!(run.state, RunState::Pending | RunState::Running)
            })
            .cloned()
    }

    pub fn update(
        &self,
        owner: &str,
        run_id: &str,
        update: impl FnOnce(Run) -> Run,
    ) -> Option<Run> {
        let mut rows = self.rows.lock();
        let row = rows
            .iter_mut()
            .find(|row| row.owner == owner && row.run.run_id == run_id)?;
        row.run = update(row.run.clone());
        Self::bump(&self.sequences, &row.run.bot_id);
        Some(row.run.clone())
    }

    pub fn log(&self, owner: &str, run_id: &str) -> Option<RunLogLink> {
        let rows = self.rows.lock();
        let row = rows
            .iter()
            .find(|row| row.owner == owner && row.run.run_id == run_id)?;
        let log = row.log.as_ref()?;
        Some(RunLogLink {
            url: log.url.clone(),
            expires_at: log.expires_at,
        })
    }

    pub fn attach_log(&self, owner: &str, run_id: &str, url: &str, expires_at: DateTime<Utc>) {
        let mut rows = self.rows.lock();
        if let Some(row) = rows
            .iter_mut()
            .find(|row| row.owner == owner && row.run.run_id == run_id)
        {
            row.log = Some(RunLog {
                url: url.to_string(),
                expires_at,
            });
            row.run.log_available = true;
        }
    }

    pub fn stream_sequence(&self, bot_id: &str) -> u64 {
        self.sequences.lock().get(bot_id).copied().unwrap_or(0)
    }

    fn matches(run: &Run, filter: &RunFilter) -> bool {
        filter
            .bot_id
            .as_ref()
            .is_none_or(|bot_id| run.bot_id == *bot_id)
            && filter.state.is_none_or(|state| run.state == state)
            && filter.from.is_none_or(|from| run.scheduled_at >= from)
            && filter.to.is_none_or(|to| run.scheduled_at <= to)
    }

    fn bump(sequences: &Mutex<HashMap<String, u64>>, bot_id: &str) {
        let mut sequences = sequences.lock();
        let next = sequences.get(bot_id).copied().unwrap_or(0) + 1;
        sequences.insert(bot_id.to_string(), next);
    }
}
