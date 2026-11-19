use crate::bots::{Run, RunFilter, RunLogLink, RunState};
use crate::streaming::channel_registry::ChannelRegistry;
use crate::streaming::stream_types::Frame;
use chrono::{DateTime, Duration, Utc};
use parking_lot::Mutex;
use std::sync::Arc;

const RUN_RETENTION: Duration = Duration::days(90);
const SKIPPED_RETENTION: Duration = Duration::days(7);

struct RunRow {
    owner: String,
    run: Run,
    log: Option<RunLog>,
}

struct RunLog {
    url: String,
    expires_at: DateTime<Utc>,
}

pub(crate) struct RunStore {
    rows: Mutex<Vec<RunRow>>,
    registry: Arc<ChannelRegistry>,
}

impl RunStore {
    pub(crate) fn new(registry: Arc<ChannelRegistry>) -> Self {
        Self {
            rows: Mutex::new(Vec::new()),
            registry,
        }
    }

    pub(crate) fn insert(&self, owner: &str, run: Run) {
        let mut rows = self.rows.lock();
        rows.push(RunRow {
            owner: owner.to_string(),
            run: run.clone(),
            log: None,
        });
        Self::publish(&self.registry, &run.bot_id, &run);
    }

    pub(crate) fn get(&self, owner: &str, run_id: &str) -> Option<Run> {
        let mut rows = self.rows.lock();
        Self::sweep(&mut rows);
        rows.iter()
            .find(|row| row.owner == owner && row.run.run_id == run_id)
            .map(|row| row.run.clone())
    }

    pub(crate) fn list(&self, owner: &str, filter: &RunFilter) -> Vec<Run> {
        let mut rows = self.rows.lock();
        Self::sweep(&mut rows);
        let mut listed: Vec<&Run> = rows
            .iter()
            .filter(|row| row.owner == owner)
            .map(|row| &row.run)
            .filter(|run| Self::matches(run, filter))
            .collect();
        listed.sort_by_key(|row| std::cmp::Reverse(row.scheduled_at));
        listed.into_iter().cloned().collect()
    }

    pub(crate) fn active(&self, bot_id: &str) -> Option<Run> {
        let mut rows = self.rows.lock();
        Self::sweep(&mut rows);
        rows.iter()
            .map(|row| &row.run)
            .find(|run| {
                run.bot_id == bot_id && matches!(run.state, RunState::Pending | RunState::Running)
            })
            .cloned()
    }

    pub(crate) fn update(
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
        let run = row.run.clone();
        Self::publish(&self.registry, &run.bot_id, &run);
        Some(run)
    }

    pub(crate) fn log(&self, owner: &str, run_id: &str) -> Option<RunLogLink> {
        let mut rows = self.rows.lock();
        Self::sweep(&mut rows);
        let row = rows
            .iter()
            .find(|row| row.owner == owner && row.run.run_id == run_id)?;
        let log = row.log.as_ref()?;
        Some(RunLogLink {
            url: log.url.clone(),
            expires_at: log.expires_at,
        })
    }

    #[allow(dead_code)]
    pub(crate) fn attach_log(
        &self,
        owner: &str,
        run_id: &str,
        url: &str,
        expires_at: DateTime<Utc>,
    ) {
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
            let run = row.run.clone();
            Self::publish(&self.registry, &run.bot_id, &run);
        }
    }

    pub(crate) fn stream_sequence(&self, bot_id: &str) -> u64 {
        self.registry.sequence(&Self::channel(bot_id))
    }

    fn channel(bot_id: &str) -> String {
        format!("runs.{bot_id}")
    }

    fn publish(registry: &ChannelRegistry, bot_id: &str, run: &Run) {
        let channel = Self::channel(bot_id);
        let frame = Frame::data(
            &channel,
            0,
            serde_json::to_value(run).expect("run serializes"),
        );
        registry.publish(&channel, frame);
    }

    fn sweep(rows: &mut Vec<RunRow>) {
        let now = Utc::now();
        rows.retain(|row| !Self::expired(&row.run, now));
    }

    fn expired(run: &Run, now: DateTime<Utc>) -> bool {
        let Some(completed_at) = run.completed_at else {
            return false;
        };
        match run.state {
            RunState::Pending | RunState::Running => false,
            RunState::Skipped => completed_at + SKIPPED_RETENTION <= now,
            RunState::Completed | RunState::Failed | RunState::Canceled => {
                completed_at + RUN_RETENTION <= now
            }
        }
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
}
