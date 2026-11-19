use crate::api::Problem;
use crate::api::{Page, PageQuery};
use crate::auth::AuthContext;
use crate::bots::RunStore;
use crate::bots::{Run, RunFilter, RunLogLink, RunPage, RunState};
use chrono::{DateTime, Utc};
use std::sync::Arc;

pub(crate) enum CancelOutcome {
    Canceled(Run),
    Requested,
    Unchanged(Run),
}

pub(crate) struct RunService {
    runs: Arc<RunStore>,
}

impl RunService {
    pub(crate) fn new(runs: Arc<RunStore>) -> Self {
        Self { runs }
    }

    pub(crate) fn list(
        &self,
        auth: &AuthContext,
        filter: &RunFilter,
        query: &PageQuery,
    ) -> RunPage {
        let runs = self.runs.list(auth.owner(), filter);
        let stream_sequence = filter
            .bot_id
            .as_ref()
            .map(|bot_id| self.runs.stream_sequence(bot_id));
        let page = Page::of(runs, query);
        RunPage {
            page,
            stream_sequence,
        }
    }

    pub(crate) fn get(&self, auth: &AuthContext, run_id: &str) -> Result<Run, Problem> {
        self.runs
            .get(auth.owner(), run_id)
            .ok_or_else(|| Problem::not_found("run does not exist"))
    }

    pub(crate) fn cancel(
        &self,
        auth: &AuthContext,
        run_id: &str,
        now: DateTime<Utc>,
    ) -> Result<CancelOutcome, Problem> {
        let owner = auth.owner();
        let run = self.get(auth, run_id)?;
        match run.state {
            RunState::Pending => {
                let canceled = self
                    .runs
                    .update(owner, run_id, |mut run| {
                        run.state = RunState::Canceled;
                        run.completed_at = Some(now);
                        run
                    })
                    .ok_or_else(|| Problem::not_found("run does not exist"))?;
                Ok(CancelOutcome::Canceled(canceled))
            }
            RunState::Running => {
                self.runs.update(owner, run_id, |mut run| {
                    run.cancel_requested = true;
                    run
                });
                Ok(CancelOutcome::Requested)
            }
            _ => Ok(CancelOutcome::Unchanged(run)),
        }
    }

    pub(crate) fn log(&self, auth: &AuthContext, run_id: &str) -> Result<RunLogLink, Problem> {
        self.runs
            .log(auth.owner(), run_id)
            .ok_or_else(|| Problem::not_found("no run log exists yet"))
    }
}
