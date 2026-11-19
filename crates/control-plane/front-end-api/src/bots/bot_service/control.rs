use super::lifecycle::BotService;
use crate::bots::{Bot, BotState, KillResponse, RunState};
use chrono::{DateTime, Utc};

impl BotService {
    pub(crate) fn kill(
        &self,
        auth: &crate::auth::AuthContext,
        account_ids: Option<Vec<String>>,
        now: DateTime<Utc>,
    ) -> KillResponse {
        let owner = auth.owner();
        let bots = match &account_ids {
            Some(account_ids) => self.bots.referencing_accounts(owner, account_ids),
            None => self.bots.all(owner),
        };
        self.stop_bots(owner, bots, now)
    }

    pub(crate) fn referencing_accounts(&self, owner: &str, account_ids: &[String]) -> Vec<Bot> {
        self.bots.referencing_accounts(owner, account_ids)
    }

    pub(crate) fn disable_bots(
        &self,
        owner: &str,
        bots: Vec<Bot>,
        now: DateTime<Utc>,
    ) -> KillResponse {
        self.stop_bots(owner, bots, now)
    }

    fn stop_bots(&self, owner: &str, bots: Vec<Bot>, now: DateTime<Utc>) -> KillResponse {
        let mut disabled_bot_ids = Vec::new();
        let mut canceled_run_ids = Vec::new();
        for bot in bots {
            let canceled = self.cancel_pending_runs(owner, &bot, now);
            canceled_run_ids.extend(canceled);
            let requested = self.request_cancel_running(owner, &bot, now);
            canceled_run_ids.extend(requested);
            if bot.state == BotState::Enabled {
                self.bots.update(owner, &bot.bot_id, now, |mut bot| {
                    bot.state = BotState::Disabled;
                    bot
                });
                disabled_bot_ids.push(bot.bot_id);
            }
        }
        KillResponse {
            disabled_bot_ids,
            canceled_run_ids,
        }
    }

    pub(super) fn cancel_pending_runs(
        &self,
        owner: &str,
        bot: &Bot,
        now: DateTime<Utc>,
    ) -> Vec<String> {
        let pending = self.runs.list(
            owner,
            &crate::bots::RunFilter {
                bot_id: Some(bot.bot_id.clone()),
                state: Some(RunState::Pending),
                from: None,
                to: None,
            },
        );
        let mut canceled = Vec::new();
        for run in pending {
            self.runs.update(owner, &run.run_id, |mut run| {
                run.state = RunState::Canceled;
                run.completed_at = Some(now);
                run
            });
            canceled.push(run.run_id);
        }
        if !canceled.is_empty() {
            self.bots.update(owner, &bot.bot_id, now, |bot| bot);
        }
        canceled
    }

    fn request_cancel_running(&self, owner: &str, bot: &Bot, _now: DateTime<Utc>) -> Vec<String> {
        let mut requested = Vec::new();
        if let Some(run) = self
            .runs
            .active(&bot.bot_id)
            .filter(|run| run.state == RunState::Running)
        {
            self.runs.update(owner, &run.run_id, |mut run| {
                run.cancel_requested = true;
                run
            });
            requested.push(run.run_id);
        }
        requested
    }
}
