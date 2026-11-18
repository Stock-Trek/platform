use super::lifecycle::BotService;
use crate::api::{Id, Problem};
use crate::auth::AuthContext;
use crate::bots::{Bot, BotState, Run, RunState, RunTrigger, SkipReason};
use chrono::{DateTime, Utc};

impl BotService {
    pub fn trigger_ad_hoc_run(
        &self,
        auth: &AuthContext,
        bot_id: &str,
        now: DateTime<Utc>,
    ) -> Result<Run, Problem> {
        let owner = auth.owner();
        let bot = self.get(auth, bot_id)?;
        if bot.state != BotState::Enabled {
            return Err(Problem::state_conflict(
                "ad-hoc runs require an enabled bot",
            ));
        }
        if self.runs.active(&bot.bot_id).is_some() {
            return Err(Problem::state_conflict("the bot already has an active run"));
        }
        // TODO: enforce the ad-hoc runs per hour per bot rate limit.
        Ok(self.materialize_run(owner, &bot, RunTrigger::AdHoc, now))
    }

    pub(super) fn materialize_run(
        &self,
        owner: &str,
        bot: &Bot,
        trigger: RunTrigger,
        now: DateTime<Utc>,
    ) -> Run {
        let run = Run {
            run_id: Id::mint("run"),
            bot_id: bot.bot_id.clone(),
            algorithm_id: bot.algorithm_id.clone(),
            build_id: Some(bot.build_id.clone()),
            strategy_name: Some(bot.strategy_name.clone()),
            trigger,
            scheduled_at: now,
            state: RunState::Pending,
            completed_at: None,
            log_available: false,
            cancel_requested: false,
            error: None,
            skip_reason: None,
        };
        self.runs.insert(owner, run.clone());
        self.bots.update(owner, &bot.bot_id, now, |mut bot| {
            bot.active_run_id = Some(run.run_id.clone());
            bot.stream_sequence += 1;
            bot
        });
        run
    }

    pub fn record_skipped_tick(
        &self,
        owner: &str,
        bot: &Bot,
        reason: SkipReason,
        now: DateTime<Utc>,
    ) {
        // TODO: the run invoker records skipped ticks when claims fail.
        let run = Run {
            run_id: Id::mint("run"),
            bot_id: bot.bot_id.clone(),
            algorithm_id: bot.algorithm_id.clone(),
            build_id: None,
            strategy_name: None,
            trigger: RunTrigger::Schedule,
            scheduled_at: now,
            state: RunState::Skipped,
            completed_at: Some(now),
            log_available: false,
            cancel_requested: false,
            error: None,
            skip_reason: Some(reason),
        };
        self.runs.insert(owner, run);
    }
}
