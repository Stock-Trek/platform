use crate::accounts::AccountStore;
use crate::algorithms::{AlgorithmState, BuildStore};
use crate::api::{Id, Page, PageQuery, Problem};
use crate::auth::AuthContext;
use crate::bots::{Bot, BotState, BotStore, CreateBotRequest, PatchBotRequest, RunStore};
use crate::limits::{LimitService, QUOTA_BOTS, QUOTA_SCHEDULED_RUNS_PER_DAY};
use chrono::{DateTime, Utc};
use std::sync::Arc;

pub struct BotService {
    pub(super) bots: Arc<BotStore>,
    pub(super) runs: Arc<RunStore>,
    pub(super) accounts: Arc<AccountStore>,
    pub(super) builds: Arc<BuildStore>,
    pub(super) limits: Arc<LimitService>,
}

impl BotService {
    pub fn new(
        bots: Arc<BotStore>,
        runs: Arc<RunStore>,
        accounts: Arc<AccountStore>,
        builds: Arc<BuildStore>,
        limits: Arc<LimitService>,
    ) -> Self {
        Self {
            bots,
            runs,
            accounts,
            builds,
            limits,
        }
    }

    pub fn create(
        &self,
        auth: &AuthContext,
        request: &CreateBotRequest,
        now: DateTime<Utc>,
    ) -> Result<Bot, Problem> {
        let owner = auth.owner();
        self.limits.check(owner, QUOTA_BOTS, 1, now)?;
        let schedule = request.schedule.clone().validated()?;
        self.limits.check(
            owner,
            QUOTA_SCHEDULED_RUNS_PER_DAY,
            schedule.runs_per_day(),
            now,
        )?;
        self.validate_configuration(
            owner,
            &request.algorithm_id,
            &request.build_id,
            &request.strategy_name,
            &request.account_ids,
        )?;
        let bot = Bot {
            bot_id: Id::mint("bot"),
            name: request.name.clone(),
            algorithm_id: request.algorithm_id.clone(),
            build_id: request.build_id.clone(),
            strategy_name: request.strategy_name.clone(),
            account_ids: request.account_ids.clone(),
            schedule,
            state: BotState::Disabled,
            active_run_id: None,
            version: 1,
            created_at: now,
            updated_at: now,
            stream_sequence: 0,
        };
        self.bots.insert(owner, bot.clone());
        Ok(bot)
    }

    pub fn list(
        &self,
        auth: &AuthContext,
        state: Option<BotState>,
        query: &PageQuery,
    ) -> Page<Bot> {
        Page::of(self.bots.list(auth.owner(), state), query)
    }

    pub fn get(&self, auth: &AuthContext, bot_id: &str) -> Result<Bot, Problem> {
        self.bots
            .get(auth.owner(), bot_id)
            .ok_or_else(|| Problem::not_found("bot does not exist"))
    }

    pub fn patch(
        &self,
        auth: &AuthContext,
        bot_id: &str,
        if_match: Option<u64>,
        request: &PatchBotRequest,
        now: DateTime<Utc>,
    ) -> Result<Bot, Problem> {
        let owner = auth.owner();
        let bot = self.get(auth, bot_id)?;
        Self::require_if_match(if_match, bot.version)?;
        let build_id = request.build_id.clone().unwrap_or(bot.build_id.clone());
        let strategy_name = request
            .strategy_name
            .clone()
            .unwrap_or(bot.strategy_name.clone());
        let account_ids = request
            .account_ids
            .clone()
            .unwrap_or(bot.account_ids.clone());
        let schedule = match &request.schedule {
            Some(schedule) => schedule.clone().validated()?,
            None => bot.schedule.clone(),
        };
        self.validate_configuration(
            owner,
            &bot.algorithm_id,
            &build_id,
            &strategy_name,
            &account_ids,
        )?;
        self.bots
            .update(owner, bot_id, now, |mut bot| {
                if let Some(name) = &request.name {
                    bot.name = name.clone();
                }
                bot.build_id = build_id;
                bot.strategy_name = strategy_name;
                bot.account_ids = account_ids;
                bot.schedule = schedule;
                bot
            })
            .ok_or_else(|| Problem::not_found("bot does not exist"))
    }

    pub fn enable(
        &self,
        auth: &AuthContext,
        bot_id: &str,
        if_match: Option<u64>,
        now: DateTime<Utc>,
    ) -> Result<Bot, Problem> {
        let owner = auth.owner();
        let bot = self.get(auth, bot_id)?;
        Self::require_if_match(if_match, bot.version)?;
        for account_id in &bot.account_ids {
            if !self.accounts.active(owner, account_id) {
                return Err(Problem::state_conflict(
                    "a referenced account is no longer linked or active",
                ));
            }
        }
        self.limits.check(
            owner,
            QUOTA_SCHEDULED_RUNS_PER_DAY,
            bot.schedule.runs_per_day(),
            now,
        )?;
        self.bots
            .update(owner, bot_id, now, |mut bot| {
                bot.state = BotState::Enabled;
                bot
            })
            .ok_or_else(|| Problem::not_found("bot does not exist"))
    }

    pub fn disable(
        &self,
        auth: &AuthContext,
        bot_id: &str,
        if_match: Option<u64>,
        now: DateTime<Utc>,
    ) -> Result<Bot, Problem> {
        let owner = auth.owner();
        let bot = self.get(auth, bot_id)?;
        Self::require_if_match(if_match, bot.version)?;
        self.cancel_pending_runs(owner, &bot, now);
        self.bots
            .update(owner, bot_id, now, |mut bot| {
                bot.state = BotState::Disabled;
                bot
            })
            .ok_or_else(|| Problem::not_found("bot does not exist"))
    }

    pub fn archive(
        &self,
        auth: &AuthContext,
        bot_id: &str,
        now: DateTime<Utc>,
    ) -> Result<Bot, Problem> {
        let owner = auth.owner();
        let bot = self.get(auth, bot_id)?;
        if bot.state != BotState::Disabled || bot.active_run_id.is_some() {
            return Err(Problem::state_conflict(
                "a bot must be disabled with no active run to be archived",
            ));
        }
        self.bots
            .update(owner, bot_id, now, |mut bot| {
                bot.state = BotState::Archived;
                bot
            })
            .ok_or_else(|| Problem::not_found("bot does not exist"))
    }

    fn validate_configuration(
        &self,
        owner: &str,
        algorithm_id: &str,
        build_id: &str,
        strategy_name: &str,
        account_ids: &[String],
    ) -> Result<(), Problem> {
        let algorithm = self
            .builds
            .algorithm(owner, algorithm_id)
            .ok_or_else(|| Problem::validation_failed("algorithmId must reference an algorithm"))?;
        if algorithm.state != AlgorithmState::Active {
            return Err(Problem::validation_failed(
                "algorithmId must reference an active algorithm",
            ));
        }
        if self
            .builds
            .ready_build(owner, algorithm_id, build_id)
            .is_none()
        {
            return Err(Problem::validation_failed(
                "buildId must be a ready build of the algorithm",
            ));
        }
        let strategies = self
            .builds
            .strategies(owner, algorithm_id, build_id)
            .unwrap_or_default();
        if !strategies
            .iter()
            .any(|strategy| strategy.name == strategy_name)
        {
            return Err(Problem::validation_failed(
                "strategyName must exist in the build",
            ));
        }
        for account_id in account_ids {
            if !self.accounts.active(owner, account_id) {
                return Err(Problem::validation_failed(
                    "every account must be a linked, active account",
                ));
            }
        }
        Ok(())
    }

    pub(super) fn require_if_match(if_match: Option<u64>, version: u64) -> Result<(), Problem> {
        match if_match {
            None => Err(Problem::precondition_required()),
            Some(received) if received == version => Ok(()),
            Some(_) => Err(Problem::precondition_failed()),
        }
    }
}
