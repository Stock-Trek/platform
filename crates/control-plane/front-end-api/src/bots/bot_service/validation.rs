use super::lifecycle::BotService;
use crate::algorithms::AlgorithmState;
use crate::api::Problem;
use crate::bots::{Bot, BotState};

impl BotService {
    pub(super) fn validate_creation(
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
        self.validate_configuration(owner, algorithm_id, build_id, strategy_name, account_ids)
    }

    pub(super) fn validate_configuration(
        &self,
        owner: &str,
        algorithm_id: &str,
        build_id: &str,
        strategy_name: &str,
        account_ids: &[String],
    ) -> Result<(), Problem> {
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

    pub(super) fn require_mutable(bot: &Bot) -> Result<(), Problem> {
        if bot.state == BotState::Archived {
            return Err(Problem::state_conflict("an archived bot is read-only"));
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
