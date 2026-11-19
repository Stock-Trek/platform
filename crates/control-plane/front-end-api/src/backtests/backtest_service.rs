use crate::algorithms::BuildStore;
use crate::api::Id;
use crate::api::Problem;
use crate::api::{Page, PageQuery};
use crate::auth::AuthContext;
use crate::backtests::BacktestStore;
use crate::backtests::{
    BacktestCreated, BacktestResultsPage, BacktestState, BacktestStatus, CreateBacktestRequest,
    ExportLink,
};
use crate::limits::{LimitService, QUOTA_CONCURRENT_BACKTESTS};
use chrono::{DateTime, Duration, Utc};
use std::sync::Arc;

const EXPORT_TTL: Duration = Duration::hours(1);
const RATE_LIMIT_RETRY_SECONDS: u64 = 60;

pub struct BacktestService {
    builds: Arc<BuildStore>,
    backtests: Arc<BacktestStore>,
    limits: Arc<LimitService>,
}

impl BacktestService {
    pub fn new(
        builds: Arc<BuildStore>,
        backtests: Arc<BacktestStore>,
        limits: Arc<LimitService>,
    ) -> Self {
        Self {
            builds,
            backtests,
            limits,
        }
    }

    pub fn create(
        &self,
        auth: &AuthContext,
        request: &CreateBacktestRequest,
        now: DateTime<Utc>,
    ) -> Result<BacktestCreated, Problem> {
        let owner = auth.owner();
        if self
            .builds
            .ready_build(owner, &request.algorithm_id, &request.build_id)
            .is_none()
        {
            return Err(Problem::validation_failed(
                "buildId must be a ready build of the algorithm",
            ));
        }
        self.validate_strategy(
            owner,
            &request.algorithm_id,
            &request.build_id,
            &request.strategy_name,
        )?;
        self.limits.check_rate(
            owner,
            QUOTA_CONCURRENT_BACKTESTS,
            1,
            RATE_LIMIT_RETRY_SECONDS,
            Utc::now(),
        )?;
        let status = BacktestStatus {
            backtest_id: Id::mint("backtest"),
            algorithm_id: request.algorithm_id.clone(),
            build_id: request.build_id.clone(),
            strategy_name: request.strategy_name.clone(),
            state: BacktestState::Queued,
            error: None,
            seed: request.seed.unwrap_or_else(Self::random_seed),
            progress: crate::backtests::BacktestProgress {
                completed: 0,
                total: 0,
            },
            created_at: now,
            started_at: None,
            completed_at: None,
            stream_sequence: 0,
        };
        self.backtests.insert(owner, status.clone());
        Ok(BacktestCreated {
            backtest_id: status.backtest_id,
            state: status.state,
        })
    }

    pub fn list(
        &self,
        auth: &AuthContext,
        state: Option<BacktestState>,
        query: &PageQuery,
    ) -> Page<BacktestStatus> {
        Page::of(self.backtests.list(auth.owner(), state), query)
    }

    pub fn results(
        &self,
        auth: &AuthContext,
        backtest_id: &str,
        query: &PageQuery,
    ) -> Result<BacktestResultsPage, Problem> {
        let status = self.not_found_or(auth.owner(), backtest_id)?;
        let (data, state, has_more) = self
            .backtests
            .results(auth.owner(), backtest_id, query.offset(), query.limit())
            .ok_or_else(|| Problem::not_found("backtest does not exist"))?;
        let complete = status.state == BacktestState::Completed;
        Ok(BacktestResultsPage {
            page: Page {
                data,
                next_cursor: has_more.then(|| (query.offset() + query.limit()).to_string()),
            },
            state,
            complete,
        })
    }

    pub fn status(&self, auth: &AuthContext, backtest_id: &str) -> Result<BacktestStatus, Problem> {
        self.not_found_or(auth.owner(), backtest_id)
    }

    pub fn cancel(
        &self,
        auth: &AuthContext,
        backtest_id: &str,
        now: DateTime<Utc>,
    ) -> Result<Option<BacktestStatus>, Problem> {
        let owner = auth.owner();
        let status = self.not_found_or(owner, backtest_id)?;
        if status.state.is_terminal() {
            return Ok(None);
        }
        self.backtests
            .update(owner, backtest_id, |mut backtest| {
                backtest.state = BacktestState::Canceled;
                backtest.completed_at = Some(now);
                backtest.stream_sequence += 1;
                backtest
            })
            .map(Some)
            .ok_or_else(|| Problem::not_found("backtest does not exist"))
    }

    pub fn not_found_or(&self, owner: &str, backtest_id: &str) -> Result<BacktestStatus, Problem> {
        self.backtests
            .status(owner, backtest_id)
            .ok_or_else(|| Problem::not_found("backtest does not exist"))
    }

    pub fn export(
        &self,
        auth: &AuthContext,
        backtest_id: &str,
        now: DateTime<Utc>,
    ) -> Result<ExportLink, Problem> {
        let status = self.not_found_or(auth.owner(), backtest_id)?;
        if !status.state.is_terminal() {
            return Err(Problem::state_conflict(
                "the export is available once the backtest is terminal",
            ));
        }
        Ok(ExportLink {
            url: format!(
                "https://storage.example.com/backtests/{}.ndjson",
                status.backtest_id
            ),
            expires_at: now + EXPORT_TTL,
            format: "ndjson",
        })
    }

    fn validate_strategy(
        &self,
        owner: &str,
        algorithm_id: &str,
        build_id: &str,
        strategy_name: &str,
    ) -> Result<(), Problem> {
        let strategies = self
            .builds
            .strategies(owner, algorithm_id, build_id)
            .unwrap_or_default();
        if strategies
            .iter()
            .any(|strategy| strategy.name == strategy_name)
        {
            Ok(())
        } else {
            Err(Problem::validation_failed(
                "strategyName must exist in the build",
            ))
        }
    }

    fn random_seed() -> i64 {
        Utc::now().timestamp_millis()
    }
}
