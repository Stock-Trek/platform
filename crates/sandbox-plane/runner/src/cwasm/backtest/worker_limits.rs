use platform_config::EnvVars;
use std::sync::LazyLock;

use crate::cwasm::backtest::BacktestError;

const MAX_WORKERS_ENV: &str = "CWASM_INVOKER_BACKTEST_MAX_WORKERS";

pub const ABSOLUTE_MAX_WORKERS: u32 = 32;
pub const DEFAULT_MAX_WORKERS: u32 = 4;
pub const MAX_SIMULATION_COUNT: u32 = 1_000_000;

pub struct WorkerLimits {
    max_workers: u32,
}

impl WorkerLimits {
    pub fn global() -> &'static Self {
        static LIMITS: LazyLock<WorkerLimits> =
            LazyLock::new(|| WorkerLimits::from_vars(&EnvVars::from_env()));
        &LIMITS
    }

    pub fn from_vars(vars: &EnvVars) -> Self {
        let configured = vars
            .optional_parsed::<u32>(MAX_WORKERS_ENV)
            .ok()
            .flatten()
            .unwrap_or(DEFAULT_MAX_WORKERS);
        Self::new(configured)
    }

    pub fn new(configured: u32) -> Self {
        Self {
            max_workers: configured.clamp(1, ABSOLUTE_MAX_WORKERS),
        }
    }

    pub fn max_workers(&self) -> u32 {
        self.max_workers
    }

    pub fn validate(
        &self,
        simulation_count: u32,
        max_concurrent: u32,
    ) -> Result<(), BacktestError> {
        if simulation_count == 0 {
            return Err(BacktestError::invalid_argument(
                "simulation count must be at least one",
            ));
        }
        if simulation_count > MAX_SIMULATION_COUNT {
            return Err(BacktestError::invalid_argument(format!(
                "simulation count must be at most {MAX_SIMULATION_COUNT}"
            )));
        }
        if max_concurrent == 0 {
            return Err(BacktestError::invalid_argument(
                "max concurrent simulations must be at least one",
            ));
        }
        Ok(())
    }

    pub fn worker_count(&self, simulation_count: u32, max_concurrent: u32) -> u32 {
        simulation_count
            .max(1)
            .min(max_concurrent.max(1))
            .min(self.max_workers)
    }
}
