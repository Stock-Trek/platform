use crate::{FreshnessPolicy, StaleOrderPolicy};
use platform_config::env_vars::EnvVars;
use platform_errors::{BuildError, BuildResult};

const MAX_AGE_SECS: &str = "PORTFOLIO_MAX_UPDATE_AGE_SECS";
const STALE_ORDER_POLICY: &str = "PORTFOLIO_STALE_ORDER_POLICY";
const DEFAULT_MAX_AGE_SECS: u64 = 60;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FreshnessConfig {
    policy: FreshnessPolicy,
}

impl FreshnessConfig {
    pub fn from_vars(vars: &EnvVars) -> BuildResult<Self> {
        let max_age_secs = vars
            .optional_parsed::<u64>(MAX_AGE_SECS)?
            .unwrap_or(DEFAULT_MAX_AGE_SECS);
        if max_age_secs == 0 {
            return Err(BuildError::config_invalid(
                MAX_AGE_SECS,
                "must be greater than zero",
            ));
        }
        let stale_order_policy = vars
            .optional(STALE_ORDER_POLICY)?
            .map(|value| value.parse())
            .transpose()?
            .unwrap_or(StaleOrderPolicy::Degrade);
        Ok(Self {
            policy: FreshnessPolicy::new(
                chrono::Duration::seconds(max_age_secs as i64),
                stale_order_policy,
            )?,
        })
    }

    pub fn policy(&self) -> FreshnessPolicy {
        self.policy
    }
}

impl Default for FreshnessConfig {
    fn default() -> Self {
        Self {
            policy: FreshnessPolicy::default_degraded(),
        }
    }
}
