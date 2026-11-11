use chrono::Duration;
use platform_errors::{BpError, BpResult};
use std::str::FromStr;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PortfolioFreshness {
    Missing,
    Fresh,
    Stale,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StaleOrderPolicy {
    Allow,
    Degrade,
    Reject,
}

impl FromStr for StaleOrderPolicy {
    type Err = BpError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value.trim().to_ascii_lowercase().as_str() {
            "allow" => Ok(Self::Allow),
            "degrade" => Ok(Self::Degrade),
            "reject" => Ok(Self::Reject),
            _ => Err(BpError::bad_value(
                "stale order policy",
                value,
                "must be allow, degrade, or reject",
            )),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FreshnessPolicy {
    max_age: Duration,
    stale_order_policy: StaleOrderPolicy,
}

impl FreshnessPolicy {
    pub fn new(max_age: Duration, stale_order_policy: StaleOrderPolicy) -> BpResult<Self> {
        if max_age <= Duration::zero() {
            return Err(BpError::bad_value(
                "portfolio max update age",
                max_age.to_string(),
                "must be positive",
            ));
        }
        Ok(Self {
            max_age,
            stale_order_policy,
        })
    }

    pub fn default_degraded() -> Self {
        Self {
            max_age: Duration::seconds(60),
            stale_order_policy: StaleOrderPolicy::Degrade,
        }
    }

    pub fn max_age(&self) -> Duration {
        self.max_age
    }

    pub fn stale_order_policy(&self) -> StaleOrderPolicy {
        self.stale_order_policy
    }

    pub fn evaluate(&self, age: Option<Duration>) -> PortfolioFreshness {
        match age {
            None => PortfolioFreshness::Missing,
            Some(age) if age <= self.max_age => PortfolioFreshness::Fresh,
            Some(_) => PortfolioFreshness::Stale,
        }
    }

    pub fn authorize_order(&self, age: Option<Duration>) -> BpResult<PortfolioFreshness> {
        let freshness = self.evaluate(age);
        if freshness == PortfolioFreshness::Stale
            && self.stale_order_policy == StaleOrderPolicy::Reject
        {
            return Err(BpError::auth_invalid_field(
                "account",
                "has stale portfolio data",
            ));
        }
        Ok(freshness)
    }
}
