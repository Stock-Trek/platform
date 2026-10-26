use crate::rate_limit::rate_limiter_state::RateLimiterState;
use platform_errors::{BpError, BpResult, RateLimitError, SystemError};
use platform_exchange_types::{
    new_types::{Nanoseconds, UsageCount},
    rate_limited::RateUsage,
};
use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
    time::Duration,
};

#[derive(Debug, Clone)]
pub struct RateLimiter {
    rate_limiters: Arc<Mutex<Vec<RateLimiterState>>>,
}

impl RateLimiter {
    pub fn new(states: &[RateLimiterState]) -> Self {
        Self {
            rate_limiters: Arc::new(Mutex::new(states.to_vec())),
        }
    }
    pub fn did_acquire(&self, cost: UsageCount) -> BpResult<()> {
        let mut limiters_guard = self
            .rate_limiters
            .lock()
            .map_err(|_| BpError::System(SystemError::PoisonedLock))?;
        let lowest_exceeded_limit = limiters_guard
            .iter()
            .filter(|limiter| limiter.cost_exceeds_capacity(cost))
            .min_by_key(|limiter| limiter.capacity_per_interval())
            .map(|limiter| (limiter.capacity_per_interval(), limiter.interval_nanos()));
        if let Some((capacity, interval_nanos)) = lowest_exceeded_limit {
            return Err(BpError::RateLimit(
                RateLimitError::RequestExceedsRateLimit {
                    cost,
                    capacity,
                    interval_nanos,
                },
            ));
        }
        for (index, limiter) in limiters_guard.iter_mut().enumerate() {
            if let Err(error) = limiter.did_consume(cost) {
                for i in 0..index {
                    let limiter = &mut limiters_guard[i];
                    limiter.refund(cost);
                }
                return Err(error);
            }
        }
        Ok(())
    }
    pub fn remaining_capacity(&self) -> BpResult<HashMap<Nanoseconds, UsageCount>> {
        let mut limiters_guard = self
            .rate_limiters
            .lock()
            .map_err(|_| BpError::System(SystemError::PoisonedLock))?;
        let mut capacities = HashMap::new();
        for limiter in limiters_guard.iter_mut() {
            capacities.insert(limiter.interval_nanos(), limiter.remaining_capacity()?);
        }
        Ok(capacities)
    }
    pub fn refund(&self, cost: UsageCount) -> BpResult<()> {
        let mut limiters_guard = self
            .rate_limiters
            .lock()
            .map_err(|_| BpError::System(SystemError::PoisonedLock))?;
        for limiter in limiters_guard.iter_mut() {
            limiter.refund(cost);
        }
        Ok(())
    }
    pub fn set_usage(&self, interval_usage: &HashMap<Nanoseconds, RateUsage>) -> BpResult<()> {
        let mut limiters_guard = self
            .rate_limiters
            .lock()
            .map_err(|_| BpError::System(SystemError::PoisonedLock))?;
        for (interval_nanos, rate_usage) in interval_usage {
            let mut matched_existing = false;
            for limiter in limiters_guard.iter_mut() {
                if limiter.interval_nanos() == *interval_nanos {
                    limiter.sync_usage(rate_usage.used, rate_usage.limit);
                    matched_existing = true;
                }
            }
            if matched_existing {
                continue;
            }
            let Some(new_limit) = rate_usage.limit else {
                continue;
            };
            if let Ok(mut limiter) = RateLimiterState::try_new(*interval_nanos, new_limit) {
                limiter.sync_usage(rate_usage.used, Some(new_limit));
                limiters_guard.push(limiter);
            }
        }
        Ok(())
    }
    pub fn is_throttled(&self) -> BpResult<bool> {
        let limiters_guard = self
            .rate_limiters
            .lock()
            .map_err(|_| BpError::System(SystemError::PoisonedLock))?;
        Ok(limiters_guard.iter().any(RateLimiterState::is_throttled))
    }
    pub fn throttle(&self, retry_after: Duration) -> BpResult<()> {
        let mut limiters_guard = self
            .rate_limiters
            .lock()
            .map_err(|_| BpError::System(SystemError::PoisonedLock))?;
        for limiter in limiters_guard.iter_mut() {
            limiter.throttle_after(retry_after);
        }
        Ok(())
    }
}
