use crate::rate_limit::rate_limiter::RateLimiter;
use parking_lot::Mutex;
use platform_errors::{BpError, BpResult, RateLimitError};
use platform_exchange_types::rate_limited::{RateLimit, RateLimitRestriction, RateUsage};
use platform_types::new_types::{DurationNanoseconds, UsageCount};
use std::{collections::HashMap, sync::Arc, time::Duration};
use strum::IntoEnumIterator;
use tracing::{debug, warn};

#[derive(Debug, Clone)]
pub struct RateLimiters {
    limiters: HashMap<RateLimitRestriction, RateLimiter>,
    acquisition_lock: Arc<Mutex<()>>,
}

impl RateLimiters {
    pub fn new(mut limiters: HashMap<RateLimitRestriction, RateLimiter>) -> Self {
        for restriction in RateLimitRestriction::iter() {
            limiters
                .entry(restriction)
                .or_insert_with(|| RateLimiter::new(&[]));
        }
        Self {
            limiters,
            acquisition_lock: Arc::new(Mutex::new(())),
        }
    }
    pub fn remaining_capacity(&self) -> BpResult<HashMap<RateLimit, UsageCount>> {
        let mut capacities = HashMap::new();
        for (restriction, limiter) in &self.limiters {
            for (interval_nanos, remaining) in limiter.remaining_capacity()? {
                capacities.insert(
                    RateLimit {
                        restriction: *restriction,
                        interval_nanos,
                    },
                    remaining,
                );
            }
        }
        Ok(capacities)
    }
    pub fn did_acquire(&self, costs: &[(RateLimitRestriction, UsageCount)]) -> BpResult<()> {
        let _guard = self.acquisition_lock.lock();
        for limiter in self.limiters.values() {
            if limiter.is_throttled()? {
                debug!("rate limiter is throttled, rejecting request");
                return Err(BpError::RateLimit(RateLimitError::RateLimited));
            }
        }
        let mut acquired = Vec::with_capacity(costs.len());
        for &(restriction, cost) in costs {
            if let Some(limiter) = self.limiters.get(&restriction) {
                if let Err(error) = limiter.did_acquire(cost) {
                    self.refund_acquired(&acquired)?;
                    return Err(error);
                }
                acquired.push((restriction, cost));
            }
        }
        Ok(())
    }
    fn refund_acquired(&self, acquired: &[(RateLimitRestriction, UsageCount)]) -> BpResult<()> {
        for &(restriction, cost) in acquired {
            self.refund_unlocked(restriction, cost)?;
        }
        Ok(())
    }
    pub fn refund(&self, restriction: RateLimitRestriction, cost: UsageCount) -> BpResult<()> {
        let _guard = self.acquisition_lock.lock();
        self.refund_unlocked(restriction, cost)
    }
    fn refund_unlocked(&self, restriction: RateLimitRestriction, cost: UsageCount) -> BpResult<()> {
        if let Some(limiter) = self.limiters.get(&restriction) {
            limiter.refund(cost)
        } else {
            Ok(())
        }
    }
    pub fn set_usage(&self, usage: &HashMap<RateLimit, RateUsage>) -> BpResult<()> {
        for (restriction, limiter) in &self.limiters {
            let restriction_usage = usage
                .iter()
                .filter(|(rate_limit, _)| rate_limit.restriction == *restriction)
                .map(|(rate_limit, rate_usage)| (rate_limit.interval_nanos, *rate_usage))
                .collect::<HashMap<DurationNanoseconds, RateUsage>>();
            limiter.set_usage(&restriction_usage)?;
        }
        Ok(())
    }
    pub fn set_retry_after(&self, retry_after: Duration) -> BpResult<()> {
        warn!(
            ?retry_after,
            "exchange requested Retry-After, throttling requests"
        );
        for limiter in self.limiters.values() {
            limiter.throttle(retry_after)?;
        }
        Ok(())
    }
}

pub(crate) struct RateLimitGuard<'a> {
    rate_limiters: &'a RateLimiters,
    costs: Vec<(RateLimitRestriction, UsageCount)>,
    armed: bool,
}

impl<'a> RateLimitGuard<'a> {
    pub(crate) fn new(
        rate_limiters: &'a RateLimiters,
        costs: Vec<(RateLimitRestriction, UsageCount)>,
    ) -> Self {
        Self {
            rate_limiters,
            costs,
            armed: true,
        }
    }
    pub(crate) fn disarm(mut self) {
        self.armed = false;
    }
}

impl Drop for RateLimitGuard<'_> {
    fn drop(&mut self) {
        if self.armed {
            for &(restriction, cost) in &self.costs {
                let _ = self.rate_limiters.refund(restriction, cost);
            }
        }
    }
}

#[cfg(test)]
mod tests;
