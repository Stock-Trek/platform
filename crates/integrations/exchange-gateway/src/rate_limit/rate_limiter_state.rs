use platform_errors::{BpError, BpResult, RateLimitError};
use platform_types::new_types::{DurationNanoseconds, NonZero, UsageCount};
use std::{
    sync::Arc,
    time::{Duration, Instant},
};

const MAX_THROTTLE_AFTER: Duration = Duration::from_secs(5 * 60);

#[derive(Clone)]
pub struct RateLimiterState {
    interval_nanos: NonZero<DurationNanoseconds>,
    capacity_per_interval: UsageCount,
    current_capacity: UsageCount,
    last_calculation: Instant,
    excess_interval_nanos: DurationNanoseconds,
    throttled_until: Option<Instant>,
    now: Arc<dyn Fn() -> Instant + Send + Sync>,
}

impl RateLimiterState {
    pub fn try_new(
        interval_nanos: DurationNanoseconds,
        capacity_per_interval: UsageCount,
    ) -> BpResult<Self> {
        Self::with_clock(
            interval_nanos,
            capacity_per_interval,
            Arc::new(Instant::now),
        )
    }
    pub(crate) fn with_clock(
        interval_nanos: DurationNanoseconds,
        capacity_per_interval: UsageCount,
        now: Arc<dyn Fn() -> Instant + Send + Sync>,
    ) -> BpResult<Self> {
        if capacity_per_interval <= UsageCount::ZERO {
            return Err(BpError::RateLimit(RateLimitError::InvalidRateLimitCapacity));
        }
        let interval_nanos = NonZero::new(interval_nanos)
            .map_err(|_| BpError::RateLimit(RateLimitError::InvalidRateLimitInterval))?;
        Ok(Self {
            interval_nanos,
            capacity_per_interval,
            current_capacity: capacity_per_interval,
            last_calculation: now(),
            excess_interval_nanos: DurationNanoseconds::ZERO,
            throttled_until: None,
            now,
        })
    }
    pub fn interval_nanos(&self) -> DurationNanoseconds {
        self.interval_nanos.get()
    }
    pub fn capacity_per_interval(&self) -> UsageCount {
        self.capacity_per_interval
    }
    pub fn cost_exceeds_capacity(&self, cost: UsageCount) -> bool {
        cost > self.capacity_per_interval
    }
    pub fn remaining_capacity(&mut self) -> BpResult<UsageCount> {
        self.update_capacity()?;
        Ok(self.current_capacity)
    }
    pub fn did_consume(&mut self, cost: UsageCount) -> BpResult<()> {
        if self.cost_exceeds_capacity(cost) {
            return Err(BpError::RateLimit(
                RateLimitError::RequestExceedsRateLimit {
                    cost,
                    capacity: self.capacity_per_interval,
                    interval_nanos: self.interval_nanos.get(),
                },
            ));
        }
        if self.is_throttled() {
            return Err(BpError::RateLimit(RateLimitError::RateLimited));
        }
        if self.did_quick_consume(cost) {
            Ok(())
        } else {
            self.update_capacity()?;
            if self.did_quick_consume(cost) {
                Ok(())
            } else {
                Err(BpError::RateLimit(RateLimitError::RateLimited))
            }
        }
    }
    pub fn refund(&mut self, cost: UsageCount) {
        self.current_capacity = (self.current_capacity + cost).min(self.capacity_per_interval);
    }
    pub fn throttle(&mut self, until: Instant) {
        if until <= self.now() {
            return;
        }
        self.set_throttle(until);
    }
    pub(crate) fn throttle_after(&mut self, duration: Duration) {
        if duration.is_zero() {
            return;
        }
        self.set_throttle(self.now() + duration.min(MAX_THROTTLE_AFTER));
    }
    fn set_throttle(&mut self, until: Instant) {
        self.current_capacity = UsageCount::ZERO;
        self.throttled_until = Some(until);
    }
    pub fn sync_usage(&mut self, used: Option<UsageCount>, limit: Option<UsageCount>) {
        if let Some(limit) = limit {
            self.capacity_per_interval = limit;
        }
        match (used, limit) {
            (Some(used), Some(limit)) => {
                if self.is_throttled() {
                    // Still inside the Retry-After window: keep the bucket
                    // empty so it refills from the deadline instead of
                    // instantly granting limit - used.
                    self.current_capacity = UsageCount::ZERO;
                } else {
                    // The deadline has passed (or there was none): realign to
                    // the server-reported usage. Clear the stale deadline so
                    // the next refill doesn't bank capacity from before the
                    // feedback arrived.
                    self.throttled_until = None;
                    self.current_capacity = self.capacity_per_interval - used.min(limit);
                }
            }
            (Some(used), None) => {
                let remaining = self.capacity_per_interval - used.min(self.capacity_per_interval);
                self.current_capacity = self.current_capacity.min(remaining);
            }
            (None, Some(limit)) => {
                self.current_capacity = self.current_capacity.min(limit);
            }
            (None, None) => {}
        }
        self.last_calculation = self.now();
        self.excess_interval_nanos = DurationNanoseconds::ZERO;
    }

    fn now(&self) -> Instant {
        (self.now)()
    }
    pub(crate) fn is_throttled(&self) -> bool {
        self.throttled_until
            .is_some_and(|throttled_until| self.now() < throttled_until)
    }
    fn did_quick_consume(&mut self, cost: UsageCount) -> bool {
        if cost > self.capacity_per_interval {
            return false;
        }
        let consumed = self.current_capacity >= cost;
        if consumed {
            self.current_capacity -= cost;
        }
        consumed
    }
    fn update_capacity(&mut self) -> BpResult<()> {
        let now = self.now();
        if let Some(throttled_until) = self.throttled_until {
            if now < throttled_until {
                return Ok(());
            }
            self.throttled_until = None;
            self.last_calculation = throttled_until;
            // The bucket is empty at `throttled_until` and time starts counting
            // from that deadline, so any interval progress banked before the
            // throttle would be counted twice and refill the bucket early.
            self.excess_interval_nanos = DurationNanoseconds::ZERO;
        }
        let elapsed_nanos =
            DurationNanoseconds(now.duration_since(self.last_calculation).as_nanos() as i64);
        let total_nanos = self.excess_interval_nanos + elapsed_nanos;
        let complete_intervals = total_nanos.checked_div(self.interval_nanos)? as u64;
        if complete_intervals > 0 {
            let capacity_to_add = self.capacity_per_interval * complete_intervals;
            let capacity_potentially_over_max = self.current_capacity + capacity_to_add;
            self.current_capacity = capacity_potentially_over_max.min(self.capacity_per_interval);
            self.excess_interval_nanos = total_nanos.checked_rem(self.interval_nanos)?;
            self.last_calculation = now;
        }
        Ok(())
    }
}

impl std::fmt::Debug for RateLimiterState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RateLimiterState")
            .field("interval_nanos", &self.interval_nanos)
            .field("capacity_per_interval", &self.capacity_per_interval)
            .field("current_capacity", &self.current_capacity)
            .field("last_calculation", &self.last_calculation)
            .field("excess_interval_nanos", &self.excess_interval_nanos)
            .field("throttled_until", &self.throttled_until)
            .finish_non_exhaustive()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;

    fn state_with_clock(now: Instant) -> RateLimiterState {
        RateLimiterState::with_clock(
            DurationNanoseconds(60_000_000_000),
            UsageCount(10),
            Arc::new(move || now),
        )
        .expect("valid rate limiter state")
    }

    #[test]
    fn zero_duration_does_not_throttle_or_drain() {
        let mut state = state_with_clock(Instant::now());
        state.throttle_after(Duration::ZERO);

        assert!(!state.is_throttled());
        assert_eq!(
            state.remaining_capacity().expect("remaining capacity"),
            UsageCount(10)
        );
    }

    #[test]
    fn past_deadline_does_not_throttle_or_drain() {
        let now = Instant::now();
        let mut state = state_with_clock(now);
        state.throttle(now - Duration::from_secs(1));

        assert!(!state.is_throttled());
        assert_eq!(
            state.remaining_capacity().expect("remaining capacity"),
            UsageCount(10)
        );
    }

    #[test]
    fn future_deadline_still_throttles_and_drains() {
        let mut state = state_with_clock(Instant::now());
        state.throttle_after(Duration::from_secs(60));

        assert!(state.is_throttled());
        assert_eq!(
            state.remaining_capacity().expect("remaining capacity"),
            UsageCount::ZERO
        );
    }

    #[test]
    fn throttle_deadline_discards_partial_interval_progress() {
        let base = Instant::now();
        let now = Arc::new(Mutex::new(base));
        let clock = {
            let now = now.clone();
            Arc::new(move || *now.lock().expect("clock lock"))
                as Arc<dyn Fn() -> Instant + Send + Sync>
        };
        let mut state = RateLimiterState::with_clock(
            DurationNanoseconds(60_000_000_000),
            UsageCount(10),
            clock,
        )
        .expect("valid rate limiter state");

        // Bank 30s of progress towards the next interval.
        *now.lock().expect("clock lock") = base + Duration::from_secs(90);
        assert_eq!(
            state.remaining_capacity().expect("remaining capacity"),
            UsageCount(10)
        );

        // The exchange throttles us for 30s from t=90s, i.e. until t=120s.
        state.throttle_after(Duration::from_secs(30));
        assert_eq!(
            state.remaining_capacity().expect("remaining capacity"),
            UsageCount::ZERO
        );

        // 30s after the throttle deadline only half an interval has elapsed since
        // the bucket was drained, so the progress banked before the throttle must
        // not be reused to refill the bucket early.
        *now.lock().expect("clock lock") = base + Duration::from_secs(150);
        assert!(!state.is_throttled());
        assert_eq!(
            state.remaining_capacity().expect("remaining capacity"),
            UsageCount::ZERO
        );

        // A full interval after the deadline the bucket refills completely.
        *now.lock().expect("clock lock") = base + Duration::from_secs(180);
        assert_eq!(
            state.remaining_capacity().expect("remaining capacity"),
            UsageCount(10)
        );
    }
}
