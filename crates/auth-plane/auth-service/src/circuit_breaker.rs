use parking_lot::Mutex;
use platform_metrics::AuthTokenCacheMetrics;
use std::time::Duration;
use tokio::time::Instant;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum BreakerState {
    Closed,
    Open,
    HalfOpen,
}

impl BreakerState {
    pub fn gauge_value(self) -> u8 {
        match self {
            Self::Closed => 0,
            Self::Open => 1,
            Self::HalfOpen => 2,
        }
    }

    pub fn metric_label(self) -> &'static str {
        match self {
            Self::Closed => "closed",
            Self::Open => "open",
            Self::HalfOpen => "half_open",
        }
    }
}

#[derive(Debug)]
struct BreakerData {
    state: BreakerState,
    failures: usize,
    opened_at: Option<Instant>,
    epoch: u64,
}

pub struct CircuitBreaker {
    failure_threshold: usize,
    open_duration: Duration,
    data: Mutex<BreakerData>,
}

impl CircuitBreaker {
    pub fn new(failure_threshold: usize, open_duration: Duration) -> Self {
        let breaker = Self {
            failure_threshold,
            open_duration,
            data: Mutex::new(BreakerData {
                state: BreakerState::Closed,
                failures: 0,
                opened_at: None,
                epoch: 0,
            }),
        };
        breaker.record_state();
        breaker
    }

    pub fn state(&self) -> BreakerState {
        self.data.lock().state
    }

    pub fn retry_after(&self) -> Duration {
        self.open_duration
    }

    pub fn allow_request(&self) -> BreakerDecision<'_> {
        let now = Instant::now();
        let mut data = self.data.lock();
        match data.state {
            BreakerState::Closed => BreakerDecision::Allowed(BreakerPermit::new(self, data.epoch)),
            BreakerState::Open => {
                let opened_at = data.opened_at.unwrap_or(now);
                let elapsed = now.saturating_duration_since(opened_at);
                if elapsed < self.open_duration {
                    return BreakerDecision::Rejected(self.open_duration - elapsed);
                }
                data.state = BreakerState::HalfOpen;
                data.epoch = data.epoch.wrapping_add(1);
                let epoch = data.epoch;
                drop(data);
                self.record_transition(BreakerState::HalfOpen);
                BreakerDecision::Allowed(BreakerPermit::new(self, epoch))
            }
            BreakerState::HalfOpen => BreakerDecision::Rejected(self.open_duration),
        }
    }

    fn record_success(&self, permit_epoch: u64) {
        let mut data = self.data.lock();
        if data.epoch != permit_epoch {
            return;
        }
        data.failures = 0;
        data.opened_at = None;
        if data.state != BreakerState::HalfOpen {
            return;
        }
        data.state = BreakerState::Closed;
        data.epoch = data.epoch.wrapping_add(1);
        drop(data);
        self.record_transition(BreakerState::Closed);
    }

    fn record_failure(&self, permit_epoch: u64, now: Instant) {
        let mut data = self.data.lock();
        if data.epoch != permit_epoch {
            return;
        }
        let should_open = match data.state {
            BreakerState::Closed => {
                data.failures = data.failures.saturating_add(1);
                data.failures >= self.failure_threshold
            }
            BreakerState::HalfOpen => true,
            BreakerState::Open => false,
        };
        if should_open {
            data.state = BreakerState::Open;
            data.opened_at = Some(now);
            data.epoch = data.epoch.wrapping_add(1);
            drop(data);
            self.record_transition(BreakerState::Open);
        }
    }

    fn record_transition(&self, state: BreakerState) {
        AuthTokenCacheMetrics::record_breaker_transition(state.metric_label());
        AuthTokenCacheMetrics::record_breaker_state(state.gauge_value());
    }

    fn record_state(&self) {
        AuthTokenCacheMetrics::record_breaker_state(self.state().gauge_value());
    }
}

pub enum BreakerDecision<'a> {
    Allowed(BreakerPermit<'a>),
    Rejected(Duration),
}

pub struct BreakerPermit<'a> {
    breaker: &'a CircuitBreaker,
    epoch: u64,
    completed: bool,
}

impl<'a> BreakerPermit<'a> {
    fn new(breaker: &'a CircuitBreaker, epoch: u64) -> Self {
        Self {
            breaker,
            epoch,
            completed: false,
        }
    }

    pub fn success(mut self) {
        self.breaker.record_success(self.epoch);
        self.completed = true;
    }

    pub fn failure(mut self) {
        self.breaker.record_failure(self.epoch, Instant::now());
        self.completed = true;
    }
}

impl Drop for BreakerPermit<'_> {
    fn drop(&mut self) {
        if !self.completed {
            self.breaker.record_failure(self.epoch, Instant::now());
        }
    }
}
