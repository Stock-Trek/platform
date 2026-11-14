use crate::circuit_breaker::CircuitBreaker;
use tokio::time::Instant;

pub struct BreakerPermit<'a> {
    breaker: &'a CircuitBreaker,
    epoch: u64,
    completed: bool,
}

impl<'a> BreakerPermit<'a> {
    pub(super) fn new(breaker: &'a CircuitBreaker, epoch: u64) -> Self {
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
