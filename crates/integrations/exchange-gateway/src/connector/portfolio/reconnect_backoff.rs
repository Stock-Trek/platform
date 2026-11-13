use std::time::Duration;

pub(super) struct ReconnectBackoff {
    base: Duration,
}

impl ReconnectBackoff {
    const MIN_JITTER_FACTOR: f64 = 0.5;
    const MAX_JITTER_FACTOR: f64 = 1.0;

    pub(super) fn new(base: Duration) -> Self {
        Self { base }
    }

    pub(super) fn delay(&self) -> Duration {
        let jitter = Self::MIN_JITTER_FACTOR
            + rand::random::<f64>() * (Self::MAX_JITTER_FACTOR - Self::MIN_JITTER_FACTOR);
        self.base.mul_f64(jitter)
    }
}
