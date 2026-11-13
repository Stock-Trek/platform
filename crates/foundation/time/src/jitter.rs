use rand::random;
use std::time::Duration;

pub struct Jitter;

impl Jitter {
    const MIN_FACTOR: f64 = 0.5;
    const MAX_FACTOR: f64 = 1.0;

    pub fn delay(base: Duration) -> Duration {
        let factor = Self::MIN_FACTOR + random::<f64>() * (Self::MAX_FACTOR - Self::MIN_FACTOR);
        base.mul_f64(factor)
    }
}
