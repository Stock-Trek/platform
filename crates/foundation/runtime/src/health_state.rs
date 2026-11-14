use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};

#[derive(Debug, Clone, Default)]
pub struct HealthState {
    ready: Arc<AtomicBool>,
}

impl HealthState {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn ready() -> Self {
        let state = Self::new();
        state.mark_ready();
        state
    }

    pub fn mark_ready(&self) {
        self.set_ready(true);
    }

    pub fn mark_not_ready(&self) {
        self.set_ready(false);
    }

    pub fn set_ready(&self, ready: bool) {
        self.ready.store(ready, Ordering::Release);
    }

    pub fn is_ready(&self) -> bool {
        self.ready.load(Ordering::Acquire)
    }
}
