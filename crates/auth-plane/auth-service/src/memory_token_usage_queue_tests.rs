use crate::{TokenUsage, TokenUsageQueue, TokenUsageReservation};
use async_trait::async_trait;
use parking_lot::Mutex;
use platform_errors::{BpError, BpResult};
use platform_metrics::auth::AuthUsageQueueMetrics;
use std::{
    collections::VecDeque,
    sync::atomic::{AtomicU64, Ordering},
};

#[derive(Default)]
struct MemoryUsageState {
    pending: VecDeque<TokenUsageReservation>,
}

pub struct MemoryTokenUsageQueue {
    capacity: usize,
    state: Mutex<MemoryUsageState>,
    next_id: AtomicU64,
}

impl MemoryTokenUsageQueue {
    pub fn new(capacity: usize) -> Self {
        Self {
            capacity,
            state: Mutex::new(MemoryUsageState::default()),
            next_id: AtomicU64::new(1),
        }
    }

    pub fn len(&self) -> usize {
        self.state.lock().pending.len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    fn capacity_error(&self) -> BpError {
        BpError::auth_store(
            "enqueue",
            "token-usage-queue",
            true,
            format!("usage queue capacity of {} has been reached", self.capacity),
        )
    }

    fn next_id(&self) -> String {
        self.next_id.fetch_add(1, Ordering::Relaxed).to_string()
    }
}

#[async_trait]
impl TokenUsageQueue for MemoryTokenUsageQueue {
    async fn enqueue(&self, mut usage: TokenUsage) -> BpResult<()> {
        usage.ensure_event_ids();
        let mut state = self.state.lock();
        if state.pending.len() >= self.capacity {
            AuthUsageQueueMetrics::record_capacity_rejection();
            return Err(self.capacity_error());
        }
        let reservation = TokenUsageReservation::new(self.next_id(), usage);
        state.pending.push_back(reservation);
        AuthUsageQueueMetrics::record_enqueued();
        Ok(())
    }

    async fn reserve(&self, limit: usize) -> BpResult<Vec<TokenUsageReservation>> {
        let mut state = self.state.lock();
        let count = limit.min(state.pending.len());
        Ok(state.pending.drain(..count).collect())
    }

    async fn acknowledge(&self, _reservation: &TokenUsageReservation) -> BpResult<()> {
        Ok(())
    }

    async fn release(&self, reservation: &TokenUsageReservation) -> BpResult<()> {
        self.state.lock().pending.push_back(reservation.clone());
        Ok(())
    }
}
