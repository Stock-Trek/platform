use crate::{ConsumeResult, ReplayKey, ReplayReservation, ReplayStore, ReplayStoreError};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use parking_lot::Mutex;
use platform_metrics::AuthReplayMetrics;
use std::{
    cmp::{Ordering, Reverse},
    collections::{BinaryHeap, HashMap},
    num::NonZeroUsize,
    sync::atomic::{AtomicU64, Ordering as AtomicOrdering},
};

pub const DEFAULT_REPLAY_CAPACITY: usize = 100_000;

#[derive(Clone, Debug)]
struct ExpirySlot {
    expires_at: DateTime<Utc>,
    reservation_id: u64,
    key: ReplayKey,
}

impl PartialEq for ExpirySlot {
    fn eq(&self, other: &Self) -> bool {
        self.expires_at == other.expires_at && self.reservation_id == other.reservation_id
    }
}

impl Eq for ExpirySlot {}

impl PartialOrd for ExpirySlot {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for ExpirySlot {
    fn cmp(&self, other: &Self) -> Ordering {
        self.expires_at
            .cmp(&other.expires_at)
            .then_with(|| self.reservation_id.cmp(&other.reservation_id))
    }
}

#[derive(Clone, Debug)]
struct ReplayEntry {
    expires_at: DateTime<Utc>,
    reservation_id: u64,
}

#[derive(Default)]
struct ReplayState {
    entries: HashMap<ReplayKey, ReplayEntry>,
    expiry: BinaryHeap<Reverse<ExpirySlot>>,
}

impl ReplayState {
    fn rebuild_expiry(&mut self) {
        self.expiry = self
            .entries
            .iter()
            .map(|(key, entry)| {
                Reverse(ExpirySlot {
                    expires_at: entry.expires_at,
                    reservation_id: entry.reservation_id,
                    key: key.clone(),
                })
            })
            .collect();
    }
}

pub struct MemoryReplayStore {
    state: Mutex<ReplayState>,
    capacity: usize,
    next_reservation_id: AtomicU64,
}

impl Default for MemoryReplayStore {
    fn default() -> Self {
        Self::new(NonZeroUsize::new(DEFAULT_REPLAY_CAPACITY).expect("non-zero capacity"))
    }
}

impl MemoryReplayStore {
    pub fn new(capacity: NonZeroUsize) -> Self {
        Self {
            state: Mutex::new(ReplayState::default()),
            capacity: capacity.get(),
            next_reservation_id: AtomicU64::new(1),
        }
    }

    fn next_reservation_id(&self) -> u64 {
        self.next_reservation_id
            .fetch_add(1, AtomicOrdering::Relaxed)
    }

    fn evict_expired(state: &mut ReplayState, now: DateTime<Utc>) {
        while let Some(Reverse(slot)) = state.expiry.pop() {
            if slot.expires_at > now {
                state.expiry.push(Reverse(slot));
                break;
            }
            let is_current = state
                .entries
                .get(&slot.key)
                .is_some_and(|entry| entry.reservation_id == slot.reservation_id);
            if is_current {
                state.entries.remove(&slot.key);
                AuthReplayMetrics::record_eviction("expired");
            }
        }
    }
}

#[async_trait]
impl ReplayStore for MemoryReplayStore {
    async fn consume(
        &self,
        key: ReplayKey,
        expires_at: DateTime<Utc>,
        now: DateTime<Utc>,
    ) -> Result<ConsumeResult, ReplayStoreError> {
        if expires_at <= now {
            return Ok(ConsumeResult::Expired);
        }
        let reservation_id = self.next_reservation_id();
        let mut state = self.state.lock();
        Self::evict_expired(&mut state, now);
        AuthReplayMetrics::record_size(state.entries.len());
        if state.entries.contains_key(&key) {
            AuthReplayMetrics::record_hit();
            return Ok(ConsumeResult::Replayed);
        }
        if state.entries.len() >= self.capacity {
            AuthReplayMetrics::record_capacity_rejection(self.capacity);
            return Err(ReplayStoreError::capacity_exceeded(self.capacity));
        }
        state.entries.insert(
            key.clone(),
            ReplayEntry {
                expires_at,
                reservation_id,
            },
        );
        state.expiry.push(Reverse(ExpirySlot {
            expires_at,
            reservation_id,
            key: key.clone(),
        }));
        AuthReplayMetrics::record_size(state.entries.len());
        Ok(ConsumeResult::Consumed(ReplayReservation::new(
            key,
            reservation_id,
        )))
    }

    async fn release(&self, reservation: &ReplayReservation) -> Result<(), ReplayStoreError> {
        let mut state = self.state.lock();
        let is_current = state
            .entries
            .get(reservation.key())
            .is_some_and(|entry| entry.reservation_id == reservation.id());
        if is_current {
            state.entries.remove(reservation.key());
            AuthReplayMetrics::record_size(state.entries.len());
        }
        let max_heap_entries = self.capacity.saturating_mul(2).max(64);
        if state.expiry.len() > max_heap_entries {
            state.rebuild_expiry();
        }
        Ok(())
    }
}
