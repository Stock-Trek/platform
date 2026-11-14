use crate::{
    backfiller::BackfillOptions,
    event_fetcher::{BackfilledEvent, MissingEventFetcher},
    gap_retry_store::{GapRetryStore, InMemoryGapRetryStore, PendingGap},
    nats_gap_marker_source::GapMarkerSource,
};
use async_trait::async_trait;
use parking_lot::Mutex;
use platform_errors::{BuildError, BuildResult};
use platform_market_data::{GapMarker, MarketEvent};
use platform_nats::EventPublisher;
use platform_types::TimestampMilliseconds;
use serde_json::json;
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

pub struct OneGapSource {
    pub gap: Option<GapMarker>,
    pub acked: Arc<Mutex<Vec<GapMarker>>>,
}

#[async_trait]
impl GapMarkerSource for OneGapSource {
    async fn next_gap(&mut self) -> BuildResult<Option<GapMarker>> {
        Ok(self.gap.take())
    }

    async fn ack(&mut self, gap: &GapMarker) -> BuildResult<()> {
        self.acked.lock().push(gap.clone());
        Ok(())
    }
}

pub struct OnceFetcher {
    pub events: Mutex<Vec<BackfilledEvent>>,
}

#[async_trait]
impl MissingEventFetcher for OnceFetcher {
    async fn fetch(&self, _gap: &GapMarker) -> BuildResult<Vec<BackfilledEvent>> {
        Ok(std::mem::take(&mut *self.events.lock()))
    }
}

pub struct StreamRangeFetcher {
    pub empty_stream: String,
}

#[async_trait]
impl MissingEventFetcher for StreamRangeFetcher {
    async fn fetch(&self, gap: &GapMarker) -> BuildResult<Vec<BackfilledEvent>> {
        if gap.stream == self.empty_stream {
            return Ok(Vec::new());
        }
        Ok((gap.missing_from..=gap.missing_to)
            .map(TestFixtures::event)
            .collect())
    }
}

#[derive(Default)]
pub struct RecordingPublisher {
    pub sequences: Mutex<Vec<u64>>,
}

#[async_trait]
impl EventPublisher for RecordingPublisher {
    async fn publish(&self, _subject: &str, payload: &[u8]) -> BuildResult<()> {
        let event: MarketEvent = serde_json::from_slice(payload)?;
        self.sequences
            .lock()
            .push(event.sequence.unwrap_or_default());
        Ok(())
    }
}

pub struct TestFixtures;

impl TestFixtures {
    pub fn event(sequence: u64) -> BackfilledEvent {
        BackfilledEvent {
            sequence,
            exchange_timestamp: TimestampMilliseconds(sequence as i64),
            data: json!({ "t": sequence }),
        }
    }

    pub fn options() -> BackfillOptions {
        BackfillOptions {
            event_subject_prefix: "market.events".to_string(),
        }
    }
}

#[derive(Clone, Copy)]
pub enum SavePolicy {
    Allow,
    AfterFirstSave,
    ForStream(&'static str),
}

pub struct ControlledStore {
    inner: Arc<InMemoryGapRetryStore>,
    policy: SavePolicy,
    saves: AtomicUsize,
}

impl ControlledStore {
    pub fn new(inner: Arc<InMemoryGapRetryStore>, policy: SavePolicy) -> Self {
        Self {
            inner,
            policy,
            saves: AtomicUsize::new(0),
        }
    }
}

#[async_trait]
impl GapRetryStore for ControlledStore {
    async fn load(&self) -> BuildResult<Vec<PendingGap>> {
        self.inner.load().await
    }

    async fn save(&self, pending: &PendingGap) -> BuildResult<()> {
        let save_number = self.saves.fetch_add(1, Ordering::SeqCst);
        let fail = match self.policy {
            SavePolicy::Allow => false,
            SavePolicy::AfterFirstSave => save_number > 0,
            SavePolicy::ForStream(stream) => pending.stream == stream,
        };
        if fail {
            return Err(BuildError::external("retry store save failed"));
        }
        self.inner.save(pending).await
    }

    async fn remove(&self, key: &str) -> BuildResult<()> {
        self.inner.remove(key).await
    }
}
