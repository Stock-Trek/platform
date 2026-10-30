use async_trait::async_trait;
use platform_errors::BpResult;
use platform_nats::{JetStream, KeyValue};
use serde::{Deserialize, Deserializer, Serialize, Serializer};
#[cfg(test)]
use std::{collections::HashMap, sync::Mutex};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SequenceObservation {
    First,
    InOrder,
    Gap { missing_from: u64, missing_to: u64 },
    Duplicate,
    OutOfOrder,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct SequenceTracker {
    /// First sequence that has not yet been published or covered by a gap
    /// marker.
    ///
    /// Stored as `u128` so the sequence immediately after `u64::MAX` is
    /// representable.
    #[serde(default, with = "next_sequence")]
    next: Option<u128>,
}

/// Durable storage for per-stream sequence progress.
#[async_trait]
pub trait SequenceStore: Send + Sync {
    async fn load(&self, key: &str) -> BpResult<Option<SequenceTracker>>;

    async fn save(&self, key: &str, tracker: &SequenceTracker) -> BpResult<()>;
}

/// Sequence state kept in process memory.
#[cfg(test)]
#[derive(Debug, Default)]
pub struct InMemorySequenceStore {
    trackers: Mutex<HashMap<String, SequenceTracker>>,
}

#[cfg(test)]
impl InMemorySequenceStore {
    pub fn new() -> Self {
        Self::default()
    }
}

#[cfg(test)]
#[async_trait]
impl SequenceStore for InMemorySequenceStore {
    async fn load(&self, key: &str) -> BpResult<Option<SequenceTracker>> {
        Ok(self
            .trackers
            .lock()
            .expect("sequence store lock")
            .get(key)
            .cloned())
    }

    async fn save(&self, key: &str, tracker: &SequenceTracker) -> BpResult<()> {
        self.trackers
            .lock()
            .expect("sequence store lock")
            .insert(key.to_string(), tracker.clone());
        Ok(())
    }
}

/// Sequence state persisted in a NATS KV bucket.
pub struct NatsSequenceStore {
    values: KeyValue,
}

impl NatsSequenceStore {
    pub async fn connect(nats_url: &str, subject_prefix: &str) -> BpResult<Self> {
        let bucket = Self::bucket_name(subject_prefix);
        let values = KeyValue::connect(nats_url, "platform-ingester-sequences", &bucket).await?;
        Ok(Self { values })
    }

    pub fn bucket_name(subject_prefix: &str) -> String {
        JetStream::kv_bucket_name(&format!("{subject_prefix}.ingester-sequences"))
    }
}

#[async_trait]
impl SequenceStore for NatsSequenceStore {
    async fn load(&self, key: &str) -> BpResult<Option<SequenceTracker>> {
        let Some(value) = self.values.get_last(key).await? else {
            return Ok(None);
        };
        Ok(Some(serde_json::from_slice(&value)?))
    }

    async fn save(&self, key: &str, tracker: &SequenceTracker) -> BpResult<()> {
        let value = serde_json::to_vec(tracker)?;
        self.values.put(key, &value).await?;
        Ok(())
    }
}

mod next_sequence {
    use super::*;

    pub fn serialize<S>(next: &Option<u128>, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match next {
            Some(next) => serializer.serialize_some(&next.to_string()),
            None => serializer.serialize_none(),
        }
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<Option<u128>, D::Error>
    where
        D: Deserializer<'de>,
    {
        let next = Option::<String>::deserialize(deserializer)?;
        next.map(|next| next.parse().map_err(serde::de::Error::custom))
            .transpose()
    }
}

impl SequenceTracker {
    /// Observes an inclusive range of update IDs without advancing the tracker.
    ///
    /// Exchanges such as Binance can pack many update IDs into one depth
    /// event. Continuity is checked against the first update ID in the range,
    /// not against `previous + 1` of the final ID.
    ///
    /// Call [`Self::commit`] only after the event has been published, so a
    /// failed publish remains visible as a gap to the next event.
    pub fn observe_range(&self, first: u64, last: u64) -> SequenceObservation {
        debug_assert!(first <= last, "sequence range must not be inverted");
        let first = first.min(last);

        let Some(next) = self.next else {
            return SequenceObservation::First;
        };

        let last = u128::from(last);
        if last < next {
            return if last + 1 == next {
                SequenceObservation::Duplicate
            } else {
                SequenceObservation::OutOfOrder
            };
        }

        let first = u128::from(first);
        if first > next {
            return SequenceObservation::Gap {
                missing_from: u64::try_from(next)
                    .expect("next sequence is within the observed range"),
                missing_to: u64::try_from(first - 1).expect("first is at most u64::MAX"),
            };
        }

        SequenceObservation::InOrder
    }

    /// Marks the inclusive range ending at `last` as successfully handled.
    pub fn commit(&mut self, last: u64) {
        self.next = Some(u128::from(last) + 1);
    }

    /// Records that an event was observed but could not be published.
    ///
    /// This establishes the event as missing when it is the first observation
    /// for the stream. Later observations already have an earlier boundary, so
    /// they keep their existing next sequence.
    pub fn record_failure(&mut self, first: u64) {
        if self.next.is_none() {
            self.next = Some(u128::from(first));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    trait SequenceTrackerTestExt {
        fn last(&self) -> Option<u64>;
    }

    impl SequenceTrackerTestExt for SequenceTracker {
        fn last(&self) -> Option<u64> {
            self.next
                .and_then(|next| next.checked_sub(1))
                .and_then(|last| u64::try_from(last).ok())
        }
    }

    fn observe(tracker: &mut SequenceTracker, first: u64, last: u64) -> SequenceObservation {
        let observation = tracker.observe_range(first, last);
        if !matches!(
            observation,
            SequenceObservation::Duplicate | SequenceObservation::OutOfOrder
        ) {
            tracker.commit(last);
        }
        observation
    }

    #[test]
    fn observation_does_not_advance_until_committed() {
        let mut tracker = SequenceTracker::default();

        assert_eq!(tracker.observe_range(100, 100), SequenceObservation::First);
        assert_eq!(tracker.last(), None);
        assert_eq!(tracker.observe_range(100, 100), SequenceObservation::First);

        tracker.commit(100);
        assert_eq!(tracker.last(), Some(100));
    }

    #[test]
    fn first_sequence_establishes_the_baseline() {
        let mut tracker = SequenceTracker::default();

        assert_eq!(observe(&mut tracker, 100, 100), SequenceObservation::First);
        assert_eq!(tracker.last(), Some(100));
    }

    #[test]
    fn consecutive_sequences_are_in_order() {
        let mut tracker = SequenceTracker::default();

        observe(&mut tracker, 100, 100);
        assert_eq!(
            observe(&mut tracker, 101, 101),
            SequenceObservation::InOrder
        );
        assert_eq!(
            observe(&mut tracker, 102, 102),
            SequenceObservation::InOrder
        );
    }

    #[test]
    fn gaps_report_the_missing_inclusive_range() {
        let mut tracker = SequenceTracker::default();

        observe(&mut tracker, 100, 100);
        assert_eq!(
            observe(&mut tracker, 104, 104),
            SequenceObservation::Gap {
                missing_from: 101,
                missing_to: 103,
            }
        );
        assert_eq!(tracker.last(), Some(104));
    }

    #[test]
    fn duplicate_and_older_sequences_do_not_advance_the_tracker() {
        let mut tracker = SequenceTracker::default();

        observe(&mut tracker, 100, 100);
        assert_eq!(
            observe(&mut tracker, 100, 100),
            SequenceObservation::Duplicate
        );
        assert_eq!(
            observe(&mut tracker, 99, 99),
            SequenceObservation::OutOfOrder
        );
        assert_eq!(tracker.last(), Some(100));
    }

    #[test]
    fn maximum_sequence_does_not_overflow() {
        let mut tracker = SequenceTracker::default();

        observe(&mut tracker, u64::MAX, u64::MAX);
        assert_eq!(
            observe(&mut tracker, u64::MAX, u64::MAX),
            SequenceObservation::Duplicate
        );
    }

    #[test]
    fn serialized_state_preserves_the_sequence_after_u64_max() {
        let mut tracker = SequenceTracker::default();
        tracker.commit(u64::MAX);

        let encoded = serde_json::to_string(&tracker).unwrap();
        let decoded: SequenceTracker = serde_json::from_str(&encoded).unwrap();

        assert_eq!(
            decoded.observe_range(u64::MAX, u64::MAX),
            SequenceObservation::Duplicate
        );
    }

    #[tokio::test]
    async fn in_memory_store_round_trips_trackers() {
        let store = InMemorySequenceStore::new();
        let mut tracker = SequenceTracker::default();
        tracker.commit(41);

        store.save("binance.btcusdt.trade", &tracker).await.unwrap();

        assert_eq!(
            store.load("binance.btcusdt.trade").await.unwrap(),
            Some(tracker)
        );
        assert_eq!(store.load("binance.ethusdt.trade").await.unwrap(), None);
    }

    #[test]
    fn sequence_bucket_is_derived_from_the_subject_prefix() {
        assert_eq!(
            NatsSequenceStore::bucket_name("market"),
            "market_ingester-sequences"
        );
    }

    #[test]
    fn ranges_spanning_many_update_ids_are_in_order() {
        let mut tracker = SequenceTracker::default();

        assert_eq!(observe(&mut tracker, 100, 105), SequenceObservation::First);
        assert_eq!(
            observe(&mut tracker, 106, 120),
            SequenceObservation::InOrder
        );
        assert_eq!(tracker.last(), Some(120));
    }

    #[test]
    fn range_gaps_are_measured_from_the_first_update_id() {
        let mut tracker = SequenceTracker::default();

        observe(&mut tracker, 100, 105);
        assert_eq!(
            observe(&mut tracker, 110, 120),
            SequenceObservation::Gap {
                missing_from: 106,
                missing_to: 109,
            }
        );
        assert_eq!(tracker.last(), Some(120));
    }

    #[test]
    fn overlapping_ranges_advance_the_final_sequence() {
        let mut tracker = SequenceTracker::default();

        observe(&mut tracker, 100, 105);
        assert_eq!(
            observe(&mut tracker, 103, 108),
            SequenceObservation::InOrder
        );
        assert_eq!(tracker.last(), Some(108));
    }

    #[test]
    fn ranges_that_end_at_the_previous_sequence_are_duplicates() {
        let mut tracker = SequenceTracker::default();

        observe(&mut tracker, 100, 105);
        assert_eq!(
            observe(&mut tracker, 103, 105),
            SequenceObservation::Duplicate
        );
        assert_eq!(tracker.last(), Some(105));
    }

    #[test]
    fn a_failed_first_event_is_a_gap_for_the_next_event() {
        let mut tracker = SequenceTracker::default();

        assert_eq!(tracker.observe_range(10, 10), SequenceObservation::First);
        tracker.record_failure(10);

        assert_eq!(
            tracker.observe_range(11, 11),
            SequenceObservation::Gap {
                missing_from: 10,
                missing_to: 10,
            }
        );
    }

    #[test]
    fn a_failed_publish_is_a_gap_for_the_next_event() {
        let mut tracker = SequenceTracker::default();

        observe(&mut tracker, 100, 100);
        assert_eq!(
            tracker.observe_range(101, 101),
            SequenceObservation::InOrder
        );
        tracker.record_failure(101);

        assert_eq!(
            tracker.observe_range(102, 102),
            SequenceObservation::Gap {
                missing_from: 101,
                missing_to: 101,
            }
        );
    }

    #[test]
    fn a_failed_overlapping_publish_does_not_move_the_boundary_backwards() {
        let mut tracker = SequenceTracker::default();

        observe(&mut tracker, 100, 105);
        assert_eq!(
            tracker.observe_range(103, 108),
            SequenceObservation::InOrder
        );
        tracker.record_failure(103);

        assert_eq!(
            tracker.observe_range(109, 109),
            SequenceObservation::Gap {
                missing_from: 106,
                missing_to: 108,
            }
        );
    }
}
