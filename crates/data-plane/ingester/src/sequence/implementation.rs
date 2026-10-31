use async_trait::async_trait;
use platform_errors::BpResult;
use platform_nats::{JetStream, KeyValue};
use serde::{Deserialize, Deserializer, Serialize, Serializer};

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
    pub(super) next: Option<u128>,
}

/// Durable storage for per-stream sequence progress.
#[async_trait]
pub trait SequenceStore: Send + Sync {
    async fn load(&self, key: &str) -> BpResult<Option<SequenceTracker>>;

    async fn save(&self, key: &str, tracker: &SequenceTracker) -> BpResult<()>;
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
