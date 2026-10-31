//! Test helpers for recording and asserting on metrics.

use metrics::{CounterFn, Key, KeyName, Metadata, Recorder, SharedString, Unit};
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicU64, Ordering},
};

/// A [`Recorder`] that captures counters in memory so tests can assert on them.
#[derive(Default)]
pub struct TestRecorder {
    counters: Mutex<Vec<RecordedCounter>>,
}

struct RecordedCounter {
    name: String,
    labels: Vec<(String, String)>,
    value: Arc<AtomicU64>,
}

impl TestRecorder {
    /// Runs `f` with a fresh local recorder and returns its captured metrics.
    pub fn capture<T>(f: impl FnOnce() -> T) -> (T, Self) {
        let recorder = Self::default();
        let value = metrics::with_local_recorder(&recorder, f);
        (value, recorder)
    }

    /// Returns the current value of the counter matching `name` and `labels`.
    ///
    /// Returns `0` when no matching counter has been recorded.
    pub fn counter(&self, name: &str, labels: &[(&str, &str)]) -> u64 {
        self.counters
            .lock()
            .expect("counters lock")
            .iter()
            .find(|counter| {
                counter.name == name
                    && labels.iter().all(|(key, value)| {
                        counter.labels.iter().any(|(label_key, label_value)| {
                            label_key == key && label_value == value
                        })
                    })
            })
            .map(|counter| counter.value.load(Ordering::Relaxed))
            .unwrap_or_default()
    }
}

impl Recorder for TestRecorder {
    fn describe_counter(&self, _: KeyName, _: Option<Unit>, _: SharedString) {}

    fn describe_gauge(&self, _: KeyName, _: Option<Unit>, _: SharedString) {}

    fn describe_histogram(&self, _: KeyName, _: Option<Unit>, _: SharedString) {}

    fn register_counter(&self, key: &Key, _: &Metadata<'_>) -> metrics::Counter {
        let name = key.name().to_string();
        let labels = key
            .labels()
            .map(|label| (label.key().to_string(), label.value().to_string()))
            .collect::<Vec<_>>();
        let mut counters = self.counters.lock().expect("counters lock");
        let value = counters
            .iter()
            .find(|counter| counter.name == name && counter.labels == labels)
            .map(|counter| counter.value.clone())
            .unwrap_or_else(|| {
                let value = Arc::new(AtomicU64::new(0));
                counters.push(RecordedCounter {
                    name,
                    labels,
                    value: value.clone(),
                });
                value
            });
        metrics::Counter::from_arc(Arc::new(AtomicCounter(value)))
    }

    fn register_gauge(&self, _: &Key, _: &Metadata<'_>) -> metrics::Gauge {
        metrics::Gauge::noop()
    }

    fn register_histogram(&self, _: &Key, _: &Metadata<'_>) -> metrics::Histogram {
        metrics::Histogram::noop()
    }
}

struct AtomicCounter(Arc<AtomicU64>);

impl CounterFn for AtomicCounter {
    fn increment(&self, value: u64) {
        self.0.fetch_add(value, Ordering::Relaxed);
    }

    fn absolute(&self, value: u64) {
        self.0.fetch_max(value, Ordering::Relaxed);
    }
}
