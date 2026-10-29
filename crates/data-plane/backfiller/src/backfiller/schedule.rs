use crate::retry::PendingGap;
use platform_types::new_types::TimestampMilliseconds;
use std::collections::BTreeMap;
use tokio::time::{Duration, Instant, sleep_until};

impl PendingGap {
    pub(super) fn next_due_key(
        pending: &BTreeMap<String, PendingGap>,
        now: TimestampMilliseconds,
    ) -> Option<String> {
        pending
            .iter()
            .find(|(_, gap)| gap.is_due(now))
            .map(|(key, _)| key.clone())
    }

    pub(super) fn next_attempt_at(
        pending: &BTreeMap<String, PendingGap>,
    ) -> Option<TimestampMilliseconds> {
        pending.values().map(|gap| gap.next_attempt_at).min()
    }

    pub(super) fn instant_at(timestamp: TimestampMilliseconds) -> Instant {
        let now = TimestampMilliseconds::now().0;
        let delay = timestamp.0.saturating_sub(now).max(0) as u64;
        Instant::now() + Duration::from_millis(delay)
    }

    pub(super) async fn sleep_until_optional(deadline: Option<Instant>) {
        match deadline {
            Some(deadline) => sleep_until(deadline).await,
            None => std::future::pending::<()>().await,
        }
    }
}
