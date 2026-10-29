use crate::retry::PendingGap;
use platform_market_data::event::GapMarker;

const FALLBACK_WINDOW_STEP: u64 = 1_000;

impl PendingGap {
    pub(super) fn missing_ranges(
        missing_from: u64,
        missing_to: u64,
        sequences: impl IntoIterator<Item = u64>,
    ) -> Vec<(u64, u64)> {
        if missing_from > missing_to {
            return Vec::new();
        }
        let mut sequences: Vec<u64> = sequences
            .into_iter()
            .filter(|sequence| *sequence >= missing_from && *sequence <= missing_to)
            .collect();
        sequences.sort_unstable();
        sequences.dedup();

        let mut ranges = Vec::new();
        let mut cursor = missing_from;
        for sequence in sequences {
            if sequence > cursor {
                ranges.push((cursor, sequence - 1));
            }
            if sequence == u64::MAX {
                return ranges;
            }
            cursor = sequence + 1;
            if cursor > missing_to {
                return ranges;
            }
        }
        if cursor <= missing_to {
            ranges.push((cursor, missing_to));
        }
        ranges
    }

    pub(super) fn widened_gap(gap: &GapMarker, attempts: u32) -> GapMarker {
        if attempts == 0 {
            return gap.clone();
        }
        let padding = FALLBACK_WINDOW_STEP.saturating_mul(u64::from(attempts.min(32)));
        let mut widened = gap.clone();
        widened.previous_sequence = gap.missing_from.saturating_sub(padding).saturating_sub(1);
        widened.missing_from = gap.missing_from.saturating_sub(padding);
        widened.missing_to = gap.missing_to.saturating_add(padding);
        widened.observed_sequence = gap.missing_to.saturating_add(padding).saturating_add(1);
        widened
    }
}
