use platform_archive::ArchiveRecord;
use platform_errors::{BpError, BpResult};

const CONTENT_ID_OFFSET_BASIS: u64 = 0xcbf2_9ce4_8422_2325;
const CONTENT_ID_PRIME: u64 = 0x0000_0100_0000_01b3;

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub(crate) struct ContentId {
    hash: u64,
}

impl ContentId {
    pub(crate) fn new() -> Self {
        Self {
            hash: CONTENT_ID_OFFSET_BASIS,
        }
    }

    pub(crate) fn update(&mut self, record: &ArchiveRecord) -> BpResult<()> {
        let bytes = serde_json::to_vec(record).map_err(BpError::external)?;
        for byte in bytes {
            self.hash ^= u64::from(byte);
            self.hash = self.hash.wrapping_mul(CONTENT_ID_PRIME);
        }
        Ok(())
    }

    pub(crate) fn value(self) -> u64 {
        self.hash
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use platform_market_data::event::MarketEvent;
    use platform_types::new_types::TimestampMilliseconds;
    use stock_trek_types::cex::cex_id::CexId;

    fn event(payload: &str) -> ArchiveRecord {
        ArchiveRecord::MarketEvent(
            MarketEvent {
                exchange: CexId::Binance,
                stream: "btc_usdt@trade".to_string(),
                sequence: Some(1),
                exchange_timestamp: Some(TimestampMilliseconds(1_000)),
                received_at: TimestampMilliseconds(1_000),
                payload: payload.to_string(),
            }
            .into(),
        )
    }

    fn content_id(record: &ArchiveRecord) -> u64 {
        let mut content_id = ContentId::new();
        content_id.update(record).unwrap();
        content_id.value()
    }

    #[test]
    fn content_ids_are_stable_and_differ_for_different_records() {
        let first = content_id(&event("payload"));
        let second = content_id(&event("payload"));
        let different = content_id(&event("other payload"));

        assert_eq!(first, second);
        assert_ne!(first, different);
        assert_ne!(first, ContentId::new().value());
    }
}
