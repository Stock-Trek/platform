#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum ArchiveSortKey {
    MarketEvent {
        timestamp: i64,
        sequence: u64,
        state_epoch: u64,
    },
    ClosedCandle {
        start_time: i64,
        close_watermark: i64,
        source_offset: u64,
        state_epoch: u64,
    },
}
