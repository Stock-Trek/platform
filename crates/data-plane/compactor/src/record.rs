/// The kind of archive data being compacted.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ArchiveKind {
    Events,
    Candles,
}

impl ArchiveKind {
    pub fn label(self) -> &'static str {
        match self {
            Self::Events => "events",
            Self::Candles => "candles",
        }
    }

    pub fn from_segment(segment: &str) -> Option<Self> {
        match segment {
            "events" => Some(Self::Events),
            "candles" => Some(Self::Candles),
            _ => None,
        }
    }
}

/// A row decoded from a raw archive Parquet file.
#[derive(Debug, Clone)]
pub enum ArchiveRecord {
    Event(EventRecord),
    Candle(CandleRecord),
}

impl ArchiveRecord {
    pub fn sort_key(&self) -> (i64, u64) {
        match self {
            Self::Event(event) => (
                event.exchange_timestamp.unwrap_or(event.received_at),
                event.sequence.unwrap_or_default(),
            ),
            Self::Candle(candle) => (candle.start_time_millis_inc, candle.revision),
        }
    }

    pub fn partition(&self) -> CompactPartition {
        match self {
            Self::Event(event) => CompactPartition::Events {
                exchange: event.exchange.clone(),
                stream: event.stream.clone(),
            },
            Self::Candle(candle) => CompactPartition::Candles {
                exchange: candle.exchange.clone(),
                base: candle.base.clone(),
                quote: candle.quote.clone(),
                interval: candle.interval.clone(),
            },
        }
    }

    pub fn key(&self) -> ArchiveRecordKey {
        match self {
            Self::Event(event) => ArchiveRecordKey::Event {
                exchange: event.exchange.clone(),
                stream: event.stream.clone(),
                sequence: event.sequence,
                payload: event.payload.clone(),
            },
            Self::Candle(candle) => ArchiveRecordKey::Candle {
                exchange: candle.exchange.clone(),
                base: candle.base.clone(),
                quote: candle.quote.clone(),
                interval: candle.interval.clone(),
                start_time_millis_inc: candle.start_time_millis_inc,
                revision: candle.revision,
            },
        }
    }
}

/// The output partition for a compacted record.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum CompactPartition {
    Events {
        exchange: String,
        stream: String,
    },
    Candles {
        exchange: String,
        base: String,
        quote: String,
        interval: String,
    },
}

impl CompactPartition {
    pub fn path(&self) -> String {
        match self {
            Self::Events { exchange, stream } => format!(
                "kind=events/exchange={}/stream={}",
                Self::segment(exchange),
                Self::segment(stream)
            ),
            Self::Candles {
                exchange,
                base,
                quote,
                interval,
            } => format!(
                "kind=candles/exchange={}/base={}/quote={}/interval={}",
                Self::segment(exchange),
                Self::segment(base),
                Self::segment(quote),
                Self::segment(interval)
            ),
        }
    }

    fn segment(value: &str) -> String {
        let value = value.trim().replace(['/', '\\'], "_");
        if value.is_empty() {
            "unknown".to_string()
        } else {
            value
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum ArchiveRecordKey {
    Event {
        exchange: String,
        stream: String,
        sequence: Option<u64>,
        payload: String,
    },
    Candle {
        exchange: String,
        base: String,
        quote: String,
        interval: String,
        start_time_millis_inc: i64,
        revision: u64,
    },
}

#[derive(Debug, Clone)]
pub struct EventRecord {
    pub exchange: String,
    pub stream: String,
    pub sequence: Option<u64>,
    pub exchange_timestamp: Option<i64>,
    pub received_at: i64,
    pub payload: String,
}

#[derive(Debug, Clone)]
pub struct CandleRecord {
    pub exchange: String,
    pub base: String,
    pub quote: String,
    pub interval: String,
    pub start_time_millis_inc: i64,
    pub end_time_millis_exc: i64,
    pub duration_millis: i64,
    pub is_candle_closed: bool,
    pub open: f64,
    pub high: f64,
    pub low: f64,
    pub close: f64,
    pub volume: f64,
    pub quote_volume: f64,
    pub vwap: f64,
    pub trade_count: u64,
    pub revision: u64,
}
