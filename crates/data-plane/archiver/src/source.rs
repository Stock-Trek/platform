use crate::config::ArchiverConfig;
use async_trait::async_trait;
use platform_archive::{ArchiveRecord, ArchiveRecordKey, ArchivedMarketEvent, STATE_EPOCH_HEADER};
use platform_errors::BpResult;
use platform_market_data::{
    candle::ClosedCandle, envelope::EnvelopeCodec, event::MarketEvent, subject::MarketEventSubject,
};
use platform_metrics::descriptions::{
    DECODE_KIND_CLOSED_CANDLE, DECODE_KIND_MARKET_EVENT, STAGE_ARCHIVER,
};
use platform_metrics::market_data::MarketDataMetrics;
use platform_nats::{JetStreamConsumer, Message};
use std::collections::VecDeque;
use tracing::warn;

const EVENTS_DURABLE_NAME: &str = "archiver-events";
const CANDLES_DURABLE_NAME: &str = "archiver-candles";

/// Source of records that should be written to the archive.
#[async_trait]
pub trait ArchiveSource: Send {
    /// Returns `None` when the source has reached its end.
    async fn next_record(&mut self) -> BpResult<Option<ArchiveRecord>>;

    /// Acknowledges the record returned by the previous call to
    /// [`Self::next_record`].
    async fn ack(&mut self, _record: &ArchiveRecord) -> BpResult<()> {
        Ok(())
    }
}

/// Reads market events and closed candles from durable JetStream consumers.
///
/// Messages are acknowledged only after the archiver has flushed the record to
/// durable storage, so records buffered for a later flush are redelivered after
/// a restart instead of being lost.
pub struct NatsArchiveSource {
    events: JetStreamConsumer,
    candles: JetStreamConsumer,
    in_flight: VecDeque<InFlight>,
}

struct InFlight {
    key: ArchiveRecordKey,
    message: Message,
    consumer: Consumer,
}

#[derive(Clone, Copy)]
enum Consumer {
    Events,
    Candles,
}

impl NatsArchiveSource {
    pub async fn connect(config: &ArchiverConfig) -> BpResult<Self> {
        let events = JetStreamConsumer::connect(
            &config.nats_url,
            "platform-archiver-events",
            &config.event_subject_prefix,
            &MarketEventSubject::prefix_wildcard(&config.event_subject_prefix),
            EVENTS_DURABLE_NAME,
        )
        .await?;
        let candles = JetStreamConsumer::connect(
            &config.nats_url,
            "platform-archiver-candles",
            &config.closed_candle_subject_prefix,
            &format!("{}.>", config.closed_candle_subject_prefix),
            CANDLES_DURABLE_NAME,
        )
        .await?;
        Ok(Self {
            events,
            candles,
            in_flight: VecDeque::new(),
        })
    }

    fn track(
        &mut self,
        record: ArchiveRecord,
        message: Message,
        consumer: Consumer,
    ) -> ArchiveRecord {
        self.in_flight.push_back(InFlight {
            key: record.key(),
            message,
            consumer,
        });
        record
    }
}

#[async_trait]
impl ArchiveSource for NatsArchiveSource {
    async fn next_record(&mut self) -> BpResult<Option<ArchiveRecord>> {
        loop {
            tokio::select! {
                message = self.events.next_message_unacked() => {
                    let Some(message) = message else {
                        continue;
                    };
                    match MarketEvent::decode(&message.payload) {
                        Ok(event) => {
                            let state_epoch = message
                                .header(STATE_EPOCH_HEADER)
                                .and_then(|value| value.parse().ok())
                                .unwrap_or_default();
                            let record = ArchiveRecord::MarketEvent(ArchivedMarketEvent::new(
                                event, state_epoch,
                            ));
                            return Ok(Some(self.track(record, message, Consumer::Events)));
                        }
                        Err(error) => {
                            MarketDataMetrics::record_decode_failure(
                                STAGE_ARCHIVER,
                                DECODE_KIND_MARKET_EVENT,
                            );
                            warn!(%error, "skipping malformed market event");
                            self.events.ack(&message).await?;
                        }
                    }
                }
                message = self.candles.next_message_unacked() => {
                    let Some(message) = message else {
                        continue;
                    };
                    match ClosedCandle::decode(&message.payload) {
                        Ok(candle) => {
                            let record = ArchiveRecord::ClosedCandle(candle);
                            return Ok(Some(self.track(record, message, Consumer::Candles)));
                        }
                        Err(error) => {
                            MarketDataMetrics::record_decode_failure(
                                STAGE_ARCHIVER,
                                DECODE_KIND_CLOSED_CANDLE,
                            );
                            warn!(%error, "skipping malformed closed candle");
                            self.candles.ack(&message).await?;
                        }
                    }
                }
            }
        }
    }

    async fn ack(&mut self, record: &ArchiveRecord) -> BpResult<()> {
        let key = record.key();
        let Some(index) = self.in_flight.iter().position(|pending| pending.key == key) else {
            return Ok(());
        };
        let Some(pending) = self.in_flight.remove(index) else {
            return Ok(());
        };
        match pending.consumer {
            Consumer::Events => self.events.ack(&pending.message).await,
            Consumer::Candles => self.candles.ack(&pending.message).await,
        }
    }
}
