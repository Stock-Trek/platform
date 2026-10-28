use crate::config::ArchiverConfig;
use async_trait::async_trait;
use platform_archive::ArchiveRecord;
use platform_errors::BpResult;
use platform_market_data::{
    candle::ClosedCandle, envelope::EnvelopeCodec, event::MarketEvent, subject::MarketEventSubject,
};
use platform_metrics::descriptions::{
    DECODE_KIND_CLOSED_CANDLE, DECODE_KIND_MARKET_EVENT, STAGE_ARCHIVER,
};
use platform_metrics::market_data::MarketDataMetrics;
use platform_nats::JetStreamConsumer;
use std::time::Duration;
use tracing::warn;

const FETCH_BATCH: usize = 1;
const FETCH_WINDOW: Duration = Duration::from_secs(30);
const RETRY_BACKOFF: Duration = Duration::from_secs(1);
const EVENTS_DURABLE_NAME: &str = "archiver-events";
const CANDLES_DURABLE_NAME: &str = "archiver-candles";

/// Source of records that should be written to the archive.
#[async_trait]
pub trait ArchiveSource: Send {
    /// Returns `None` when the source has reached its end.
    async fn next_record(&mut self) -> BpResult<Option<ArchiveRecord>>;
}

/// Reads market events and closed candles from durable JetStream consumers.
///
/// Messages are explicitly acknowledged after delivery to the archiver, so a
/// restart resumes from the last acknowledged record instead of skipping
/// everything published while the archiver was down.
pub struct NatsArchiveSource {
    events: JetStreamConsumer,
    candles: JetStreamConsumer,
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
        Ok(Self { events, candles })
    }
}

#[async_trait]
impl ArchiveSource for NatsArchiveSource {
    async fn next_record(&mut self) -> BpResult<Option<ArchiveRecord>> {
        loop {
            tokio::select! {
                message = self.events.fetch_retrying(FETCH_BATCH, FETCH_WINDOW, RETRY_BACKOFF) => {
                    let Some(message) = message.into_iter().next() else {
                        continue;
                    };
                    if let Err(error) = self.events.ack(&message).await {
                        warn!(%error, "failed to acknowledge market event; waiting for redelivery");
                        continue;
                    }
                    match MarketEvent::decode(&message.payload) {
                        Ok(event) => return Ok(Some(ArchiveRecord::MarketEvent(event))),
                        Err(error) => {
                            MarketDataMetrics::record_decode_failure(
                                STAGE_ARCHIVER,
                                DECODE_KIND_MARKET_EVENT,
                            );
                            warn!(%error, "skipping malformed market event");
                        }
                    }
                }
                message = self.candles.fetch_retrying(FETCH_BATCH, FETCH_WINDOW, RETRY_BACKOFF) => {
                    let Some(message) = message.into_iter().next() else {
                        continue;
                    };
                    if let Err(error) = self.candles.ack(&message).await {
                        warn!(%error, "failed to acknowledge closed candle; waiting for redelivery");
                        continue;
                    }
                    match ClosedCandle::decode(&message.payload) {
                        Ok(candle) => return Ok(Some(ArchiveRecord::ClosedCandle(candle))),
                        Err(error) => {
                            MarketDataMetrics::record_decode_failure(
                                STAGE_ARCHIVER,
                                DECODE_KIND_CLOSED_CANDLE,
                            );
                            warn!(%error, "skipping malformed closed candle");
                        }
                    }
                }
            }
        }
    }
}
