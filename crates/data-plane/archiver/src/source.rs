use crate::config::ArchiverConfig;
use platform_archive::ArchiveRecord;
use platform_errors::BpResult;
use platform_market_data::{
    candle::ClosedCandle, envelope::EnvelopeCodec, event::MarketEvent, subject::MarketEventSubject,
};
use platform_metrics::descriptions::{
    DECODE_KIND_CLOSED_CANDLE, DECODE_KIND_MARKET_EVENT, STAGE_ARCHIVER,
};
use platform_metrics::market_data::MarketDataMetrics;
use platform_nats::NatsSubscriber;
use tracing::{info, warn};

/// Reads market events and closed candles from NATS.
///
/// TODO: use a durable JetStream consumer with explicit acknowledgements and
/// deterministic message ids. Core NATS subscriptions are used for this first
/// cut, so messages received while the archiver is down are not replayed.
pub struct NatsArchiveSource {
    events: NatsSubscriber,
    candles: NatsSubscriber,
    events_open: bool,
    candles_open: bool,
}

impl NatsArchiveSource {
    pub async fn connect(config: &ArchiverConfig) -> BpResult<Self> {
        let events = NatsSubscriber::connect(
            &config.nats_url,
            "platform-archiver-events",
            &MarketEventSubject::prefix_wildcard(&config.event_subject_prefix),
            "1",
        )
        .await?;
        let candles = NatsSubscriber::connect(
            &config.nats_url,
            "platform-archiver-candles",
            &format!("{}.>", config.closed_candle_subject_prefix),
            "2",
        )
        .await?;
        Ok(Self {
            events,
            candles,
            events_open: true,
            candles_open: true,
        })
    }

    pub async fn next_record(&mut self) -> BpResult<Option<ArchiveRecord>> {
        loop {
            if !self.events_open && !self.candles_open {
                return Ok(None);
            }

            tokio::select! {
                message = self.events.next_message(), if self.events_open => {
                    match message? {
                        Some((_subject, payload)) => match MarketEvent::decode(&payload) {
                            Ok(event) => return Ok(Some(ArchiveRecord::MarketEvent(event))),
                            Err(error) => {
                                MarketDataMetrics::record_decode_failure(
                                    STAGE_ARCHIVER,
                                    DECODE_KIND_MARKET_EVENT,
                                );
                                warn!(%error, "skipping malformed market event");
                            }
                        },
                        None => {
                            self.events_open = false;
                            info!("market event subscription ended");
                        }
                    }
                }
                message = self.candles.next_message(), if self.candles_open => {
                    match message? {
                        Some((_subject, payload)) => match ClosedCandle::decode(&payload) {
                            Ok(candle) => return Ok(Some(ArchiveRecord::ClosedCandle(candle))),
                            Err(error) => {
                                MarketDataMetrics::record_decode_failure(
                                    STAGE_ARCHIVER,
                                    DECODE_KIND_CLOSED_CANDLE,
                                );
                                warn!(%error, "skipping malformed closed candle");
                            }
                        },
                        None => {
                            self.candles_open = false;
                            info!("closed candle subscription ended");
                        }
                    }
                }
            }
        }
    }
}
