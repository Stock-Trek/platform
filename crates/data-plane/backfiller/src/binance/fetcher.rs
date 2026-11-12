use crate::{
    binance::{
        event_page_source::BinanceEventPageSource,
        market_stream::{MarketStream, MarketStreamKind},
    },
    config::BackfillerConfig,
    event_fetcher::{BackfilledEvent, MissingEventFetcher, PaginatedFetcher},
};
use async_trait::async_trait;
use platform_errors::{BuildError, BuildResult, ExchangeError};
use platform_exchange_gateway::{Connector, ReqwestHttpClient};
use platform_exchange_types::{
    api_key_credential::ApiKeyCredentials, binance::exchange::BinanceExchange, encode::ByteEncoder,
    encrypt::EncryptionAlgorithm, signer::Signer,
};
use platform_market_data::GapMarker;
use std::{sync::Arc, time::Duration};
use stock_trek_types::cex::cex_id::CexId;

const REQUEST_TIMEOUT: Duration = Duration::from_secs(30);
const MAX_RETRIES: u8 = 3;

/// Reconstructs Binance gaps from REST snapshots.
pub struct BinanceGapFetcher {
    connector: Arc<Connector<BinanceExchange, ReqwestHttpClient>>,
    api_key: String,
}

impl BinanceGapFetcher {
    pub fn try_new(config: &BackfillerConfig, credentials: ApiKeyCredentials) -> BuildResult<Self> {
        if config.exchange != CexId::Binance {
            return Err(BuildError::Exchange(ExchangeError::UnsupportedExchange {
                exchange: config.exchange.to_string(),
            }));
        }
        if config.signing_algorithm != EncryptionAlgorithm::Ed25519 {
            return Err(BuildError::config_invalid(
                "BACKFILLER_EXCHANGE_SIGNING_ALGORITHM",
                "only ed25519 is supported",
            ));
        }
        let signer = Signer::try_new(credentials, config.signing_algorithm, ByteEncoder::Base64)?;
        let api_key = signer.api_key();
        let connector = Connector::try_new_http_reqwest(
            config.exchange_http_url.clone(),
            BinanceExchange,
            signer,
            REQUEST_TIMEOUT,
            MAX_RETRIES,
        )
        .map_err(|error| BuildError::exchange("create Binance backfill connector", error))?;
        Ok(Self {
            connector: Arc::new(connector),
            api_key,
        })
    }

    fn source(&self) -> BinanceEventPageSource {
        BinanceEventPageSource::from_arc(Arc::clone(&self.connector), self.api_key.clone())
    }
}

#[async_trait]
impl MissingEventFetcher for BinanceGapFetcher {
    async fn fetch(&self, gap: &GapMarker) -> BuildResult<Vec<BackfilledEvent>> {
        let stream = MarketStream::parse(&gap.stream)?;
        let source = self.source();
        match stream.kind {
            MarketStreamKind::Trade => {
                PaginatedFetcher::new(source)
                    .fetch_range(&stream.symbol, gap.missing_from, gap.missing_to)
                    .await
            }
            MarketStreamKind::Depth => source.fetch_depth(&stream.symbol).await,
            MarketStreamKind::BookTicker => {
                // The REST book-ticker endpoint only returns the current
                // snapshot and has no update id. Stamp it with the sequence
                // that exposed the gap so it supersedes the whole missing
                // range instead of being treated as one historical event.
                source
                    .fetch_book_ticker(&stream.symbol, gap.observed_sequence)
                    .await
            }
            MarketStreamKind::Kline(interval) => Err(BuildError::bad_value(
                "stream",
                gap.stream.as_str(),
                format!(
                    "kline_{interval} gap backfill is not supported because kline events have no sequence"
                ),
            )),
        }
    }
}
