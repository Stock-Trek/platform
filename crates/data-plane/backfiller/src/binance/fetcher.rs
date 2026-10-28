use crate::{
    binance::{event_page_source::BinanceEventPageSource, stream::MarketStream},
    config::BackfillerConfig,
    fetcher::{BackfilledEvent, MissingEventFetcher, PaginatedFetcher},
};
use async_trait::async_trait;
use platform_errors::{BpError, BpResult, ExchangeError};
use platform_exchange_gateway::connector::Connector;
use platform_exchange_types::{
    binance::exchange::BinanceExchange, encode::ByteEncoder, signer::Signer,
};
use platform_market_data::event::GapMarker;
use std::time::Duration;
use stock_trek_types::cex::cex_id::CexId;

const REQUEST_TIMEOUT: Duration = Duration::from_secs(30);
const MAX_RETRIES: u8 = 3;

/// Reconstructs Binance trade gaps from the API-key protected historical-trades
/// endpoint.
pub struct BinanceGapFetcher {
    fetcher: PaginatedFetcher<BinanceEventPageSource>,
}

impl BinanceGapFetcher {
    pub fn try_new(config: &BackfillerConfig) -> BpResult<Self> {
        if config.exchange != CexId::Binance {
            return Err(BpError::Exchange(ExchangeError::UnsupportedExchange {
                exchange: config.exchange.to_string(),
            }));
        }
        // TODO: support the exchange's testnet credentials and signing mode.
        let signer = Signer::new_unencrypted(ByteEncoder::HexLower);
        let connector = Connector::try_new_http_reqwest(
            config.exchange_http_url.clone(),
            BinanceExchange,
            signer,
            REQUEST_TIMEOUT,
            MAX_RETRIES,
        )
        .map_err(|error| BpError::exchange("create Binance backfill connector", error))?;
        Ok(Self {
            fetcher: PaginatedFetcher::new(BinanceEventPageSource::new(
                connector,
                config.exchange_api_key.clone(),
            )),
        })
    }
}

#[async_trait]
impl MissingEventFetcher for BinanceGapFetcher {
    async fn fetch(&self, gap: &GapMarker) -> BpResult<Vec<BackfilledEvent>> {
        let stream = MarketStream::parse(&gap.stream)?;
        self.fetcher
            .fetch_range(&stream.symbol, gap.missing_from, gap.missing_to)
            .await
    }
}
