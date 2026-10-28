use crate::{
    binance::{parser::BinanceEventParser, stream::MarketStream},
    config::BackfillerConfig,
    event_page_source::EventPageSource,
    fetcher::{BackfilledEvent, MissingEventFetcher, PaginatedFetcher},
};
use async_trait::async_trait;
use platform_errors::{BpError, BpResult, ExchangeError};
use platform_exchange_gateway::{
    clients::reqwest::ReqwestHttpClient, connector::Connector, submission::SubmissionOutcome,
};
use platform_exchange_types::{
    binance::{
        exchange::BinanceExchange,
        trades::{BinanceHistoricalTradesRequest, BinanceTradeResponse},
    },
    encode::ByteEncoder,
    signer::Signer,
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
            fetcher: PaginatedFetcher::new(BinanceEventPageSource {
                connector,
                api_key: config.exchange_api_key.clone(),
            }),
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

struct BinanceEventPageSource {
    connector: Connector<BinanceExchange, ReqwestHttpClient>,
    api_key: Option<String>,
}

impl BinanceEventPageSource {
    async fn fetch_trade_page(
        &self,
        symbol: &str,
        from_sequence: u64,
        limit: u32,
        api_key: String,
    ) -> BpResult<Vec<BinanceTradeResponse>> {
        let request = BinanceHistoricalTradesRequest::new(
            symbol.to_string(),
            from_sequence,
            limit,
            Some(api_key),
        );
        let submission = self.connector.submit_http(request).map_err(|error| {
            BpError::exchange("submit Binance historical trades request", error)
        })?;
        match submission
            .wait()
            .await
            .map_err(|error| BpError::exchange("await Binance historical trades response", error))?
        {
            SubmissionOutcome::Submitted(response) => response.historical_trades(),
            SubmissionOutcome::Unknown { .. } => {
                Err(BpError::Exchange(ExchangeError::UnknownSubmissionOutcome {
                    exchange: "Binance",
                }))
            }
        }
    }
}

#[async_trait]
impl EventPageSource for BinanceEventPageSource {
    async fn fetch_page(
        &self,
        symbol: &str,
        from_sequence: u64,
        limit: u32,
    ) -> BpResult<Vec<BackfilledEvent>> {
        let Some(api_key) = self.api_key.clone() else {
            // TODO: allow the API key to come from a secret manager instead of
            // the process environment.
            return Err(BpError::config_missing("BACKFILLER_EXCHANGE_API_KEY"));
        };
        let trades = self
            .fetch_trade_page(symbol, from_sequence, limit, api_key)
            .await?;
        Ok(trades
            .iter()
            .map(|trade| BinanceEventParser::trade(symbol, trade))
            .collect())
    }
}
