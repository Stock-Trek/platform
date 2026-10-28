use crate::{
    binance::stream::{MarketStream, StreamKind},
    config::BackfillerConfig,
    fetcher::{BackfilledEvent, MissingEventFetcher},
};
use async_trait::async_trait;
use platform_errors::{BpError, BpResult, ExchangeError};
use platform_exchange_gateway::{
    clients::reqwest::ReqwestHttpClient, connector::Connector, submission::SubmissionOutcome,
};
use platform_exchange_types::{
    binance::{
        exchange::BinanceExchange,
        trades::{
            BinanceAggTradeResponse, BinanceAggTradesRequest, BinanceHistoricalTradesRequest,
            BinanceTradeResponse,
        },
    },
    encode::ByteEncoder,
    signer::Signer,
};
use platform_market_data::event::GapMarker;
use serde_json::json;
use std::time::Duration;
use stock_trek_types::cex::cex_id::CexId;

const REQUEST_TIMEOUT: Duration = Duration::from_secs(30);
const MAX_RETRIES: u8 = 3;
const PAGE_LIMIT: u64 = 1_000;

/// Reconstructs Binance trade gaps from the public aggregate-trades endpoint and
/// the API-key protected historical-trades endpoint.
pub struct BinanceGapFetcher {
    connector: Connector<BinanceExchange, ReqwestHttpClient>,
    api_key: Option<String>,
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
            connector,
            api_key: config.exchange_api_key.clone(),
        })
    }

    async fn fetch_agg_trades(
        &self,
        gap: &GapMarker,
        stream: &MarketStream,
    ) -> BpResult<Vec<BackfilledEvent>> {
        let mut next_sequence = gap.missing_from;
        let mut events = Vec::new();
        while next_sequence <= gap.missing_to {
            let remaining = gap.missing_to - next_sequence + 1;
            let limit = remaining.min(PAGE_LIMIT) as u32;
            let page = self
                .fetch_agg_trade_page(&stream.symbol, next_sequence, limit)
                .await?;
            if page.is_empty() {
                break;
            }
            let mut highest_sequence = next_sequence.saturating_sub(1);
            for trade in &page {
                highest_sequence = highest_sequence.max(trade.a);
                if trade.a >= next_sequence && trade.a <= gap.missing_to {
                    events.push(Self::agg_trade_event(stream, trade));
                }
            }
            if highest_sequence < next_sequence {
                return Err(BpError::Exchange(ExchangeError::BadResponse(format!(
                    "Binance returned aggregate trades before the requested id {next_sequence}"
                ))));
            }
            if highest_sequence == u64::MAX {
                break;
            }
            next_sequence = highest_sequence + 1;
        }
        Ok(events)
    }

    async fn fetch_trades(
        &self,
        gap: &GapMarker,
        stream: &MarketStream,
    ) -> BpResult<Vec<BackfilledEvent>> {
        let Some(api_key) = self.api_key.clone() else {
            // TODO: allow the API key to come from a secret manager instead of
            // the process environment.
            return Err(BpError::config_missing("BACKFILLER_EXCHANGE_API_KEY"));
        };
        let mut next_sequence = gap.missing_from;
        let mut events = Vec::new();
        while next_sequence <= gap.missing_to {
            let remaining = gap.missing_to - next_sequence + 1;
            let limit = remaining.min(PAGE_LIMIT) as u32;
            let page = self
                .fetch_trade_page(&stream.symbol, next_sequence, limit, api_key.clone())
                .await?;
            if page.is_empty() {
                break;
            }
            let mut highest_sequence = next_sequence.saturating_sub(1);
            for trade in &page {
                highest_sequence = highest_sequence.max(trade.id);
                if trade.id >= next_sequence && trade.id <= gap.missing_to {
                    events.push(Self::trade_event(stream, trade));
                }
            }
            if highest_sequence < next_sequence {
                return Err(BpError::Exchange(ExchangeError::BadResponse(format!(
                    "Binance returned trades before the requested id {next_sequence}"
                ))));
            }
            if highest_sequence == u64::MAX {
                break;
            }
            next_sequence = highest_sequence + 1;
        }
        Ok(events)
    }

    async fn fetch_agg_trade_page(
        &self,
        symbol: &str,
        from_id: u64,
        limit: u32,
    ) -> BpResult<Vec<BinanceAggTradeResponse>> {
        let request = BinanceAggTradesRequest::new(symbol.to_string(), from_id, limit);
        let submission = self
            .connector
            .submit_http(request)
            .map_err(|error| BpError::exchange("submit Binance aggregate trades request", error))?;
        match submission
            .wait()
            .await
            .map_err(|error| BpError::exchange("await Binance aggregate trades response", error))?
        {
            SubmissionOutcome::Submitted(response) => response.agg_trades(),
            SubmissionOutcome::Unknown { .. } => {
                Err(BpError::Exchange(ExchangeError::UnknownSubmissionOutcome {
                    exchange: "Binance",
                }))
            }
        }
    }

    async fn fetch_trade_page(
        &self,
        symbol: &str,
        from_id: u64,
        limit: u32,
        api_key: String,
    ) -> BpResult<Vec<BinanceTradeResponse>> {
        let request =
            BinanceHistoricalTradesRequest::new(symbol.to_string(), from_id, limit, Some(api_key));
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

    fn agg_trade_event(stream: &MarketStream, trade: &BinanceAggTradeResponse) -> BackfilledEvent {
        BackfilledEvent {
            sequence: trade.a,
            exchange_timestamp: trade.T,
            data: json!({
                "e": "aggTrade",
                "E": trade.T,
                "s": stream.symbol,
                "a": trade.a,
                "p": trade.p,
                "q": trade.q,
                "f": trade.f,
                "l": trade.l,
                "T": trade.T,
                "m": trade.m,
            }),
        }
    }

    fn trade_event(stream: &MarketStream, trade: &BinanceTradeResponse) -> BackfilledEvent {
        BackfilledEvent {
            sequence: trade.id,
            exchange_timestamp: trade.time,
            data: json!({
                "e": "trade",
                "E": trade.time,
                "s": stream.symbol,
                "t": trade.id,
                "p": trade.price,
                "q": trade.qty,
                "T": trade.time,
                "m": trade.isBuyerMaker,
                "M": trade.isBestMatch,
            }),
        }
    }
}

#[async_trait]
impl MissingEventFetcher for BinanceGapFetcher {
    async fn fetch(&self, gap: &GapMarker) -> BpResult<Vec<BackfilledEvent>> {
        if gap.missing_from > gap.missing_to {
            return Err(BpError::Exchange(ExchangeError::BadResponse(format!(
                "gap {} on {} has missing_from after missing_to",
                gap.stream, gap.exchange
            ))));
        }
        let stream = MarketStream::parse(&gap.stream)?;
        match stream.kind {
            StreamKind::AggTrade => self.fetch_agg_trades(gap, &stream).await,
            StreamKind::Trade => self.fetch_trades(gap, &stream).await,
        }
    }
}
