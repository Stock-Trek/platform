use crate::{
    binance::parser::BinanceEventParser, event_page_source::EventPageSource,
    fetcher::BackfilledEvent,
};
use async_trait::async_trait;
use platform_errors::{BpError, BpResult, ExchangeError};
use platform_exchange_gateway::{
    clients::reqwest::ReqwestHttpClient, connector::Connector, submission::SubmissionOutcome,
};
use platform_exchange_types::{
    binance::{
        book_ticker::BinanceBookTickerRequest,
        depth::BinanceDepthRequest,
        exchange::BinanceExchange,
        klines::{BinanceKlineResponse, BinanceKlinesInterval, BinanceKlinesRequest},
        klines_limit::BinanceKlinesLimit,
        response::{BinanceResponse, BinanceResponsePayload},
        trades::{BinanceHistoricalTradesRequest, BinanceTradeResponse},
    },
    request::ETHttpRequest,
};
use platform_types::new_types::TimestampMilliseconds;
use std::sync::Arc;

const KLINES_PAGE_LIMIT: u32 = BinanceKlinesLimit::MAXIMUM;

/// Reads Binance REST pages used to reconstruct market-data gaps.
pub struct BinanceEventPageSource {
    connector: Arc<Connector<BinanceExchange, ReqwestHttpClient>>,
    api_key: Option<String>,
}

impl BinanceEventPageSource {
    pub fn from_arc(
        connector: Arc<Connector<BinanceExchange, ReqwestHttpClient>>,
        api_key: Option<String>,
    ) -> Self {
        Self { connector, api_key }
    }

    async fn submit<Request>(&self, request: Request) -> BpResult<Request::Response>
    where
        Request: ETHttpRequest<Exchange = BinanceExchange> + Send,
    {
        let submission = self
            .connector
            .submit_http(request)
            .map_err(|error| BpError::exchange("submit Binance backfill request", error))?;
        match submission
            .wait()
            .await
            .map_err(|error| BpError::exchange("await Binance backfill response", error))?
        {
            SubmissionOutcome::Submitted(response) => Ok(response),
            SubmissionOutcome::Unknown { .. } => {
                Err(BpError::Exchange(ExchangeError::UnknownSubmissionOutcome {
                    exchange: "Binance",
                }))
            }
        }
    }

    pub async fn fetch_depth(&self, symbol: &str) -> BpResult<Vec<BackfilledEvent>> {
        let request = BinanceDepthRequest::new(symbol.to_string(), Some(1_000));
        let response = self.submit(request).await?.depth()?;
        Ok(vec![BinanceEventParser::depth(
            symbol,
            &response,
            TimestampMilliseconds::now(),
        )])
    }

    pub async fn fetch_book_ticker(
        &self,
        symbol: &str,
        sequence: u64,
    ) -> BpResult<Vec<BackfilledEvent>> {
        let request = BinanceBookTickerRequest::new(symbol.to_string());
        let response = self.submit(request).await?.book_ticker()?;
        Ok(vec![BinanceEventParser::book_ticker(
            symbol,
            sequence,
            &response.bidPrice,
            &response.bidQty,
            &response.askPrice,
            &response.askQty,
            TimestampMilliseconds::now(),
        )])
    }

    pub async fn fetch_klines(
        &self,
        symbol: &str,
        interval: &BinanceKlinesInterval,
        missing_from: u64,
        missing_to: u64,
    ) -> BpResult<Vec<BackfilledEvent>> {
        let step = interval.millis().max(1);
        let mut next = i64::try_from(missing_from).unwrap_or(i64::MAX);
        let end = i64::try_from(missing_to).unwrap_or(i64::MAX);
        let mut events = Vec::new();
        while next <= end {
            let mut request = BinanceKlinesRequest::new(symbol.to_string(), interval.clone());
            request.startTime = Some(TimestampMilliseconds(next));
            request.endTime = Some(TimestampMilliseconds(end));
            request.limit = Some(BinanceKlinesLimit::try_new(KLINES_PAGE_LIMIT)?);
            let response = self.submit(request).await?;
            let klines = raw_klines(response)?;
            if klines.is_empty() {
                break;
            }
            let mut highest = next.saturating_sub(step);
            for kline in &klines {
                highest = highest.max(kline.open_time().0);
                events.push(BinanceEventParser::kline(symbol, interval, kline));
            }
            if highest < next {
                return Err(BpError::Exchange(ExchangeError::BadResponse(format!(
                    "{symbol} returned klines before the requested open time {next}"
                ))));
            }
            next = highest.saturating_add(step);
        }
        Ok(events)
    }

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
        self.submit(request).await?.historical_trades()
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

fn raw_klines(
    response: BinanceResponse<Vec<BinanceKlineResponse>>,
) -> BpResult<Vec<BinanceKlineResponse>> {
    match response.payload {
        BinanceResponsePayload::Success(payload) => Ok(payload),
        BinanceResponsePayload::Failure(error) => {
            let retry_after = response
                .metadata
                .retry_after
                .or_else(|| error.retry_after());
            Err(BpError::Exchange(
                error.exchange_error(response.metadata.status, retry_after),
            ))
        }
    }
}
