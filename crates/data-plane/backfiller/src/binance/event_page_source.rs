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
        trades::{BinanceHistoricalTradesRequest, BinanceTradeResponse},
    },
    request::ETHttpRequest,
};
use platform_types::new_types::TimestampMilliseconds;
use std::sync::Arc;

/// Reads Binance REST pages used to reconstruct market-data gaps.
pub struct BinanceEventPageSource {
    connector: Arc<Connector<BinanceExchange, ReqwestHttpClient>>,
    api_key: String,
}

impl BinanceEventPageSource {
    pub fn from_arc(
        connector: Arc<Connector<BinanceExchange, ReqwestHttpClient>>,
        api_key: String,
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
        observed_sequence: u64,
    ) -> BpResult<Vec<BackfilledEvent>> {
        let request = BinanceBookTickerRequest::new(symbol.to_string());
        let response = self.submit(request).await?.book_ticker()?;
        Ok(vec![BinanceEventParser::book_ticker(
            symbol,
            observed_sequence,
            &response.bidPrice,
            &response.bidQty,
            &response.askPrice,
            &response.askQty,
            TimestampMilliseconds::now(),
        )])
    }

    async fn fetch_trade_page(
        &self,
        symbol: &str,
        from_sequence: u64,
        limit: u32,
        api_key: &str,
    ) -> BpResult<Vec<BinanceTradeResponse>> {
        let request = BinanceHistoricalTradesRequest::new(
            symbol.to_string(),
            from_sequence,
            limit,
            Some(api_key.to_string()),
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
        let trades = self
            .fetch_trade_page(symbol, from_sequence, limit, &self.api_key)
            .await?;
        Ok(trades
            .iter()
            .map(|trade| BinanceEventParser::trade(symbol, trade))
            .collect())
    }
}
