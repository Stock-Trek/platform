use crate::{
    binance::parser::BinanceEventParser,
    event_page_source::EventPageSource,
    fetcher::BackfilledEvent,
};
use async_trait::async_trait;
use platform_errors::{BpError, BpResult, ExchangeError};
use platform_exchange_gateway::{
    clients::reqwest::ReqwestHttpClient, connector::Connector, submission::SubmissionOutcome,
};
use platform_exchange_types::binance::{
    exchange::BinanceExchange,
    trades::{BinanceHistoricalTradesRequest, BinanceTradeResponse},
};

/// Reads Binance historical trade pages from the API-key protected REST
/// endpoint.
pub struct BinanceEventPageSource {
    connector: Connector<BinanceExchange, ReqwestHttpClient>,
    api_key: Option<String>,
}

impl BinanceEventPageSource {
    pub fn new(
        connector: Connector<BinanceExchange, ReqwestHttpClient>,
        api_key: Option<String>,
    ) -> Self {
        Self { connector, api_key }
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
