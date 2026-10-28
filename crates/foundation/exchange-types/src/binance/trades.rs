use crate::{
    binance::{
        exchange::BinanceExchange,
        response::{BinanceResponse, BinanceResponsePayload},
    },
    http::{HttpMethod, HttpRequest},
    new_types::{TimestampMilliseconds, UsageCount},
    rate_limited::RateLimitRestriction,
    request::{ETHttpRequest, ETRequest},
    signer::Signer,
};
use platform_errors::{BpError, BpResult};
use query_params::QueryParams;
use serde::{Deserialize, Serialize};
use serde_with::skip_serializing_none;

#[allow(non_snake_case)]
#[skip_serializing_none]
#[derive(Serialize, Debug, Clone, Hash, QueryParams)]
struct BinanceHistoricalTradesQuery {
    symbol: String,
    fromId: Option<u64>,
    limit: Option<u32>,
}

/// Binance historical-trades HTTP request (`GET /api/v3/historicalTrades`).
///
/// Historical trade ids are the sequence carried by `@trade` streams. The
/// endpoint requires an API key header even though the request is unsigned, so
/// the key is carried on the request instead of being derived by the signer.
#[derive(Serialize, Debug, Clone)]
pub struct BinanceHistoricalTradesRequest {
    #[serde(flatten)]
    query: BinanceHistoricalTradesQuery,
    #[serde(skip)]
    api_key: Option<String>,
}

impl BinanceHistoricalTradesRequest {
    pub fn new(symbol: String, from_id: u64, limit: u32, api_key: Option<String>) -> Self {
        Self {
            query: BinanceHistoricalTradesQuery {
                symbol,
                fromId: Some(from_id),
                limit: Some(limit),
            },
            api_key,
        }
    }
}

pub type BinanceHistoricalTradesResponse = Vec<BinanceTradeResponse>;

#[allow(non_snake_case)]
#[derive(Debug, Clone, Deserialize)]
pub struct BinanceTradeResponse {
    pub id: u64,
    pub price: String,
    pub qty: String,
    pub quoteQty: String,
    pub time: TimestampMilliseconds,
    pub isBuyerMaker: bool,
    pub isBestMatch: bool,
}

impl BinanceResponse<BinanceHistoricalTradesResponse> {
    pub fn historical_trades(self) -> BpResult<Vec<BinanceTradeResponse>> {
        match self.payload {
            BinanceResponsePayload::Success(payload) => Ok(payload),
            BinanceResponsePayload::Failure(error) => {
                let retry_after = self.metadata.retry_after.or_else(|| error.retry_after());
                Err(BpError::Exchange(
                    error.exchange_error(self.metadata.status, retry_after),
                ))
            }
        }
    }
}

impl ETRequest for BinanceHistoricalTradesRequest {
    type Exchange = BinanceExchange;

    fn is_signed(&self) -> bool {
        false
    }
    fn is_idempotent(&self) -> bool {
        true
    }
    fn rate_limit_usage(&self, restriction: RateLimitRestriction) -> UsageCount {
        match restriction {
            RateLimitRestriction::Weight => UsageCount(20),
            _ => UsageCount::ZERO,
        }
    }
    fn set_api_key(&mut self, api_key: Option<String>) {
        if api_key.is_some() {
            self.api_key = api_key;
        }
    }
    fn set_timestamp(&mut self, _timestamp: TimestampMilliseconds) {}
    fn query_params(&self, percent_encode: bool) -> String {
        self.query.query_params(true, percent_encode)
    }
}

impl ETHttpRequest for BinanceHistoricalTradesRequest {
    type Response = BinanceResponse<BinanceHistoricalTradesResponse>;
    type VerificationRequest = Self;

    fn http_method(&self) -> HttpMethod {
        HttpMethod::GET
    }
    fn http_endpoint(&self) -> &'static str {
        "historicalTrades"
    }
    fn try_into_http(self, _signer: &Signer) -> BpResult<HttpRequest> {
        let path = self.http_endpoint().to_string();
        let query = self.query.query_params(true, true);
        let headers = self
            .api_key
            .map(|api_key| vec![("X-MBX-APIKEY".to_string(), api_key)])
            .unwrap_or_default();
        Ok(HttpRequest {
            method: HttpMethod::GET,
            path,
            query: (!query.is_empty()).then_some(query),
            headers,
            body: None,
        })
    }
}
