use crate::{
    binance::{
        exchange::BinanceExchange,
        klines_limit::BinanceKlinesLimit,
        request::BinanceRequestFactory,
        response::{BinanceResponse, BinanceResponsePayload},
    },
    http::{HttpMethod, HttpRequest},
    klines::{Kline, KlinesHttpRequest, KlinesResponse, KlinesWebsocketRequest},
    new_types::{TimestampMilliseconds, UsageCount},
    rate_limited::RateLimitRestriction,
    request::{ETHttpRequest, ETRequest, ETWebsocketRequest, WebsocketResponseMatcher},
    signer::Signer,
    websocket_id::ETWebsocketId,
};
use platform_errors::{BpError, BpResult, ExchangeError};
use query_params::QueryParams;
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use serde_with::skip_serializing_none;
use strum::Display;

#[allow(non_snake_case)]
#[skip_serializing_none]
#[derive(Serialize, Debug, Clone, Hash, QueryParams)]
pub struct BinanceKlinesRequest {
    pub symbol: String,
    pub interval: BinanceKlinesInterval,
    pub startTime: Option<TimestampMilliseconds>,
    pub endTime: Option<TimestampMilliseconds>,
    pub timezone: Option<String>,
    pub limit: Option<BinanceKlinesLimit>,
}

impl BinanceKlinesRequest {
    pub fn new(symbol: String, interval: BinanceKlinesInterval) -> Self {
        Self {
            symbol,
            interval,
            startTime: None,
            endTime: None,
            timezone: None,
            limit: None,
        }
    }
}

pub type BinanceKlinesResponse = Vec<BinanceKlineResponse>;

#[derive(Debug, Clone, Deserialize)]
pub struct BinanceKlineResponse(
    TimestampMilliseconds,
    String,
    String,
    String,
    String,
    String,
    TimestampMilliseconds,
    #[allow(unused)] String,
    u32,
    #[allow(unused)] String,
    #[allow(unused)] String,
    #[allow(unused)] String,
);

#[allow(non_camel_case_types)]
#[derive(Serialize, Deserialize, Display, Debug, Clone, PartialEq, Eq, Hash)]
pub enum BinanceKlinesInterval {
    #[serde(rename = "1s")]
    ONE_SECOND,
    #[serde(rename = "1m")]
    ONE_MINUTE,
    #[serde(rename = "3m")]
    THREE_MINUTE,
    #[serde(rename = "5m")]
    FIVE_MINUTE,
    #[serde(rename = "15m")]
    FIFTEEN_MINUTE,
    #[serde(rename = "30m")]
    THIRTY_MINUTE,
    #[serde(rename = "1h")]
    ONE_HOUR,
    #[serde(rename = "2h")]
    TWO_HOUR,
    #[serde(rename = "4h")]
    FOUR_HOUR,
    #[serde(rename = "6h")]
    SIX_HOUR,
    #[serde(rename = "8h")]
    EIGHT_HOUR,
    #[serde(rename = "12h")]
    TWELVE_HOUR,
    #[serde(rename = "1d")]
    ONE_DAY,
    #[serde(rename = "3d")]
    THREE_DAY,
    #[serde(rename = "1w")]
    ONE_WEEK,
    #[serde(rename = "1M")]
    ONE_MONTH,
}

impl KlinesHttpRequest<BinanceResponse<BinanceKlinesResponse>> for BinanceKlinesRequest {}
impl KlinesWebsocketRequest<BinanceResponse<BinanceKlinesResponse>> for BinanceKlinesRequest {}

impl KlinesResponse for BinanceResponse<BinanceKlinesResponse> {
    fn klines(&self) -> BpResult<Vec<Kline>> {
        match &self.payload {
            BinanceResponsePayload::Failure(error) => {
                let retry_after = self.metadata.retry_after.or_else(|| error.retry_after());
                Err(BpError::Exchange(
                    error.exchange_error(self.metadata.status, retry_after),
                ))
            }
            BinanceResponsePayload::Success(payload) => {
                let mut result = Vec::new();
                for binance_kline in payload {
                    let kline = binance_kline.try_into()?;
                    result.push(kline);
                }
                Ok(result)
            }
        }
    }
}

impl TryFrom<&BinanceKlineResponse> for Kline {
    type Error = BpError;
    fn try_from(value: &BinanceKlineResponse) -> Result<Self, Self::Error> {
        let open_time = value.0;
        let close_time = value.6;
        let open = BinanceKlineResponse::str_to_decimal("open", &value.1)?;
        let high = BinanceKlineResponse::str_to_decimal("high", &value.2)?;
        let low = BinanceKlineResponse::str_to_decimal("low", &value.3)?;
        let close = BinanceKlineResponse::str_to_decimal("close", &value.4)?;
        let volume = BinanceKlineResponse::str_to_decimal("volume", &value.5)?;
        let trades = value.8;
        Ok(Kline {
            open_time,
            close_time,
            open,
            high,
            low,
            close,
            volume,
            trades,
        })
    }
}

impl BinanceKlineResponse {
    fn str_to_decimal(name: &str, value: &str) -> BpResult<Decimal> {
        Decimal::from_str_exact(value).map_err(|e| {
            BpError::Exchange(ExchangeError::BadValue {
                name: name.into(),
                value: value.into(),
                reason: e.to_string(),
            })
        })
    }
}

impl ETRequest for BinanceKlinesRequest {
    type Exchange = BinanceExchange;

    fn is_signed(&self) -> bool {
        false
    }
    fn is_idempotent(&self) -> bool {
        true
    }
    fn rate_limit_usage(&self, restriction: RateLimitRestriction) -> UsageCount {
        match restriction {
            RateLimitRestriction::Weight => UsageCount(2),
            _ => UsageCount::ZERO,
        }
    }
    fn set_api_key(&mut self, _api_key: Option<String>) {}
    fn set_timestamp(&mut self, _timestamp: TimestampMilliseconds) {}
    fn query_params(&self, percent_encode: bool) -> String {
        self.query_params(true, percent_encode)
    }
}

impl ETHttpRequest for BinanceKlinesRequest {
    type Response = BinanceResponse<BinanceKlinesResponse>;
    type VerificationRequest = Self;

    fn http_endpoint(&self) -> &'static str {
        "klines"
    }
    fn http_method(&self) -> HttpMethod {
        HttpMethod::GET
    }
    fn try_into_http(self, signer: &Signer) -> BpResult<HttpRequest> {
        BinanceRequestFactory::try_into_http(self, signer)
    }
}

impl ETWebsocketRequest for BinanceKlinesRequest {
    type Response = BinanceResponse<BinanceKlinesResponse>;
    type VerificationRequest = Self;

    fn websocket_method_name(&self) -> &'static str {
        "klines"
    }
    fn try_into_websocket(
        self,
        signer: &Signer,
        id: ETWebsocketId,
    ) -> BpResult<(String, WebsocketResponseMatcher)> {
        BinanceRequestFactory::try_into_websocket(self, signer, id)
    }
}
