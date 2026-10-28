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
    String,
    u32,
    String,
    String,
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

impl TryFrom<&str> for BinanceKlinesInterval {
    type Error = BpError;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        match value.trim() {
            "1s" => Ok(Self::ONE_SECOND),
            "1m" => Ok(Self::ONE_MINUTE),
            "3m" => Ok(Self::THREE_MINUTE),
            "5m" => Ok(Self::FIVE_MINUTE),
            "15m" => Ok(Self::FIFTEEN_MINUTE),
            "30m" => Ok(Self::THIRTY_MINUTE),
            "1h" => Ok(Self::ONE_HOUR),
            "2h" => Ok(Self::TWO_HOUR),
            "4h" => Ok(Self::FOUR_HOUR),
            "6h" => Ok(Self::SIX_HOUR),
            "8h" => Ok(Self::EIGHT_HOUR),
            "12h" => Ok(Self::TWELVE_HOUR),
            "1d" => Ok(Self::ONE_DAY),
            "3d" => Ok(Self::THREE_DAY),
            "1w" => Ok(Self::ONE_WEEK),
            "1M" => Ok(Self::ONE_MONTH),
            other => Err(BpError::Exchange(ExchangeError::BadValue {
                name: "interval".to_string(),
                value: other.to_string(),
                reason: "unsupported kline interval".to_string(),
            })),
        }
    }
}

impl BinanceKlinesInterval {
    pub fn millis(&self) -> i64 {
        match self {
            Self::ONE_SECOND => 1_000,
            Self::ONE_MINUTE => 60_000,
            Self::THREE_MINUTE => 180_000,
            Self::FIVE_MINUTE => 300_000,
            Self::FIFTEEN_MINUTE => 900_000,
            Self::THIRTY_MINUTE => 1_800_000,
            Self::ONE_HOUR => 3_600_000,
            Self::TWO_HOUR => 7_200_000,
            Self::FOUR_HOUR => 14_400_000,
            Self::SIX_HOUR => 21_600_000,
            Self::EIGHT_HOUR => 28_800_000,
            Self::TWELVE_HOUR => 43_200_000,
            Self::ONE_DAY => 86_400_000,
            Self::THREE_DAY => 259_200_000,
            Self::ONE_WEEK => 604_800_000,
            Self::ONE_MONTH => 2_592_000_000,
        }
    }
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
    pub fn open_time(&self) -> TimestampMilliseconds {
        self.0
    }

    pub fn close_time(&self) -> TimestampMilliseconds {
        self.6
    }

    pub fn open(&self) -> &str {
        &self.1
    }

    pub fn high(&self) -> &str {
        &self.2
    }

    pub fn low(&self) -> &str {
        &self.3
    }

    pub fn close(&self) -> &str {
        &self.4
    }

    pub fn volume(&self) -> &str {
        &self.5
    }

    pub fn trades(&self) -> u32 {
        self.8
    }

    pub fn quote_volume(&self) -> &str {
        &self.7
    }

    pub fn taker_buy_base_volume(&self) -> &str {
        &self.9
    }

    pub fn taker_buy_quote_volume(&self) -> &str {
        &self.10
    }

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
