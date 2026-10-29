use crate::{
    binance::rate_limits::{BinanceRateLimit, BinanceRateLimitInterval, BinanceRateLimitType},
    http::HttpResponse,
    rate_limited::{RateLimit, RateLimitRestriction, RateUsage},
    response::{ETHttpResponse, ETResponse, ETWebsocketResponse},
    websocket_id::ETWebsocketId,
};
use platform_errors::{BinanceError, BpError, BpResult, ExchangeError};
use platform_types::new_types::{DurationNanoseconds, DurationSeconds, UsageCount};
use serde::{Deserialize, de::DeserializeOwned};
use std::{collections::HashMap, fmt::Debug};

#[derive(Debug, Clone, Deserialize)]
pub struct BinanceResponse<R> {
    pub metadata: BinanceMetadata,
    pub payload: BinanceResponsePayload<R>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct BinanceMetadata {
    pub retry_after: Option<DurationSeconds>,
    pub status: u16,
    pub usage: HashMap<RateLimit, RateUsage>,
    pub websocket_id: Option<ETWebsocketId>,
}

#[derive(Debug, Clone, Deserialize)]
pub enum BinanceResponsePayload<R> {
    Success(R),
    Failure(BinanceError),
}

#[allow(non_snake_case)]
#[derive(Deserialize, Debug, Clone)]
struct BinanceWebsocketResponse {
    pub error: Option<BinanceError>,
    pub id: Option<ETWebsocketId>,
    #[serde(default)]
    pub rateLimits: Vec<BinanceRateLimit>,
    pub result: Option<serde_json::Value>,
    pub status: u16,
}

impl<R> BinanceResponse<R>
where
    R: DeserializeOwned,
{
    fn parse_usage_from_headers(headers: &Vec<(String, String)>) -> HashMap<RateLimit, RateUsage> {
        let mut usage = HashMap::new();
        for (name, value) in headers {
            let name = name.to_ascii_lowercase();
            let restriction = if name.starts_with("x-mbx-order-count-") {
                Some(RateLimitRestriction::OrderCount)
            } else if name.starts_with("x-mbx-used-weight-") {
                Some(RateLimitRestriction::Weight)
            } else {
                None
            };
            if let Some(restriction) = restriction {
                let Some((_, last_part)) = name.rsplit_once('-') else {
                    continue;
                };
                // Suffixes are `<count><unit>`, e.g. `1m` or `10s`. They come
                // from server-controlled headers, so malformed values must be
                // ignored rather than panicking.
                let Some(letter) = last_part.chars().next_back() else {
                    continue;
                };
                let num_str = &last_part[..last_part.len() - letter.len_utf8()];
                let num = num_str.parse::<u64>().ok();
                let interval_nanos = BinanceRateLimitInterval::try_from(letter)
                    .ok()
                    .and_then(|i| DurationNanoseconds::try_from(i).ok());
                if let Some(num) = num
                    && let Some(interval_nanos) = interval_nanos
                    && let Some(total_nanos) = i64::try_from(num)
                        .ok()
                        .and_then(|num| num.checked_mul(interval_nanos.0))
                    && let Ok(used) = value.parse::<u64>()
                {
                    usage.insert(
                        RateLimit {
                            restriction,
                            interval_nanos: DurationNanoseconds(total_nanos),
                        },
                        RateUsage {
                            limit: None,
                            used: Some(UsageCount(used)),
                        },
                    );
                }
            }
        }
        usage
    }
    fn parse_usage(rate_limits: &Vec<BinanceRateLimit>) -> HashMap<RateLimit, RateUsage> {
        let mut usage = HashMap::new();
        for rate_limit in rate_limits {
            let nanoseconds: DurationNanoseconds = match rate_limit.interval.try_into() {
                Err(_) => continue,
                Ok(nanos) => nanos,
            };
            let interval_nanos = nanoseconds * i64::from(rate_limit.intervalNum);
            let restriction = match rate_limit.rateLimitType {
                BinanceRateLimitType::CONNECTIONS => RateLimitRestriction::Connection,
                BinanceRateLimitType::ORDERS => RateLimitRestriction::OrderCount,
                BinanceRateLimitType::RAW_REQUESTS => RateLimitRestriction::RawRequests,
                BinanceRateLimitType::REQUEST_WEIGHT => RateLimitRestriction::Weight,
                BinanceRateLimitType::Unknown => continue,
            };
            let limit = Some(UsageCount(
                u64::try_from(rate_limit.limit).unwrap_or_default(),
            ));
            let used = rate_limit
                .count
                .map(|c| UsageCount(u64::try_from(c).unwrap_or_default()));
            usage.insert(
                RateLimit {
                    restriction,
                    interval_nanos,
                },
                RateUsage { limit, used },
            );
        }
        usage
    }
    fn failure_from_unexpected_body(
        status: u16,
        body: &[u8],
        error: serde_json::Error,
    ) -> BinanceResponsePayload<R> {
        if (200..300).contains(&status) {
            BinanceResponsePayload::Failure(BinanceError {
                code: -1,
                msg: format!("Could not deserialize response body as the expected type: {error}"),
                data: None,
            })
        } else {
            BinanceResponsePayload::Failure(BinanceError {
                code: i64::from(status),
                msg: String::from_utf8_lossy(body).into_owned(),
                data: None,
            })
        }
    }
}

impl<R> ETResponse for BinanceResponse<R>
where
    R: Clone + Debug,
{
    fn retry_after(&self) -> Option<DurationSeconds> {
        self.metadata.retry_after
    }
    fn rate_limit_usage(&self) -> Option<&HashMap<RateLimit, RateUsage>> {
        Some(&self.metadata.usage)
    }
}

impl<R> ETHttpResponse for BinanceResponse<R>
where
    R: Clone + Debug + DeserializeOwned,
{
    fn try_from_http(response: HttpResponse) -> BpResult<Self> {
        let mut retry_after: Option<DurationSeconds> = match response
            .headers
            .iter()
            .find(|tuple| tuple.0.eq_ignore_ascii_case("Retry-After"))
        {
            None => None,
            Some(tuple) => match tuple.1.parse::<u64>() {
                Err(_) => {
                    return Err(BpError::Exchange(ExchangeError::ParseError(
                        "Retry-After".into(),
                    )));
                }
                Ok(parsed) => Some(DurationSeconds(i64::try_from(parsed).unwrap_or(i64::MAX))),
            },
        };
        let mut usage = Self::parse_usage_from_headers(&response.headers);
        let payload = match serde_json::from_slice::<serde_json::Value>(&response.body) {
            Ok(response_value) => {
                if let Some(rate_limits_value) = response_value.get("rateLimits")
                    && let Ok(rate_limits) =
                        serde_json::from_value::<Vec<BinanceRateLimit>>(rate_limits_value.clone())
                {
                    for (rate_limit, rate_usage) in Self::parse_usage(&rate_limits) {
                        usage.insert(rate_limit, rate_usage);
                    }
                }
                match BinanceError::deserialize(&response_value) {
                    Ok(error) => {
                        if retry_after.is_none() {
                            retry_after = error.retry_after();
                        }
                        BinanceResponsePayload::Failure(error)
                    }
                    Err(_) => match R::deserialize(&response_value) {
                        Ok(result) => BinanceResponsePayload::Success(result),
                        Err(error) => Self::failure_from_unexpected_body(
                            response.status,
                            &response.body,
                            error,
                        ),
                    },
                }
            }
            Err(error) => {
                Self::failure_from_unexpected_body(response.status, &response.body, error)
            }
        };
        let metadata = BinanceMetadata {
            usage,
            retry_after,
            websocket_id: None,
            status: response.status,
        };
        Ok(BinanceResponse { metadata, payload })
    }
}

impl<R> ETWebsocketResponse for BinanceResponse<R>
where
    R: Clone + Debug + DeserializeOwned,
{
    fn try_from_websocket(response: serde_json::Value) -> BpResult<Self> {
        let websocket_response: BinanceWebsocketResponse = serde_json::from_value(response)
            .map_err(|error| BpError::Exchange(ExchangeError::DeserializeResponse(error)))?;
        let usage = Self::parse_usage(&websocket_response.rateLimits);
        let retry_after = websocket_response
            .error
            .as_ref()
            .and_then(BinanceError::retry_after);
        let metadata = BinanceMetadata {
            usage,
            retry_after,
            websocket_id: websocket_response.id,
            status: websocket_response.status,
        };
        let payload = if let Some(error) = websocket_response.error {
            BinanceResponsePayload::Failure(error)
        } else if let Some(result) = websocket_response.result {
            match serde_json::from_value::<R>(result) {
                Ok(result) => BinanceResponsePayload::Success(result),
                // The frame parsed but its success payload is not of the
                // expected result type. Degrade to a failure instead of
                // failing the entire frame parse.
                Err(error) => BinanceResponsePayload::Failure(BinanceError {
                    code: -1,
                    msg: format!("Could not deserialize websocket result: {error}"),
                    data: None,
                }),
            }
        } else {
            // No error and no result, treat as a generic failure
            BinanceResponsePayload::Failure(BinanceError {
                code: -1,
                msg: "Websocket response missing both error and result".to_string(),
                data: None,
            })
        };
        Ok(BinanceResponse { metadata, payload })
    }
}
