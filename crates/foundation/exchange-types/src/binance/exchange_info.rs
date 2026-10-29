use crate::{
    binance::{
        exchange::BinanceExchange,
        filters::{BinanceExchangeFilter, BinanceSymbolFilter},
        rate_limits::BinanceRateLimit,
        request::BinanceRequestFactory,
        response::BinanceResponse,
        supporting_types::{BinanceOrderType, BinanceSelfTradeProtection},
    },
    encode::ByteEncoder,
    http::{HttpMethod, HttpRequest},
    rate_limited::RateLimitRestriction,
    request::{ETHttpRequest, ETRequest, ETWebsocketRequest, WebsocketResponseMatcher},
    signer::Signer,
    ticker::Ticker,
    websocket_id::ETWebsocketId,
};
use platform_errors::BpResult;
use platform_types::new_types::{TimestampMilliseconds, UsageCount};
use serde::{Deserialize, Serialize};
use strum::Display;

#[allow(non_snake_case)]
#[derive(Serialize, Debug, Clone, Hash)]
#[serde(untagged)]
pub enum BinanceExchangeInfoRequest {
    All {
        #[serde(skip_serializing_if = "Option::is_none")]
        symbolStatus: Option<BinanceExchangeInfoSymbolStatus>,
    },
    Symbol {
        symbol: String,
    },
    Symbols {
        symbols: Vec<String>,
    },
    Permissions {
        permissions: BinanceExchangeInfoPermissions,
        #[serde(skip_serializing_if = "Option::is_none")]
        symbolStatus: Option<BinanceExchangeInfoSymbolStatus>,
    },
}

impl Default for BinanceExchangeInfoRequest {
    fn default() -> Self {
        Self::All { symbolStatus: None }
    }
}

#[derive(Serialize, Debug, Clone, Hash)]
#[serde(untagged)]
pub enum BinanceExchangeInfoPermissions {
    Single(BinanceExchangeInfoPermission),
    List(Vec<BinanceExchangeInfoPermission>),
}

#[derive(Serialize, Deserialize, Debug, Display, Clone, Copy, PartialEq, Eq, Hash)]
#[allow(non_camel_case_types)]
#[non_exhaustive]
pub enum BinanceExchangeInfoPermission {
    LEVERAGED,
    MARGIN,
    SPOT,
    #[serde(other)]
    Unknown,
}

#[derive(Serialize, Debug, Display, Clone, Copy, PartialEq, Eq, Hash)]
pub enum BinanceExchangeInfoSymbolStatus {
    TRADING,
    HALT,
    BREAK,
}

#[allow(non_snake_case)]
#[derive(Deserialize, Debug, Clone)]
pub struct BinanceExchangeInfoResponse {
    pub exchangeFilters: Vec<BinanceExchangeFilter>,
    pub rateLimits: Vec<BinanceRateLimit>,
    pub serverTime: i64,
    pub sors: Option<Vec<BinanceExchangeInfoSors>>,
    pub symbols: Vec<BinanceExchangeInfoSymbol>,
    pub timezone: String,
}

#[allow(non_snake_case)]
#[derive(Deserialize, Debug, Clone)]
pub struct BinanceExchangeInfoSors {
    pub baseAsset: Ticker,
    pub symbols: Vec<Ticker>,
}

#[allow(non_snake_case)]
#[derive(Deserialize, Debug, Clone)]
pub struct BinanceExchangeInfoSymbol {
    pub allowTrailingStop: bool,
    pub allowedSelfTradePreventionModes: Vec<BinanceSelfTradeProtection>,
    pub amendAllowed: bool,
    pub baseAsset: Ticker,
    pub baseAssetPrecision: u8,
    pub baseCommissionPrecision: u8,
    pub cancelReplaceAllowed: bool,
    pub defaultSelfTradePreventionMode: BinanceSelfTradeProtection,
    pub filters: Vec<BinanceSymbolFilter>,
    pub icebergAllowed: bool,
    pub isMarginTradingAllowed: bool,
    pub isSpotTradingAllowed: bool,
    pub ocoAllowed: bool,
    pub opoAllowed: bool,
    pub orderTypes: Vec<BinanceOrderType>,
    pub otoAllowed: bool,
    pub pegInstructionsAllowed: bool,
    pub permissionSets: Vec<Vec<BinanceExchangeInfoPermission>>,
    pub permissions: Vec<BinanceExchangeInfoPermission>,
    pub quoteAsset: Ticker,
    pub quoteAssetPrecision: u8,
    pub quoteCommissionPrecision: u8,
    pub quoteOrderQtyMarketAllowed: bool,
    pub quotePrecision: u8,
    pub status: String,
    pub symbol: String,
}

impl BinanceExchangeInfoRequest {
    pub fn query_params(&self, _percent_encode: bool) -> String {
        let mut pairs = Vec::new();
        match self {
            Self::All { symbolStatus } => {
                if let Some(symbol_status) = symbolStatus {
                    pairs.push(format!("symbolStatus={symbol_status}"));
                }
            }
            Self::Symbol { symbol } => pairs.push(format!("symbol={symbol}")),
            Self::Symbols { symbols } => pairs.push(format!(
                "symbols={}",
                ByteEncoder::Percent.encode(Self::json_array(symbols).as_bytes())
            )),
            Self::Permissions {
                permissions,
                symbolStatus,
            } => {
                match permissions {
                    BinanceExchangeInfoPermissions::Single(permission) => {
                        pairs.push(format!("permissions={permission}"));
                    }
                    BinanceExchangeInfoPermissions::List(permissions) => pairs.push(format!(
                        "permissions={}",
                        ByteEncoder::Percent.encode(Self::json_array(permissions).as_bytes())
                    )),
                }
                if let Some(symbol_status) = symbolStatus {
                    pairs.push(format!("symbolStatus={symbol_status}"));
                }
            }
        }
        pairs.join("&")
    }
    fn json_array(values: &[impl std::fmt::Display]) -> String {
        let values = values
            .iter()
            .map(|value| format!("\"{value}\""))
            .collect::<Vec<_>>()
            .join(",");
        format!("[{values}]")
    }
}

impl ETRequest for BinanceExchangeInfoRequest {
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
    fn set_api_key(&mut self, _api_key: Option<String>) {}
    fn set_timestamp(&mut self, _timestamp: TimestampMilliseconds) {}
    fn query_params(&self, percent_encode: bool) -> String {
        self.query_params(percent_encode)
    }
}

impl ETHttpRequest for BinanceExchangeInfoRequest {
    type Response = BinanceResponse<BinanceExchangeInfoResponse>;
    type VerificationRequest = Self;

    fn http_endpoint(&self) -> &'static str {
        "exchangeInfo"
    }
    fn http_method(&self) -> HttpMethod {
        HttpMethod::GET
    }
    fn try_into_http(self, signer: &Signer) -> BpResult<HttpRequest> {
        BinanceRequestFactory::try_into_http(self, signer)
    }
}

impl ETWebsocketRequest for BinanceExchangeInfoRequest {
    type Response = BinanceResponse<BinanceExchangeInfoResponse>;
    type VerificationRequest = Self;

    fn websocket_method_name(&self) -> &'static str {
        "exchangeInfo"
    }
    fn try_into_websocket(
        self,
        signer: &Signer,
        id: ETWebsocketId,
    ) -> BpResult<(String, WebsocketResponseMatcher)> {
        BinanceRequestFactory::try_into_websocket(self, signer, id)
    }
}
