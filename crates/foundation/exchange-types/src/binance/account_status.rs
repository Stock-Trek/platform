use crate::{
    binance::{
        exchange::BinanceExchange, exchange_info::BinanceExchangeInfoPermission,
        recv_window::BinanceRecvWindow, request::BinanceRequestFactory, response::BinanceResponse,
    },
    http::{HttpMethod, HttpRequest},
    new_types::{TimestampMilliseconds, UsageCount},
    rate_limited::RateLimitRestriction,
    request::{ETHttpRequest, ETRequest, ETWebsocketRequest, WebsocketResponseMatcher},
    signer::Signer,
    websocket_id::ETWebsocketId,
};
use platform_errors::BpResult;
use query_params::QueryParams;
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use serde_with::skip_serializing_none;

#[allow(non_snake_case)]
#[skip_serializing_none]
#[derive(Serialize, Debug, Clone, Hash, QueryParams)]
pub struct BinanceAccountStatusRequest {
    apiKey: Option<String>,
    timestamp: i64,
    pub omitZeroBalances: Option<bool>,
    pub recvWindow: Option<BinanceRecvWindow>,
}

impl BinanceAccountStatusRequest {
    pub fn new() -> Self {
        Self {
            apiKey: None,
            timestamp: 0,
            omitZeroBalances: None,
            recvWindow: None,
        }
    }
}

impl Default for BinanceAccountStatusRequest {
    fn default() -> Self {
        Self::new()
    }
}

#[allow(non_snake_case)]
#[derive(Deserialize, Debug, Clone)]
pub struct BinanceAccountStatusResponse {
    pub accountType: String,
    pub balances: Vec<BinanceAccountBalance>,
    pub brokered: Option<bool>,
    pub buyerCommission: i64,
    pub canDeposit: bool,
    pub canTrade: bool,
    pub canWithdraw: bool,
    pub commissionRates: BinanceAccountCommissionRates,
    pub makerCommission: i64,
    pub permissions: Vec<BinanceExchangeInfoPermission>,
    pub preventSor: Option<bool>,
    pub requireSelfTradePrevention: Option<bool>,
    pub sellerCommission: i64,
    pub takerCommission: i64,
    pub uid: Option<i64>,
    pub updateTime: i64,
}

#[allow(non_snake_case)]
#[derive(Deserialize, Debug, Clone)]
pub struct BinanceAccountBalance {
    pub asset: String,
    pub free: Decimal,
    pub locked: Decimal,
}

#[allow(non_snake_case)]
#[derive(Deserialize, Debug, Clone)]
pub struct BinanceAccountCommissionRates {
    pub buyer: Decimal,
    pub maker: Decimal,
    pub seller: Decimal,
    pub taker: Decimal,
}

impl ETRequest for BinanceAccountStatusRequest {
    type Exchange = BinanceExchange;

    fn is_signed(&self) -> bool {
        true
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
        self.apiKey = api_key;
    }
    fn set_timestamp(&mut self, timestamp: TimestampMilliseconds) {
        self.timestamp = timestamp.0;
    }
    fn query_params(&self, percent_encode: bool) -> String {
        self.query_params(true, percent_encode)
    }
}

impl ETHttpRequest for BinanceAccountStatusRequest {
    type Response = BinanceResponse<BinanceAccountStatusResponse>;
    type VerificationRequest = Self;

    fn http_endpoint(&self) -> &'static str {
        "account"
    }
    fn http_method(&self) -> HttpMethod {
        HttpMethod::GET
    }
    fn try_into_http(self, signer: &Signer) -> BpResult<HttpRequest> {
        BinanceRequestFactory::try_into_http(self, signer)
    }
}

impl ETWebsocketRequest for BinanceAccountStatusRequest {
    type Response = BinanceResponse<BinanceAccountStatusResponse>;
    type VerificationRequest = Self;

    fn websocket_method_name(&self) -> &'static str {
        "account.status"
    }
    fn try_into_websocket(
        self,
        signer: &Signer,
        id: ETWebsocketId,
    ) -> BpResult<(String, WebsocketResponseMatcher)> {
        BinanceRequestFactory::try_into_websocket(self, signer, id)
    }
}
