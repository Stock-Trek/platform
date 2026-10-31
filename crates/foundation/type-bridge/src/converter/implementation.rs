use crate::{
    TBExchangeInfo, asset_ids_to_tickers::AssetIdsToTickers, asset_limits::TBAssetLimits,
    floats_to_decimals::FloatsToDecimals, semantic_checker::SemanticChecker,
    ticker_mapping::TickerMapping, trading_precision::TradingPrecision,
};
use platform_errors::{BpError, BpResult, ValidationError};
use platform_exchange_types::{
    exchange::ETExchange,
    request::{ETHttpRequest, ETWebsocketRequest},
    response::{ETHttpResponse, ETWebsocketResponse},
    ticker::Ticker,
};
use rust_decimal::Decimal;
use stock_trek_types::cex::{
    asset_id::AssetId, capability::CexCapability, order_request::OrderRequest,
    order_response::OrderResponse, preferences::CexPreferences, trading_pair::TradingPair,
};

pub trait Converter {
    type Exchange: ETExchange;
    type AssetLimitsRequest: Sized
        + ETHttpRequest<Exchange = Self::Exchange, Response = Self::AssetLimitsResponse>
        + ETWebsocketRequest<Exchange = Self::Exchange, Response = Self::AssetLimitsResponse>;
    type AssetLimitsResponse: Sized + ETHttpResponse + ETWebsocketResponse;
    type ExchangeInfoRequest: Sized
        + ETHttpRequest<Exchange = Self::Exchange, Response = Self::ExchangeInfoResponse>
        + ETWebsocketRequest<Exchange = Self::Exchange, Response = Self::ExchangeInfoResponse>;
    type ExchangeInfoResponse: Sized + ETHttpResponse + ETWebsocketResponse;
    type OrderRequest: Sized
        + ETHttpRequest<Exchange = Self::Exchange, Response = Self::OrderResponse>
        + ETWebsocketRequest<Exchange = Self::Exchange, Response = Self::OrderResponse>;
    type OrderResponse: Sized + ETHttpResponse + ETWebsocketResponse;

    fn capabilities(&self) -> &Vec<CexCapability>;
    fn ticker_mapping(&self) -> &TickerMapping;
    /// Combine this converter with the trading precisions fetched from the
    /// exchange. Only the resulting [`TradingConverter`] can build order
    /// requests, so an order can never be built without exchange info.
    fn into_trading_converter(self, exchange_info: TBExchangeInfo) -> TradingConverter<Self>
    where
        Self: Sized,
    {
        TradingConverter::new(self, exchange_info)
    }
    fn asset_limits_request(
        &self,
        trading_pair: &TradingPair,
        cex_preferences: &CexPreferences,
    ) -> BpResult<Self::AssetLimitsRequest>;
    fn exchange_info_request(&self) -> BpResult<Self::ExchangeInfoRequest>;
    fn order_request(
        &self,
        order_request: OrderRequest<AssetId, f64>,
        trading_precision: TradingPrecision,
        cex_preferences: &CexPreferences,
    ) -> BpResult<Self::OrderRequest>;
    fn asset_limits_response(&self, response: Self::AssetLimitsResponse)
    -> BpResult<TBAssetLimits>;
    fn exchange_info_response(
        &self,
        response: Self::ExchangeInfoResponse,
    ) -> BpResult<TBExchangeInfo>;
    fn order_response(&self, response: Self::OrderResponse) -> BpResult<OrderResponse>;
    fn try_convert_precise(
        &self,
        order_request: OrderRequest<AssetId, f64>,
        cex_preferences: &CexPreferences,
        trading_precision: TradingPrecision,
    ) -> BpResult<OrderRequest<Ticker, Decimal>> {
        let order_request_with_precision =
            FloatsToDecimals::new(trading_precision, cex_preferences.rounding)
                .floats_to_decimals(order_request)?;
        SemanticChecker.consistent_or_err(&order_request_with_precision, self.capabilities())?;
        AssetIdsToTickers::asset_ids_to_tickers(order_request_with_precision, self.ticker_mapping())
    }
}

/// A converter paired with the trading precisions fetched from its exchange.
///
/// Order requests are deliberately only available on this type. It ties the
/// precision lookup to the exchange info response and removes the need to
/// mutate a converter after it has been created.
#[derive(Debug, Clone)]
pub struct TradingConverter<C> {
    converter: C,
    exchange_info: TBExchangeInfo,
}

impl<C> TradingConverter<C> {
    pub fn new(converter: C, exchange_info: TBExchangeInfo) -> Self {
        Self {
            converter,
            exchange_info,
        }
    }
    pub fn converter(&self) -> &C {
        &self.converter
    }
    pub fn exchange_info(&self) -> &TBExchangeInfo {
        &self.exchange_info
    }
}

impl<C> TradingConverter<C>
where
    C: Converter,
{
    pub fn trading_precision(&self, trading_pair: &TradingPair) -> BpResult<TradingPrecision> {
        self.exchange_info
            .trading_precision(trading_pair)
            .ok_or_else(|| {
                BpError::Validation(ValidationError::MissingValue(format!(
                    "Trading precision for trading pair {trading_pair:?}"
                )))
            })
    }
    pub fn order_request(
        &self,
        order_request: OrderRequest<AssetId, f64>,
        cex_preferences: &CexPreferences,
    ) -> BpResult<C::OrderRequest> {
        let trading_pair = match &order_request {
            OrderRequest::Limit { base, quote, .. }
            | OrderRequest::MarketBuy { base, quote, .. }
            | OrderRequest::MarketSell { base, quote, .. } => TradingPair::new(*base, *quote),
        };
        let trading_precision = self.trading_precision(&trading_pair)?;
        self.converter
            .order_request(order_request, trading_precision, cex_preferences)
    }
}
