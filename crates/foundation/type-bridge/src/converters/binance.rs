use crate::{
    Converter, MinMaxStep, TBExchangeInfo, TickerMapping, TradingPrecision,
    asset_limits::TBAssetLimits,
};
use bimap::BiHashMap;
use platform_errors::{BpError, BpResult, ExchangeError, ValidationError};
use platform_exchange_types::binance::{
    asset_limits::{BinanceAssetLimitsRequest, BinanceAssetLimitsResponse},
    exchange::BinanceExchange,
    exchange_info::{
        BinanceExchangeInfoPermission, BinanceExchangeInfoPermissions, BinanceExchangeInfoRequest,
        BinanceExchangeInfoResponse, BinanceExchangeInfoSymbol, BinanceExchangeInfoSymbolStatus,
    },
    filters::{BinanceAssetFilter, BinanceSymbolFilter},
    recv_window::BinanceRecvWindow,
    response::{BinanceResponse, BinanceResponsePayload},
    spot_order::{BinanceNewOrderResponseType, BinanceSpotOrderRequest, BinanceSpotOrderResponse},
    supporting_types::{
        BinanceOrderStatus, BinanceOrderType, BinanceSelfTradeProtection, BinanceSide,
        BinanceTimeInForce,
    },
};
use platform_types::new_types::DurationMilliseconds;
use std::collections::HashMap;
use std::sync::LazyLock;
use stock_trek_types::cex::{
    activation::Activation, asset_id::AssetId, capability::CexCapability,
    order_request::OrderRequest, order_response::OrderResponse, order_status::OrderStatus,
    preferences::CexPreferences, quantity::Quantity, side::Side, time_in_force::TimeInForce,
    trading_pair::TradingPair, trigger_direction::TriggerDirection,
};

pub struct BinanceConverter {
    capabilities: Vec<CexCapability>,
    ticker_mapping: &'static TickerMapping,
}

impl Default for BinanceConverter {
    fn default() -> Self {
        Self {
            capabilities: vec![],
            ticker_mapping: Self::ticker_mapping_static(),
        }
    }
}

impl Converter for BinanceConverter {
    type Exchange = BinanceExchange;
    type AssetLimitsRequest = BinanceAssetLimitsRequest;
    type AssetLimitsResponse = BinanceResponse<BinanceAssetLimitsResponse>;
    type ExchangeInfoRequest = BinanceExchangeInfoRequest;
    type ExchangeInfoResponse = BinanceResponse<BinanceExchangeInfoResponse>;
    type OrderRequest = BinanceSpotOrderRequest;
    type OrderResponse = BinanceResponse<BinanceSpotOrderResponse>;

    fn capabilities(&self) -> &Vec<CexCapability> {
        &self.capabilities
    }
    fn ticker_mapping(&self) -> &TickerMapping {
        self.ticker_mapping
    }
    fn asset_limits_request(
        &self,
        trading_pair: &TradingPair,
        cex_preferences: &CexPreferences,
    ) -> BpResult<Self::AssetLimitsRequest> {
        let symbol = match self.ticker_mapping().symbol(trading_pair) {
            None => {
                return Err(BpError::Validation(ValidationError::MissingValue(format!(
                    "Symbol for trading pair: {:?}",
                    trading_pair
                ))));
            }
            Some(symbol) => symbol,
        };
        let mut request = BinanceAssetLimitsRequest::new(symbol);
        request.recvWindow = BinanceRecvWindow::try_new(DurationMilliseconds(
            cex_preferences.max_network_delay_millis as i64,
        ))
        .ok();
        Ok(request)
    }
    fn exchange_info_request(&self) -> BpResult<Self::ExchangeInfoRequest> {
        Ok(BinanceExchangeInfoRequest::Permissions {
            permissions: BinanceExchangeInfoPermissions::List(vec![
                BinanceExchangeInfoPermission::SPOT,
            ]),
            symbolStatus: Some(BinanceExchangeInfoSymbolStatus::TRADING),
        })
    }
    fn order_request(
        &self,
        order_request: OrderRequest<AssetId, f64>,
        trading_precision: TradingPrecision,
        cex_preferences: &CexPreferences,
    ) -> BpResult<Self::OrderRequest> {
        let precise =
            self.try_convert_precise(order_request, cex_preferences, trading_precision)?;
        let (base, quote, side, activation, quantity, quote_order_qty, price, time_in_force, tag) =
            match precise {
                OrderRequest::Limit {
                    base,
                    quote,
                    side,
                    activation,
                    limit_price,
                    time_in_force,
                    quantity,
                    tag,
                } => {
                    let (quantity, quote_order_qty) = match quantity {
                        Quantity::OfBase(quantity) => (Some(quantity), None),
                        Quantity::OfQuote(quantity) => (None, Some(quantity)),
                    };
                    (
                        base,
                        quote,
                        side,
                        activation,
                        quantity,
                        quote_order_qty,
                        Some(limit_price),
                        Some(time_in_force),
                        tag,
                    )
                }
                OrderRequest::MarketBuy {
                    base,
                    quote,
                    activation,
                    quote_quantity,
                    tag,
                } => (
                    base,
                    quote,
                    Side::Buy,
                    activation,
                    None,
                    Some(quote_quantity),
                    None,
                    None,
                    tag,
                ),
                OrderRequest::MarketSell {
                    base,
                    quote,
                    activation,
                    base_quantity,
                    tag,
                } => (
                    base,
                    quote,
                    Side::Sell,
                    activation,
                    Some(base_quantity),
                    None,
                    None,
                    None,
                    tag,
                ),
            };

        let side = match side {
            Side::Buy => BinanceSide::BUY,
            Side::Sell => BinanceSide::SELL,
        };
        let stop_price = match activation {
            Activation::PriceTriggered {
                activation_price, ..
            }
            | Activation::Trailing {
                activation_price, ..
            } => Some(activation_price),
            Activation::Immediate => None,
        };
        let is_limit = price.is_some();
        let r#type = match activation {
            Activation::PriceTriggered { direction, .. }
            | Activation::Trailing { direction, .. } => match (direction, is_limit) {
                (TriggerDirection::Above, true) => BinanceOrderType::TAKE_PROFIT_LIMIT,
                (TriggerDirection::Above, false) => BinanceOrderType::TAKE_PROFIT,
                (TriggerDirection::Below, true) => BinanceOrderType::STOP_LOSS_LIMIT,
                (TriggerDirection::Below, false) => BinanceOrderType::STOP_LOSS,
            },
            Activation::Immediate => {
                if is_limit {
                    BinanceOrderType::LIMIT
                } else {
                    BinanceOrderType::MARKET
                }
            }
        };
        let time_in_force = match time_in_force {
            None => None,
            Some(time_in_force) => Some(match time_in_force {
                TimeInForce::FillOrKill => BinanceTimeInForce::FOK,
                TimeInForce::GoodTillCancelled => BinanceTimeInForce::GTC,
                TimeInForce::ImmediateOrCancel => BinanceTimeInForce::IOC,
                other => {
                    return Err(BpError::Validation(ValidationError::Unsupported {
                        name: "time_in_force".into(),
                        value: other.to_string(),
                    }));
                }
            }),
        };
        let trailing_delta = match activation {
            Activation::Trailing {
                callback_rate_bps, ..
            } => Some(i64::from(callback_rate_bps)),
            _ => None,
        };
        let recv_window = BinanceRecvWindow::try_new(DurationMilliseconds(i64::from(
            cex_preferences.max_network_delay_millis,
        )))
        .ok();

        let mut request = BinanceSpotOrderRequest::new(
            format!("{}{}", base, quote),
            side,
            r#type,
            quantity.unwrap_or_default(),
        );
        request.newClientOrderId = Some(tag.0);
        request.newOrderRespType = Some(BinanceNewOrderResponseType::FULL);
        request.price = price;
        request.quantity = quantity;
        request.quoteOrderQty = quote_order_qty;
        request.recvWindow = recv_window;
        request.selfTradePreventionMode = Some(BinanceSelfTradeProtection::NONE);
        request.stopPrice = stop_price;
        request.timeInForce = time_in_force;
        request.trailingDelta = trailing_delta;
        Ok(request)
    }
    fn asset_limits_response(
        &self,
        response: BinanceResponse<BinanceAssetLimitsResponse>,
    ) -> BpResult<TBAssetLimits> {
        let payload = Self::success_payload(response)?;
        let ticker_mapping = self.ticker_mapping();
        let mut asset_limits = HashMap::new();
        for asset_filter in payload.assetFilters {
            match asset_filter {
                BinanceAssetFilter::MAX_ASSET { asset, limit } => {
                    if let Some(asset_id) = ticker_mapping.asset_id(&asset) {
                        asset_limits.insert(*asset_id, limit);
                    }
                }
                unknown => {
                    tracing::warn!(
                        ?unknown,
                        "ignoring unsupported asset filter; asset limits may be incomplete"
                    );
                }
            }
        }
        Ok(TBAssetLimits { asset_limits })
    }
    fn exchange_info_response(
        &self,
        response: Self::ExchangeInfoResponse,
    ) -> BpResult<TBExchangeInfo> {
        let payload = Self::success_payload(response)?;
        let asset_tickers = self.ticker_mapping();
        let mut trading_precisions = HashMap::new();
        for symbol in payload.symbols {
            let BinanceExchangeInfoSymbol {
                baseAsset,
                filters,
                quoteAsset,
                ..
            } = symbol;
            let base_option = asset_tickers.asset_id(&baseAsset);
            let quote_option = asset_tickers.asset_id(&quoteAsset);
            if let Some(base) = base_option
                && let Some(quote) = quote_option
            {
                let trading_pair = TradingPair::new(*base, *quote);
                let base_limit = Self::binance_base_limit(&filters)?;
                let base_market = Self::binance_base_market(&filters, base_limit);
                let quote = Self::binance_quote(&filters)?;
                let trading_precision = TradingPrecision::new(base_limit, base_market, quote);
                trading_precisions.insert(trading_pair, trading_precision);
            }
        }
        Ok(TBExchangeInfo { trading_precisions })
    }
    fn order_response(&self, response: Self::OrderResponse) -> BpResult<OrderResponse> {
        let payload = Self::success_payload(response)?;
        let BinanceSpotOrderResponse {
            clientOrderId,
            cummulativeQuoteQty,
            executedQty,
            orderId,
            status,
            ..
        } = payload;
        let status = match status {
            Some(BinanceOrderStatus::CANCELED) => OrderStatus::Canceled,
            Some(BinanceOrderStatus::EXPIRED) => OrderStatus::Canceled,
            Some(BinanceOrderStatus::EXPIRED_IN_MATCH) => OrderStatus::Expired,
            Some(BinanceOrderStatus::FILLED) => OrderStatus::Filled,
            Some(BinanceOrderStatus::NEW) => OrderStatus::New,
            Some(BinanceOrderStatus::PARTIALLY_FILLED) => OrderStatus::PartiallyFilled,
            Some(BinanceOrderStatus::PENDING_CANCEL) => OrderStatus::PendingCancel,
            Some(BinanceOrderStatus::PENDING_NEW) => OrderStatus::New,
            Some(BinanceOrderStatus::REJECTED) => OrderStatus::Rejected,
            Some(other) => {
                return Err(BpError::Validation(ValidationError::Unsupported {
                    name: "status".into(),
                    value: other.to_string(),
                }));
            }
            None => {
                return Err(BpError::Validation(ValidationError::MissingValue(
                    "status".into(),
                )));
            }
        };
        Ok(OrderResponse {
            client_order_id: clientOrderId,
            filled_base_quantity: executedQty.unwrap_or_default(),
            filled_quote_quantity: cummulativeQuoteQty.unwrap_or_default(),
            order_id: orderId.to_string(),
            status,
        })
    }
}

impl BinanceConverter {
    fn success_payload<R>(response: BinanceResponse<R>) -> BpResult<R> {
        match response.payload {
            BinanceResponsePayload::Success(payload) => Ok(payload),
            BinanceResponsePayload::Failure(error) => Err(BpError::Exchange(
                ExchangeError::BadResponse(error.to_string()),
            )),
        }
    }

    /// Returns the shared Binance ticker mapping, building it only once.
    pub fn ticker_mapping_static() -> &'static TickerMapping {
        static TICKER_MAPPING: LazyLock<TickerMapping> =
            LazyLock::new(BinanceConverter::build_ticker_mapping);
        &TICKER_MAPPING
    }

    fn build_ticker_mapping() -> TickerMapping {
        let mut map = BiHashMap::new();
        map.insert(AssetId::Aave, "AAVE".into());
        map.insert(AssetId::Arbitrum, "ARB".into());
        map.insert(AssetId::Atom, "ATOM".into());
        map.insert(AssetId::Avalanche, "AVAX".into());
        map.insert(AssetId::BNB, "BNB".into());
        map.insert(AssetId::Bitcoin, "BTC".into());
        map.insert(AssetId::BitcoinCash, "BCH".into());
        map.insert(AssetId::Celo, "CELO".into());
        map.insert(AssetId::Chainlink, "LINK".into());
        map.insert(AssetId::Cronos, "CRO".into());
        map.insert(AssetId::Dai, "DAI".into());
        map.insert(AssetId::Dogecoin, "DOGE".into());
        map.insert(AssetId::Ethereum, "ETH".into());
        map.insert(AssetId::Fantom, "FTM".into());
        map.insert(AssetId::Gnosis, "GNO".into());
        map.insert(AssetId::Litecoin, "LTC".into());
        map.insert(AssetId::Moonbeam, "GLMR".into());
        map.insert(AssetId::NEAR, "NEAR".into());
        map.insert(AssetId::Optimism, "OP".into());
        map.insert(AssetId::Osmosis, "OSMO".into());
        map.insert(AssetId::Polygon, "POL".into());
        map.insert(AssetId::Solana, "SOL".into());
        map.insert(AssetId::TRON, "TRX".into());
        map.insert(AssetId::TetherUSD, "USDT".into());
        map.insert(AssetId::USDCoin, "USDC".into());
        map.insert(AssetId::Uniswap, "UNI".into());
        map.insert(AssetId::WrappedBitcoin, "WBTC".into());
        map.insert(AssetId::WrappedEthereum, "WETH".into());
        TickerMapping::new(map, None)
    }
    fn binance_base_limit(filters: &Vec<BinanceSymbolFilter>) -> BpResult<MinMaxStep> {
        for filter in filters {
            if let BinanceSymbolFilter::LOT_SIZE {
                minQty,
                maxQty,
                stepSize,
            } = filter
            {
                return Ok(MinMaxStep {
                    min: *minQty,
                    max: *maxQty,
                    step: *stepSize,
                });
            }
        }
        Err(BpError::Validation(ValidationError::MissingValue(
            "LOT_SIZE filter in symbol filters".into(),
        )))
    }
    fn binance_base_market(
        filters: &Vec<BinanceSymbolFilter>,
        base_limit: MinMaxStep,
    ) -> MinMaxStep {
        let MinMaxStep { min, max, step } = base_limit;
        for filter in filters {
            if let BinanceSymbolFilter::MARKET_LOT_SIZE {
                minQty,
                maxQty,
                stepSize,
            } = filter
            {
                return MinMaxStep {
                    min: (*minQty).max(min),
                    max: (*maxQty).min(max),
                    step: (*stepSize).max(step),
                };
            }
        }
        base_limit
    }
    fn binance_quote(filters: &Vec<BinanceSymbolFilter>) -> BpResult<MinMaxStep> {
        for filter in filters {
            if let BinanceSymbolFilter::PRICE_FILTER {
                maxPrice,
                minPrice,
                tickSize,
            } = filter
            {
                return Ok(MinMaxStep {
                    min: *minPrice,
                    max: *maxPrice,
                    step: *tickSize,
                });
            }
        }
        Err(BpError::Validation(ValidationError::MissingValue(
            "PRICE_FILTER in symbol filters".into(),
        )))
    }
}
