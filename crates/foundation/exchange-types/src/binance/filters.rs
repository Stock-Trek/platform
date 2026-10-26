use crate::ticker::Ticker;
use rust_decimal::Decimal;
use serde::Deserialize;

#[derive(Debug, Clone, Deserialize)]
#[serde(tag = "filterType")]
#[allow(non_snake_case, non_camel_case_types)]
#[non_exhaustive]
pub enum BinanceExchangeFilter {
    EXCHANGE_MAX_NUM_ORDERS {
        maxNumOrders: i64,
    },
    EXCHANGE_MAX_NUM_ALGO_ORDERS {
        maxNumAlgoOrders: i64,
    },
    EXCHANGE_MAX_NUM_ICEBERG_ORDERS {
        maxNumIcebergOrders: i64,
    },
    EXCHANGE_MAX_NUM_ORDER_LISTS {
        maxNumOrderLists: i64,
    },
    #[serde(other)]
    Unknown,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(tag = "filterType")]
#[allow(non_snake_case, non_camel_case_types)]
#[non_exhaustive]
pub enum BinanceSymbolFilter {
    PRICE_FILTER {
        minPrice: Decimal,
        maxPrice: Decimal,
        tickSize: Decimal,
    },
    PERCENT_PRICE {
        multiplierUp: Decimal,
        multiplierDown: Decimal,
        avgPriceMins: i64,
    },
    PERCENT_PRICE_BY_SIDE {
        bidMultiplierUp: Decimal,
        bidMultiplierDown: Decimal,
        askMultiplierUp: Decimal,
        askMultiplierDown: Decimal,
        avgPriceMins: i64,
    },
    LOT_SIZE {
        minQty: Decimal,
        maxQty: Decimal,
        stepSize: Decimal,
    },
    MIN_NOTIONAL {
        minNotional: Decimal,
        applyToMarket: bool,
        avgPriceMins: i64,
    },
    NOTIONAL {
        minNotional: Decimal,
        applyMinToMarket: bool,
        maxNotional: Decimal,
        applyMaxToMarket: bool,
        avgPriceMins: i64,
    },
    ICEBERG_PARTS {
        limit: i64,
    },
    MARKET_LOT_SIZE {
        minQty: Decimal,
        maxQty: Decimal,
        stepSize: Decimal,
    },
    MAX_NUM_ORDERS {
        maxNumOrders: i64,
    },
    MAX_NUM_ALGO_ORDERS {
        maxNumAlgoOrders: i64,
    },
    MAX_NUM_ICEBERG_ORDERS {
        maxNumIcebergOrders: i64,
    },
    MAX_POSITION {
        maxPosition: Decimal,
    },
    TRAILING_DELTA {
        minTrailingAboveDelta: i64,
        maxTrailingAboveDelta: i64,
        minTrailingBelowDelta: i64,
        maxTrailingBelowDelta: i64,
    },
    MAX_NUM_ORDER_AMENDS {
        maxNumOrderAmends: i64,
    },
    MAX_NUM_ORDER_LISTS {
        maxNumOrderLists: i64,
    },
    #[serde(other)]
    Unknown,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(tag = "filterType")]
#[allow(non_snake_case, non_camel_case_types)]
#[non_exhaustive]
pub enum BinanceAssetFilter {
    MAX_ASSET {
        asset: Ticker,
        limit: Decimal,
    },
    #[serde(other)]
    Unknown,
}
