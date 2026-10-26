pub mod asset_ids_to_tickers;
pub mod asset_limits;
pub mod converter;
pub mod converters;
pub mod exchange_info;
pub mod floats_to_decimals;
pub mod response;
pub mod semantic_checker;
pub mod session_authentication;
pub mod ticker_mapping;
pub mod trading_precision;
pub mod util;

pub use platform_errors::{BpError, BpResult};

pub use crate::{
    asset_limits::TBAssetLimits,
    converter::{Converter, TradingConverter},
    converters::binance::BinanceConverter,
    exchange_info::TBExchangeInfo,
    response::TBResponse,
    session_authentication::TBSessionAuthentication,
    ticker_mapping::TickerMapping,
    trading_precision::{MinMaxStep, TradingPrecision},
};

pub mod prelude {
    pub use crate::{
        BpError, BpResult, Converter, MinMaxStep, TBExchangeInfo, TBResponse,
        TBSessionAuthentication, TickerMapping, TradingConverter, TradingPrecision,
    };
}

pub mod types {
    pub mod stock_trek {
        pub mod cex {
            pub use stock_trek_types::cex::{
                activation::Activation,
                asset_id::AssetId,
                capability::CexCapability,
                cex_id::CexId,
                order_request::OrderRequest,
                order_response::OrderResponse,
                preferences::{CexPreferences, CexRoundingPreferences},
                price_basis::PriceBasis,
                quantity::Quantity,
                side::Side,
                status::Status,
                tag::Tag,
                time_in_force::TimeInForce,
                trading_pair::TradingPair,
                trigger_direction::TriggerDirection,
                trigger_mode::TriggerMode,
            };
        }
    }
    pub mod cex {
        pub use platform_exchange_types;
    }
}
