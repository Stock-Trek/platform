use crate::ticker_mapping::TickerMapping;
use platform_errors::{BpError, BpResult, ValidationError};
use platform_exchange_types::ticker::Ticker;
use stock_trek_types::cex::{asset_id::AssetId, order_request::OrderRequest};

#[derive(Debug, Clone)]
pub(crate) struct AssetIdsToTickers;

impl AssetIdsToTickers {
    pub fn asset_ids_to_tickers<N>(
        order_request: OrderRequest<AssetId, N>,
        ticker_mapping: &TickerMapping,
    ) -> BpResult<OrderRequest<Ticker, N>> {
        let (base, quote) = match order_request {
            OrderRequest::Limit { base, quote, .. }
            | OrderRequest::MarketBuy { base, quote, .. }
            | OrderRequest::MarketSell { base, quote, .. } => (base, quote),
        };
        let base = ticker_mapping
            .ticker(&base)
            .ok_or(BpError::Validation(ValidationError::Unsupported {
                name: "AssetId".into(),
                value: base.to_string(),
            }))?
            .clone();
        let quote = ticker_mapping
            .ticker(&quote)
            .ok_or(BpError::Validation(ValidationError::Unsupported {
                name: "AssetId".into(),
                value: quote.to_string(),
            }))?
            .clone();
        let order_request = match order_request {
            OrderRequest::Limit {
                side,
                activation,
                limit_price,
                time_in_force,
                quantity,
                tag,
                ..
            } => OrderRequest::Limit {
                base,
                quote,
                side,
                activation,
                limit_price,
                time_in_force,
                quantity,
                tag,
            },
            OrderRequest::MarketBuy {
                activation,
                quote_quantity,
                tag,
                ..
            } => OrderRequest::MarketBuy {
                base,
                quote,
                activation,
                quote_quantity,
                tag,
            },
            OrderRequest::MarketSell {
                activation,
                base_quantity,
                tag,
                ..
            } => OrderRequest::MarketSell {
                base,
                quote,
                activation,
                base_quantity,
                tag,
            },
        };
        Ok(order_request)
    }
}
