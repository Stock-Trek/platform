use crate::{
    convert::{required::IsRequired, string::JsonString},
    generated::dto::{
        ActivationDto, LimitOrderRequestDto, MarketBuyOrderRequestDto, MarketSellOrderRequestDto,
        OrderRequestDto, QuantityDto, activation_dto::Activation as ActivationVariant,
        order_request_dto::OrderRequest as OrderRequestVariant,
        quantity_dto::Quantity as QuantityVariant,
    },
};
use rust_decimal::Decimal;
use stock_trek::{
    errors::StockTrekError,
    types::{Activation, AssetId, OrderRequest, Quantity, Tag},
};

impl TryFrom<OrderRequestDto> for OrderRequest<AssetId, Decimal> {
    type Error = StockTrekError;

    fn try_from(value: OrderRequestDto) -> Result<Self, Self::Error> {
        let order_request = value.order_request.required("order_request")?;
        match order_request {
            OrderRequestVariant::Limit(limit_dto) => limit_dto.try_into(),
            OrderRequestVariant::MarketBuy(market_buy_dto) => market_buy_dto.try_into(),
            OrderRequestVariant::MarketSell(market_sell_dto) => market_sell_dto.try_into(),
        }
    }
}

impl TryFrom<LimitOrderRequestDto> for OrderRequest<AssetId, Decimal> {
    type Error = StockTrekError;

    fn try_from(value: LimitOrderRequestDto) -> Result<Self, Self::Error> {
        let LimitOrderRequestDto {
            base_dto,
            quote_dto,
            side,
            activation_dto,
            limit_price_dto,
            time_in_force,
            quantity_dto,
            tag,
        } = value;
        Ok(OrderRequest::Limit {
            base: base_dto.required("base_dto")?.try_into()?,
            quote: quote_dto.required("quote_dto")?.try_into()?,
            side: JsonString::deserialize(&side)?,
            activation: activation_dto.required("activation_dto")?.try_into()?,
            limit_price: limit_price_dto.required("limit_price_dto")?.try_into()?,
            time_in_force: JsonString::deserialize(&time_in_force)?,
            quantity: quantity_dto.required("quantity_dto")?.try_into()?,
            tag: Tag::new(&tag),
        })
    }
}

impl TryFrom<MarketBuyOrderRequestDto> for OrderRequest<AssetId, Decimal> {
    type Error = StockTrekError;

    fn try_from(value: MarketBuyOrderRequestDto) -> Result<Self, Self::Error> {
        let MarketBuyOrderRequestDto {
            base_dto,
            quote_dto,
            activation_dto,
            quote_quantity_dto,
            tag,
        } = value;
        Ok(OrderRequest::MarketBuy {
            base: base_dto.required("base_dto")?.try_into()?,
            quote: quote_dto.required("quote_dto")?.try_into()?,
            activation: activation_dto.required("activation_dto")?.try_into()?,
            quote_quantity: quote_quantity_dto
                .required("quote_quantity_dto")?
                .try_into()?,
            tag: Tag::new(&tag),
        })
    }
}

impl TryFrom<MarketSellOrderRequestDto> for OrderRequest<AssetId, Decimal> {
    type Error = StockTrekError;

    fn try_from(value: MarketSellOrderRequestDto) -> Result<Self, Self::Error> {
        let MarketSellOrderRequestDto {
            base_dto,
            quote_dto,
            activation_dto,
            base_quantity_dto,
            tag,
        } = value;
        Ok(OrderRequest::MarketSell {
            base: base_dto.required("base_dto")?.try_into()?,
            quote: quote_dto.required("quote_dto")?.try_into()?,
            activation: activation_dto.required("activation_dto")?.try_into()?,
            base_quantity: base_quantity_dto
                .required("base_quantity_dto")?
                .try_into()?,
            tag: Tag::new(&tag),
        })
    }
}

impl TryFrom<ActivationDto> for Activation<Decimal> {
    type Error = StockTrekError;

    fn try_from(value: ActivationDto) -> Result<Self, Self::Error> {
        let activation = value.activation.required("activation")?;
        match activation {
            ActivationVariant::Immediate(_) => Ok(Activation::Immediate),
            ActivationVariant::PriceTriggered(price_triggered_dto) => {
                Ok(Activation::PriceTriggered {
                    activation_price: price_triggered_dto
                        .activation_price_dto
                        .required("activation_price_dto")?
                        .try_into()?,
                    basis: JsonString::deserialize(&price_triggered_dto.basis)?,
                    direction: JsonString::deserialize(&price_triggered_dto.direction)?,
                    mode: JsonString::deserialize(&price_triggered_dto.mode)?,
                })
            }
            ActivationVariant::Trailing(trailing_dto) => Ok(Activation::Trailing {
                activation_price: trailing_dto
                    .activation_price_dto
                    .required("activation_price_dto")?
                    .try_into()?,
                basis: JsonString::deserialize(&trailing_dto.basis)?,
                callback_rate_bps: trailing_dto.callback_rate_bps,
                direction: JsonString::deserialize(&trailing_dto.direction)?,
            }),
        }
    }
}

impl TryFrom<QuantityDto> for Quantity<Decimal> {
    type Error = StockTrekError;

    fn try_from(value: QuantityDto) -> Result<Self, Self::Error> {
        let quantity = value.quantity.required("quantity")?;
        match quantity {
            QuantityVariant::OfBase(of_base_dto) => Ok(Quantity::OfBase(of_base_dto.try_into()?)),
            QuantityVariant::OfQuote(of_quote_dto) => {
                Ok(Quantity::OfQuote(of_quote_dto.try_into()?))
            }
        }
    }
}
