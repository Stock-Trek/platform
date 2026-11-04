use crate::{
    convert::string::JsonString,
    generated::dto::{
        AssetCountDto, CexAccountPortfolioDto, CexAccountsDto, PendingOrderDto, PortfolioDto,
    },
};
use rust_decimal::prelude::ToPrimitive;
use stock_trek::{
    CexAccountPortfolio, PendingOrder, Portfolio,
    errors::{StockTrekError, ValueError},
    types::{AccountId, AssetId, CexId, Tag},
};

impl TryFrom<PortfolioDto> for Portfolio {
    type Error = StockTrekError;

    fn try_from(value: PortfolioDto) -> Result<Self, Self::Error> {
        let PortfolioDto { cex_accounts_dto } = value;
        let mut portfolios = Vec::new();
        for (cex_name, cex_accounts_dto) in cex_accounts_dto {
            let cex_id: CexId = JsonString::deserialize(&cex_name)?;
            let CexAccountsDto {
                account_portfolios_dto,
            } = cex_accounts_dto;
            for (account_name, account_portfolio_dto) in account_portfolios_dto {
                let account_id = AccountId::new(&account_name);
                let account_portfolio = account_portfolio_dto.try_into()?;
                portfolios.push((cex_id, account_id, account_portfolio));
            }
        }
        Portfolio::new(portfolios)
    }
}

impl TryFrom<CexAccountPortfolioDto> for CexAccountPortfolio {
    type Error = StockTrekError;

    fn try_from(value: CexAccountPortfolioDto) -> Result<Self, Self::Error> {
        let CexAccountPortfolioDto {
            asset_counts_dto,
            pending_orders_dto,
        } = value;
        let asset_counts = asset_counts_dto
            .into_iter()
            .map(
                |asset_count_dto| -> Result<(AssetId, f64), StockTrekError> {
                    let (asset_id, count) = asset_count_dto.try_into()?;
                    let total =
                        count
                            .to_f64()
                            .ok_or(StockTrekError::Value(ValueError::IncorrectType {
                                expected: "f64".to_string(),
                                found: count.to_string(),
                            }))?;
                    Ok((asset_id, total))
                },
            )
            .collect::<Result<Vec<(AssetId, f64)>, _>>()?;
        let mut pending_orders = Vec::new();
        for (tag, orders_dto) in pending_orders_dto {
            let orders = orders_dto
                .pending_orders_dto
                .into_iter()
                .map(PendingOrder::try_from)
                .collect::<Result<Vec<_>, _>>()?;
            pending_orders.push((Tag::new(&tag), orders));
        }
        CexAccountPortfolio::new(asset_counts, pending_orders)
    }
}

impl TryFrom<PendingOrderDto> for PendingOrder {
    type Error = StockTrekError;

    fn try_from(value: PendingOrderDto) -> Result<Self, Self::Error> {
        use crate::convert::required::IsRequired;
        let PendingOrderDto {
            order_request_dto,
            filled_base_quantity_dto,
            filled_quote_quantity_dto,
        } = value;
        PendingOrder::new(
            order_request_dto
                .required("order_request_dto")?
                .try_into()?,
            filled_base_quantity_dto
                .required("filled_base_quantity_dto")?
                .try_into()?,
            filled_quote_quantity_dto
                .required("filled_quote_quantity_dto")?
                .try_into()?,
        )
    }
}

impl TryFrom<AssetCountDto> for (AssetId, f64) {
    type Error = StockTrekError;

    fn try_from(value: AssetCountDto) -> Result<Self, Self::Error> {
        let AssetCountDto {
            asset_id_dto,
            count_dto,
        } = value;
        use crate::convert::required::IsRequired;
        let count = count_dto.required("count_dto")?.try_into()?;
        let asset_id = asset_id_dto.required("asset_id_dto")?.try_into()?;
        Ok((asset_id, count))
    }
}
