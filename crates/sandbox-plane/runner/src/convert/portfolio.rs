use crate::{
    convert::string::JsonString,
    generated::dto::{
        AssetCountDto, CexAccountPortfolioDto, CexAccountsDto, PendingOrderDto, PortfolioDto,
    },
};
use rust_decimal::Decimal;
use std::collections::HashMap;
use stock_trek::{
    CexAccountPortfolio, PendingOrder, Portfolio,
    errors::StockTrekError,
    types::{AccountId, AssetId, CexId, Tag},
};

impl TryFrom<PortfolioDto> for Portfolio {
    type Error = StockTrekError;

    fn try_from(value: PortfolioDto) -> Result<Self, Self::Error> {
        let PortfolioDto { cex_accounts_dto } = value;
        let mut portfolios = HashMap::new();
        for (cex_name, cex_accounts_dto) in cex_accounts_dto {
            let cex_id: CexId = JsonString::deserialize(&cex_name)?;
            let accounts = cex_accounts_dto.try_into()?;
            portfolios.insert(cex_id, accounts);
        }
        Ok(Portfolio::new(portfolios))
    }
}

impl TryFrom<CexAccountsDto> for HashMap<AccountId, CexAccountPortfolio> {
    type Error = StockTrekError;

    fn try_from(value: CexAccountsDto) -> Result<Self, Self::Error> {
        let CexAccountsDto {
            account_portfolios_dto,
        } = value;
        let mut accounts = HashMap::new();
        for (account_name, account_portfolio_dto) in account_portfolios_dto {
            let account_id = AccountId::new(&account_name);
            accounts.insert(account_id, account_portfolio_dto.try_into()?);
        }
        Ok(accounts)
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
            .map(TryInto::try_into)
            .collect::<Result<HashMap<AssetId, Decimal>, _>>()?;
        let mut pending_orders = HashMap::new();
        for (tag, orders_dto) in pending_orders_dto {
            let orders = orders_dto
                .pending_orders_dto
                .into_iter()
                .map(PendingOrder::try_from)
                .collect::<Result<Vec<_>, _>>()?;
            pending_orders.insert(Tag::new(&tag), orders);
        }
        Ok(CexAccountPortfolio::new(asset_counts, pending_orders))
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
        Ok(PendingOrder::new(
            order_request_dto
                .required("order_request_dto")?
                .try_into()?,
            filled_base_quantity_dto
                .required("filled_base_quantity_dto")?
                .try_into()?,
            filled_quote_quantity_dto
                .required("filled_quote_quantity_dto")?
                .try_into()?,
        ))
    }
}

impl TryFrom<AssetCountDto> for (AssetId, Decimal) {
    type Error = StockTrekError;

    fn try_from(value: AssetCountDto) -> Result<Self, Self::Error> {
        let AssetCountDto {
            asset_id_dto,
            count_dto,
        } = value;
        use crate::convert::required::IsRequired;
        Ok((
            asset_id_dto.required("asset_id_dto")?.try_into()?,
            count_dto.required("count_dto")?.try_into()?,
        ))
    }
}
