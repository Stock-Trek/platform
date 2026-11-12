use crate::cwasm::backtest::Balances;
use platform_runner::dto::{
    AssetCountDto, AssetIdDto, CexAccountPortfolioDto, CexAccountsDto, DecimalDto, PortfolioDto,
};
use rust_decimal::Decimal;
use serde_json::Value;
use std::collections::HashMap;

pub(super) struct Portfolio;

impl Portfolio {
    pub(super) fn asset_totals(balances: &Balances) -> Value {
        serde_json::to_value(balances).unwrap_or(Value::Null)
    }

    fn key_string<T: serde::Serialize + std::fmt::Display>(value: &T) -> String {
        match serde_json::to_value(value) {
            Ok(Value::String(key)) => key,
            _ => value.to_string(),
        }
    }

    pub(super) fn portfolio_dto(balances: &Balances) -> PortfolioDto {
        PortfolioDto {
            cex_accounts_dto: balances
                .iter()
                .map(|(cex, accounts)| {
                    (
                        Self::key_string(cex),
                        CexAccountsDto {
                            account_portfolios_dto: accounts
                                .iter()
                                .map(|(account, assets)| {
                                    (
                                        account.0.clone(),
                                        CexAccountPortfolioDto {
                                            asset_counts_dto: assets
                                                .iter()
                                                .map(|(asset, total)| AssetCountDto {
                                                    asset_id_dto: Some(AssetIdDto {
                                                        asset_id: Self::key_string(asset),
                                                    }),
                                                    count_dto: Some(Self::decimal_dto_f64(*total)),
                                                })
                                                .collect(),
                                            pending_orders_dto: HashMap::new(),
                                        },
                                    )
                                })
                                .collect(),
                        },
                    )
                })
                .collect(),
        }
    }

    fn decimal_dto_f64(value: f64) -> DecimalDto {
        let decimal = Decimal::from_f64_retain(value).unwrap_or(Decimal::ZERO);
        Self::decimal_dto(decimal)
    }

    pub(super) fn decimal_dto(decimal: Decimal) -> DecimalDto {
        let unpacked = decimal.unpack();
        DecimalDto {
            lo: unpacked.lo,
            mid: unpacked.mid,
            hi: unpacked.hi,
            scale: unpacked.scale,
            negative: unpacked.negative,
        }
    }
}
