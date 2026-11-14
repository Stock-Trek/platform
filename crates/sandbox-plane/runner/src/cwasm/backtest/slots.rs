use crate::{dto::StrategyContextDto, string::JsonString};
use std::collections::BTreeMap;
use stock_trek::types::{AssetId, CexId};

use crate::cwasm::backtest::{
    market_data::{MarketData, Series},
    spec::Spec,
};

pub struct MarketSlot {
    pub cex: CexId,
    pub cex_key: String,
    pub pair: (AssetId, AssetId),
    pub base_index: usize,
    pub quote_index: usize,
    pub series: BTreeMap<u32, Series>,
}

impl MarketSlot {
    pub fn build_all(
        strategy_context: &StrategyContextDto,
        data: &MarketData,
        pairs: &[(AssetId, AssetId)],
    ) -> Result<Vec<MarketSlot>, i32> {
        let mut cex_keys: BTreeMap<CexId, String> = BTreeMap::new();
        for key in strategy_context.exchange_asset_markets_dto.keys() {
            if let Ok(cex) = JsonString::deserialize::<CexId>(key) {
                cex_keys.insert(cex, key.clone());
            }
        }
        let mut slots = Vec::new();
        for (cex, pairs_data) in data {
            let Some(cex_key) = cex_keys.get(cex) else {
                return Err(Spec::invalid_argument(&format!(
                    "strategy context has no {cex} markets"
                )));
            };
            for pair in pairs {
                let (base_index, quote_index) = Self::find(strategy_context, cex_key, pair)?;
                slots.push(MarketSlot {
                    cex: *cex,
                    cex_key: cex_key.clone(),
                    pair: *pair,
                    base_index,
                    quote_index,
                    series: pairs_data[pair].clone(),
                });
            }
        }
        Ok(slots)
    }

    fn find(
        strategy_context: &StrategyContextDto,
        cex_key: &str,
        pair: &(AssetId, AssetId),
    ) -> Result<(usize, usize), i32> {
        let Some(asset_markets) = strategy_context.exchange_asset_markets_dto.get(cex_key) else {
            return Err(Spec::invalid_argument(&format!(
                "strategy context has no {cex_key} markets"
            )));
        };
        let base_index = asset_markets
            .markets_by_base_dto
            .iter()
            .position(|market_by_base| {
                market_by_base
                    .base_asset_id_dto
                    .as_ref()
                    .and_then(|dto| JsonString::deserialize::<AssetId>(&dto.asset_id).ok())
                    == Some(pair.0)
            })
            .ok_or_else(|| {
                Spec::invalid_argument(&format!(
                    "strategy context has no {cex_key} {} market",
                    pair.0
                ))
            })?;
        let quote_index = asset_markets.markets_by_base_dto[base_index]
            .markets_by_quote_dto
            .iter()
            .position(|market_by_quote| {
                market_by_quote
                    .quote_asset_id_dto
                    .as_ref()
                    .and_then(|dto| JsonString::deserialize::<AssetId>(&dto.asset_id).ok())
                    == Some(pair.1)
            })
            .ok_or_else(|| {
                Spec::invalid_argument(&format!(
                    "strategy context has no {cex_key} {}/{} market",
                    pair.0, pair.1
                ))
            })?;
        Ok((base_index, quote_index))
    }
}
