use crate::trading_precision::TradingPrecision;
use std::collections::HashMap;
use stock_trek_types::cex::trading_pair::TradingPair;

#[derive(Debug, Clone)]
pub struct TBExchangeInfo {
    pub trading_precisions: HashMap<TradingPair, TradingPrecision>,
}

impl TBExchangeInfo {
    pub fn trading_precision(&self, trading_pair: &TradingPair) -> Option<TradingPrecision> {
        self.trading_precisions.get(trading_pair).copied()
    }
}
