use chrono::{DateTime, Utc};
use platform_errors::BpResult;
use platform_exchange_gateway::async_trait;
use platform_exchange_types::klines::Kline;
use stock_trek_types::cex::trading_pair::TradingPair;

#[async_trait]
pub trait ExchangeUpdater: Send + Sync {
    async fn klines(
        &self,
        trading_pair: &TradingPair,
        start_time: DateTime<Utc>,
        end_time: DateTime<Utc>,
    ) -> BpResult<Vec<Kline>>;
}
