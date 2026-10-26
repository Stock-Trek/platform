use crate::{constants::KLINE_LIMIT, updaters::exchange_updater::ExchangeUpdater};
use chrono::{DateTime, Utc};
use platform_errors::{BpError, BpResult, ExchangeError};
use platform_exchange_gateway::{
    async_trait, clients::reqwest::ReqwestHttpClient, connector::Connector,
    submission::SubmissionOutcome,
};
use platform_exchange_types::{
    binance::{
        exchange::BinanceExchange,
        klines::{BinanceKlinesInterval, BinanceKlinesRequest},
        klines_limit::BinanceKlinesLimit,
    },
    encode::ByteEncoder,
    klines::{Kline, KlinesResponse},
    new_types::TimestampMilliseconds,
    signer::Signer,
};
use platform_type_bridge::{BinanceConverter, Converter};
use std::time::Duration;
use stock_trek_types::cex::trading_pair::TradingPair;

const REQUEST_TIMEOUT: Duration = Duration::from_secs(30);
const MAX_RETRIES: u8 = 3;

pub struct BinanceUpdater {
    converter: BinanceConverter,
    connector: Connector<BinanceExchange, ReqwestHttpClient>,
}

#[async_trait]
impl ExchangeUpdater for BinanceUpdater {
    async fn klines(
        &self,
        trading_pair: &TradingPair,
        start_time: DateTime<Utc>,
        end_time: DateTime<Utc>,
    ) -> BpResult<Vec<Kline>> {
        let Some(symbol) = self.converter.ticker_mapping().symbol(trading_pair) else {
            return Err(BpError::Exchange(ExchangeError::MissingSymbol {
                trading_pair: format!("{trading_pair:?}"),
            }));
        };
        let mut request = BinanceKlinesRequest::new(symbol, BinanceKlinesInterval::ONE_MINUTE);
        let (start_millis, end_millis) = Self::kline_time_bounds(start_time, end_time);
        request.startTime = Some(TimestampMilliseconds(start_millis));
        request.endTime = Some(TimestampMilliseconds(end_millis));
        request.limit = KLINE_LIMIT
            .map(|limit| {
                BinanceKlinesLimit::try_new(limit)
                    .map_err(|error| BpError::exchange("build Binance klines limit", error))
            })
            .transpose()?;
        let submission = self
            .connector
            .submit_http(request)
            .map_err(|error| BpError::exchange("submit Binance klines request", error))?;
        match submission
            .wait()
            .await
            .map_err(|error| BpError::exchange("await Binance klines response", error))?
        {
            SubmissionOutcome::Submitted(response) => response.klines(),
            SubmissionOutcome::Unknown { .. } => {
                Err(BpError::Exchange(ExchangeError::UnknownSubmissionOutcome {
                    exchange: "Binance",
                }))
            }
        }
    }
}

impl BinanceUpdater {
    fn kline_time_bounds(start_time: DateTime<Utc>, end_time: DateTime<Utc>) -> (i64, i64) {
        (
            start_time.timestamp_millis(),
            end_time.timestamp_millis() - 1,
        )
    }

    pub fn try_new(url: String) -> BpResult<Self> {
        let converter = BinanceConverter::default();
        let signer = Signer::new_unencrypted(ByteEncoder::HexLower);
        let connector = Connector::try_new_http_reqwest(
            url,
            BinanceExchange,
            signer,
            REQUEST_TIMEOUT,
            MAX_RETRIES,
        )
        .map_err(|error| BpError::exchange("create Binance connector", error))?;
        Ok(Self {
            converter,
            connector,
        })
    }
}
