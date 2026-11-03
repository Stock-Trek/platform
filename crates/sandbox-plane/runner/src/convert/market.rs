use crate::{
    convert::{
        market_aligned_window::MarketCandlesMap, market_rolling_window::MarketCandleMap,
        market_tick::MarketTickVec, required::IsRequired,
    },
    generated::dto::MarketDto,
};
use stock_trek::{
    errors::StockTrekError,
    markets::{
        Market, MarketAlignedWindow, MarketBuilder, MarketOrderBook, MarketRollingWindow,
        MarketTicks,
    },
};

impl TryFrom<MarketDto> for Market {
    type Error = StockTrekError;

    fn try_from(value: MarketDto) -> Result<Self, Self::Error> {
        let MarketDto {
            aligned_dto,
            base_increment_dto,
            minimum_notional_dto,
            order_book_dto,
            quote_increment_dto,
            rolling_dto,
            ticks_dto,
        } = value;
        let base_increment = base_increment_dto
            .required("base_increment_dto")?
            .try_into()?;
        let quote_increment = quote_increment_dto
            .required("quote_increment_dto")?
            .try_into()?;
        let minimum_notional = minimum_notional_dto
            .required("minimum_notional_dto")?
            .try_into()?;
        let mut builder = MarketBuilder::new(base_increment, quote_increment, minimum_notional);
        let ticks: MarketTicks =
            MarketTickVec(ticks_dto.required("ticks_dto")?.ticks_dto).try_into()?;
        for tick in ticks.ticks() {
            builder.tick(
                tick.timestamp_millis(),
                tick.bid().clone(),
                tick.ask().clone(),
                tick.last().clone(),
            );
        }
        let order_book: MarketOrderBook = order_book_dto.required("order_book_dto")?.try_into()?;
        for quote in order_book.bids() {
            builder.order_book_bid(quote.clone());
        }
        for quote in order_book.asks() {
            builder.order_book_ask(quote.clone());
        }
        let rolling: MarketRollingWindow = MarketCandleMap(rolling_dto).try_into()?;
        for (window, candle) in rolling.candles() {
            builder.rolling_set_candle(*window, candle.clone());
        }
        let aligned: MarketAlignedWindow = MarketCandlesMap(aligned_dto).try_into()?;
        for (window, candles) in aligned.candles() {
            for candle in candles {
                builder.aligned_push_candle(*window, candle.clone());
            }
        }
        builder.build()
    }
}
