use crate::{
    convert::{
        decimal::ToDecimal, market_aligned_window::MarketCandlesMap,
        market_rolling_window::MarketCandleMap, market_tick::MarketTickVec, required::IsRequired,
        string::JsonString,
    },
    generated::dto::*,
};
use rust_decimal::Decimal;
use std::collections::HashMap;
use stock_trek::{
    Portfolio,
    markets::{
        AlignedWindow, Market, MarketAlignedWindow, MarketCandle, MarketOhlcv, MarketOrderBook,
        MarketQuote, MarketRollingWindow, MarketTick, MarketTicks, RollingWindow,
    },
    signals::SignalContext,
    types::{
        AccountId, Activation, AssetId, CexId, OrderRequest, PriceBasis, Quantity, Side, Tag,
        TimeInForce, TriggerDirection, TriggerMode,
    },
};

struct Fixtures;

impl Fixtures {
    fn decimal_dto(value: Decimal) -> DecimalDto {
        let unpacked = value.unpack();
        DecimalDto {
            lo: unpacked.lo,
            mid: unpacked.mid,
            hi: unpacked.hi,
            scale: unpacked.scale,
            negative: unpacked.negative,
        }
    }

    fn asset_id_dto(name: &str) -> AssetIdDto {
        AssetIdDto {
            asset_id: name.to_string(),
        }
    }

    fn ohlcv_dto() -> MarketOhlcvDto {
        MarketOhlcvDto {
            open_dto: Some(Self::decimal_dto(Decimal::new(1, 0))),
            high_dto: Some(Self::decimal_dto(Decimal::new(2, 0))),
            low_dto: Some(Self::decimal_dto(Decimal::new(3, 0))),
            close_dto: Some(Self::decimal_dto(Decimal::new(4, 0))),
            volume_dto: Some(Self::decimal_dto(Decimal::new(5, 0))),
            quote_volume_dto: Some(Self::decimal_dto(Decimal::new(6, 0))),
            vwap_dto: Some(Self::decimal_dto(Decimal::new(7, 0))),
        }
    }

    fn candle_dto() -> MarketCandleDto {
        MarketCandleDto {
            start_time_millis_inc: 1_000,
            end_time_millis_exc: 2_000,
            duration_millis: 1_000,
            is_candle_closed: true,
            ohlcv_dto: Some(Self::ohlcv_dto()),
            trade_count: 10,
        }
    }

    fn quote_dto(price: i64, quantity: i64) -> MarketQuoteDto {
        MarketQuoteDto {
            price_dto: Some(Self::decimal_dto(Decimal::new(price, 0))),
            quantity_dto: Some(Self::decimal_dto(Decimal::new(quantity, 0))),
        }
    }

    fn tick_dto(timestamp_millis: u64) -> MarketTickDto {
        MarketTickDto {
            timestamp_millis,
            bid_dto: Some(Self::quote_dto(1, 2)),
            ask_dto: Some(Self::quote_dto(3, 4)),
            last_dto: Some(Self::quote_dto(5, 6)),
        }
    }

    fn market_dto() -> MarketDto {
        MarketDto {
            base_increment_dto: Some(Self::decimal_dto(Decimal::new(1, 2))),
            quote_increment_dto: Some(Self::decimal_dto(Decimal::new(2, 2))),
            minimum_notional_dto: Some(Self::decimal_dto(Decimal::new(3, 2))),
            ticks_dto: Some(MarketTicksDto {
                ticks_dto: vec![Self::tick_dto(1_000)],
            }),
            rolling_dto: HashMap::from([(0, Self::candle_dto())]),
            aligned_dto: HashMap::from([(
                0,
                MarketCandlesDto {
                    candles_dto: vec![Self::candle_dto()],
                },
            )]),
            order_book_dto: Some(MarketOrderBookDto {
                bids_dto: vec![Self::quote_dto(1, 2)],
                asks_dto: vec![Self::quote_dto(3, 4)],
            }),
        }
    }

    fn immediate_activation_dto() -> ActivationDto {
        ActivationDto {
            activation: Some(activation_dto::Activation::Immediate(
                ImmediateActivationDto {},
            )),
        }
    }

    fn price_triggered_activation_dto() -> ActivationDto {
        ActivationDto {
            activation: Some(activation_dto::Activation::PriceTriggered(
                PriceTriggeredActivationDto {
                    activation_price_dto: Some(Self::decimal_dto(Decimal::new(250, 1))),
                    basis: "Mark".to_string(),
                    direction: "Above".to_string(),
                    mode: "Cross".to_string(),
                },
            )),
        }
    }

    fn trailing_activation_dto() -> ActivationDto {
        ActivationDto {
            activation: Some(activation_dto::Activation::Trailing(
                TrailingActivationDto {
                    activation_price_dto: Some(Self::decimal_dto(Decimal::new(100, 0))),
                    basis: "Last".to_string(),
                    callback_rate_bps: 50,
                    direction: "Below".to_string(),
                },
            )),
        }
    }

    fn portfolio_dto() -> PortfolioDto {
        let pending_order = PendingOrderDto {
            order_request_dto: Some(OrderRequestDto {
                order_request: Some(order_request_dto::OrderRequest::MarketBuy(
                    MarketBuyOrderRequestDto {
                        base_dto: Some(Self::asset_id_dto("Bitcoin")),
                        quote_dto: Some(Self::asset_id_dto("TetherUSD")),
                        activation_dto: Some(Self::immediate_activation_dto()),
                        quote_quantity_dto: Some(Self::decimal_dto(Decimal::new(500, 0))),
                        tag: "main".to_string(),
                    },
                )),
            }),
            filled_base_quantity_dto: Some(Self::decimal_dto(Decimal::new(1, 0))),
            filled_quote_quantity_dto: Some(Self::decimal_dto(Decimal::new(100, 0))),
        };

        PortfolioDto {
            cex_accounts_dto: HashMap::from([(
                "Binance".to_string(),
                CexAccountsDto {
                    account_portfolios_dto: HashMap::from([(
                        "primary".to_string(),
                        CexAccountPortfolioDto {
                            asset_counts_dto: vec![AssetCountDto {
                                asset_id_dto: Some(Self::asset_id_dto("Bitcoin")),
                                count_dto: Some(Self::decimal_dto(Decimal::new(3, 0))),
                            }],
                            pending_orders_dto: HashMap::from([(
                                "main".to_string(),
                                PendingOrdersDto {
                                    pending_orders_dto: vec![pending_order],
                                },
                            )]),
                        },
                    )]),
                },
            )]),
        }
    }
}

#[test]
fn decimal_dto_converts_to_decimal() {
    let dto = Fixtures::decimal_dto(Decimal::new(3141, 3));
    assert_eq!(dto.to_decimal().unwrap(), Decimal::new(3141, 3));
}

#[test]
fn decimal_dto_converts_negative_to_decimal() {
    let dto = Fixtures::decimal_dto(Decimal::new(-3141, 3));
    assert_eq!(Decimal::try_from(dto).unwrap(), Decimal::new(-3141, 3));
}

#[test]
fn decimal_dto_converts_to_f64() {
    let dto = Fixtures::decimal_dto(Decimal::new(1234, 2));
    assert_eq!(f64::try_from(dto).unwrap(), 12.34);
}

#[test]
fn decimal_dto_accepts_max_scale() {
    let dto = DecimalDto {
        lo: 1,
        mid: 0,
        hi: 0,
        scale: Decimal::MAX_SCALE,
        negative: false,
    };
    assert!(dto.to_decimal().is_ok());
}

#[test]
fn decimal_dto_rejects_scale_above_max() {
    let dto = DecimalDto {
        lo: 1,
        mid: 0,
        hi: 0,
        scale: Decimal::MAX_SCALE + 1,
        negative: false,
    };
    assert!(dto.to_decimal().is_err());
}

#[test]
fn required_unwraps_some() {
    assert_eq!(Some(7u32).required("value").unwrap(), 7);
}

#[test]
fn required_errors_on_none() {
    assert!(None::<u32>.required("value").is_err());
}

#[test]
fn from_string_deserializes_enum_variant() {
    assert_eq!(JsonString::deserialize::<Side>("Buy").unwrap(), Side::Buy);
    assert_eq!(
        JsonString::deserialize::<TimeInForce>("FillOrKill").unwrap(),
        TimeInForce::FillOrKill
    );
}

#[test]
fn from_string_errors_on_unknown_variant() {
    assert!(JsonString::deserialize::<Side>("Sideways").is_err());
}

#[test]
fn asset_id_dto_converts() {
    assert_eq!(
        AssetId::try_from(Fixtures::asset_id_dto("Bitcoin")).unwrap(),
        AssetId::Bitcoin
    );
}

#[test]
fn asset_id_dto_errors_for_unknown_asset() {
    assert!(AssetId::try_from(Fixtures::asset_id_dto("NotAnAsset")).is_err());
}

#[test]
fn market_ohlcv_dto_converts() {
    let ohlcv = MarketOhlcv::try_from(Fixtures::ohlcv_dto()).unwrap();
    assert_eq!(ohlcv.open, 1.0);
    assert_eq!(ohlcv.high, 2.0);
    assert_eq!(ohlcv.low, 3.0);
    assert_eq!(ohlcv.close, 4.0);
    assert_eq!(ohlcv.volume, 5.0);
    assert_eq!(ohlcv.quote_volume, 6.0);
    assert_eq!(ohlcv.vwap, 7.0);
}

#[test]
fn market_ohlcv_dto_requires_all_fields() {
    let mut dto = Fixtures::ohlcv_dto();
    dto.vwap_dto = None;
    assert!(MarketOhlcv::try_from(dto).is_err());
}

#[test]
fn market_candle_dto_converts() {
    let candle = MarketCandle::try_from(Fixtures::candle_dto()).unwrap();
    assert_eq!(candle.start_time_millis_inc, 1_000);
    assert_eq!(candle.end_time_millis_exc, 2_000);
    assert_eq!(candle.duration_millis, 1_000);
    assert!(candle.is_candle_closed);
    assert_eq!(candle.trade_count, 10);
    assert_eq!(candle.ohlcv.open, 1.0);
    assert_eq!(candle.ohlcv.vwap, 7.0);
}

#[test]
fn market_candle_dto_requires_ohlcv() {
    let mut dto = Fixtures::candle_dto();
    dto.ohlcv_dto = None;
    assert!(MarketCandle::try_from(dto).is_err());
}

#[test]
fn market_quote_dto_converts() {
    let quote = MarketQuote::try_from(Fixtures::quote_dto(11, 22)).unwrap();
    assert_eq!(quote.price, 11.0);
    assert_eq!(quote.quantity, 22.0);
}

#[test]
fn market_quote_dto_requires_price_and_quantity() {
    let mut dto = Fixtures::quote_dto(11, 22);
    dto.quantity_dto = None;
    assert!(MarketQuote::try_from(dto).is_err());
}

#[test]
fn market_tick_dto_converts() {
    let tick = MarketTick::try_from(Fixtures::tick_dto(1_234)).unwrap();
    assert_eq!(tick.timestamp_millis, 1_234);
    assert_eq!(tick.bid.price, 1.0);
    assert_eq!(tick.bid.quantity, 2.0);
    assert_eq!(tick.ask.price, 3.0);
    assert_eq!(tick.ask.quantity, 4.0);
    assert_eq!(tick.last.price, 5.0);
    assert_eq!(tick.last.quantity, 6.0);
}

#[test]
fn market_tick_dto_requires_quotes() {
    let mut dto = Fixtures::tick_dto(1);
    dto.ask_dto = None;
    assert!(MarketTick::try_from(dto).is_err());
}

#[test]
fn market_tick_vec_converts() {
    let ticks = MarketTicks::try_from(MarketTickVec(vec![
        Fixtures::tick_dto(1),
        Fixtures::tick_dto(2),
    ]))
    .unwrap();
    assert_eq!(ticks.ticks.len(), 2);
    assert_eq!(ticks.ticks[1].timestamp_millis, 2);
}

#[test]
fn market_order_book_dto_converts() {
    let order_book = MarketOrderBook::try_from(MarketOrderBookDto {
        bids_dto: vec![Fixtures::quote_dto(1, 2)],
        asks_dto: vec![Fixtures::quote_dto(3, 4)],
    })
    .unwrap();
    assert_eq!(order_book.bids.len(), 1);
    assert_eq!(order_book.bids[0].price, 1.0);
    assert_eq!(order_book.bids[0].quantity, 2.0);
    assert_eq!(order_book.asks.len(), 1);
    assert_eq!(order_book.asks[0].price, 3.0);
    assert_eq!(order_book.asks[0].quantity, 4.0);
}

#[test]
fn market_rolling_window_converts() {
    let window = MarketRollingWindow::try_from(MarketCandleMap(HashMap::from([(
        RollingWindow::Seconds5 as u32,
        Fixtures::candle_dto(),
    )])))
    .unwrap();
    let candle = window.candles.get(&RollingWindow::Seconds5).unwrap();
    assert_eq!(candle.trade_count, 10);
}

#[test]
fn market_rolling_window_rejects_unknown_window() {
    let map = MarketCandleMap(HashMap::from([(255, Fixtures::candle_dto())]));
    assert!(MarketRollingWindow::try_from(map).is_err());
}

#[test]
fn market_rolling_window_rejects_out_of_range_window() {
    let map = MarketCandleMap(HashMap::from([(256, Fixtures::candle_dto())]));
    assert!(MarketRollingWindow::try_from(map).is_err());
}

#[test]
fn market_aligned_window_converts() {
    let window = MarketAlignedWindow::try_from(MarketCandlesMap(HashMap::from([(
        AlignedWindow::Seconds5 as u32,
        MarketCandlesDto {
            candles_dto: vec![Fixtures::candle_dto(), Fixtures::candle_dto()],
        },
    )])))
    .unwrap();
    assert_eq!(
        window.candles.get(&AlignedWindow::Seconds5).unwrap().len(),
        2
    );
}

#[test]
fn market_aligned_window_rejects_unknown_window() {
    let map = MarketCandlesMap(HashMap::from([(
        255,
        MarketCandlesDto {
            candles_dto: vec![Fixtures::candle_dto()],
        },
    )]));
    assert!(MarketAlignedWindow::try_from(map).is_err());
}

#[test]
fn market_dto_converts() {
    let market = Market::try_from(Fixtures::market_dto()).unwrap();
    assert_eq!(market.base_increment, 0.01);
    assert_eq!(market.quote_increment, 0.02);
    assert_eq!(market.minimum_notional, 0.03);
    assert_eq!(market.ticks.ticks.len(), 1);
    assert_eq!(market.rolling.candles.len(), 1);
    assert_eq!(market.aligned.candles.len(), 1);
    assert_eq!(market.order_book.bids.len(), 1);
    assert_eq!(market.order_book.asks.len(), 1);
}

#[test]
fn market_dto_requires_core_fields() {
    let mut dto = Fixtures::market_dto();
    dto.ticks_dto = None;
    assert!(Market::try_from(dto).is_err());
}

#[test]
fn order_request_dto_requires_variant() {
    let dto = OrderRequestDto {
        order_request: None,
    };
    assert!(OrderRequest::<AssetId, Decimal>::try_from(dto).is_err());
}

#[test]
fn limit_order_request_converts() {
    let dto = OrderRequestDto {
        order_request: Some(order_request_dto::OrderRequest::Limit(
            LimitOrderRequestDto {
                base_dto: Some(Fixtures::asset_id_dto("Bitcoin")),
                quote_dto: Some(Fixtures::asset_id_dto("TetherUSD")),
                side: "Buy".to_string(),
                activation_dto: Some(Fixtures::price_triggered_activation_dto()),
                limit_price_dto: Some(Fixtures::decimal_dto(Decimal::new(100, 0))),
                time_in_force: "GoodTillCancelled".to_string(),
                quantity_dto: Some(QuantityDto {
                    quantity: Some(quantity_dto::Quantity::OfBase(Fixtures::decimal_dto(
                        Decimal::new(5, 0),
                    ))),
                }),
                tag: "my-tag".to_string(),
            },
        )),
    };

    let request: OrderRequest<AssetId, Decimal> = dto.try_into().unwrap();
    match request {
        OrderRequest::Limit {
            base,
            quote,
            side,
            activation,
            limit_price,
            time_in_force,
            quantity,
            tag,
        } => {
            assert_eq!(base, AssetId::Bitcoin);
            assert_eq!(quote, AssetId::TetherUSD);
            assert_eq!(side, Side::Buy);
            assert_eq!(limit_price, Decimal::new(100, 0));
            assert_eq!(time_in_force, TimeInForce::GoodTillCancelled);
            assert_eq!(tag, Tag::new("my-tag"));
            match activation {
                Activation::PriceTriggered {
                    activation_price,
                    basis,
                    direction,
                    mode,
                } => {
                    assert_eq!(activation_price, Decimal::new(250, 1));
                    assert_eq!(basis, PriceBasis::Mark);
                    assert_eq!(direction, TriggerDirection::Above);
                    assert_eq!(mode, TriggerMode::Cross);
                }
                other => panic!("expected price triggered activation, got {other:?}"),
            }
            match quantity {
                Quantity::OfBase(value) => assert_eq!(value, Decimal::new(5, 0)),
                Quantity::OfQuote(value) => panic!("expected base quantity, got {value}"),
            }
        }
        other => panic!("expected limit order request, got {other:?}"),
    }
}

#[test]
fn market_buy_order_request_converts() {
    let dto = OrderRequestDto {
        order_request: Some(order_request_dto::OrderRequest::MarketBuy(
            MarketBuyOrderRequestDto {
                base_dto: Some(Fixtures::asset_id_dto("Ethereum")),
                quote_dto: Some(Fixtures::asset_id_dto("USDCoin")),
                activation_dto: Some(Fixtures::immediate_activation_dto()),
                quote_quantity_dto: Some(Fixtures::decimal_dto(Decimal::new(1000, 0))),
                tag: "dca".to_string(),
            },
        )),
    };

    let request: OrderRequest<AssetId, Decimal> = dto.try_into().unwrap();
    match request {
        OrderRequest::MarketBuy {
            base,
            quote,
            activation,
            quote_quantity,
            tag,
        } => {
            assert_eq!(base, AssetId::Ethereum);
            assert_eq!(quote, AssetId::USDCoin);
            assert!(matches!(activation, Activation::Immediate));
            assert_eq!(quote_quantity, Decimal::new(1000, 0));
            assert_eq!(tag, Tag::new("dca"));
        }
        other => panic!("expected market buy order request, got {other:?}"),
    }
}

#[test]
fn market_sell_order_request_converts() {
    let dto = OrderRequestDto {
        order_request: Some(order_request_dto::OrderRequest::MarketSell(
            MarketSellOrderRequestDto {
                base_dto: Some(Fixtures::asset_id_dto("Solana")),
                quote_dto: Some(Fixtures::asset_id_dto("TetherUSD")),
                activation_dto: Some(Fixtures::immediate_activation_dto()),
                base_quantity_dto: Some(Fixtures::decimal_dto(Decimal::new(2, 0))),
                tag: "exit".to_string(),
            },
        )),
    };

    let request: OrderRequest<AssetId, Decimal> = dto.try_into().unwrap();
    match request {
        OrderRequest::MarketSell {
            base,
            quote,
            activation,
            base_quantity,
            tag,
        } => {
            assert_eq!(base, AssetId::Solana);
            assert_eq!(quote, AssetId::TetherUSD);
            assert!(matches!(activation, Activation::Immediate));
            assert_eq!(base_quantity, Decimal::new(2, 0));
            assert_eq!(tag, Tag::new("exit"));
        }
        other => panic!("expected market sell order request, got {other:?}"),
    }
}

#[test]
fn immediate_activation_dto_converts() {
    let activation: Activation<Decimal> = Fixtures::immediate_activation_dto().try_into().unwrap();
    assert!(matches!(activation, Activation::Immediate));
}

#[test]
fn trailing_activation_dto_converts() {
    let activation: Activation<Decimal> = Fixtures::trailing_activation_dto().try_into().unwrap();
    match activation {
        Activation::Trailing {
            activation_price,
            basis,
            callback_rate_bps,
            direction,
        } => {
            assert_eq!(activation_price, Decimal::new(100, 0));
            assert_eq!(basis, PriceBasis::Last);
            assert_eq!(callback_rate_bps, 50);
            assert_eq!(direction, TriggerDirection::Below);
        }
        other => panic!("expected trailing activation, got {other:?}"),
    }
}

#[test]
fn activation_dto_requires_variant() {
    let dto = ActivationDto { activation: None };
    assert!(Activation::<Decimal>::try_from(dto).is_err());
}

#[test]
fn quantity_dto_converts_of_quote() {
    let dto = QuantityDto {
        quantity: Some(quantity_dto::Quantity::OfQuote(Fixtures::decimal_dto(
            Decimal::new(25, 1),
        ))),
    };
    let quantity: Quantity<Decimal> = dto.try_into().unwrap();
    match quantity {
        Quantity::OfQuote(value) => assert_eq!(value, Decimal::new(25, 1)),
        Quantity::OfBase(value) => panic!("expected quote quantity, got {value}"),
    }
}

#[test]
fn quantity_dto_requires_variant() {
    let dto = QuantityDto { quantity: None };
    assert!(Quantity::<Decimal>::try_from(dto).is_err());
}

#[test]
fn portfolio_dto_converts() {
    let portfolio = Portfolio::try_from(Fixtures::portfolio_dto()).unwrap();
    let account_id = AccountId::new("primary");

    assert!(portfolio.has_cex_account(&CexId::Binance, &account_id));
    assert_eq!(portfolio.asset_total(&AssetId::Bitcoin), Decimal::new(3, 0));
    assert_eq!(
        portfolio.asset_total_in_cex_account(&AssetId::Bitcoin, &CexId::Binance, &account_id),
        Decimal::new(3, 0)
    );
    assert_eq!(portfolio.pending_orders(), 1.0);
    assert_eq!(portfolio.pending_orders_with_tag(&Tag::new("main")), 1.0);
    assert_eq!(
        portfolio.pending_orders_in_cex_account(&CexId::Binance, &account_id),
        1.0
    );
    assert_eq!(
        portfolio.asset_reserved(&AssetId::TetherUSD),
        Decimal::new(400, 0)
    );
}

#[test]
fn portfolio_dto_requires_pending_order_fields() {
    let mut dto = Fixtures::portfolio_dto();
    let accounts = dto.cex_accounts_dto.get_mut("Binance").unwrap();
    let account = accounts.account_portfolios_dto.get_mut("primary").unwrap();
    let orders = account.pending_orders_dto.get_mut("main").unwrap();
    orders.pending_orders_dto[0].order_request_dto = None;

    assert!(Portfolio::try_from(dto).is_err());
}

#[test]
fn strategy_context_dto_converts() {
    let dto = StrategyContextDto {
        exchange_asset_markets_dto: HashMap::from([(
            "Binance".to_string(),
            AssetMarketsDto {
                markets_by_base_dto: vec![MarketByBaseDto {
                    base_asset_id_dto: Some(Fixtures::asset_id_dto("Bitcoin")),
                    markets_by_quote_dto: vec![MarketByQuoteDto {
                        quote_asset_id_dto: Some(Fixtures::asset_id_dto("TetherUSD")),
                        market_dto: Some(Fixtures::market_dto()),
                    }],
                }],
            },
        )]),
    };

    let context = SignalContext::try_from(dto).unwrap();
    let market = context
        .cex_market_for(&CexId::Binance, &AssetId::Bitcoin, &AssetId::TetherUSD)
        .unwrap();
    assert_eq!(market.base_increment, 0.01);
    assert_eq!(market.order_book.bids.len(), 1);
    assert!(
        context
            .cex_market_for(&CexId::Binance, &AssetId::Ethereum, &AssetId::TetherUSD)
            .is_none()
    );
}

#[test]
fn strategy_context_dto_requires_base_asset() {
    let dto = StrategyContextDto {
        exchange_asset_markets_dto: HashMap::from([(
            "Binance".to_string(),
            AssetMarketsDto {
                markets_by_base_dto: vec![MarketByBaseDto {
                    base_asset_id_dto: None,
                    markets_by_quote_dto: Vec::new(),
                }],
            },
        )]),
    };

    assert!(SignalContext::try_from(dto).is_err());
}
