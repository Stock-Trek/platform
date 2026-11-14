use platform_exchange_types::binance::klines::BinanceKlinesResponse;
use platform_types::TimestampMilliseconds;

#[test]
fn parses_klines_with_ignored_trailing_field() {
    let json = r#"[
        [
            1499040000000,
            "0.01634790",
            "0.80000000",
            "0.01575800",
            "0.01577100",
            "148976.11427815",
            1499644799999,
            "2434.19055334",
            308,
            "1756.87402397",
            "28.46694368",
            "17928899.62484339"
        ]
    ]"#;

    let klines = serde_json::from_str::<BinanceKlinesResponse>(json).unwrap();

    assert_eq!(klines.len(), 1);
    assert_eq!(klines[0].open_time(), TimestampMilliseconds(1499040000000));
    assert_eq!(klines[0].close_time(), TimestampMilliseconds(1499644799999));
    assert_eq!(klines[0].open(), "0.01634790");
    assert_eq!(klines[0].volume(), "148976.11427815");
    assert_eq!(klines[0].trades(), 308);
    assert_eq!(klines[0].taker_buy_quote_volume(), "28.46694368");
}
