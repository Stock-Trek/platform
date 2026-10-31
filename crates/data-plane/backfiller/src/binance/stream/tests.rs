use super::*;

#[test]
fn parses_trade_streams() {
    let stream = MarketStream::parse("btcusdt@trade").unwrap();

    assert_eq!(stream.symbol, "BTCUSDT");
    assert_eq!(stream.kind, MarketStreamKind::Trade);
}

#[test]
fn parses_depth_streams() {
    for value in [
        "btcusdt@depth",
        "btcusdt@depth@100ms",
        "btcusdt@depth5",
        "btcusdt@depth20@100ms",
    ] {
        assert_eq!(
            MarketStream::parse(value).unwrap().kind,
            MarketStreamKind::Depth
        );
    }
}

#[test]
fn parses_book_ticker_streams() {
    let stream = MarketStream::parse("btcusdt@bookTicker").unwrap();

    assert_eq!(stream.symbol, "BTCUSDT");
    assert_eq!(stream.kind, MarketStreamKind::BookTicker);
}

#[test]
fn parses_kline_streams_with_their_interval() {
    let stream = MarketStream::parse("btcusdt@kline_1m").unwrap();

    assert_eq!(stream.symbol, "BTCUSDT");
    assert_eq!(
        stream.kind,
        MarketStreamKind::Kline(BinanceKlinesInterval::ONE_MINUTE)
    );
}

#[test]
fn rejects_unsupported_or_malformed_streams() {
    for value in [
        "btcusdt",
        "btcusdt@aggTrade",
        "btcusdt@depthfoo",
        "btcusdt@kline_nope",
        "@trade",
    ] {
        assert!(MarketStream::parse(value).is_err(), "{value} was accepted");
    }
}
