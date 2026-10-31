use super::*;

#[test]
fn parses_json_stream_format() {
    assert!(matches!(
        " json ".parse::<StreamFormat>().unwrap(),
        StreamFormat::Json(_)
    ));
}

#[test]
fn parses_binary_stream_format() {
    assert!(matches!(
        "binary".parse::<StreamFormat>().unwrap(),
        StreamFormat::Binary(_)
    ));
}

#[test]
fn rejects_unknown_stream_formats() {
    assert!(matches!(
        "protobuf".parse::<StreamFormat>(),
        Err(IngesterError::UnsupportedStreamFormat { .. })
    ));
}

#[test]
fn maps_binance_api_key_header() {
    assert_eq!(
        WebsocketSource::api_key_header(CexId::Binance).unwrap(),
        "X-MBX-APIKEY"
    );
}

#[tokio::test]
async fn rejects_overlapping_trade_streams_before_connecting() {
    let source = WebsocketSource::new(
        CexId::Binance,
        "wss://example.invalid/stream?streams=btcusdt@trade/btcusdt@aggTrade",
        None,
        StreamFormat::for_exchange("json", CexId::Binance).unwrap(),
    );
    let (sender, _receiver) = mpsc::channel(1);

    assert!(source.run(sender).await.is_err());
}
