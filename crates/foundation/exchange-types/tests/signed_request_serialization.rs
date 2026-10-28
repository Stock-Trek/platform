use platform_exchange_types::{
    binance::{
        account_status::BinanceAccountStatusRequest,
        amend_order::BinanceAmendOrderRequest,
        asset_limits::BinanceAssetLimitsRequest,
        cancel_all_open_orders::BinanceCancelAllOpenOrdersRequest,
        cancel_order::BinanceCancelOrderRequest,
        open_orders_status::BinanceOpenOrdersStatusRequest,
        order_status::BinanceOrderStatusRequest,
        recv_window::BinanceRecvWindow,
        spot_order::BinanceSpotOrderRequest,
        supporting_types::{BinanceOrderType, BinanceSide},
        websocket_session_logon::BinanceSessionLogonRequest,
    },
    new_types::{Milliseconds, TimestampMilliseconds},
    request::ETRequest,
};
use rust_decimal::Decimal;

const TIMESTAMP: i64 = 1_700_000_000_000;

fn recv_window() -> BinanceRecvWindow {
    BinanceRecvWindow::try_new(Milliseconds(1_000)).expect("valid recvWindow")
}

fn assert_signed_serialization<Request>(mut request: Request, expected: &[&str])
where
    Request: ETRequest,
{
    assert!(request.is_signed());
    request.set_api_key(Some("api-key".into()));
    request.set_timestamp(TimestampMilliseconds(TIMESTAMP));

    let params = request.query_params(false);
    let params: Vec<&str> = params.split('&').collect();

    assert_eq!(params.first(), Some(&"apiKey=api-key"));
    assert!(params.contains(&"timestamp=1700000000000"));
    assert!(params.contains(&"recvWindow=1000"));
    for expected in expected {
        assert!(
            params.contains(expected),
            "missing {expected} in {params:?}"
        );
    }
}

#[test]
fn account_status_request_serializes_common_signed_fields() {
    let mut request = BinanceAccountStatusRequest::new();
    request.recvWindow = Some(recv_window());
    request.omitZeroBalances = Some(true);
    assert_signed_serialization(request, &["omitZeroBalances=true"]);
}

#[test]
fn amend_order_request_serializes_common_signed_fields() {
    let mut request = BinanceAmendOrderRequest::new("BTCUSDT", Decimal::new(5, 1));
    request.recvWindow = Some(recv_window());
    assert_signed_serialization(request, &["newQty=0.5", "symbol=BTCUSDT"]);
}

#[test]
fn asset_limits_request_serializes_common_signed_fields() {
    let mut request = BinanceAssetLimitsRequest::new("BTCUSDT");
    request.recvWindow = Some(recv_window());
    assert_signed_serialization(request, &["symbol=BTCUSDT"]);
}

#[test]
fn cancel_all_open_orders_request_serializes_common_signed_fields() {
    let mut request = BinanceCancelAllOpenOrdersRequest::new("BTCUSDT");
    request.recvWindow = Some(recv_window());
    assert_signed_serialization(request, &["symbol=BTCUSDT"]);
}

#[test]
fn cancel_order_request_serializes_common_signed_fields() {
    let mut request = BinanceCancelOrderRequest::new("BTCUSDT");
    request.recvWindow = Some(recv_window());
    request.orderId = Some(42);
    assert_signed_serialization(request, &["orderId=42", "symbol=BTCUSDT"]);
}

#[test]
fn open_orders_status_request_serializes_common_signed_fields() {
    let mut request = BinanceOpenOrdersStatusRequest::new();
    request.recvWindow = Some(recv_window());
    request.symbol = Some("BTCUSDT".into());
    assert_signed_serialization(request, &["symbol=BTCUSDT"]);
}

#[test]
fn order_status_request_serializes_common_signed_fields() {
    let mut request = BinanceOrderStatusRequest::new("BTCUSDT");
    request.recvWindow = Some(recv_window());
    request.origClientOrderId = Some("client-order".into());
    assert_signed_serialization(
        request,
        &["origClientOrderId=client-order", "symbol=BTCUSDT"],
    );
}

#[test]
fn spot_order_request_serializes_common_signed_fields() {
    let mut request = BinanceSpotOrderRequest::new(
        "BTCUSDT",
        BinanceSide::BUY,
        BinanceOrderType::LIMIT,
        Decimal::new(5, 1),
    );
    request.recvWindow = Some(recv_window());
    assert_signed_serialization(
        request,
        &["quantity=0.5", "side=BUY", "symbol=BTCUSDT", "type=LIMIT"],
    );
}

#[test]
fn session_logon_request_serializes_common_signed_fields() {
    let mut request = BinanceSessionLogonRequest::new();
    request.recvWindow = Some(recv_window());
    assert_signed_serialization(request, &[]);
}
