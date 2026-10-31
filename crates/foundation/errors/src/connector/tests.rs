use super::*;

fn source() -> BpError {
    BpError::external("test failure")
}

#[test]
fn send_failure_reports_the_send_outcome() {
    assert_eq!(
        ConnectorError::send_not_sent(source()).send_failure(),
        Some(SendFailure::NotSent)
    );
    assert_eq!(
        ConnectorError::send_failed(source()).send_failure(),
        Some(SendFailure::Failed)
    );
    assert_eq!(
        ConnectorError::send_unknown(source()).send_failure(),
        Some(SendFailure::Unknown)
    );
}

#[test]
fn non_send_errors_have_no_send_failure() {
    assert_eq!(ConnectorError::NotConnected.send_failure(), None);
}

#[test]
fn converting_a_bp_error_does_not_claim_the_request_was_not_sent() {
    let converted = ConnectorError::from(source());

    assert_eq!(converted.send_failure(), Some(SendFailure::Unknown));
    assert!(
        !converted
            .send_failure()
            .is_some_and(SendFailure::is_not_sent)
    );
}

#[test]
fn converting_an_existing_connector_error_preserves_it() {
    let converted = ConnectorError::from(BpError::Connector(ConnectorError::NotConnected));

    assert!(matches!(converted, ConnectorError::NotConnected));
    assert_eq!(converted.send_failure(), None);
}
