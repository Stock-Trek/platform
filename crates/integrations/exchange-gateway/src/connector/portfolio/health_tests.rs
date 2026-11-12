use super::{
    CONNECTOR_HEALTH_STREAM_PREFIX, CONNECTOR_HEALTH_SUBJECT, ConnectorHealth, ConnectorState,
};
use chrono::Utc;
use stock_trek_types::cex::cex_id::CexId;

#[test]
fn connector_health_round_trips_for_the_published_subject() {
    let health = ConnectorHealth::new(
        CexId::Binance,
        ConnectorState::Connected,
        2,
        Some(4),
        Utc::now(),
    );
    let decoded = ConnectorHealth::from_bytes(&health.to_bytes().expect("encode")).expect("decode");

    assert_eq!(decoded, health);
    assert_eq!(decoded.state(), ConnectorState::Connected);
    assert_eq!(decoded.last_update_age_seconds(), Some(4));
    assert!(CONNECTOR_HEALTH_SUBJECT.starts_with(CONNECTOR_HEALTH_STREAM_PREFIX));
}
