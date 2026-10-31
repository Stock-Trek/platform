use super::*;

#[test]
fn parses_nats_url_with_auth_and_default_port() {
    let address = NatsAddress::parse("nats://user:secret@localhost").unwrap();

    assert_eq!(address.host, "localhost");
    assert_eq!(address.port, DEFAULT_NATS_PORT);
    assert_eq!(address.username.as_deref(), Some("user"));
    assert_eq!(address.password.as_deref(), Some("secret"));
    assert_eq!(address.server_url(), "nats://localhost:4222");
}

#[test]
fn rejects_non_nats_urls() {
    assert!(NatsAddress::parse("http://localhost:4222").is_err());
}
