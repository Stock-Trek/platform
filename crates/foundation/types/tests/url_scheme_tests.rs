use platform_types::UrlScheme;
use url::Url;

#[test]
fn parses_known_schemes() {
    for (value, expected) in [
        ("http", UrlScheme::Http),
        ("https", UrlScheme::Https),
        ("ws", UrlScheme::Ws),
        ("wss", UrlScheme::Wss),
        ("nats", UrlScheme::Nats),
        ("redis", UrlScheme::Redis),
        ("rediss", UrlScheme::Rediss),
        ("valkey", UrlScheme::Valkey),
        ("valkeys", UrlScheme::Valkeys),
    ] {
        assert_eq!(UrlScheme::from_scheme(value), Some(expected));
        assert_eq!(expected.as_str(), value);
    }
}

#[test]
fn rejects_unknown_schemes() {
    assert_eq!(UrlScheme::from_scheme("ftp"), None);
    assert_eq!(UrlScheme::from_scheme("HTTP"), None);
}

#[test]
fn parses_schemes_from_urls() {
    let url = Url::parse("https://example.test/path").unwrap();
    assert_eq!(UrlScheme::from_url(&url), Some(UrlScheme::Https));
}

#[test]
fn classifies_scheme_families() {
    assert!(UrlScheme::Http.is_http());
    assert!(UrlScheme::Https.is_http());
    assert!(!UrlScheme::Ws.is_http());

    assert!(UrlScheme::Https.is_https());
    assert!(!UrlScheme::Http.is_https());

    assert!(UrlScheme::Nats.is_nats());
    assert!(!UrlScheme::Http.is_nats());

    for scheme in [
        UrlScheme::Redis,
        UrlScheme::Rediss,
        UrlScheme::Valkey,
        UrlScheme::Valkeys,
    ] {
        assert!(scheme.is_valkey());
    }
    assert!(!UrlScheme::Http.is_valkey());
}
