use super::*;

#[test]
fn parses_delta_seconds() {
    assert_eq!(
        RetryAfter::parse_retry_after("120"),
        Some(Duration::from_secs(120))
    );
}

#[test]
fn ignores_immediate_delta_seconds() {
    assert_eq!(RetryAfter::parse_retry_after("0"), None);
}

#[test]
fn ignores_invalid_values() {
    assert_eq!(RetryAfter::parse_retry_after("not-a-date"), None);
}

#[test]
fn ignores_http_date_forms_in_the_past() {
    for value in [
        "Sun, 06 Nov 1994 08:49:37 GMT",
        "Sunday, 06-Nov-94 08:49:37 GMT",
        "Sun Nov  6 08:49:37 1994",
    ] {
        assert_eq!(RetryAfter::parse_retry_after(value), None);
    }
}

#[test]
fn parses_http_date_in_the_future() {
    let value = httpdate::fmt_http_date(SystemTime::now() + Duration::from_secs(3_600));
    let parsed = RetryAfter::parse_retry_after(&value).expect("future HTTP-date should parse");
    assert!(parsed > Duration::from_secs(3_500) && parsed <= Duration::from_secs(3_600));
}
