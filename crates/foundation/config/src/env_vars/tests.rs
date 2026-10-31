use super::*;
use platform_errors::ConfigError;

fn env_vars<I, K, V>(values: I) -> EnvVars
where
    I: IntoIterator<Item = (K, V)>,
    K: Into<OsString>,
    V: Into<OsString>,
{
    EnvVars::from_map(values)
}

#[test]
fn required_reads_a_value() {
    let vars = env_vars([("NAME", "value")]);
    assert_eq!(vars.required("NAME").unwrap(), "value");
}

#[test]
fn required_reports_missing_and_empty_values() {
    let vars = env_vars(Vec::<(String, String)>::new());
    assert!(matches!(
        vars.required("MISSING").unwrap_err(),
        BpError::Config(ConfigError::Missing { name }) if name == "MISSING"
    ));

    let vars = env_vars([("EMPTY", "")]);
    assert!(matches!(
        vars.required("EMPTY").unwrap_err(),
        BpError::Config(ConfigError::Empty { name }) if name == "EMPTY"
    ));
}

#[test]
fn required_parsed_reports_invalid_values() {
    let vars = env_vars([("COUNT", "not a number")]);
    assert!(matches!(
        vars.required_parsed::<usize>("COUNT").unwrap_err(),
        BpError::Config(ConfigError::Invalid { name, .. }) if name == "COUNT"
    ));
}

#[test]
fn optional_parsed_uses_defaults_only_for_missing_or_empty_values() {
    let vars = env_vars([("COUNT", "42"), ("EMPTY", "")]);
    assert_eq!(vars.optional_parsed_or("COUNT", 1).unwrap(), 42);
    assert_eq!(vars.optional_parsed_or("MISSING", 7).unwrap(), 7);
    assert_eq!(vars.optional_parsed_or("EMPTY", 9).unwrap(), 9);

    let vars = env_vars([("COUNT", "not a number")]);
    assert!(matches!(
        vars.optional_parsed_or("COUNT", 1).unwrap_err(),
        BpError::Config(ConfigError::Invalid { name, .. }) if name == "COUNT"
    ));
}

#[test]
fn required_nonzero_rejects_zero() {
    let vars = env_vars([("COUNT", "0")]);
    assert!(vars.required_nonzero_usize("COUNT").is_err());
    assert!(vars.required_nonzero_u64("COUNT").is_err());
}

#[test]
fn required_duration_uses_seconds() {
    let vars = env_vars([("DURATION_SECS", "42")]);
    assert_eq!(
        vars.required_duration_secs("DURATION_SECS").unwrap(),
        Duration::from_secs(42)
    );
}

#[test]
fn positive_durations_reject_zero_and_invalid_values() {
    let vars = env_vars([("INTERVAL_SECS", "30")]);
    assert_eq!(
        vars.required_positive_duration_secs("INTERVAL_SECS")
            .unwrap(),
        Duration::from_secs(30)
    );
    assert_eq!(
        vars.optional_positive_duration_secs("INTERVAL_SECS", 5)
            .unwrap(),
        Duration::from_secs(30)
    );
    assert_eq!(
        env_vars(Vec::<(String, String)>::new())
            .optional_positive_duration_secs("INTERVAL_SECS", 5)
            .unwrap(),
        Duration::from_secs(5)
    );

    for value in ["0", "not a number"] {
        let vars = env_vars([("INTERVAL_SECS", value)]);
        assert!(
            vars.required_positive_duration_secs("INTERVAL_SECS")
                .is_err()
        );
        assert!(
            vars.optional_positive_duration_secs("INTERVAL_SECS", 5)
                .is_err()
        );
    }
}

#[test]
fn required_url_validates_the_scheme() {
    let vars = env_vars([("URL", "wss://example.test/ws")]);
    assert_eq!(
        vars.required_url("URL", &["ws", "wss"]).unwrap(),
        "wss://example.test/ws"
    );
    assert!(vars.required_url("URL", &["http", "https"]).is_err());
}

#[test]
fn required_timestamp_accepts_rfc3339_and_dates() {
    let vars = env_vars([("START", "2024-01-02")]);
    assert_eq!(
        vars.required_timestamp("START").unwrap().to_rfc3339(),
        "2024-01-02T00:00:00+00:00"
    );

    let vars = env_vars([("START", "2024-01-02T03:04:05Z")]);
    assert_eq!(
        vars.required_timestamp("START").unwrap().to_rfc3339(),
        "2024-01-02T03:04:05+00:00"
    );
}

#[test]
fn required_log_filter_rejects_invalid_directives() {
    let vars = env_vars([("RUST_LOG", "not a valid [directive")]);
    assert!(vars.required_log_filter("RUST_LOG").is_err());
}

#[test]
#[cfg(unix)]
fn required_reports_not_unicode_without_exposing_the_value() {
    use std::os::unix::ffi::OsStringExt;

    let vars = env_vars([(
        OsString::from("SECRET"),
        OsString::from_vec(b"super-secret-\xff".to_vec()),
    )]);
    let error = vars.required("SECRET").unwrap_err();

    assert!(matches!(
        &error,
        BpError::Config(ConfigError::NotUnicode { name }) if name == "SECRET"
    ));
    assert!(!error.to_string().contains("super-secret"));
}
