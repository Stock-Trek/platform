use super::*;
use platform_errors::{BpError, GitHubError};

fn allowlist() -> HostAllowlist {
    HostAllowlist::from_hosts([DEFAULT_HOST, "ghe.example.com"]).unwrap()
}

fn enterprise_allowlist() -> HostAllowlist {
    HostAllowlist::from_hosts([DEFAULT_HOST, "ghe.example.com:8443"]).unwrap()
}

#[test]
fn from_hosts_always_allows_default_host() {
    let allowlist = HostAllowlist::from_hosts(["ghe.example.com"]).unwrap();
    assert_eq!(
        allowlist.validate_url("https://github.com/").unwrap(),
        DEFAULT_HOST
    );
    assert_eq!(allowlist.validate("github.com:443").unwrap(), DEFAULT_HOST);
}

#[test]
fn from_hosts_forces_default_port_for_default_host() {
    let allowlist = HostAllowlist::from_hosts(["github.com:8443"]).unwrap();
    assert_eq!(
        allowlist.validate_url("https://github.com/").unwrap(),
        DEFAULT_HOST
    );
    assert!(allowlist.validate("github.com:8443").is_err());
}

#[test]
fn accepts_https_allowlisted_host() {
    assert_eq!(
        allowlist()
            .validate_url("https://github.com/Stock-Trek/platform")
            .unwrap(),
        "github.com"
    );
}

#[test]
fn accepts_enterprise_host_and_explicit_https_port() {
    assert_eq!(
        allowlist()
            .validate_url("https://ghe.example.com/")
            .unwrap(),
        "ghe.example.com"
    );
    assert_eq!(
        allowlist()
            .validate_url("https://ghe.example.com:443/")
            .unwrap(),
        "ghe.example.com"
    );
}

#[test]
fn accepts_enterprise_host_on_configured_port() {
    assert_eq!(
        enterprise_allowlist()
            .validate_url("https://ghe.example.com:8443/")
            .unwrap(),
        "ghe.example.com:8443"
    );
}

#[test]
fn rejects_enterprise_host_on_unconfigured_port() {
    assert!(
        enterprise_allowlist()
            .validate_url("https://ghe.example.com/")
            .is_err()
    );
    assert!(
        enterprise_allowlist()
            .validate_url("https://ghe.example.com:443/")
            .is_err()
    );
    assert!(
        enterprise_allowlist()
            .validate_url("https://ghe.example.com:9999/")
            .is_err()
    );
}

#[test]
fn lowercases_uppercase_host() {
    assert_eq!(
        allowlist().validate_url("https://GITHUB.COM/").unwrap(),
        "github.com"
    );
    assert_eq!(
        allowlist().validate_url("HTTPS://GitHub.Com/").unwrap(),
        "github.com"
    );
}

#[test]
fn rejects_non_https_schemes() {
    for url in [
        "http://github.com/",
        "ftp://github.com/",
        "file:///etc/passwd",
        "ws://github.com/",
        "//github.com/",
    ] {
        assert!(allowlist().validate_url(url).is_err(), "accepted {url}");
    }
}

#[test]
fn rejects_non_allowlisted_host() {
    assert!(matches!(
        allowlist().validate_url("https://evil.com/"),
        Err(BpError::GitHub(GitHubError::HostNotAllowed { .. }))
    ));
}

#[test]
fn rejects_non_443_ports() {
    for url in [
        "https://github.com:80/",
        "https://github.com:8443/",
        "https://github.com:22/",
    ] {
        assert!(allowlist().validate_url(url).is_err(), "accepted {url}");
    }
}

#[test]
fn rejects_userinfo_tricks() {
    for url in [
        "https://github.com@evil.com/",
        "https://evil.com@github.com/",
        "https://user:secret@github.com/",
    ] {
        assert!(allowlist().validate_url(url).is_err(), "accepted {url}");
    }
}

#[test]
fn rejects_percent_encoded_non_allowlisted_host() {
    assert!(allowlist().validate_url("https://evil%2Ecom/").is_err());
}

#[test]
fn normalizes_percent_encoded_allowlisted_host() {
    assert_eq!(
        allowlist().validate_url("https://%67ithub.com/").unwrap(),
        "github.com"
    );
}

#[test]
fn validate_lowercases_and_strips_default_port() {
    assert_eq!(
        allowlist().validate("GITHUB.COM:443").unwrap(),
        "github.com"
    );
}

#[test]
fn validate_rejects_non_default_port() {
    assert!(allowlist().validate("github.com:80").is_err());
    assert!(allowlist().validate("github.com:8443").is_err());
}

#[test]
fn validate_allows_configured_enterprise_port() {
    assert_eq!(
        enterprise_allowlist()
            .validate("ghe.example.com:8443")
            .unwrap(),
        "ghe.example.com:8443"
    );
}

#[test]
fn configured_explicit_443_is_treated_as_default() {
    let allowlist = HostAllowlist::from_hosts([DEFAULT_HOST, "ghe.example.com:443"]).unwrap();
    for url in ["https://ghe.example.com/", "https://ghe.example.com:443/"] {
        assert_eq!(
            allowlist.validate_url(url).unwrap(),
            "ghe.example.com",
            "url={url}"
        );
    }
    assert_eq!(
        allowlist.validate("ghe.example.com:443").unwrap(),
        "ghe.example.com"
    );
}

#[test]
fn validate_url_and_validate_agree_on_every_host() {
    for (url, host) in [
        ("https://github.com/", "github.com"),
        ("https://github.com:443/", "github.com"),
        ("https://ghe.example.com:8443/", "ghe.example.com:8443"),
        ("https://ghe.example.com/", "ghe.example.com"),
        ("https://ghe.example.com:9999/", "ghe.example.com:9999"),
        ("https://github.com:8443/", "github.com:8443"),
        ("https://evil.com/", "evil.com"),
    ] {
        let from_url = enterprise_allowlist().validate_url(url);
        let from_host = enterprise_allowlist().validate(host);
        assert_eq!(
            from_url.is_ok(),
            from_host.is_ok(),
            "url={url} host={host} disagreed: {from_url:?} vs {from_host:?}"
        );
        if let (Ok(from_url), Ok(from_host)) = (from_url, from_host) {
            assert_eq!(from_url, from_host, "url={url} host={host}");
        }
    }
}
