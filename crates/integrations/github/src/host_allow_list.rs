use platform_errors::{BpError, BpResult, GitHubError};
use std::{
    collections::HashMap,
    net::{Ipv4Addr, Ipv6Addr},
};
use url::{Host, Url};

pub const DEFAULT_HOST: &str = "github.com";
const ENTERPRISE_ALLOWED_HOSTS: &str = "GITHUB_ENTERPRISE_ALLOWED_HOSTS";
const DEFAULT_HTTPS_PORT: u16 = 443;

/// The port policy for a single allowlisted host. Ports are owned by the
/// allowlist entry, never derived from an incoming URL, so every caller
/// applies the same rule for the same host.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct AllowedPort(Option<u16>);

impl AllowedPort {
    /// The default HTTPS policy, used for hosts configured without an explicit
    /// port (such as `github.com`). It accepts an absent port or `:443`.
    const DEFAULT: Self = Self(None);

    fn configured(port: Option<u16>) -> Self {
        match port {
            Some(DEFAULT_HTTPS_PORT) => Self::DEFAULT,
            other => Self(other),
        }
    }

    /// Whether `port`, parsed from untrusted input, is permitted by this
    /// policy. An absent port and an explicit `443` are equivalent.
    fn allows(self, port: Option<u16>) -> bool {
        match self.0 {
            None => port.is_none() || port == Some(DEFAULT_HTTPS_PORT),
            Some(allowed) => port == Some(allowed),
        }
    }

    /// The port to use when rebuilding an outbound host. Both "absent" and
    /// `443` are rendered without an explicit port.
    fn effective(self) -> Option<u16> {
        match self.0 {
            None | Some(DEFAULT_HTTPS_PORT) => None,
            Some(port) => Some(port),
        }
    }
}

#[derive(Debug, Clone)]
pub struct HostAllowlist {
    hosts: HashMap<String, AllowedPort>,
}

impl HostAllowlist {
    /// Builds an allowlist from an explicit set of hosts. Useful for tests and
    /// for embedders that resolve the host list from somewhere other than the
    /// environment.
    ///
    /// [`DEFAULT_HOST`] is always included with the default HTTPS port, even if
    /// it is omitted from `hosts` or configured with another port.
    pub fn from_hosts<I, S>(hosts: I) -> BpResult<Self>
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        let mut map = HashMap::new();
        for host in hosts {
            let (name, port) = Self::validated(host.as_ref())?;
            map.insert(name, AllowedPort::configured(port));
        }
        map.insert(DEFAULT_HOST.to_owned(), AllowedPort::DEFAULT);
        Ok(Self { hosts: map })
    }

    pub fn allows(&self, host: &str) -> bool {
        self.validate(host).is_ok()
    }

    pub fn validate(&self, host: &str) -> Result<String, BpError> {
        let (name, port) = Self::parse(host.trim()).ok_or_else(|| {
            BpError::GitHub(GitHubError::HostInvalid {
                host: host.to_owned(),
            })
        })?;
        self.check(&name, port, host)
    }

    pub fn normalize(host: &str) -> Option<String> {
        let (name, port) = Self::parse(host.trim())?;
        Some(Self::render(&name, port))
    }

    /// Validates a URL from an untrusted source (such as an `html_url` field on
    /// a GitHub webhook) and returns a host that is safe to use for an outbound
    /// request.
    ///
    /// The URL is parsed with a real URL parser. The scheme must be `https`,
    /// userinfo is rejected, and the host and port are checked against the
    /// allowlist entry. The returned host is rebuilt from the allowlist's
    /// policy, so callers must use it rather than any part of the original
    /// string.
    pub fn validate_url(&self, url: &str) -> Result<String, BpError> {
        let parsed = Url::parse(url).map_err(|_| {
            BpError::GitHub(GitHubError::HostInvalid {
                host: url.to_owned(),
            })
        })?;
        if parsed.scheme() != "https" {
            return Err(BpError::GitHub(GitHubError::HostInvalid {
                host: url.to_owned(),
            }));
        }
        if !parsed.username().is_empty() || parsed.password().is_some() {
            return Err(BpError::GitHub(GitHubError::HostInvalid {
                host: url.to_owned(),
            }));
        }
        let (name, port) = Self::host_from_url(&parsed).ok_or_else(|| {
            BpError::GitHub(GitHubError::HostInvalid {
                host: url.to_owned(),
            })
        })?;
        self.check(&name, port, url)
    }

    /// The single policy check shared by [`HostAllowlist::validate`] and
    /// [`HostAllowlist::validate_url`]. `name` is an already-normalized host
    /// and `port` is the port parsed from `input`; the returned host is rebuilt
    /// from the port owned by the allowlist entry.
    fn check(&self, name: &str, port: Option<u16>, input: &str) -> Result<String, BpError> {
        let Some(policy) = self.hosts.get(name) else {
            return Err(BpError::GitHub(GitHubError::HostNotAllowed {
                host: input.to_owned(),
            }));
        };
        if !policy.allows(port) {
            return Err(BpError::GitHub(GitHubError::HostNotAllowed {
                host: input.to_owned(),
            }));
        }
        Ok(Self::render(name, policy.effective()))
    }

    fn host_from_url(url: &Url) -> Option<(String, Option<u16>)> {
        let name = match url.host()? {
            Host::Domain(name) => Self::normalize_name(name)?,
            Host::Ipv4(address) => address.to_string(),
            Host::Ipv6(address) => format!("[{address}]"),
        };
        Some((name, url.port()))
    }

    fn validated(host: &str) -> BpResult<(String, Option<u16>)> {
        Self::parse(host).ok_or_else(|| {
            BpError::config_invalid(
                ENTERPRISE_ALLOWED_HOSTS,
                format!(
                    "{host:?} is not a hostname, IP address, or host:port (expected e.g. \"ghe.example.com\", \"ghe.example.com:8443\", or \"[2001:db8::1]:8443\")"
                ),
            )
        })
    }

    fn parse(authority: &str) -> Option<(String, Option<u16>)> {
        let (name, port) = Self::split_port(authority)?;
        Some((Self::normalize_name(name)?, port))
    }

    fn render(name: &str, port: Option<u16>) -> String {
        match port {
            Some(port) if port != DEFAULT_HTTPS_PORT => format!("{name}:{port}"),
            _ => name.to_owned(),
        }
    }

    fn split_port(authority: &str) -> Option<(&str, Option<u16>)> {
        if let Some(bracketed) = authority.strip_prefix('[') {
            let (name, rest) = bracketed.split_once(']')?;
            let port = match rest {
                "" => None,
                rest => Some(Self::parse_port(rest.strip_prefix(':')?)?),
            };
            name.parse::<Ipv6Addr>().ok()?;
            return Some((name, port));
        }
        if authority.matches(':').count() > 1 {
            authority.parse::<Ipv6Addr>().ok()?;
            return Some((authority, None));
        }
        match authority.rsplit_once(':') {
            Some((name, port)) => Some((name, Some(Self::parse_port(port)?))),
            None => Some((authority, None)),
        }
    }

    fn parse_port(port: &str) -> Option<u16> {
        let port: u16 = port.parse().ok()?;
        (port != 0).then_some(port)
    }

    fn normalize_name(name: &str) -> Option<String> {
        if name.contains(':') {
            let address: Ipv6Addr = name.parse().ok()?;
            return Some(format!("[{address}]"));
        }
        if let Ok(address) = name.parse::<Ipv4Addr>() {
            return Some(address.to_string());
        }
        let name = name.trim_end_matches('.').to_ascii_lowercase();
        if name.is_empty() || name.len() > 253 {
            return None;
        }
        name.split('.')
            .all(|label| {
                !label.is_empty()
                    && label.len() <= 63
                    && !label.starts_with('-')
                    && !label.ends_with('-')
                    && label.chars().all(|c| c.is_ascii_alphanumeric() || c == '-')
            })
            .then_some(name)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
}
