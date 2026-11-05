use platform_config::env_names::GITHUB_ENTERPRISE_ALLOWED_HOSTS;
use platform_errors::{BpError, BpResult, GitHubError};
use std::{
    collections::HashMap,
    net::{Ipv4Addr, Ipv6Addr},
};
use url::{Host, Url};

pub const DEFAULT_HOST: &str = "github.com";
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
                GITHUB_ENTERPRISE_ALLOWED_HOSTS,
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
