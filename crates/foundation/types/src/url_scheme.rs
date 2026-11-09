use url::Url;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum UrlScheme {
    Http,
    Https,
    Ws,
    Wss,
    Nats,
    Redis,
    Rediss,
    Valkey,
    Valkeys,
}

impl UrlScheme {
    pub const HTTP: [Self; 2] = [Self::Http, Self::Https];
    pub const WEBSOCKET: [Self; 2] = [Self::Ws, Self::Wss];

    pub fn from_url(url: &Url) -> Option<Self> {
        Self::from_scheme(url.scheme())
    }

    pub fn from_scheme(scheme: &str) -> Option<Self> {
        match scheme {
            "http" => Some(Self::Http),
            "https" => Some(Self::Https),
            "ws" => Some(Self::Ws),
            "wss" => Some(Self::Wss),
            "nats" => Some(Self::Nats),
            "redis" => Some(Self::Redis),
            "rediss" => Some(Self::Rediss),
            "valkey" => Some(Self::Valkey),
            "valkeys" => Some(Self::Valkeys),
            _ => None,
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Self::Http => "http",
            Self::Https => "https",
            Self::Ws => "ws",
            Self::Wss => "wss",
            Self::Nats => "nats",
            Self::Redis => "redis",
            Self::Rediss => "rediss",
            Self::Valkey => "valkey",
            Self::Valkeys => "valkeys",
        }
    }

    pub fn is_http(self) -> bool {
        matches!(self, Self::Http | Self::Https)
    }

    pub fn is_https(self) -> bool {
        self == Self::Https
    }

    pub fn is_nats(self) -> bool {
        self == Self::Nats
    }

    pub fn is_valkey(self) -> bool {
        matches!(
            self,
            Self::Redis | Self::Rediss | Self::Valkey | Self::Valkeys
        )
    }
}
