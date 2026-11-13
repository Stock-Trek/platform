# New ETExchangePortfolio design

```rust
//! Generic abstraction over private WebSocket stream authentication.
//!
//! Three orthogonal concerns:
//!   1. `CredentialProvider`   — how to obtain / renew / revoke a right-to-listen
//!   2. `AuthBinder`           — how that right is attached to a WS session
//!   3. `SessionLifecyclePolicy` — when to renew, reconnect, or re-auth (declarative)

use async_trait::async_trait;
use std::time::{Duration, Instant};
use url::Url;

# [derive(Debug)]
pub enum AuthError {
    Acquire(String),
    Renew(String),
    Revoke(String),
    Bind(String),
    Sign(String),
    Transport(String),
    Expired,
}

/// Opaque credential. Different exchanges stuff different things in `value`
/// (a listenKey, a JWT, an opaque token). The engine treats it as bytes.
# [derive(Clone, Debug)]
pub struct Credential {
    pub value: String,
    pub issued_at: Instant,
    pub expires_at: Option<Instant>,
    pub renewable: bool,
}

impl Credential {
    pub fn is_expired(&self, now: Instant) -> bool {
        self.expires_at.map_or(false, |e| now >= e)
    }

    pub fn expires_within(&self, now: Instant, window: Duration) -> bool {
        self.expires_at
            .map_or(false, |e| e.saturating_duration_since(now) <= window)
    }
}

/// Carries both identity and signing ability, so binders never touch secrets.
pub trait Signer: Send + Sync {
    fn api_key(&self) -> &str;
    fn sign(&self, payload: &[u8]) -> Result<Vec<u8>, AuthError>;
}

/// Connection primitives a post-connect binder operates on.
pub struct ConnectRequest {
    pub url: Url,
    pub headers: Vec<(String, String)>,
}

# [async_trait]
pub trait WsConnection: Send + Sync {
    async fn send_text(&self, payload: &str) -> Result<(), AuthError>;
    async fn send_binary(&self, payload: &[u8]) -> Result<(), AuthError>;
}

pub struct IncomingFrame {
    pub text: Option<String>,
    pub binary: Option<Vec<u8>>,
}

/// ---------------------------------------------------------------------------
/// Trait 1 — How do I obtain the right to listen?
/// ---------------------------------------------------------------------------
///
/// `acquire` returns `Ok(None)` when no credential is required
/// (Bybit, OKX, Bitget, Gate — authentication happens entirely over the socket).
# [async_trait]
pub trait CredentialProvider: Send + Sync {
    /// Idempotent per account: Binance returns the same listenKey if one exists.
    async fn acquire(&self) -> Result<Option<Credential>, AuthError>;

    /// Extend or rotate an existing credential.
    /// Only called when `credential.renewable == true` and the lifecycle policy
    /// schedules renewal. MUST NOT touch the socket.
    async fn renew(&self, credential: &Credential) -> Result<Credential, AuthError>;

    /// Best-effort invalidation on graceful close.
    async fn revoke(&self, credential: &Credential) -> Result<(), AuthError>;
}

/// ---------------------------------------------------------------------------
/// Trait 2 — How is the right attached to the WS session?
/// ---------------------------------------------------------------------------
///
/// Both hooks have no-op defaults, so:
///   - pre-connect binders (Binance URL, Kraken query) implement only `prepare_connect`
///   - post-connect binders (Bybit/OKX/Bitget/Gate login frame) implement only `bind`
pub type ConfirmationPredicate = Box<dyn Fn(&IncomingFrame) -> bool + Send + Sync>;

# [async_trait]
pub trait AuthBinder: Send + Sync {
    /// URL / query / header binding.
    fn prepare_connect(
        &self,
        _req: &mut ConnectRequest,
        _cred: Option<&Credential>,
        _signer: &dyn Signer,
    ) -> Result<(), AuthError> {
        Ok(())
    }

    /// Frame-based binding after the socket is open.
    async fn bind(
        &self,
        _conn: &dyn WsConnection,
        _cred: Option<&Credential>,
        _signer: &dyn Signer,
    ) -> Result<(), AuthError> {
        Ok(())
    }

    /// If the exchange emits an explicit ack, the engine waits for this.
    /// Return `None` if the stream just begins flowing (Binance Futures).
    fn confirmation_predicate(&self) -> Option<ConfirmationPredicate> {
        None
    }
}

/// ---------------------------------------------------------------------------
/// Trait 3 — When to renew, reconnect, or re-auth? (pure descriptor, no behavior)
/// ---------------------------------------------------------------------------
pub struct RenewalSchedule {
    pub interval: Duration,  // e.g. 60 min
    pub lead_time: Duration, // fire at expiry − lead_time, e.g. 10 min
}

pub trait SessionLifecyclePolicy: Send + Sync {
    /// Hard cap on the WS connection itself (24h on Binance/Bybit; `None` = unbounded).
    fn max_session_lifetime(&self) -> Option<Duration>;

    /// Non-disruptive renewal of the *credential* (Binance Futures keepalive).
    /// `None` means the credential outlives the session, or doesn't exist.
    fn scheduled_renewal(&self) -> Option<RenewalSchedule>;

    /// On socket drop, can the current credential be re-used?
    /// `false` for single-use tokens (Kraken); `true` for Binance/Coinbase.
    fn reconnect_reuses_credential(&self) -> bool;

    /// Should the engine call `revoke()` on graceful close?
    fn revoke_on_close(&self) -> bool;
}

/// ---------------------------------------------------------------------------
/// Adapter bundle — one of each, plus a signer.
/// ---------------------------------------------------------------------------
pub struct ExchangeAdapter {
    pub credential_provider: Box<dyn CredentialProvider>,
    pub auth_binder: Box<dyn AuthBinder>,
    pub lifecycle: Box<dyn SessionLifecyclePolicy>,
    pub signer: Box<dyn Signer>,
}
```
