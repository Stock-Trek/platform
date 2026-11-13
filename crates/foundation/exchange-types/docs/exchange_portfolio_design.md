# Exchange portfolio design

The portfolio integration separates three orthogonal concerns:

1. `CredentialProvider` obtains, renews, and revokes the right to listen.
2. `AuthBinder` attaches that right to a WebSocket session.
3. `SessionLifecycle` describes when to renew, reconnect, or re-authenticate.

An exchange composes one implementation of each concern into a `PortfolioAuth`, which is exposed through the `ExchangeAdapter` trait. The gateway only depends on that trait, so exchange-specific auth code stays in the exchange crate.

```rust
pub enum AuthError {
    Acquire(String),
    Renew(String),
    Revoke(String),
    Bind(String),
    Sign(String),
    Transport(String),
    Expired,
}

pub struct Credential {
    pub value: String,
    pub issued_at: Instant,
    pub expires_at: Option<Instant>,
    pub renewable: bool,
}

#[async_trait]
pub trait CredentialProvider: Send + Sync {
    async fn acquire(&self) -> Result<Option<Credential>, AuthError>;
    async fn renew(&self, credential: &Credential) -> Result<Credential, AuthError>;
    async fn revoke(&self, credential: &Credential) -> Result<(), AuthError>;
}

pub type ConfirmationPredicate = Box<dyn Fn(&IncomingFrame) -> bool + Send + Sync>;

#[async_trait]
pub trait AuthBinder: Send + Sync {
    fn prepare_connect(
        &self,
        request: &mut ConnectRequest,
        credential: Option<&Credential>,
        signer: &Signer,
    ) -> Result<(), AuthError> {
        Ok(())
    }

    async fn bind(
        &self,
        connection: &dyn WsConnection,
        credential: Option<&Credential>,
        signer: &Signer,
    ) -> Result<(), AuthError> {
        Ok(())
    }

    fn confirmation_predicate(&self) -> Option<ConfirmationPredicate> {
        None
    }
}

pub struct RenewalSchedule {
    pub interval: Duration,
    pub lead_time: Duration,
}

pub struct SessionLifecycle {
    max_session_lifetime: Option<Duration>,
    scheduled_renewal: Option<RenewalSchedule>,
    reconnect_reuses_credential: bool,
    revoke_on_close: bool,
}

pub trait ExchangeAdapter: Send + Sync {
    fn credential_provider(&self) -> &dyn CredentialProvider;
    fn auth_binder(&self) -> &dyn AuthBinder;
    fn lifecycle(&self) -> SessionLifecycle;
    fn signer(&self) -> &Signer;
}

pub struct PortfolioAuth<P, B> {
    credential_provider: P,
    auth_binder: B,
    lifecycle: SessionLifecycle,
    signer: Signer,
}
```

`ETExchangePortfolio` also supplies the exchange-specific REST snapshot request, snapshot parser, and account-event parser. It builds its adapter from a `PortfolioAuthContext` containing the account credentials, HTTP client, REST URL, and renewal interval.

```rust
pub trait ETExchangePortfolio: ETExchange {
    type SnapshotRequest: ETHttpRequest<Exchange = Self> + Send;
    type Adapter: ExchangeAdapter;

    fn account_snapshot_request() -> Self::SnapshotRequest;
    fn account_snapshot(
        response: <Self::SnapshotRequest as ETHttpRequest>::Response,
        mapping: &dyn Fn(&str) -> Option<AssetId>,
    ) -> BuildResult<AccountSnapshot>;
    fn account_event(
        value: &serde_json::Value,
        mapping: &dyn Fn(&str) -> Option<AssetId>,
    ) -> BuildResult<Option<AccountEvent>>;
    fn adapter(&self, context: PortfolioAuthContext) -> BuildResult<Self::Adapter>;
}
```
