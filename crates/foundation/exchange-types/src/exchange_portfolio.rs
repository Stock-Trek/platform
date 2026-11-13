pub use crate::portfolio::{
    AuthBinder, AuthError, ConfirmationPredicate, ConnectRequest, Credential, CredentialProvider,
    ExchangeAdapter, IncomingFrame, PortfolioSigner, RenewalSchedule, SessionLifecyclePolicy,
    Signer, WsConnection,
};
use crate::{
    exchange::ETExchange,
    portfolio::{AccountEvent, AccountSnapshot},
    request::ETHttpRequest,
};
use platform_errors::BuildResult;
use stock_trek_types::cex::asset_id::AssetId;

pub trait ETExchangePortfolio: ETExchange {
    type SnapshotRequest: ETHttpRequest<Exchange = Self> + Send;

    fn account_snapshot_request() -> Self::SnapshotRequest;
    fn account_snapshot(
        response: <Self::SnapshotRequest as ETHttpRequest>::Response,
        mapping: &dyn Fn(&str) -> Option<AssetId>,
    ) -> BuildResult<AccountSnapshot>;
    fn account_event(
        value: &serde_json::Value,
        mapping: &dyn Fn(&str) -> Option<AssetId>,
    ) -> BuildResult<Option<AccountEvent>>;
}
