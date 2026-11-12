use super::{AccountEvent, AccountSnapshot};
use crate::{
    api_key_credential::ApiKeyCredentials, exchange::ETExchange, request::ETHttpRequest,
    signer::Signer,
};
use platform_errors::BpResult;
use stock_trek_types::cex::{asset_id::AssetId, cex_id::CexId};

pub trait PortfolioExchange: ETExchange<Credentials = ApiKeyCredentials> {
    type ListenKeyRequest: ETHttpRequest<Exchange = Self> + Send;
    type SnapshotRequest: ETHttpRequest<Exchange = Self> + Send;

    fn cex_id(&self) -> CexId;

    fn signer(credentials: &ApiKeyCredentials) -> BpResult<Signer>;

    fn create_listen_key_request() -> Self::ListenKeyRequest;

    fn keepalive_listen_key_request(listen_key: &str) -> Self::ListenKeyRequest;

    fn close_listen_key_request(listen_key: &str) -> Self::ListenKeyRequest;

    fn listen_key(
        response: <Self::ListenKeyRequest as ETHttpRequest>::Response,
    ) -> BpResult<String>;

    fn listen_key_ack(
        response: <Self::ListenKeyRequest as ETHttpRequest>::Response,
    ) -> BpResult<()>;

    fn account_snapshot_request() -> Self::SnapshotRequest;

    fn account_snapshot(
        response: <Self::SnapshotRequest as ETHttpRequest>::Response,
        mapping: &dyn Fn(&str) -> Option<AssetId>,
    ) -> BpResult<AccountSnapshot>;

    fn account_event(
        value: &serde_json::Value,
        mapping: &dyn Fn(&str) -> Option<AssetId>,
    ) -> BpResult<Option<AccountEvent>>;
}
