use crate::{
    api_key_credential::ApiKeyCredentials,
    klines::{KlinesHttpRequest, KlinesResponse, KlinesWebsocketRequest},
    portfolio::{AccountEvent, AccountSnapshot},
    rate_limited::RateLimit,
    request::ETHttpRequest,
    response::{ETHttpResponse, ETWebsocketResponse},
    server_time::{ServerTimeHttpRequest, ServerTimeResponse, ServerTimeWebsocketRequest},
    signer::Signer,
    urls::Urls,
};
use platform_errors::BuildResult;
use platform_types::UsageCount;
use std::collections::HashMap;
use stock_trek_types::cex::{asset_id::AssetId, cex_id::CexId};

pub trait ETExchange {
    type Credentials;
    type ListenKeyRequest: ETHttpRequest<Exchange = Self> + Send;
    type SnapshotRequest: ETHttpRequest<Exchange = Self> + Send;
    type KlinesRequestHttp: KlinesHttpRequest<Self::KlinesResponseHttp>;
    type KlinesResponseHttp: ETHttpResponse + KlinesResponse;
    type KlinesRequestWebsocket: KlinesWebsocketRequest<Self::KlinesResponseWebsocket>;
    type KlinesResponseWebsocket: ETWebsocketResponse + KlinesResponse;
    type ServerTimeRequestHttp: ServerTimeHttpRequest<Self::ServerTimeResponseHttp>;
    type ServerTimeResponseHttp: ETHttpResponse + ServerTimeResponse;
    type ServerTimeRequestWebsocket: ServerTimeWebsocketRequest<Self::ServerTimeResponseWebsocket>;
    type ServerTimeResponseWebsocket: ETWebsocketResponse + ServerTimeResponse;

    fn name(&self) -> &'static str;
    fn default_capacity(&self) -> HashMap<RateLimit, UsageCount>;
    fn server_time_request_http(&self) -> Self::ServerTimeRequestHttp;
    fn server_time_request_websocket(&self) -> Self::ServerTimeRequestWebsocket;
    fn urls(&self) -> &impl Urls;
    fn cex_id(&self) -> CexId;
    fn signer(credentials: &ApiKeyCredentials) -> BuildResult<Signer>;
    fn create_listen_key_request() -> Self::ListenKeyRequest;
    fn keepalive_listen_key_request(listen_key: &str) -> Self::ListenKeyRequest;
    fn close_listen_key_request(listen_key: &str) -> Self::ListenKeyRequest;
    fn listen_key(
        response: <Self::ListenKeyRequest as ETHttpRequest>::Response,
    ) -> BuildResult<String>;
    fn listen_key_ack(
        response: <Self::ListenKeyRequest as ETHttpRequest>::Response,
    ) -> BuildResult<()>;
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
