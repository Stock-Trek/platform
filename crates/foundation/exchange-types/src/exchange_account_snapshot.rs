pub use crate::portfolio::{AccountEvent, AccountSnapshot};
use crate::{exchange::ETExchange, request::ETHttpRequest};
use platform_errors::BuildResult;
use stock_trek_types::cex::asset_id::AssetId;

pub struct AccountSnapshotContext<'a> {
    mapping: &'a dyn Fn(&str) -> Option<AssetId>,
}

impl<'a> AccountSnapshotContext<'a> {
    pub fn new(mapping: &'a dyn Fn(&str) -> Option<AssetId>) -> Self {
        Self { mapping }
    }

    pub fn mapping(&self) -> &dyn Fn(&str) -> Option<AssetId> {
        self.mapping
    }
}

pub trait ETExchangeAccountSnapshot: ETExchange {
    type SnapshotRequest: ETHttpRequest<Exchange = Self> + Send;

    fn account_snapshot_request() -> Self::SnapshotRequest;
    fn account_snapshot(
        response: <Self::SnapshotRequest as ETHttpRequest>::Response,
        context: &AccountSnapshotContext<'_>,
    ) -> BuildResult<AccountSnapshot>;
    fn account_event(
        value: &serde_json::Value,
        context: &AccountSnapshotContext<'_>,
    ) -> BuildResult<Option<AccountEvent>>;
}
