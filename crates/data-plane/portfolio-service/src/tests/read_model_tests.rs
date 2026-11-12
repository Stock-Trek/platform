use crate::tests::test_feed_tests::Fixture;
use crate::{
    PortfolioUpdateMetadata,
    read_model::{OwnershipRecord, PortfolioReadModel, PortfolioRecord},
};
use chrono::Utc;
use stock_trek_types::cex::asset_id::AssetId;

struct ReadModelFixture;

impl ReadModelFixture {
    fn record(
        account: crate::AccountKey,
        total: f64,
        version: u64,
    ) -> (PortfolioRecord, PortfolioUpdateMetadata) {
        let now = Utc::now();
        let metadata = PortfolioUpdateMetadata::new(version, now, None, now);
        let portfolio = Fixture::portfolio(AssetId::Bitcoin, total);
        (
            PortfolioRecord::from_parts(account, &portfolio, &metadata),
            metadata,
        )
    }
}

#[tokio::test]
async fn restores_ownership_portfolios_and_feed_subscriptions() {
    let (feed, service) = Fixture::service();
    let account = Fixture::account("account-1");
    let (record, _) = ReadModelFixture::record(account.clone(), 2.5, 7);
    let model = PortfolioReadModel::new(
        vec![OwnershipRecord::new(account.clone(), "alice", 4)],
        vec![record],
    );

    service.restore(model).unwrap();
    service.sync_feed().await.unwrap();

    assert!(service.is_tracked_for_owner("alice", &account));
    assert_eq!(service.ownership_version(&account), Some(4));
    assert_eq!(
        service
            .account_portfolio(&account)
            .unwrap()
            .asset_total(AssetId::Bitcoin),
        2.5
    );
    assert_eq!(service.update_metadata(&account).unwrap().version(), 7);
    assert_eq!(feed.calls()[0].accounts.len(), 1);
}

#[tokio::test]
async fn ignores_portfolios_for_untracked_accounts() {
    let (_, service) = Fixture::service();
    let account = Fixture::account("orphan");
    let (record, _) = ReadModelFixture::record(account.clone(), 9.0, 1);

    service
        .restore(PortfolioReadModel::new(Vec::new(), vec![record]))
        .unwrap();

    assert!(service.account_portfolio(&account).is_none());
    assert!(service.update_metadata(&account).is_none());
}

#[test]
fn portfolio_records_round_trip_through_json() {
    let account = Fixture::account("account-1");
    let (record, metadata) = ReadModelFixture::record(account.clone(), 3.0, 2);
    let payload = serde_json::to_vec(&record).unwrap();
    let restored: PortfolioRecord = serde_json::from_slice(&payload).unwrap();
    assert_eq!(restored.account(), &account);
    assert_eq!(restored.metadata().version(), metadata.version());
    let portfolio = restored.into_portfolio().unwrap();

    assert_eq!(portfolio.asset_total(AssetId::Bitcoin), 3.0);
}
