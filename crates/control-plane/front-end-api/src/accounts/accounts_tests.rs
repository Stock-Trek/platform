use crate::accounts::account_types::{
    AccountSummary, ConnectorState, CredentialsRequest, LinkAccountRequest,
};
use crate::accounts::portfolio_types::{OrderType, PendingOrder, PortfolioSnapshot};
use crate::accounts::{AccountService, AccountStore};
use crate::algorithms::BuildStore;
use crate::auth::{AuthContext, Scope};
use crate::backtests::BacktestStore;
use crate::bots::{BotService, BotStore, RunStore};
use crate::limits::{LimitService, LimitSources};
use crate::streaming::ChannelRegistry;
use chrono::{TimeZone, Utc};
use serde_json::json;
use std::sync::Arc;
use stock_trek_types::cex::{
    asset_id::AssetId, cex_id::CexId, side::Side, tag::Tag, trading_pair::TradingPair,
};

struct Fixture;

impl Fixture {
    fn at() -> chrono::DateTime<Utc> {
        Utc.with_ymd_and_hms(2026, 7, 14, 9, 0, 0).unwrap()
    }

    fn link_request(cex_id: &str) -> Result<LinkAccountRequest, serde_json::Error> {
        serde_json::from_value(json!({
            "cexId": cex_id,
            "label": "main",
            "apiKey": "0123456789abcdef",
            "apiSecret": "fedcba9876543210"
        }))
    }

    fn pending_order() -> PendingOrder {
        PendingOrder {
            tag: Tag::new("order_9x2"),
            symbol: TradingPair::new(AssetId::Bitcoin, AssetId::TetherUSD),
            side: Side::Sell,
            order_type: OrderType::Limit,
            quantity: "0.100000000".to_string(),
            price: "104250.000000000".to_string(),
            placed_at: Self::at(),
        }
    }

    fn account_summary() -> AccountSummary {
        AccountSummary {
            account_id: "account_3kq".to_string(),
            cex_id: CexId::Binance,
            label: "main".to_string(),
            active: true,
            connector: crate::accounts::Connector::connected(Self::at()),
            version: 1,
            updated_at: Self::at(),
        }
    }
}

#[test]
fn link_requests_accept_stock_trek_exchange_names() {
    let request = Fixture::link_request("Binance").expect("decode");
    assert_eq!(request.cex_id, CexId::Binance);
}

#[test]
fn link_requests_reject_unknown_fields() {
    let request = serde_json::from_value::<LinkAccountRequest>(json!({
        "cexId": "Binance",
        "label": "main",
        "apiKey": "0123456789abcdef",
        "apiSecret": "fedcba9876543210",
        "encryptionAlgorithm": "hmac_sha256"
    }));
    assert!(request.is_err());
}

#[test]
fn link_requests_reject_unknown_exchanges() {
    assert!(Fixture::link_request("binance").is_err());
    assert!(Fixture::link_request("robinhood").is_err());
}

#[test]
fn a_pending_snapshot_serializes_with_the_exchange_name() {
    let snapshot = PortfolioSnapshot::pending("account_3kq", CexId::Binance);
    let encoded = serde_json::to_value(&snapshot).expect("encode");
    assert_eq!(encoded["accountId"], "account_3kq");
    assert_eq!(encoded["cexId"], "Binance");
    assert!(encoded["asOf"].is_null());
    assert!(encoded["assets"].as_array().expect("array").is_empty());
    assert!(
        encoded["pendingOrders"]
            .as_array()
            .expect("array")
            .is_empty()
    );
}

#[test]
fn pending_orders_serialize_stock_trek_types() {
    let encoded = serde_json::to_value(Fixture::pending_order()).expect("encode");
    assert_eq!(encoded["tag"], "order_9x2");
    assert_eq!(encoded["symbol"]["base"], "Bitcoin");
    assert_eq!(encoded["symbol"]["quote"], "TetherUSD");
    assert_eq!(encoded["side"], "Sell");
    assert_eq!(encoded["type"], "limit");
}

#[test]
fn account_summaries_carry_the_stock_trek_exchange() {
    let encoded = serde_json::to_value(Fixture::account_summary()).expect("encode");
    assert_eq!(encoded["cexId"], "Binance");
}

#[test]
fn account_summaries_never_return_credential_metadata() {
    let encoded = serde_json::to_value(Fixture::account_summary()).expect("encode");
    assert!(encoded.get("apiKey").is_none());
    assert!(encoded.get("apiSecret").is_none());
    assert!(encoded.get("encryptionAlgorithm").is_none());
}

struct ServiceFixture {
    service: AccountService,
    store: Arc<AccountStore>,
    auth: AuthContext,
}

impl ServiceFixture {
    const OWNER: &'static str = "user_7gk2";
    const VALID_KEY: &'static str = "0123456789abcdef";
    const VALID_SECRET: &'static str = "fedcba9876543210";

    fn new() -> Self {
        let registry = || Arc::new(ChannelRegistry::new());
        let store = Arc::new(AccountStore::new());
        let bots = Arc::new(BotService::new(
            Arc::new(BotStore::new()),
            Arc::new(RunStore::new(registry())),
            store.clone(),
            Arc::new(BuildStore::new(registry())),
            Arc::new(LimitService::new(LimitSources {
                accounts: store.clone(),
                builds: Arc::new(BuildStore::new(registry())),
                bots: Arc::new(BotStore::new()),
                backtests: Arc::new(BacktestStore::new(registry())),
            })),
        ));
        Self {
            service: AccountService::new(store.clone(), bots),
            store,
            auth: AuthContext::new(Self::OWNER, "token", vec![Scope::Read, Scope::Manage]),
        }
    }

    fn link(&self) -> AccountSummary {
        let request = LinkAccountRequest {
            cex_id: CexId::Binance,
            label: "main".to_string(),
            api_key: Self::VALID_KEY.to_string(),
            api_secret: Self::VALID_SECRET.to_string(),
        };
        self.service
            .link(&self.auth, &request, Utc::now())
            .expect("link")
    }

    fn connector(&self, account_id: &str) -> ConnectorState {
        self.store
            .get(Self::OWNER, account_id)
            .expect("stored")
            .connector
            .state
    }
}

#[tokio::test(start_paused = true)]
async fn linking_stays_connecting_until_the_connector_validates() {
    let fixture = ServiceFixture::new();
    let summary = fixture.link();
    assert_eq!(summary.connector.state, ConnectorState::Connecting);
    assert_eq!(
        fixture.connector(&summary.account_id),
        ConnectorState::Connecting
    );
    tokio::time::sleep(std::time::Duration::from_secs(1)).await;
    assert_eq!(
        fixture.connector(&summary.account_id),
        ConnectorState::Connected
    );
}

#[tokio::test(start_paused = true)]
async fn replacing_credentials_revalidates_in_the_background() {
    let fixture = ServiceFixture::new();
    let summary = fixture.link();
    tokio::time::sleep(std::time::Duration::from_secs(1)).await;
    let request = CredentialsRequest {
        api_key: "too-short".to_string(),
        api_secret: ServiceFixture::VALID_SECRET.to_string(),
    };
    let replaced = fixture
        .service
        .replace_credentials(&fixture.auth, &summary.account_id, &request, Utc::now())
        .expect("replace");
    assert_eq!(replaced.connector.state, ConnectorState::Connecting);
    tokio::time::sleep(std::time::Duration::from_secs(1)).await;
    assert_eq!(
        fixture.connector(&summary.account_id),
        ConnectorState::Error
    );
}

#[tokio::test(start_paused = true)]
async fn replaced_credentials_follow_the_same_lifecycle() {
    let fixture = ServiceFixture::new();
    let summary = fixture.link();
    let request = CredentialsRequest {
        api_key: ServiceFixture::VALID_KEY.to_string(),
        api_secret: ServiceFixture::VALID_SECRET.to_string(),
    };
    let replaced = fixture
        .service
        .replace_credentials(&fixture.auth, &summary.account_id, &request, Utc::now())
        .expect("replace");
    assert_eq!(replaced.connector.state, ConnectorState::Connecting);
    tokio::time::sleep(std::time::Duration::from_secs(1)).await;
    assert_eq!(
        fixture.connector(&summary.account_id),
        ConnectorState::Connected
    );
}
