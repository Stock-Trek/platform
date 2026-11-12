use crate::tests::support_tests::{ORDER_REQUEST, TestFixture};
use crate::{ApiToken, AuthService, TokenExpiry, TokenOptions, TokenScope};
use chrono::{Duration, Utc};
use platform_errors::{AuthError, BuildError};

struct AdministrationFixture;

impl AdministrationFixture {
    async fn add_with_scopes<I, S>(
        service: &AuthService,
        id: &str,
        owner: &str,
        scopes: I,
    ) -> ApiToken
    where
        S: std::borrow::Borrow<TokenScope>,
        I: IntoIterator<Item = S>,
    {
        let options = TokenOptions::new(TokenExpiry::After(Duration::days(30)), scopes);
        let now = Utc::now();
        service
            .add_token(id, owner, now, options, now)
            .await
            .unwrap()
    }

    fn assert_missing_scope(error: BuildError, expected: &str) {
        assert!(matches!(
            error,
            BuildError::Auth(AuthError::MissingScope { ref scope, .. }) if scope == expected
        ));
    }
}

#[tokio::test]
async fn token_reads_require_the_tokens_read_scope() {
    let service = TestFixture::service();
    let reader = AdministrationFixture::add_with_scopes(
        &service,
        "reader",
        "owner-a",
        [TokenScope::TokensRead],
    )
    .await;
    let target = TestFixture::add_token(&service, "target", "owner-b").await;
    let reader = service
        .token_reader(reader.id(), reader.secret(), Utc::now())
        .await
        .unwrap();

    assert_eq!(reader.token(target.id()).await.unwrap().id(), target.id());

    let order_token = AdministrationFixture::add_with_scopes(
        &service,
        "order-token",
        "owner-c",
        [TokenScope::OrdersWrite],
    )
    .await;
    let error = service
        .token_reader(order_token.id(), order_token.secret(), Utc::now())
        .await
        .err()
        .unwrap();

    AdministrationFixture::assert_missing_scope(error, "tokens:read");
}

#[tokio::test]
async fn token_writes_require_the_tokens_write_scope() {
    let service = TestFixture::service();
    let writer = AdministrationFixture::add_with_scopes(
        &service,
        "writer",
        "owner-a",
        [TokenScope::TokensWrite],
    )
    .await;
    let administrator = service
        .token_administrator(writer.id(), writer.secret(), Utc::now())
        .await
        .unwrap();
    let options = TokenOptions::new(TokenExpiry::After(Duration::days(1)), TokenScope::DEFAULT);
    let created_at = Utc::now();
    let created = administrator
        .add_token("created", "owner-b", created_at, options, created_at)
        .await
        .unwrap();

    assert_eq!(created.id(), "created");
    administrator
        .delete_token(created.id(), Utc::now())
        .await
        .unwrap();
    assert!(service.token(created.id()).await.is_err());

    let reader = AdministrationFixture::add_with_scopes(
        &service,
        "reader",
        "owner-c",
        [TokenScope::TokensRead],
    )
    .await;
    let error = service
        .token_administrator(reader.id(), reader.secret(), Utc::now())
        .await
        .err()
        .unwrap();

    AdministrationFixture::assert_missing_scope(error, "tokens:write");

    let error = service
        .token_reader(writer.id(), writer.secret(), Utc::now())
        .await
        .err()
        .unwrap();

    AdministrationFixture::assert_missing_scope(error, "tokens:read");
}

#[tokio::test]
async fn tokens_all_grants_both_token_permissions() {
    let service = TestFixture::service();
    let token = AdministrationFixture::add_with_scopes(
        &service,
        "all-tokens",
        "owner-a",
        [TokenScope::TokensAll],
    )
    .await;

    assert!(
        service
            .token_reader(token.id(), token.secret(), Utc::now())
            .await
            .is_ok()
    );
    assert!(
        service
            .token_administrator(token.id(), token.secret(), Utc::now())
            .await
            .is_ok()
    );
}

#[tokio::test]
async fn admin_grants_every_scope() {
    let required = [
        TokenScope::All,
        TokenScope::OrdersRead,
        TokenScope::OrdersWrite,
        TokenScope::OrdersAll,
        TokenScope::TokensRead,
        TokenScope::TokensWrite,
        TokenScope::TokensAll,
        TokenScope::Admin,
    ];

    for scope in required {
        assert!(TokenScope::Admin.matches(scope));
    }

    let service = TestFixture::service();
    let admin =
        AdministrationFixture::add_with_scopes(&service, "admin", "owner-a", [TokenScope::Admin])
            .await;
    let now = Utc::now();

    assert!(
        service
            .token_reader(admin.id(), admin.secret(), now)
            .await
            .is_ok()
    );
    assert!(
        service
            .sign_order_request(admin.id(), admin.secret(), ORDER_REQUEST, now, now)
            .await
            .is_ok()
    );
}
