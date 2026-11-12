use crate::tests::support_tests::TestFixture;
use crate::{ApiToken, AuthService, TokenExpiry, TokenOptions, TokenScope};
use chrono::{Duration, Utc};
use secrecy::ExposeSecret;

struct ViewFixture;

impl ViewFixture {
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
}

#[tokio::test]
async fn token_reads_return_redacted_views() {
    let service = TestFixture::service();
    let reader =
        ViewFixture::add_with_scopes(&service, "reader", "owner-a", [TokenScope::TokensRead]).await;
    let target = TestFixture::add_token(&service, "target", "owner-b").await;
    let secret = target.secret().expose_secret().to_string();
    let now = Utc::now();
    let reader = service
        .token_reader(reader.id(), reader.secret(), now)
        .await
        .unwrap();

    let view = reader.token(target.id()).await.unwrap();
    assert_eq!(view.id(), target.id());
    assert_eq!(view.owner(), target.owner());
    assert_eq!(view.created_at(), target.created_at());
    assert_eq!(view.metadata(), target.metadata());
    assert!(!format!("{view:?}").contains(&secret));

    let views = reader.tokens_for_owner(target.owner()).await.unwrap();
    assert_eq!(views.len(), 1);
    assert_eq!(views[0].id(), target.id());
    assert!(!format!("{:?}", views[0]).contains(&secret));

    let active = reader
        .active_tokens_for_owner(target.owner(), now)
        .await
        .unwrap();
    assert_eq!(active.len(), 1);
    assert_eq!(active[0].id(), target.id());

    let selected = reader.token_for_owner(target.owner(), now).await.unwrap();
    assert_eq!(selected.id(), target.id());

    let scoped = reader
        .token_for_owner_with_scope(target.owner(), TokenScope::OrdersWrite, now)
        .await
        .unwrap();
    assert_eq!(scoped.id(), target.id());
    assert!(!format!("{scoped:?}").contains(&secret));
}
