use crate::AccountKey;
use stock_trek_types::cex::{account_id::AccountId, cex_id::CexId};

#[test]
fn account_key_exposes_its_parts() {
    let account = AccountKey::new(CexId::Binance, "primary");
    assert_eq!(account.cex_id(), CexId::Binance);
    assert_eq!(account.account_id(), &AccountId::new("primary"));
}

#[test]
fn account_key_converts_from_a_tuple() {
    let account = AccountKey::from((CexId::Binance, AccountId::new("primary")));
    assert_eq!(
        account.into_parts(),
        (CexId::Binance, AccountId::new("primary"))
    );
}
