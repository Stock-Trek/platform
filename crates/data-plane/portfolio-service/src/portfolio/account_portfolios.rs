use super::AccountPortfolio;
use std::collections::BTreeMap;
use stock_trek_types::cex::{account_id::AccountId, cex_id::CexId};

pub type AccountPortfolios = BTreeMap<CexId, BTreeMap<AccountId, AccountPortfolio>>;
