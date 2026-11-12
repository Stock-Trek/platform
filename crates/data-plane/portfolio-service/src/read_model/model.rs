use crate::read_model::{OwnershipRecord, PortfolioRecord};

#[derive(Debug, Default)]
pub struct PortfolioReadModel {
    ownership: Vec<OwnershipRecord>,
    portfolios: Vec<PortfolioRecord>,
}

impl PortfolioReadModel {
    pub fn new(ownership: Vec<OwnershipRecord>, portfolios: Vec<PortfolioRecord>) -> Self {
        Self {
            ownership,
            portfolios,
        }
    }

    pub fn into_parts(self) -> (Vec<OwnershipRecord>, Vec<PortfolioRecord>) {
        (self.ownership, self.portfolios)
    }
}
