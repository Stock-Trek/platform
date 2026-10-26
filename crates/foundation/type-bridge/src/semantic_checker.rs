use platform_errors::{BpError, BpResult, ValidationError};
use rust_decimal::Decimal;
use stock_trek_types::cex::{
    asset_id::AssetId, capability::CexCapability, order_request::OrderRequest, quantity::Quantity,
};

#[derive(Debug, Clone)]
pub(crate) struct SemanticChecker;

impl SemanticChecker {
    pub fn consistent_or_err(
        &self,
        order_request: &OrderRequest<AssetId, Decimal>,
        capabilities: &[CexCapability],
    ) -> BpResult<()> {
        match order_request {
            OrderRequest::Limit {
                quantity: Quantity::OfQuote(..),
                ..
            } => self.check_capability(capabilities, &CexCapability::QuoteQuantityOnLimitOrders),
            _ => Ok(()),
        }
    }
    fn check_capability(
        &self,
        capabilities: &[CexCapability],
        required_capability: &CexCapability,
    ) -> BpResult<()> {
        if !capabilities.contains(required_capability) {
            Err(BpError::Validation(ValidationError::Unsupported {
                name: "CexCapability".into(),
                value: (*required_capability).to_string(),
            }))
        } else {
            Ok(())
        }
    }
}
