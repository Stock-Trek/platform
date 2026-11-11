use crate::{AccountKey, PortfolioService};
use chrono::{DateTime, Utc};
use platform_auth_service::{ApiToken, AuthService, JwksCache, OrderJwtClaims, SignedOrderRequest};
use platform_errors::{BpError, BpResult};
use platform_messages::order_request::UnsignedOrderRequest;
use serde::Serialize;
use std::sync::Arc;
use stock_trek_types::cex::{
    account_id::AccountId, asset_id::AssetId, cex_id::CexId, order_request::OrderRequest,
};

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SignedOrderSubmission {
    cex: CexId,
    account: AccountId,
    owner: String,
    token_id: String,
    order_request: OrderRequest<AssetId, f64>,
    signature: String,
}

impl SignedOrderSubmission {
    pub fn new(request: &UnsignedOrderRequest, signed: &SignedOrderRequest) -> Self {
        Self {
            cex: request.cex(),
            account: request.account().clone(),
            owner: request.owner().trim().to_string(),
            token_id: signed.token_id().to_string(),
            order_request: request.order_request().clone(),
            signature: signed.envelope().to_string(),
        }
    }

    pub fn cex(&self) -> CexId {
        self.cex
    }

    pub fn account(&self) -> &AccountId {
        &self.account
    }

    pub fn owner(&self) -> &str {
        &self.owner
    }

    pub fn token_id(&self) -> &str {
        &self.token_id
    }

    pub fn order_request(&self) -> &OrderRequest<AssetId, f64> {
        &self.order_request
    }

    pub fn signature(&self) -> &str {
        &self.signature
    }

    pub fn canonical_bytes(&self) -> BpResult<Vec<u8>> {
        UnsignedOrderRequest::new(
            self.cex,
            self.account.clone(),
            self.owner.clone(),
            self.order_request.clone(),
        )
        .canonical_bytes()
        .map_err(BpError::external)
    }
}

pub struct OrderRequestService {
    auth_service: AuthService,
    jwt_verifier: JwksCache,
    portfolio: Option<Arc<PortfolioService>>,
}

impl OrderRequestService {
    pub fn new(auth_service: AuthService) -> Self {
        let jwt_verifier = JwksCache::from_jwks(auth_service.jwks().clone());
        Self::with_jwks_cache(auth_service, jwt_verifier)
    }

    pub fn with_jwks_cache(auth_service: AuthService, jwt_verifier: JwksCache) -> Self {
        Self {
            auth_service,
            jwt_verifier,
            portfolio: None,
        }
    }

    pub fn with_jwks_url(auth_service: AuthService, url: &str) -> BpResult<Self> {
        Ok(Self::with_jwks_cache(auth_service, JwksCache::new(url)?))
    }

    pub fn with_portfolio_service(mut self, portfolio: Arc<PortfolioService>) -> Self {
        self.portfolio = Some(portfolio);
        self
    }

    pub async fn authenticate(&self, jwt: &str, now: DateTime<Utc>) -> BpResult<OrderJwtClaims> {
        self.jwt_verifier.verify(jwt, now).await
    }

    pub async fn verify_jwt(
        &self,
        jwt: &str,
        request: &UnsignedOrderRequest,
        now: DateTime<Utc>,
    ) -> BpResult<()> {
        let claims = self.authenticate(jwt, now).await?;
        let owner = ApiToken::validated_owner(request.owner())?;
        if claims.owner() != owner {
            return Err(BpError::auth_invalid_jwt(
                "owner does not match the order request",
            ));
        }
        Ok(())
    }

    pub async fn sign_for_caller(
        &self,
        request: &UnsignedOrderRequest,
        claims: &OrderJwtClaims,
    ) -> BpResult<SignedOrderSubmission> {
        let owner = ApiToken::validated_owner(claims.owner())?;
        let request = request.clone().with_owner(owner);
        self.authorize_account(owner, &request)?;
        let order_request = request.canonical_bytes().map_err(BpError::external)?;
        let now = Utc::now();
        let signed = self
            .auth_service
            .sign_order_request_for_token(claims.token_id(), &order_request, now, now)
            .await?;
        Ok(SignedOrderSubmission::new(&request, &signed))
    }

    fn authorize_account(&self, owner: &str, request: &UnsignedOrderRequest) -> BpResult<()> {
        let Some(portfolio) = &self.portfolio else {
            return Ok(());
        };
        let account = AccountKey::from((request.cex(), request.account().clone()));
        if portfolio.is_tracked_for_owner(owner, &account) {
            Ok(())
        } else {
            Err(BpError::auth_invalid_field(
                "account",
                "is not owned by the authenticated owner",
            ))
        }
    }
}
