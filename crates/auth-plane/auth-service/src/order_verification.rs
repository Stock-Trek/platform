use crate::{AuthService, OrderRequestSigner, TokenScope};
use chrono::{DateTime, Utc};
use platform_errors::BpResult;

pub(crate) const CLOCK_SKEW_SECONDS: i64 = 60;

impl AuthService {
    pub async fn verify_order_request(
        &self,
        token_id: &str,
        order_request: &[u8],
        signature: &str,
        now: DateTime<Utc>,
    ) -> BpResult<bool> {
        let Some(token) = self.signature_token(token_id, now).await? else {
            return Ok(false);
        };
        if let Err(error) = Self::ensure_active(&token, now) {
            return self
                .reject_signature(token_id, Some(&token), now, &error.to_string())
                .await;
        }
        if let Err(error) = Self::ensure_scope(&token, TokenScope::OrdersWrite) {
            return self
                .reject_signature(token_id, Some(&token), now, &error.to_string())
                .await;
        }
        let (signed, context) = match self.validate_order_signature(token_id, signature, now) {
            Ok(validated) => validated,
            Err(detail) => {
                return self
                    .reject_signature(token_id, Some(&token), now, detail)
                    .await;
            }
        };
        let signer = OrderRequestSigner::new(token.secret())?;
        if !signer.verify_order_request(&context, order_request, signed.signature())? {
            return self
                .reject_signature(token_id, Some(&token), now, "signature mismatch")
                .await;
        }
        let reservation = match self.reserve_order_request_replay(&context, now).await {
            Ok(Some(reservation)) => reservation,
            Ok(None) => {
                return self
                    .reject_signature(token_id, Some(&token), now, "replayed signature")
                    .await;
            }
            Err(error) => {
                return self
                    .reject_signature_with_error(token_id, Some(&token), now, error)
                    .await;
            }
        };
        self.commit_signature_use(token, token_id, now, &reservation)
            .await
    }
}
