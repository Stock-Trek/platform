use platform_auth_service::ApiToken;
use platform_config::env_vars::EnvVars;
use platform_errors::{BpError, BpResult};
use secrecy::SecretString;

const ORDER_REQUEST_TOKEN_ID: &str = "CWASM_INVOKER_ORDER_REQUEST_TOKEN_ID";
const ORDER_REQUEST_SIGNING_SECRET: &str = "CWASM_INVOKER_ORDER_REQUEST_SIGNING_SECRET";

#[derive(Debug, Clone)]
pub struct OrderRequestSigningConfig {
    pub token_id: String,
    pub secret: SecretString,
}

impl OrderRequestSigningConfig {
    pub fn from_vars(vars: &EnvVars) -> BpResult<Option<Self>> {
        let token_id = vars.optional(ORDER_REQUEST_TOKEN_ID)?;
        let secret = vars.optional(ORDER_REQUEST_SIGNING_SECRET)?;
        match (token_id, secret) {
            (None, None) => Ok(None),
            (Some(token_id), Some(secret)) => {
                ApiToken::validate_id(&token_id)?;
                Ok(Some(Self {
                    token_id,
                    secret: SecretString::from(secret),
                }))
            }
            (Some(_), None) => Err(BpError::config_missing(ORDER_REQUEST_SIGNING_SECRET)),
            (None, Some(_)) => Err(BpError::config_missing(ORDER_REQUEST_TOKEN_ID)),
        }
    }
}
