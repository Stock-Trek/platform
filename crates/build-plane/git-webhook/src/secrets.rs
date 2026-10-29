use aws_sdk_secretsmanager::{
    Client as SecretsManagerClient,
    error::{ProvideErrorMetadata, SdkError},
    operation::get_secret_value::GetSecretValueError,
};
use platform_aws::SdkConfig;
use platform_errors::{BpError, BpResult, SecretsError};
use platform_time::{deadline::Deadline, timeouts::Timeouts};
use std::{collections::HashMap, error::Error as StdError};

/// Secret payloads are stored as a JSON object so one Secrets Manager secret
/// can hold every value the webhook needs.
#[derive(Clone)]
pub struct Secrets(HashMap<String, String>);

impl Secrets {
    pub async fn load(
        sdk_config: &SdkConfig,
        timeouts: Timeouts,
        deadline: Deadline,
        config: &SecretsConfig,
    ) -> BpResult<Self> {
        let client = SecretsManagerClient::new(sdk_config);
        let timeout = timeouts.operation_timeout(deadline)?;
        let response = tokio::time::timeout(
            timeout,
            client
                .get_secret_value()
                .secret_id(&config.secret_id)
                .send(),
        )
        .await
        .map_err(|_| {
            BpError::Timeout(platform_errors::TimeoutError::Operation {
                operation: "AWS Secrets Manager get_secret_value",
            })
        })?
        .map_err(|error| Self::store_error(error, &config.secret_id))?;

        let payload = Self::payload(&response)?;
        Ok(Self(Self::parse_payload(&config.secret_id, payload)?))
    }

    /// Returns the plaintext value. Binary secrets are supported for
    /// compatibility, but must contain UTF-8 because every value we consume is
    /// ultimately a string.
    fn payload(
        response: &aws_sdk_secretsmanager::operation::get_secret_value::GetSecretValueOutput,
    ) -> BpResult<&str> {
        if let Some(secret_string) = response.secret_string() {
            return Ok(secret_string);
        }
        if let Some(secret_binary) = response.secret_binary() {
            return std::str::from_utf8(secret_binary.as_ref()).map_err(|error| {
                BpError::Secrets(SecretsError::Store {
                    operation: "decode_secret_binary",
                    target: "AWS Secrets Manager secret".into(),
                    retryable: false,
                    source: Box::new(error),
                })
            });
        }
        Err(BpError::Secrets(SecretsError::Missing {
            name: "SecretString".into(),
        }))
    }

    fn parse_payload(secret_id: &str, payload: &str) -> BpResult<HashMap<String, String>> {
        let values =
            serde_json::from_str::<HashMap<String, String>>(payload).map_err(|source| {
                BpError::Secrets(SecretsError::InvalidPayload {
                    target: secret_id.to_owned(),
                    source,
                })
            })?;
        if values.is_empty() {
            return Err(BpError::Secrets(SecretsError::Missing {
                name: secret_id.to_owned(),
            }));
        }
        Ok(values)
    }

    fn store_error(
        error: SdkError<GetSecretValueError, aws_sdk_secretsmanager::config::http::HttpResponse>,
        secret_id: &str,
    ) -> BpError {
        let retryable = !matches!(
            Self::service_error_code(&error),
            Some(
                "AccessDeniedException"
                    | "DecryptionFailure"
                    | "InvalidParameterException"
                    | "InvalidRequestException"
                    | "ResourceNotFoundException"
            )
        ) && !matches!(error, SdkError::ConstructionFailure(_));

        BpError::Secrets(SecretsError::Store {
            operation: "get_secret_value",
            target: secret_id.to_owned(),
            retryable,
            source: Box::new(error) as Box<dyn StdError + Send + Sync>,
        })
    }

    fn service_error_code(
        error: &SdkError<GetSecretValueError, aws_sdk_secretsmanager::config::http::HttpResponse>,
    ) -> Option<&str> {
        match error {
            SdkError::ServiceError(context) => context.err().code(),
            _ => None,
        }
    }

    pub(crate) fn get(&self, name: &str) -> BpResult<String> {
        self.0
            .get(name)
            .cloned()
            .ok_or_else(|| BpError::Secrets(SecretsError::Missing { name: name.into() }))
    }
}

#[derive(Debug, Clone)]
pub struct SecretsConfig {
    pub secret_id: String,
}

impl SecretsConfig {
    pub fn new(secret_id: String) -> Self {
        Self { secret_id }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_a_json_object_of_secret_values() {
        let secrets = Secrets::parse_payload(
            "webhook",
            r#"{"GITHUB_WEBHOOK_SECRET":"top-secret","OTHER":"value"}"#,
        )
        .unwrap();

        assert_eq!(
            secrets.get("GITHUB_WEBHOOK_SECRET").map(String::as_str),
            Some("top-secret")
        );
        assert_eq!(secrets.get("OTHER").map(String::as_str), Some("value"));
    }

    #[test]
    fn rejects_invalid_and_empty_payloads() {
        assert!(matches!(
            Secrets::parse_payload("webhook", "not json"),
            Err(BpError::Secrets(SecretsError::InvalidPayload { .. }))
        ));
        assert!(matches!(
            Secrets::parse_payload("webhook", "{}"),
            Err(BpError::Secrets(SecretsError::Missing { .. }))
        ));
    }

    #[test]
    fn missing_keys_are_reported_as_secret_errors() {
        let secrets = Secrets(HashMap::new());
        assert!(matches!(
            secrets.get("GITHUB_WEBHOOK_SECRET"),
            Err(BpError::Secrets(SecretsError::Missing { .. }))
        ));
    }
}
