use crate::{Aws, SdkConfig};
use aws_sdk_ssm::{
    error::ProvideErrorMetadata,
    operation::get_parameters_by_path::{GetParametersByPathError, GetParametersByPathOutput},
};
use platform_errors::{BpError, BpResult, Retryability, SecretsError, TimeoutError};
use platform_time::{deadline::Deadline, timeouts::Timeouts};
use std::collections::HashMap;

/// Environment variable containing the SSM parameter path to load secrets from.
pub const SSM_PATH_ENV: &str = "SECRETS_SSM_PATH";

/// Environment variable controlling whether SSM parameters are decrypted.
pub const SSM_DECRYPT_ENV: &str = "SECRETS_SSM_DECRYPT";

const SSM_OPERATION: &str = "SSM GetParametersByPath";

/// A snapshot of the secrets under an SSM parameter path.
#[derive(Clone, Default)]
pub struct Secrets(HashMap<String, String>);

impl Secrets {
    /// Loads every parameter under `config.path`, bounded by the supplied
    /// operation timeout and deadline.
    pub async fn load(
        sdk_config: &SdkConfig,
        timeouts: Timeouts,
        deadline: Deadline,
        config: &SsmConfig,
    ) -> BpResult<Self> {
        let client = Aws::ssm_client(sdk_config);
        Self::within_deadline(
            timeouts,
            deadline,
            SSM_OPERATION,
            Self::load_with_client(&client, config),
        )
        .await
    }

    /// Loads every parameter under `config.path` using an existing SSM client.
    pub async fn load_with_client(
        client: &aws_sdk_ssm::Client,
        config: &SsmConfig,
    ) -> BpResult<Self> {
        Self::fetch(client, config).await.map(Self)
    }

    async fn fetch(
        client: &aws_sdk_ssm::Client,
        config: &SsmConfig,
    ) -> BpResult<HashMap<String, String>> {
        let mut secrets = HashMap::new();
        let mut paginator = client
            .get_parameters_by_path()
            .path(&config.path)
            .recursive(true)
            .with_decryption(config.decrypt)
            .into_paginator()
            .send();
        while let Some(page) = paginator.next().await {
            let page = page.map_err(|error| Self::load_error(&config.path, error))?;
            Self::insert_page(&mut secrets, &page);
        }
        if secrets.is_empty() {
            return Err(BpError::Secrets(SecretsError::Empty {
                path: config.path.clone(),
            }));
        }
        Ok(secrets)
    }

    fn insert_page(secrets: &mut HashMap<String, String>, page: &GetParametersByPathOutput) {
        for parameter in page.parameters() {
            let Some(name) = parameter
                .name()
                .and_then(|name| name.rsplit('/').next())
                .filter(|name| !name.is_empty())
            else {
                continue;
            };
            secrets.insert(
                name.to_string(),
                parameter.value().unwrap_or_default().to_string(),
            );
        }
    }

    fn load_error<R>(
        path: &str,
        error: aws_sdk_ssm::error::SdkError<GetParametersByPathError, R>,
    ) -> BpError
    where
        R: std::fmt::Debug + Send + Sync + 'static,
    {
        let retryable = Self::retryability(&error).is_retryable();
        BpError::Secrets(SecretsError::Load {
            operation: SSM_OPERATION,
            path: path.to_string(),
            retryable,
            source: Box::new(error),
        })
    }

    fn retryability<R>(
        error: &aws_sdk_ssm::error::SdkError<GetParametersByPathError, R>,
    ) -> Retryability {
        match error {
            aws_sdk_ssm::error::SdkError::ConstructionFailure(_) => Retryability::Permanent,
            aws_sdk_ssm::error::SdkError::ServiceError(context) => {
                let code = context.err().code().unwrap_or_default();
                Retryability::from_ssm_error_code(code)
            }
            aws_sdk_ssm::error::SdkError::TimeoutError(_)
            | aws_sdk_ssm::error::SdkError::DispatchFailure(_)
            | aws_sdk_ssm::error::SdkError::ResponseError(_) => Retryability::Retryable,
            _ => Retryability::Retryable,
        }
    }

    async fn within_deadline<T>(
        timeouts: Timeouts,
        deadline: Deadline,
        operation: &'static str,
        future: impl std::future::Future<Output = BpResult<T>>,
    ) -> BpResult<T> {
        let timeout = timeouts.operation_timeout(deadline)?;
        tokio::time::timeout(timeout, future)
            .await
            .map_err(|_| BpError::Timeout(TimeoutError::Operation { operation }))?
    }

    /// Returns the named secret, or a configuration error when it is absent.
    pub fn get(&self, name: &str) -> BpResult<String> {
        self.0
            .get(name)
            .cloned()
            .ok_or_else(|| BpError::config_missing(name))
    }
}

/// The SSM parameter path and decryption mode used to load secrets.
#[derive(Debug, Clone)]
pub struct SsmConfig {
    pub path: String,
    pub decrypt: bool,
}

impl SsmConfig {
    pub fn new(mut path: String, decrypt: bool) -> BpResult<Self> {
        path = path.trim().to_string();
        if path.is_empty() {
            return Err(BpError::config_empty(SSM_PATH_ENV));
        }
        if !path.starts_with('/') {
            path.insert(0, '/');
        }
        if path.ends_with('/') {
            return Err(BpError::config_invalid(
                SSM_PATH_ENV,
                "must not end with '/'",
            ));
        }
        Ok(Self { path, decrypt })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use aws_sdk_ssm::types::Parameter;

    fn page(names: &[(&str, Option<&str>)]) -> GetParametersByPathOutput {
        GetParametersByPathOutput::builder()
            .set_parameters(Some(
                names
                    .iter()
                    .map(|(name, value)| {
                        Parameter::builder()
                            .name(*name)
                            .set_value(value.map(str::to_string))
                            .build()
                    })
                    .collect::<Vec<_>>(),
            ))
            .build()
    }

    #[test]
    fn path_is_normalised_and_validated() {
        assert_eq!(
            SsmConfig::new("platform/git-webhook/production".into(), true)
                .unwrap()
                .path,
            "/platform/git-webhook/production"
        );
        assert!(SsmConfig::new("platform/git-webhook/production/".into(), true).is_err());
        assert!(SsmConfig::new("   ".into(), true).is_err());
    }

    #[test]
    fn parameters_are_keyed_by_their_final_path_segment() {
        let mut secrets = HashMap::new();
        Secrets::insert_page(
            &mut secrets,
            &page(&[
                (
                    "/platform/git-webhook/production/GITHUB_WEBHOOK_SECRET",
                    Some("value"),
                ),
                ("/platform/git-webhook/production/OTHER", Some("other")),
            ]),
        );

        assert_eq!(
            secrets.get("GITHUB_WEBHOOK_SECRET").map(String::as_str),
            Some("value")
        );
        assert_eq!(secrets.get("OTHER").map(String::as_str), Some("other"));
    }

    #[test]
    fn parameters_without_a_name_or_value_are_handled() {
        let mut secrets = HashMap::new();
        Secrets::insert_page(
            &mut secrets,
            &page(&[
                ("/platform/git-webhook/production/", Some("ignored")),
                (
                    "/platform/git-webhook/production/GITHUB_WEBHOOK_SECRET",
                    None,
                ),
            ]),
        );

        assert_eq!(
            secrets.get("GITHUB_WEBHOOK_SECRET").map(String::as_str),
            Some("")
        );
        assert!(!secrets.contains_key(""));
    }

    #[test]
    fn missing_secrets_are_a_configuration_error() {
        let secrets = Secrets::default();

        assert!(matches!(
            secrets.get("BACKFILLER_EXCHANGE_API_KEY"),
            Err(BpError::Config(_))
        ));
    }
}
