use crate::infisical::auth_flow::InfisicalAwsAuth;
use aws_config::SdkConfig;
use platform_errors::{BpError, BpResult, HttpError};
use platform_time::{
    deadline::Deadline,
    timeouts::{Timeout, Timeouts},
};
use serde::Deserialize;
use std::collections::HashMap;

#[derive(Clone)]
pub struct Secrets(HashMap<String, String>);

impl Secrets {
    pub async fn load(
        sdk_config: &SdkConfig,
        timeouts: Timeouts,
        deadline: Deadline,
        config: &InfisicalConfig,
    ) -> BpResult<Self> {
        let http_client = Self::http_client(timeouts)?;
        let auth = InfisicalAwsAuth::new(http_client, config.base_url.clone(), timeouts);
        let secrets = auth
            .try_load_secrets(
                sdk_config,
                deadline,
                &config.identity_id,
                &config.project_id,
                &config.environment,
            )
            .await?;
        Ok(Self(secrets))
    }

    fn http_client(timeouts: Timeouts) -> BpResult<reqwest::Client> {
        let mut builder = reqwest::ClientBuilder::default();
        if let Timeout::Bounded(connect_timeout) = timeouts.connect {
            builder = builder.connect_timeout(connect_timeout.get());
        }
        if let Timeout::Bounded(request_timeout) = timeouts.operation {
            builder = builder.timeout(request_timeout.get());
        }
        builder
            .build()
            .map_err(|error| BpError::Http(HttpError::Reqwest(error)))
    }

    pub(crate) fn get(&self, name: &str) -> BpResult<String> {
        self.0
            .get(name)
            .cloned()
            .ok_or_else(|| BpError::config_missing(name))
    }
}

#[derive(Debug, Clone)]
pub struct InfisicalConfig {
    pub identity_id: String,
    pub project_id: String,
    pub environment: String,
    pub base_url: String,
}

impl InfisicalConfig {
    pub fn new(
        identity_id: String,
        project_id: String,
        environment: String,
        base_url: String,
    ) -> Self {
        Self {
            identity_id,
            project_id,
            environment,
            base_url,
        }
    }
}

#[derive(Deserialize)]
pub struct InfisicalSecretsResponse {
    pub secrets: Vec<InfisicalSecretResponse>,
}

#[derive(Deserialize)]
pub struct InfisicalSecretResponse {
    #[serde(rename = "secretKey")]
    pub secret_key: String,
    #[serde(rename = "secretValue")]
    pub secret_value: String,
}
