//! Shared AWS configuration and client construction.

use aws_config::{BehaviorVersion, SdkConfig, timeout::TimeoutConfig};
use aws_sdk_dynamodb::Client as DynamoDbClient;
use aws_sdk_lambda::Client as LambdaClient;
use aws_sdk_s3::Client as S3Client;
use aws_sdk_s3_transfer_manager::Client as TransferClient;
use aws_sdk_ssm::Client as SsmClient;
use platform_time::Timeouts;

/// Shared AWS configuration and client construction.
pub struct Aws;

impl Aws {
    /// Loads the default AWS SDK configuration with the supplied connect and
    /// operation timeouts.
    pub async fn load_sdk_config(timeouts: Timeouts) -> SdkConfig {
        let mut timeout_config = TimeoutConfig::builder();
        timeout_config
            .set_connect_timeout(timeouts.connect.into())
            .set_operation_timeout(timeouts.operation.into());
        aws_config::defaults(BehaviorVersion::latest())
            .timeout_config(timeout_config.build())
            .load()
            .await
    }

    /// Loads the default AWS SDK configuration without overriding its timeouts.
    pub async fn load_default_sdk_config() -> SdkConfig {
        aws_config::defaults(BehaviorVersion::latest()).load().await
    }

    /// Builds an S3 client from shared SDK configuration.
    pub fn s3_client(sdk_config: &SdkConfig) -> S3Client {
        S3Client::new(sdk_config)
    }

    /// Builds a DynamoDB client from shared SDK configuration.
    pub fn dynamodb_client(sdk_config: &SdkConfig) -> DynamoDbClient {
        DynamoDbClient::new(sdk_config)
    }

    /// Builds a Lambda client from shared SDK configuration.
    pub fn lambda_client(sdk_config: &SdkConfig) -> LambdaClient {
        LambdaClient::new(sdk_config)
    }

    /// Builds an S3 client that talks to a custom endpoint using path-style
    /// addressing.
    pub fn s3_client_with_path_style_endpoint(
        sdk_config: &SdkConfig,
        endpoint_url: &str,
    ) -> S3Client {
        S3Client::from_conf(
            aws_sdk_s3::config::Builder::from(sdk_config)
                .endpoint_url(endpoint_url)
                .force_path_style(true)
                .build(),
        )
    }

    /// Builds an S3 transfer manager from the same SDK configuration used by
    /// the ordinary S3 client.
    pub fn transfer_client(sdk_config: &SdkConfig) -> TransferClient {
        TransferClient::new(
            aws_sdk_s3_transfer_manager::Config::builder()
                .s3_config(sdk_config)
                .build(),
        )
    }

    /// Builds an SSM client from shared SDK configuration.
    pub fn ssm_client(sdk_config: &SdkConfig) -> SsmClient {
        SsmClient::new(sdk_config)
    }
}
