use crate::aws_ops::{lambda::Lambda, s3::S3};
use aws_config::{BehaviorVersion, SdkConfig, timeout::TimeoutConfig};
use platform_sqs::sqs::Sqs;
use platform_time::timeouts::Timeouts;
pub struct Aws {
    pub lambda: Lambda,
    pub s3: S3,
    pub sqs: Sqs,
}

impl Aws {
    pub async fn load_sdk_config(timeouts: Timeouts) -> SdkConfig {
        let mut timeout_config = TimeoutConfig::builder();
        timeout_config
            .set_connect_timeout(timeouts.connect.into())
            .set_operation_timeout(timeouts.operation.into());
        let timeout_config = timeout_config.build();
        aws_config::defaults(BehaviorVersion::latest())
            .timeout_config(timeout_config)
            .load()
            .await
    }

    pub fn new(lambda: Lambda, s3: S3, sqs: Sqs) -> Self {
        Self { lambda, s3, sqs }
    }
}
