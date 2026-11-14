use crate::{
    dto::github::github_event_converter::GitHubEventConverter, event_response::EventResponse,
};
use lambda_http::Request;
use platform_aws::SsmSecrets;
use platform_errors::PlatformResult;
use platform_github::HostAllowlist;

pub trait EventConverter: Send + Sync {
    fn event_response(&self, event: &Request) -> PlatformResult<EventResponse>;
}

pub struct EventConverters;

impl EventConverters {
    pub fn all(
        secrets: &SsmSecrets,
        allowed_hosts: HostAllowlist,
    ) -> PlatformResult<Vec<Box<dyn EventConverter>>> {
        Ok(vec![Box::new(GitHubEventConverter::new(
            secrets,
            allowed_hosts,
        )?)])
    }
}
