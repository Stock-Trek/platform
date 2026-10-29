use super::GitHubEventConverter;
use crate::{
    dto::github::github_types::{GitHubInstallation, GitHubRefType, GitHubRepository},
    event_response::EventResponse,
};
use hmac::{Hmac, KeyInit, Mac};
use platform_errors::{BpError, BpResult, GitHubError};
use platform_messages::sqs_message::{GitSource, SqsDetail, SqsMessage, SqsRefType};
use sha2::Sha256;
use subtle::ConstantTimeEq;

impl GitHubEventConverter {
    pub(crate) fn is_zero_sha(sha: &str) -> bool {
        !sha.is_empty() && sha.bytes().all(|byte| byte == b'0')
    }

    pub(crate) fn repo_ids(repositories: Vec<GitHubRepository>) -> Vec<String> {
        repositories.into_iter().map(Self::repo_id).collect()
    }

    pub(crate) fn repo_id(repository: GitHubRepository) -> String {
        repository.id.to_string()
    }

    pub(crate) fn ref_name(r#ref: &str) -> String {
        r#ref
            .strip_prefix("refs/heads/")
            .or_else(|| r#ref.strip_prefix("refs/tags/"))
            .unwrap_or(r#ref)
            .to_string()
    }

    pub(crate) fn commit_ref(r#ref: &str) -> Option<(&str, SqsRefType)> {
        if let Some(branch_name) = r#ref.strip_prefix("refs/heads/") {
            Some((branch_name, SqsRefType::Branch))
        } else if let Some(tag_name) = r#ref.strip_prefix("refs/tags/") {
            Some((tag_name, SqsRefType::Tag))
        } else {
            None
        }
    }

    pub(crate) fn ref_type(ref_type: &GitHubRefType) -> Option<SqsRefType> {
        match ref_type {
            GitHubRefType::Branch => Some(SqsRefType::Branch),
            GitHubRefType::Tag => Some(SqsRefType::Tag),
            GitHubRefType::Other => None,
        }
    }

    pub(crate) fn host(&self, installation: &GitHubInstallation) -> BpResult<String> {
        self.allowed_hosts
            .validate_url(&installation.html_url)
            .map_err(|error| match error {
                BpError::GitHub(GitHubError::HostInvalid { .. }) => {
                    BpError::GitHub(GitHubError::HostInvalid {
                        host: installation.html_url.clone(),
                    })
                }
                BpError::GitHub(GitHubError::HostNotAllowed { .. }) => {
                    BpError::GitHub(GitHubError::HostNotAllowed {
                        host: installation.html_url.clone(),
                    })
                }
                _ => error,
            })
    }

    pub(crate) fn delivery_id(request: &lambda_http::Request) -> Option<String> {
        request
            .headers()
            .get("x-github-delivery")
            .and_then(|v| v.to_str().ok())
            .filter(|delivery_id| !delivery_id.is_empty())
            .map(str::to_owned)
    }

    pub(crate) fn accept(
        host: String,
        delivery_id: Option<String>,
        installation_id: u64,
        detail: SqsDetail,
    ) -> BpResult<EventResponse> {
        let message = SqsMessage {
            source: GitSource::GitHub {
                host,
                delivery_id,
                installation_id,
            },
            detail,
        };
        Ok(EventResponse::Accept(message.chunk()?))
    }

    pub(crate) fn verify_signature(request: &lambda_http::Request, secret: &str) -> bool {
        let signature = request
            .headers()
            .get("x-hub-signature-256")
            .and_then(|v| v.to_str().ok())
            .unwrap_or("");
        let private_key = secret.as_bytes();
        let body_bytes = request.body();
        let Some(expected_hex) = signature.strip_prefix("sha256=") else {
            return false;
        };
        let Ok(mut mac) = Hmac::<Sha256>::new_from_slice(private_key) else {
            return false;
        };
        mac.update(body_bytes);
        let computed = mac.finalize().into_bytes();
        let Ok(expected) = hex::decode(expected_hex) else {
            return false;
        };
        computed.ct_eq(&expected).into()
    }
}
