use crate::{
    dto::{
        event_converter::EventConverter,
        github::{
            github_event_installation::{GitHubEventInstallation, GitHubEventInstallationAction},
            github_event_installation_repositories::{
                GitHubEventInstallationRepositories, GitHubEventInstallationRepositoriesAction,
            },
            github_event_push::GitHubEventPush,
            github_event_ref::GitHubEventRef,
            github_event_repository::{GitHubEventRepository, GitHubEventRepositoryAction},
            github_event_type::GitHubEventType,
            github_types::{
                GitHubInstallation, GitHubRefType, GitHubRepository, GitHubRepositorySelection,
            },
        },
    },
    event_response::EventResponse,
    infisical::secrets::Secrets,
};
use platform_errors::{BpError, BpResult, GitHubError, HttpError};
use platform_github::host_allow_list::HostAllowlist;
use platform_messages::sqs_message::{GitSource, SqsDetail, SqsMessage, SqsRefType};
use platform_metrics::event::EventMetrics;
use hmac::{Hmac, KeyInit, Mac};
use sha2::Sha256;
use subtle::ConstantTimeEq;

const GITHUB_WEBHOOK_SECRET: &str = "GITHUB_WEBHOOK_SECRET";
const UNKNOWN_ACTION: &str = "unknown";
const NO_ACTION: &str = "none";

pub struct GitHubEventConverter {
    secret: String,
    allowed_hosts: HostAllowlist,
}

impl GitHubEventConverter {
    pub fn new(secrets: &Secrets, allowed_hosts: HostAllowlist) -> BpResult<Self> {
        let secret = secrets.get(GITHUB_WEBHOOK_SECRET)?;
        if secret.is_empty() {
            Err(BpError::config_empty(GITHUB_WEBHOOK_SECRET))?;
        }
        Ok(Self {
            secret,
            allowed_hosts,
        })
    }
}

impl EventConverter for GitHubEventConverter {
    fn event_response(&self, request: &lambda_http::Request) -> BpResult<EventResponse> {
        let Some(event_type_str) = request
            .headers()
            .get("x-github-event")
            .and_then(|v| v.to_str().ok())
        else {
            return Ok(EventResponse::WrongGitProvider);
        };
        if !Self::verify_signature(request, &self.secret) {
            return Ok(EventResponse::Unauthorized);
        }
        let delivery_id = Self::delivery_id(request);
        let event_type = GitHubEventType::from(event_type_str);
        tracing::info!(delivery_id = ?delivery_id, "event type: {event_type}");
        match event_type {
            GitHubEventType::Create => self.ref_response(request, delivery_id),
            GitHubEventType::Delete => {
                EventMetrics::record_ignored_event(event_type_str, NO_ACTION);
                Ok(EventResponse::Ignore)
            }
            GitHubEventType::Installation => self.installation_response(request, delivery_id),
            GitHubEventType::InstallationRepositories => {
                self.installation_repositories_response(request, delivery_id)
            }
            GitHubEventType::Other => {
                EventMetrics::record_ignored_event(event_type_str, UNKNOWN_ACTION);
                Ok(EventResponse::Ignore)
            }
            GitHubEventType::Ping => self.ping_response(),
            GitHubEventType::Push => self.push_response(request, delivery_id),
            GitHubEventType::Repository => self.repository_response(request, delivery_id),
        }
    }
}

// responses
impl GitHubEventConverter {
    fn installation_response(
        &self,
        request: &lambda_http::Request,
        delivery_id: Option<String>,
    ) -> BpResult<EventResponse> {
        let event: GitHubEventInstallation = serde_json::from_slice(request.body())
            .map_err(|error| BpError::Http(HttpError::Serde(error)))?;
        let installation_id = event.installation.id;
        let action = event.action;
        match action {
            GitHubEventInstallationAction::Created | GitHubEventInstallationAction::Unsuspend => {}
            _ => {
                EventMetrics::record_ignored_event("installation", &action.to_string());
                return Ok(EventResponse::Ignore);
            }
        }
        match event.installation.repository_selection {
            GitHubRepositorySelection::All => {
                let host = self.host(&event.installation)?;
                Self::accept(host, delivery_id, installation_id, SqsDetail::AllReposAdd)
            }
            GitHubRepositorySelection::Selected if !event.repositories.is_empty() => {
                let host = self.host(&event.installation)?;
                Self::accept(
                    host,
                    delivery_id,
                    installation_id,
                    SqsDetail::MultiRepoAdd {
                        repo_ids: Self::repo_ids(event.repositories),
                    },
                )
            }
            _ => {
                EventMetrics::record_ignored_event("installation", &action.to_string());
                Ok(EventResponse::Ignore)
            }
        }
    }

    fn installation_repositories_response(
        &self,
        request: &lambda_http::Request,
        delivery_id: Option<String>,
    ) -> BpResult<EventResponse> {
        let event: GitHubEventInstallationRepositories = serde_json::from_slice(request.body())
            .map_err(|error| BpError::Http(HttpError::Serde(error)))?;
        let action = event.action;
        match action {
            GitHubEventInstallationRepositoriesAction::Added => {}
            _ => {
                EventMetrics::record_ignored_event(
                    "installation_repositories",
                    &action.to_string(),
                );
                return Ok(EventResponse::Ignore);
            }
        }
        let repositories = event.repositories_added;
        if repositories.is_empty() {
            EventMetrics::record_ignored_event("installation_repositories", &action.to_string());
            return Ok(EventResponse::Ignore);
        }
        let host = self.host(&event.installation)?;
        let detail = SqsDetail::MultiRepoAdd {
            repo_ids: Self::repo_ids(repositories),
        };
        Self::accept(host, delivery_id, event.installation.id, detail)
    }

    fn repository_response(
        &self,
        request: &lambda_http::Request,
        delivery_id: Option<String>,
    ) -> BpResult<EventResponse> {
        let event: GitHubEventRepository = serde_json::from_slice(request.body())
            .map_err(|error| BpError::Http(HttpError::Serde(error)))?;
        let installation_id = event.installation.id;
        let repo_id = Self::repo_id(event.repository);
        let action = event.action;
        match action {
            GitHubEventRepositoryAction::Created => {
                let host = self.host(&event.installation)?;
                Self::accept(
                    host,
                    delivery_id,
                    installation_id,
                    SqsDetail::RepoAdd { repo_id },
                )
            }
            _ => {
                EventMetrics::record_ignored_event("repository", &action.to_string());
                Ok(EventResponse::Ignore)
            }
        }
    }

    fn ref_response(
        &self,
        request: &lambda_http::Request,
        delivery_id: Option<String>,
    ) -> BpResult<EventResponse> {
        let event: GitHubEventRef = serde_json::from_slice(request.body())
            .map_err(|error| BpError::Http(HttpError::Serde(error)))?;
        let Some(ref_type) = Self::ref_type(&event.ref_type) else {
            EventMetrics::record_ignored_event("create", &event.ref_type.to_string());
            return Ok(EventResponse::Ignore);
        };
        let host = self.host(&event.installation)?;
        let detail = SqsDetail::RefCreate {
            repo_id: Self::repo_id(event.repository),
            ref_name: Self::ref_name(&event.r#ref),
            ref_type,
        };
        Self::accept(host, delivery_id, event.installation.id, detail)
    }

    fn ping_response(&self) -> BpResult<EventResponse> {
        Ok(EventResponse::Pong)
    }

    fn push_response(
        &self,
        request: &lambda_http::Request,
        delivery_id: Option<String>,
    ) -> BpResult<EventResponse> {
        let event: GitHubEventPush = serde_json::from_slice(request.body())
            .map_err(|error| BpError::Http(HttpError::Serde(error)))?;
        let GitHubEventPush {
            after,
            created,
            deleted,
            installation,
            r#ref,
            repository,
            ..
        } = event;
        if created || deleted || Self::is_zero_sha(&after) {
            EventMetrics::record_ignored_event("push", NO_ACTION);
            return Ok(EventResponse::Ignore);
        }
        let Some((ref_name, ref_type)) = Self::commit_ref(&r#ref) else {
            EventMetrics::record_ignored_event("push", UNKNOWN_ACTION);
            return Ok(EventResponse::Ignore);
        };
        let host = self.host(&installation)?;
        let detail = SqsDetail::Commit {
            repo_id: Self::repo_id(repository),
            ref_name: ref_name.into(),
            ref_type,
            commit_hash: after,
        };
        Self::accept(host, delivery_id, installation.id, detail)
    }
}

// helpers
impl GitHubEventConverter {
    fn is_zero_sha(sha: &str) -> bool {
        !sha.is_empty() && sha.bytes().all(|byte| byte == b'0')
    }

    fn repo_ids(repositories: Vec<GitHubRepository>) -> Vec<String> {
        repositories.into_iter().map(Self::repo_id).collect()
    }

    fn repo_id(repository: GitHubRepository) -> String {
        repository.id.to_string()
    }

    fn ref_name(r#ref: &str) -> String {
        r#ref
            .strip_prefix("refs/heads/")
            .or_else(|| r#ref.strip_prefix("refs/tags/"))
            .unwrap_or(r#ref)
            .to_string()
    }

    fn commit_ref(r#ref: &str) -> Option<(&str, SqsRefType)> {
        if let Some(branch_name) = r#ref.strip_prefix("refs/heads/") {
            Some((branch_name, SqsRefType::Branch))
        } else if let Some(tag_name) = r#ref.strip_prefix("refs/tags/") {
            Some((tag_name, SqsRefType::Tag))
        } else {
            None
        }
    }

    fn ref_type(ref_type: &GitHubRefType) -> Option<SqsRefType> {
        match ref_type {
            GitHubRefType::Branch => Some(SqsRefType::Branch),
            GitHubRefType::Tag => Some(SqsRefType::Tag),
            GitHubRefType::Other => None,
        }
    }

    fn host(&self, installation: &GitHubInstallation) -> BpResult<String> {
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

    fn delivery_id(request: &lambda_http::Request) -> Option<String> {
        request
            .headers()
            .get("x-github-delivery")
            .and_then(|v| v.to_str().ok())
            .filter(|delivery_id| !delivery_id.is_empty())
            .map(str::to_owned)
    }

    fn accept(
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

    fn verify_signature(request: &lambda_http::Request, secret: &str) -> bool {
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

#[cfg(test)]
mod tests {
    use super::*;
    use lambda_http::Body;

    const SECRET: &str = "top-secret";

    fn converter() -> GitHubEventConverter {
        converter_with_hosts(["github.com", "ghe.example.com"])
    }

    fn converter_with_hosts<I, S>(hosts: I) -> GitHubEventConverter
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        GitHubEventConverter {
            secret: SECRET.to_owned(),
            allowed_hosts: HostAllowlist::from_hosts(hosts).unwrap(),
        }
    }

    fn host_from_converter(converter: &GitHubEventConverter, html_url: &str) -> BpResult<String> {
        let body = installation_payload(html_url);
        let request = signed_request(SECRET, "installation", &body);
        match converter.event_response(&request)? {
            EventResponse::Accept(messages) => Ok(messages[0].source.host().to_owned()),
            other => panic!("expected accept, got {other:?}"),
        }
    }

    fn signed_request(secret: &str, event_type: &str, body: &[u8]) -> lambda_http::Request {
        let mut mac = Hmac::<Sha256>::new_from_slice(secret.as_bytes()).unwrap();
        mac.update(body);
        let signature = hex::encode(mac.finalize().into_bytes());
        http::Request::builder()
            .header("x-github-event", event_type)
            .header("x-hub-signature-256", format!("sha256={signature}"))
            .body(Body::from(body.to_vec()))
            .unwrap()
    }

    fn installation_payload(html_url: &str) -> Vec<u8> {
        format!(
            r#"{{"action":"created","installation":{{"id":123,"repository_selection":"all","html_url":{html_url:?}}}}}"#
        )
        .into_bytes()
    }

    fn host_from_installation(html_url: &str) -> BpResult<String> {
        host_from_converter(&converter(), html_url)
    }

    #[test]
    fn verifies_valid_signature() {
        let body = br#"{"action":"ping"}"#;
        let request = signed_request(SECRET, "ping", body);
        assert!(GitHubEventConverter::verify_signature(&request, SECRET));
    }

    #[test]
    fn rejects_missing_signature() {
        let request = http::Request::builder()
            .header("x-github-event", "ping")
            .body(Body::from(br#"{"action":"ping"}"#.to_vec()))
            .unwrap();
        assert!(!GitHubEventConverter::verify_signature(&request, SECRET));
    }

    #[test]
    fn rejects_wrong_signature() {
        let body = br#"{"action":"ping"}"#;
        let request = signed_request("not-the-secret", "ping", body);
        assert!(!GitHubEventConverter::verify_signature(&request, SECRET));
    }

    #[test]
    fn rejects_tampered_body() {
        let body = br#"{"action":"ping"}"#;
        let mut request = signed_request(SECRET, "ping", body);
        *request.body_mut() = Body::from(br#"{"action":"pong"}"#.to_vec());
        assert!(!GitHubEventConverter::verify_signature(&request, SECRET));
    }

    #[test]
    fn rejects_malformed_signature_header() {
        let body = br#"{"action":"ping"}"#;
        let request = http::Request::builder()
            .header("x-github-event", "ping")
            .header("x-hub-signature-256", "sha1=deadbeef")
            .body(Body::from(body.to_vec()))
            .unwrap();
        assert!(!GitHubEventConverter::verify_signature(&request, SECRET));
    }

    #[test]
    fn installation_event_normalizes_uppercase_host() {
        assert_eq!(
            host_from_installation("https://GITHUB.COM/").unwrap(),
            "github.com"
        );
    }

    #[test]
    fn installation_event_accepts_enterprise_host() {
        assert_eq!(
            host_from_installation("https://ghe.example.com/").unwrap(),
            "ghe.example.com"
        );
    }

    #[test]
    fn installation_event_rejects_disallowed_host() {
        let body = installation_payload("https://evil.com/");
        let request = signed_request(SECRET, "installation", &body);
        assert!(matches!(
            converter().event_response(&request),
            Err(BpError::GitHub(GitHubError::HostNotAllowed { host: url })) if url == "https://evil.com/"
        ));
    }

    #[test]
    fn installation_event_rejects_non_https_scheme() {
        assert!(matches!(
            host_from_installation("http://github.com/"),
            Err(BpError::GitHub(GitHubError::HostInvalid { host: url })) if url == "http://github.com/"
        ));
    }

    #[test]
    fn installation_event_accepts_enterprise_host_on_configured_port() {
        let converter = converter_with_hosts(["github.com", "ghe.example.com:8443"]);
        assert_eq!(
            host_from_converter(&converter, "https://ghe.example.com:8443/").unwrap(),
            "ghe.example.com:8443"
        );
    }

    #[test]
    fn installation_event_rejects_enterprise_host_on_unconfigured_port() {
        let converter = converter_with_hosts(["github.com", "ghe.example.com:8443"]);
        for url in [
            "https://ghe.example.com/",
            "https://ghe.example.com:443/",
            "https://ghe.example.com:9999/",
        ] {
            assert!(
                host_from_converter(&converter, url).is_err(),
                "accepted {url}"
            );
        }
    }

    #[test]
    fn installation_event_rejects_github_com_on_custom_port() {
        let converter = converter_with_hosts(["github.com", "ghe.example.com:8443"]);
        assert!(host_from_converter(&converter, "https://github.com:8443/").is_err());
    }

    #[test]
    fn installation_event_rejects_userinfo_trick() {
        assert!(matches!(
            host_from_installation("https://github.com@evil.com/"),
            Err(BpError::GitHub(GitHubError::HostInvalid { host: url })) if url == "https://github.com@evil.com/"
        ));
    }
}
