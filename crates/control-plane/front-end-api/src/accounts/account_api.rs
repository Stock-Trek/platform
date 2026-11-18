use crate::accounts::AccountService;
use crate::accounts::account_types::{CredentialsRequest, LinkAccountRequest};
use crate::api::{ApiRequest, Page, PageQuery, Problem, Respond};
use crate::auth::{Scope, SessionService};
use http_body_util::Full;
use hyper::{Method, Response, StatusCode, body::Bytes};
use std::sync::Arc;

pub struct AccountApi {
    accounts: Arc<AccountService>,
    sessions: Arc<SessionService>,
    idempotency: Arc<crate::api::IdempotencyStore>,
}

impl AccountApi {
    pub fn new(
        accounts: Arc<AccountService>,
        sessions: Arc<SessionService>,
        idempotency: Arc<crate::api::IdempotencyStore>,
    ) -> Self {
        Self {
            accounts,
            sessions,
            idempotency,
        }
    }

    pub fn route(&self, request: &ApiRequest) -> Result<Response<Full<Bytes>>, Problem> {
        match request.segments().as_slice() {
            ["accounts"] => match *request.method() {
                Method::POST => self.link(request),
                Method::GET => self.list(request),
                _ => Err(Problem::method_not_allowed(&[Method::POST, Method::GET])),
            },
            ["accounts", account_id] => match *request.method() {
                Method::GET => self.get(request, account_id),
                Method::DELETE => self.unlink(request, account_id),
                _ => Err(Problem::method_not_allowed(&[Method::GET, Method::DELETE])),
            },
            ["accounts", account_id, "credentials"] => match *request.method() {
                Method::PUT => self.replace_credentials(request, account_id),
                _ => Err(Problem::method_not_allowed(&[Method::PUT])),
            },
            ["accounts", account_id, "portfolio"] => match *request.method() {
                Method::GET => self.portfolio(request, account_id),
                _ => Err(Problem::method_not_allowed(&[Method::GET])),
            },
            ["portfolios"] => match *request.method() {
                Method::GET => self.portfolios(request),
                _ => Err(Problem::method_not_allowed(&[Method::GET])),
            },
            _ => Err(Problem::not_found("no such endpoint")),
        }
    }

    fn link(&self, request: &ApiRequest) -> Result<Response<Full<Bytes>>, Problem> {
        let auth = self.sessions.authorize_admin(request)?;
        request.require_json_content_type()?;
        self.idempotency
            .apply(request, auth.owner(), "link-account", || {
                let link: LinkAccountRequest = request.json()?;
                let link = link.validated()?;
                let account = self.accounts.link(&auth, &link, chrono::Utc::now())?;
                Ok(Respond::json_with_etag(
                    StatusCode::CREATED,
                    &account,
                    account.etag_version(),
                ))
            })
    }

    fn list(&self, request: &ApiRequest) -> Result<Response<Full<Bytes>>, Problem> {
        let auth = self.sessions.authorize_request(request, Scope::Read)?;
        let accounts = self.accounts.list(auth.owner());
        let page = Page::of(accounts, &PageQuery::parse(request));
        Ok(Respond::json(StatusCode::OK, &page))
    }

    fn get(
        &self,
        request: &ApiRequest,
        account_id: &str,
    ) -> Result<Response<Full<Bytes>>, Problem> {
        let auth = self.sessions.authorize_request(request, Scope::Read)?;
        let account = self.accounts.get(auth.owner(), account_id)?;
        Ok(Respond::json_with_etag(
            StatusCode::OK,
            &account,
            account.etag_version(),
        ))
    }

    fn replace_credentials(
        &self,
        request: &ApiRequest,
        account_id: &str,
    ) -> Result<Response<Full<Bytes>>, Problem> {
        let auth = self.sessions.authorize_admin(request)?;
        request.require_json_content_type()?;
        let credentials: CredentialsRequest = request.json()?;
        self.accounts
            .replace_credentials(&auth, account_id, &credentials, chrono::Utc::now())?;
        Ok(Respond::empty(StatusCode::NO_CONTENT))
    }

    fn unlink(
        &self,
        request: &ApiRequest,
        account_id: &str,
    ) -> Result<Response<Full<Bytes>>, Problem> {
        let auth = self.sessions.authorize_admin(request)?;
        let force = request.query("force").is_some_and(|value| value == "true");
        match self
            .accounts
            .unlink(&auth, account_id, force, chrono::Utc::now())?
        {
            Some(response) => Ok(Respond::json(StatusCode::OK, &response)),
            None => Ok(Respond::empty(StatusCode::NO_CONTENT)),
        }
    }

    fn portfolio(
        &self,
        request: &ApiRequest,
        account_id: &str,
    ) -> Result<Response<Full<Bytes>>, Problem> {
        let auth = self.sessions.authorize_request(request, Scope::Read)?;
        let snapshot = self.accounts.snapshot(auth.owner(), account_id)?;
        Ok(Respond::json(StatusCode::OK, &snapshot))
    }

    fn portfolios(&self, request: &ApiRequest) -> Result<Response<Full<Bytes>>, Problem> {
        let auth = self.sessions.authorize_request(request, Scope::Read)?;
        let snapshots = self.accounts.snapshots(auth.owner());
        let page = Page::of(snapshots, &PageQuery::parse(request));
        Ok(Respond::json(StatusCode::OK, &page))
    }
}
