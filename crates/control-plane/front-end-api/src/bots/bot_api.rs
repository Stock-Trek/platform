use crate::api::ApiRequest;
use crate::api::IdempotencyStore;
use crate::api::PageQuery;
use crate::api::Problem;
use crate::api::Respond;
use crate::auth::Scope;
use crate::auth::SessionService;
use crate::bots::BotService;
use crate::bots::{BotState, CreateBotRequest, KillRequest, PatchBotRequest};
use chrono::Utc;
use http_body_util::Full;
use hyper::body::Bytes;
use hyper::{Method, Response, StatusCode};
use std::sync::Arc;

pub struct BotApi {
    bots: Arc<BotService>,
    sessions: Arc<SessionService>,
    idempotency: Arc<IdempotencyStore>,
}

impl BotApi {
    pub fn new(
        bots: Arc<BotService>,
        sessions: Arc<SessionService>,
        idempotency: Arc<IdempotencyStore>,
    ) -> Self {
        Self {
            bots,
            sessions,
            idempotency,
        }
    }

    pub fn route(&self, request: &ApiRequest) -> Result<Response<Full<Bytes>>, Problem> {
        match request.segments().as_slice() {
            ["bots"] => match *request.method() {
                Method::POST => self.create(request),
                Method::GET => self.list(request),
                _ => Err(Problem::method_not_allowed(&[Method::POST, Method::GET])),
            },
            ["bots", bot_id] => match *request.method() {
                Method::GET => self.get(request, bot_id),
                Method::PATCH => self.patch(request, bot_id),
                Method::DELETE => self.archive(request, bot_id),
                _ => Err(Problem::method_not_allowed(&[
                    Method::GET,
                    Method::PATCH,
                    Method::DELETE,
                ])),
            },
            ["bots", bot_id, "enable"] => match *request.method() {
                Method::POST => self.enable(request, bot_id),
                _ => Err(Problem::method_not_allowed(&[Method::POST])),
            },
            ["bots", bot_id, "disable"] => match *request.method() {
                Method::POST => self.disable(request, bot_id),
                _ => Err(Problem::method_not_allowed(&[Method::POST])),
            },
            ["bots", bot_id, "runs"] => match *request.method() {
                Method::POST => self.trigger_run(request, bot_id),
                _ => Err(Problem::method_not_allowed(&[Method::POST])),
            },
            ["kill"] => match *request.method() {
                Method::POST => self.kill(request),
                _ => Err(Problem::method_not_allowed(&[Method::POST])),
            },
            _ => Err(Problem::not_found("no such endpoint")),
        }
    }

    fn create(&self, request: &ApiRequest) -> Result<Response<Full<Bytes>>, Problem> {
        let auth = self.sessions.authorize_request(request, Scope::Manage)?;
        request.require_json_content_type()?;
        self.idempotency
            .apply(request, auth.owner(), "create-bot", || {
                let create: CreateBotRequest = request.json()?;
                let bot = self.bots.create(&auth, &create, Utc::now())?;
                Ok(Respond::json_with_etag(
                    StatusCode::CREATED,
                    &bot,
                    bot.version,
                ))
            })
    }

    fn list(&self, request: &ApiRequest) -> Result<Response<Full<Bytes>>, Problem> {
        let auth = self.sessions.authorize_request(request, Scope::Read)?;
        let state = Self::state_filter(request)?;
        let page = self.bots.list(&auth, state, &PageQuery::parse(request)?);
        Ok(Respond::json(StatusCode::OK, &page))
    }

    fn get(&self, request: &ApiRequest, bot_id: &str) -> Result<Response<Full<Bytes>>, Problem> {
        let auth = self.sessions.authorize_request(request, Scope::Read)?;
        let bot = self.bots.get(&auth, bot_id)?;
        Ok(Respond::json_with_etag(StatusCode::OK, &bot, bot.version))
    }

    fn patch(&self, request: &ApiRequest, bot_id: &str) -> Result<Response<Full<Bytes>>, Problem> {
        let auth = self.sessions.authorize_request(request, Scope::Manage)?;
        request.require_merge_patch_content_type()?;
        let patch = PatchBotRequest::merge_patch(&request.json::<serde_json::Value>()?)?;
        let bot = self
            .bots
            .patch(&auth, bot_id, request.if_match()?, &patch, Utc::now())?;
        Ok(Respond::json_with_etag(StatusCode::OK, &bot, bot.version))
    }

    fn enable(&self, request: &ApiRequest, bot_id: &str) -> Result<Response<Full<Bytes>>, Problem> {
        let auth = self.sessions.authorize_admin(request)?;
        let bot = self
            .bots
            .enable(&auth, bot_id, request.if_match()?, Utc::now())?;
        Ok(Respond::json_with_etag(StatusCode::OK, &bot, bot.version))
    }

    fn disable(
        &self,
        request: &ApiRequest,
        bot_id: &str,
    ) -> Result<Response<Full<Bytes>>, Problem> {
        let auth = self.sessions.authorize_request(request, Scope::Manage)?;
        let bot = self
            .bots
            .disable(&auth, bot_id, request.if_match()?, Utc::now())?;
        Ok(Respond::json_with_etag(StatusCode::OK, &bot, bot.version))
    }

    fn archive(
        &self,
        request: &ApiRequest,
        bot_id: &str,
    ) -> Result<Response<Full<Bytes>>, Problem> {
        let auth = self.sessions.authorize_request(request, Scope::Manage)?;
        self.bots.archive(&auth, bot_id, Utc::now())?;
        Ok(Respond::empty(StatusCode::NO_CONTENT))
    }

    fn trigger_run(
        &self,
        request: &ApiRequest,
        bot_id: &str,
    ) -> Result<Response<Full<Bytes>>, Problem> {
        let auth = self.sessions.authorize_request(request, Scope::Manage)?;
        self.idempotency
            .apply(request, auth.owner(), "trigger-run", || {
                let run = self.bots.trigger_ad_hoc_run(&auth, bot_id, Utc::now())?;
                Ok(Respond::json(StatusCode::ACCEPTED, &run))
            })
    }

    fn kill(&self, request: &ApiRequest) -> Result<Response<Full<Bytes>>, Problem> {
        let auth = self.sessions.authorize_request(request, Scope::Manage)?;
        if !request.body().is_empty() {
            request.require_json_content_type()?;
        }
        let kill: Option<KillRequest> = request.json_optional()?;
        let response = self
            .bots
            .kill(&auth, kill.and_then(|kill| kill.account_ids), Utc::now());
        Ok(Respond::json(StatusCode::ACCEPTED, &response))
    }

    fn state_filter(request: &ApiRequest) -> Result<Option<BotState>, Problem> {
        let Some(value) = request.query("state") else {
            return Ok(None);
        };
        BotState::parse(value)
            .map(Some)
            .ok_or_else(|| Problem::validation_failed("unknown bot state"))
    }
}
