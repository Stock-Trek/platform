mod http;
#[cfg(test)]
mod tests;
mod websocket;

use crate::{
    auto_resync_connector::AutoResyncConnector,
    clients::client::{HttpClient, WebsocketClient},
    clients::{iris::IrisWebsocketClient, reqwest::ReqwestHttpClient},
    clock::Clock,
    rate_limit::{
        rate_limiter::RateLimiter, rate_limiter_state::RateLimiterState,
        rate_limiters::RateLimiters,
    },
    websocket_listener::WebsocketListener,
};
use iris::{Config as IrisConfig, DisconnectedBehavior, ServerCloseBehavior};
use platform_errors::{BpError, BpResult, SendFailure};
use platform_exchange_types::{
    exchange::ETExchange,
    new_types::{TimestampMilliseconds, UsageCount},
    rate_limited::{RateLimit, RateLimitRestriction},
    request::ETRequest,
    response::ETResponse,
    signer::Signer,
    urls::Protocol,
};
use std::{collections::HashMap, sync::Arc, time::Duration};
use strum::IntoEnumIterator;

pub struct Connector<Exchange, Client> {
    exchange: Exchange,
    rate_limiters: RateLimiters,
    clock: Clock,
    signer: Signer,
    client: Client,
    request_timeout: Duration,
    max_retries: u8,
    websocket_listener: Option<Arc<WebsocketListener>>,
}

impl Connector<(), ()> {
    pub fn try_new_http<Exchange, Client>(
        url: String,
        exchange: Exchange,
        signer: Signer,
        client_creator: impl FnOnce(String) -> BpResult<Client>,
        request_timeout: Duration,
        max_retries: u8,
    ) -> BpResult<Connector<Exchange, Client>>
    where
        Exchange: ETExchange,
        Client: HttpClient + Send + Sync,
    {
        Protocol::Http
            .validate_url("url", &url)
            .map_err(BpError::external)?;
        let client = client_creator(url)?;
        let rate_limiters = Self::rate_limiters(exchange.default_capacity())?;
        Ok(Connector {
            exchange,
            rate_limiters,
            clock: Clock::default(),
            signer,
            client,
            request_timeout,
            max_retries,
            websocket_listener: None,
        })
    }
    pub fn try_new_http_reqwest<Exchange>(
        url: String,
        exchange: Exchange,
        signer: Signer,
        request_timeout: Duration,
        max_retries: u8,
    ) -> BpResult<Connector<Exchange, ReqwestHttpClient>>
    where
        Exchange: ETExchange,
    {
        let client_creator = Box::new(move |url: String| ReqwestHttpClient::try_new(&url));
        Self::try_new_http(
            url,
            exchange,
            signer,
            client_creator,
            request_timeout,
            max_retries,
        )
    }
    #[allow(clippy::type_complexity)]
    pub fn try_new_websocket<Exchange, Client>(
        url: String,
        exchange: Exchange,
        signer: Signer,
        client_creator: impl FnOnce(String, Arc<WebsocketListener>) -> BpResult<Client>,
        request_timeout: Duration,
        max_retries: u8,
    ) -> BpResult<Connector<Exchange, Client>>
    where
        Exchange: ETExchange,
        Client: WebsocketClient,
    {
        Protocol::Websocket
            .validate_url("url", &url)
            .map_err(BpError::external)?;
        let websocket_listener = Arc::new(WebsocketListener::new());
        let client = client_creator(url, websocket_listener.clone())?;
        let rate_limiters = Self::rate_limiters(exchange.default_capacity())?;
        Ok(Connector {
            exchange,
            rate_limiters,
            clock: Clock::default(),
            signer,
            client,
            request_timeout,
            max_retries,
            websocket_listener: Some(websocket_listener),
        })
    }
    #[allow(clippy::type_complexity)]
    pub fn try_new_websocket_iris<Exchange>(
        url: String,
        exchange: Exchange,
        signer: Signer,
        mut iris_config: IrisConfig,
        request_timeout: Duration,
        max_retries: u8,
    ) -> BpResult<Connector<Exchange, IrisWebsocketClient>>
    where
        Exchange: ETExchange,
    {
        iris_config = iris_config
            .with_disconnected_behavior(DisconnectedBehavior::DropAllQueued)
            .with_server_close_behavior(ServerCloseBehavior::Reconnect);
        let client_creator = move |url: String, websocket_listener| {
            Ok(IrisWebsocketClient::with_config(
                &url,
                iris_config,
                websocket_listener,
            ))
        };
        Self::try_new_websocket(
            url,
            exchange,
            signer,
            client_creator,
            request_timeout,
            max_retries,
        )
    }
    fn rate_limiters(default_capacity: HashMap<RateLimit, UsageCount>) -> BpResult<RateLimiters> {
        let mut limiter_states = HashMap::new();
        for (rate_limit, capacity) in default_capacity {
            let RateLimit {
                restriction,
                interval_nanos,
            } = rate_limit;
            let states = limiter_states.entry(restriction).or_insert_with(Vec::new);
            let state = RateLimiterState::try_new(interval_nanos, capacity)?;
            states.push(state);
        }
        let limiters = limiter_states
            .iter()
            .map(|(restriction, states)| (*restriction, RateLimiter::new(states)))
            .collect::<HashMap<RateLimitRestriction, RateLimiter>>();
        Ok(RateLimiters::new(limiters))
    }
}

impl<Exchange, Client> Connector<Exchange, Client>
where
    Exchange: ETExchange,
{
    pub fn into_auto_resync(self) -> AutoResyncConnector<Exchange, Client>
    where
        Exchange: Send + Sync + 'static,
        Client: Send + Sync + 'static,
    {
        AutoResyncConnector::new(Arc::new(self))
    }
    pub fn duration_since_last_sync(&self) -> BpResult<Option<Duration>> {
        self.clock.duration_since_last_sync()
    }
    pub fn remaining_rate_limit_capacity(&self) -> BpResult<HashMap<RateLimit, UsageCount>> {
        self.rate_limiters.remaining_capacity()
    }
    pub fn server_time_estimate(&self) -> BpResult<TimestampMilliseconds> {
        self.clock.server_time_estimate()
    }
    fn validate_rate_limits<Request>(
        &self,
        request: &Request,
    ) -> BpResult<Vec<(RateLimitRestriction, UsageCount)>>
    where
        Request: ETRequest,
    {
        let costs = RateLimitRestriction::iter()
            .filter_map(|restriction| {
                let cost = request.rate_limit_usage(restriction);
                (cost > UsageCount::ZERO).then_some((restriction, cost))
            })
            .collect::<Vec<_>>();
        self.rate_limiters.did_acquire(&costs)?;
        Ok(costs)
    }
    fn refund_if_not_sent(
        &self,
        error: &BpError,
        costs: &mut Vec<(RateLimitRestriction, UsageCount)>,
    ) -> BpResult<()> {
        if error.send_failure().is_some_and(SendFailure::is_not_sent) {
            self.refund(std::mem::take(costs))?;
        }
        Ok(())
    }
    fn refund(&self, costs: Vec<(RateLimitRestriction, UsageCount)>) -> BpResult<()> {
        for (restriction, cost) in costs {
            self.rate_limiters.refund(restriction, cost)?;
        }
        Ok(())
    }
    fn on_send_failure<T>(
        &self,
        error: BpError,
        costs: Vec<(RateLimitRestriction, UsageCount)>,
    ) -> BpResult<T> {
        if error.send_failure().is_some_and(SendFailure::is_not_sent) {
            self.refund(costs)?;
        }
        Err(error)
    }
    fn set_rate_limits(&self, response: &impl ETResponse) -> BpResult<()> {
        if let Some(usage) = response.rate_limit_usage() {
            self.rate_limiters.set_usage(usage)?;
        }
        if let Some(retry_after_seconds) = response.retry_after() {
            let retry_after = Duration::from_secs(retry_after_seconds.0.max(0) as u64);
            self.rate_limiters.set_retry_after(retry_after)?;
        }
        Ok(())
    }
}

impl<Exchange, Client> std::fmt::Debug for Connector<Exchange, Client> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Connector")
            .field("rate_limiters", &self.rate_limiters)
            .field("clock", &self.clock)
            .field("signer", &"<signer>")
            .field("client", &"<client>")
            .finish()
    }
}
