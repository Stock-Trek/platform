use crate::{
    ApiToken, CachedTokenStore, circuit_breaker::BreakerDecision, flight_gates::FlightOutcome,
    token_cache::CacheLookup,
};
use platform_errors::{AuthError, BpError, BpResult, ErrorType};
use platform_metrics::auth::AuthTokenCacheMetrics;
use std::{
    sync::Arc,
    time::{Duration, Instant},
};

impl CachedTokenStore {
    pub(crate) async fn cached_get(&self, id: &str) -> BpResult<Option<ApiToken>> {
        match self.cache.lookup(id, Instant::now()) {
            CacheLookup::Fresh(Some(token)) => {
                AuthTokenCacheMetrics::record_hit("token");
                return Ok(Some(token));
            }
            CacheLookup::Fresh(None) => {
                AuthTokenCacheMetrics::record_hit("missing");
                return Ok(None);
            }
            CacheLookup::Stale(_) | CacheLookup::Miss => {}
        }

        let gate = self.gates.gate(id);
        let flight = gate.lock().await;
        if let Some(outcome) = flight.outcome() {
            return self.flight_result(id, outcome);
        }

        match self.cache.lookup(id, Instant::now()) {
            CacheLookup::Fresh(Some(token)) => {
                AuthTokenCacheMetrics::record_hit("token");
                flight.complete(FlightOutcome::Found(Some(Arc::new(token.clone()))));
                return Ok(Some(token));
            }
            CacheLookup::Fresh(None) => {
                AuthTokenCacheMetrics::record_hit("missing");
                flight.complete(FlightOutcome::Found(None));
                return Ok(None);
            }
            CacheLookup::Stale(_) | CacheLookup::Miss => {}
        }

        AuthTokenCacheMetrics::record_miss();
        let result = self.load(id).await;
        let outcome = match &result {
            Ok(Some(token)) => FlightOutcome::Found(Some(Arc::new(token.clone()))),
            Ok(None) => FlightOutcome::Found(None),
            Err(_) => FlightOutcome::Failed,
        };
        flight.complete(outcome);
        result
    }

    fn flight_result(&self, id: &str, outcome: FlightOutcome) -> BpResult<Option<ApiToken>> {
        match outcome {
            FlightOutcome::Found(token) => Ok(token.map(Arc::unwrap_or_clone)),
            FlightOutcome::Failed => {
                self.stale_or_error(id, Self::unavailable(self.breaker.retry_after()))
            }
        }
    }

    async fn load(&self, id: &str) -> BpResult<Option<ApiToken>> {
        let generation = self.cache.generation(id);
        let epoch = self.cache.epoch();
        let observed_at = Instant::now();
        let permit = match self.breaker.allow_request() {
            BreakerDecision::Allowed(permit) => permit,
            BreakerDecision::Rejected(retry_after) => {
                return self.stale_or_error(id, Self::unavailable(retry_after));
            }
        };
        let started_at = Instant::now();
        let result = self.inner.get(id).await;
        AuthTokenCacheMetrics::record_vault_read(started_at.elapsed());
        match result {
            Ok(token) => {
                permit.success();
                match &token {
                    Some(token) => {
                        self.cache.insert_loaded_token(
                            id,
                            token.clone(),
                            generation,
                            epoch,
                            observed_at,
                        );
                    }
                    None => {
                        self.cache
                            .insert_loaded_missing(id, generation, epoch, observed_at);
                    }
                }
                Ok(token)
            }
            Err(error) => {
                if Self::breaker_failure(&error) {
                    permit.failure();
                } else {
                    permit.success();
                }
                AuthTokenCacheMetrics::record_error("read", error.error_type());
                self.stale_or_error(id, error)
            }
        }
    }

    fn stale_or_error(&self, id: &str, error: BpError) -> BpResult<Option<ApiToken>> {
        match self.cache.lookup(id, Instant::now()) {
            CacheLookup::Fresh(Some(token)) => {
                AuthTokenCacheMetrics::record_hit("token");
                Ok(Some(token))
            }
            CacheLookup::Fresh(None) => {
                AuthTokenCacheMetrics::record_hit("missing");
                Ok(None)
            }
            CacheLookup::Stale(token) => {
                AuthTokenCacheMetrics::record_stale_serve();
                Ok(Some(token))
            }
            CacheLookup::Miss => Err(error),
        }
    }

    fn breaker_failure(error: &BpError) -> bool {
        match error {
            BpError::Auth(AuthError::Unavailable { .. }) => true,
            BpError::Auth(AuthError::Store { retryable, .. }) => *retryable,
            _ => false,
        }
    }

    fn unavailable(retry_after: Duration) -> BpError {
        BpError::Auth(AuthError::Unavailable { retry_after })
    }
}
