use crate::{
    ConsumeResult, ReplayKey, ReplayReservation, ReplayStore, ReplayStoreError,
    ValkeyReplayStoreConfig, valkey_error::ValkeyError,
};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use platform_errors::BuildResult;
use platform_metrics::AuthReplayMetrics;
use redis::{
    Client, Script,
    aio::{ConnectionManager, ConnectionManagerConfig},
};
use tokio::sync::OnceCell;

const RELEASE_SCRIPT: &str = r#"
if redis.call("GET", KEYS[1]) == ARGV[1] then
    return redis.call("DEL", KEYS[1])
end
return 0
"#;

pub struct ValkeyReplayStore {
    config: ValkeyReplayStoreConfig,
    client: Client,
    connection: OnceCell<ConnectionManager>,
    manager_config: ConnectionManagerConfig,
    release_script: Script,
}

impl ValkeyReplayStore {
    pub fn new(config: ValkeyReplayStoreConfig) -> BuildResult<Self> {
        let client = Client::open(config.url()).map_err(|error| {
            platform_errors::BuildError::config_invalid_source("valkey_url", error)
        })?;
        let manager_config = ConnectionManagerConfig::new()
            .set_connection_timeout(config.timeout())
            .set_response_timeout(config.timeout())
            .set_number_of_retries(0);
        Ok(Self {
            config,
            client,
            connection: OnceCell::new(),
            manager_config,
            release_script: Script::new(RELEASE_SCRIPT),
        })
    }

    pub async fn connect(config: ValkeyReplayStoreConfig) -> BuildResult<Self> {
        let store = Self::new(config)?;
        ValkeyError::result(store.connection().await)?;
        Ok(store)
    }

    fn key(&self, replay_key: &ReplayKey) -> String {
        format!(
            "{}:{}:{}",
            self.config.key_prefix(),
            replay_key.token_id(),
            replay_key.nonce()
        )
    }

    async fn connection(&self) -> Result<ConnectionManager, ReplayStoreError> {
        self.connection
            .get_or_try_init(|| async {
                ConnectionManager::new_with_config(self.client.clone(), self.manager_config.clone())
                    .await
                    .map_err(|error| ValkeyError::store("connect", error))
            })
            .await
            .cloned()
    }

    fn next_reservation_id() -> u64 {
        rand::random::<u64>().max(1)
    }

    fn ttl_millis(expires_at: DateTime<Utc>, now: DateTime<Utc>) -> i64 {
        expires_at
            .signed_duration_since(now)
            .num_milliseconds()
            .max(1)
    }

    #[cfg(test)]
    pub fn key_for_test(&self, replay_key: &ReplayKey) -> String {
        self.key(replay_key)
    }
}

#[async_trait]
impl ReplayStore for ValkeyReplayStore {
    async fn consume(
        &self,
        key: ReplayKey,
        expires_at: DateTime<Utc>,
        now: DateTime<Utc>,
    ) -> Result<ConsumeResult, ReplayStoreError> {
        if expires_at <= now {
            return Ok(ConsumeResult::Expired);
        }
        let mut connection = self.connection().await?;
        let reservation_id = Self::next_reservation_id();
        let stored: Option<String> = redis::cmd("SET")
            .arg(self.key(&key))
            .arg(reservation_id)
            .arg("NX")
            .arg("PX")
            .arg(Self::ttl_millis(expires_at, now))
            .query_async(&mut connection)
            .await
            .map_err(|error| ValkeyError::store("consume", error))?;
        if stored.is_some() {
            Ok(ConsumeResult::Consumed(ReplayReservation::new(
                key,
                reservation_id,
            )))
        } else {
            AuthReplayMetrics::record_hit();
            Ok(ConsumeResult::Replayed)
        }
    }

    async fn release(&self, reservation: &ReplayReservation) -> Result<(), ReplayStoreError> {
        let mut connection = self.connection().await?;
        let _: i64 = self
            .release_script
            .key(self.key(reservation.key()))
            .arg(reservation.id())
            .invoke_async(&mut connection)
            .await
            .map_err(|error| ValkeyError::store("release", error))?;
        Ok(())
    }
}
