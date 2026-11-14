use crate::{
    TokenUsage, TokenUsageQueue, TokenUsageQueueConfig, TokenUsageReservation,
    token_usage_queue_config::ValkeyTokenUsageReply,
};
use async_trait::async_trait;
use platform_errors::{PlatformError, PlatformResult};
use platform_metrics::AuthUsageQueueMetrics;
use redis::{
    Client, RedisError, Script, Value,
    aio::{ConnectionManager, ConnectionManagerConfig},
};
use tokio::sync::OnceCell;

const GROUP: &str = "auth-usage-workers";
const CONSUMER: &str = "auth-usage-worker";

const ENQUEUE_SCRIPT: &str = r#"
if redis.call("XLEN", KEYS[1]) >= tonumber(ARGV[1]) then
    return 0
end
redis.call("XADD", KEYS[1], "*", "payload", ARGV[2])
return 1
"#;

const ACK_SCRIPT: &str = r#"
redis.call("XACK", KEYS[1], ARGV[1], ARGV[2])
return redis.call("XDEL", KEYS[1], ARGV[2])
"#;

const RELEASE_SCRIPT: &str = r#"
redis.call("XACK", KEYS[1], ARGV[1], ARGV[2])
redis.call("XDEL", KEYS[1], ARGV[2])
redis.call("XADD", KEYS[1], "*", "payload", ARGV[3])
return 1
"#;

pub struct ValkeyTokenUsageQueue {
    config: TokenUsageQueueConfig,
    client: Client,
    connection: OnceCell<ConnectionManager>,
    manager_config: ConnectionManagerConfig,
    group_ready: OnceCell<()>,
    enqueue_script: Script,
    ack_script: Script,
    release_script: Script,
}

impl ValkeyTokenUsageQueue {
    pub fn new(config: TokenUsageQueueConfig) -> PlatformResult<Self> {
        config.validate()?;
        let client = Client::open(config.url())
            .map_err(|error| PlatformError::config_invalid_source("usage_queue_url", error))?;
        let manager_config = ConnectionManagerConfig::new()
            .set_connection_timeout(config.timeout())
            .set_response_timeout(config.timeout())
            .set_number_of_retries(0);
        Ok(Self {
            config,
            client,
            connection: OnceCell::new(),
            manager_config,
            group_ready: OnceCell::new(),
            enqueue_script: Script::new(ENQUEUE_SCRIPT),
            ack_script: Script::new(ACK_SCRIPT),
            release_script: Script::new(RELEASE_SCRIPT),
        })
    }

    pub async fn connect(config: TokenUsageQueueConfig) -> PlatformResult<Self> {
        let queue = Self::new(config)?;
        queue.ensure_group().await?;
        Ok(queue)
    }

    fn stream_key(&self) -> String {
        format!("{}:pending", self.config.key_prefix())
    }

    async fn connection(&self) -> PlatformResult<ConnectionManager> {
        self.connection
            .get_or_try_init(|| async {
                ConnectionManager::new_with_config(self.client.clone(), self.manager_config.clone())
                    .await
                    .map_err(|error| ValkeyTokenUsageReply::error("connect", error))
            })
            .await
            .cloned()
    }

    async fn ensure_group(&self) -> PlatformResult<()> {
        self.group_ready
            .get_or_try_init(|| async {
                let mut connection = self.connection().await?;
                let result: Result<(), RedisError> = redis::cmd("XGROUP")
                    .arg("CREATE")
                    .arg(self.stream_key())
                    .arg(GROUP)
                    .arg("0")
                    .arg("MKSTREAM")
                    .query_async(&mut connection)
                    .await;
                match result {
                    Ok(()) => Ok(()),
                    Err(error) if error.code() == Some("BUSYGROUP") => Ok(()),
                    Err(error) => Err(ValkeyTokenUsageReply::error("create_group", error)),
                }
            })
            .await?;
        Ok(())
    }

    async fn read_group(
        &self,
        connection: &mut ConnectionManager,
        id: &str,
        limit: usize,
    ) -> PlatformResult<Vec<TokenUsageReservation>> {
        let reply: Value = redis::cmd("XREADGROUP")
            .arg("GROUP")
            .arg(GROUP)
            .arg(CONSUMER)
            .arg("COUNT")
            .arg(limit)
            .arg("STREAMS")
            .arg(self.stream_key())
            .arg(id)
            .query_async(connection)
            .await
            .map_err(|error| ValkeyTokenUsageReply::error("reserve", error))?;
        ValkeyTokenUsageReply::parse(reply)
    }

    fn capacity_error(&self) -> PlatformError {
        PlatformError::auth_store(
            "enqueue",
            "token-usage-queue",
            true,
            format!(
                "usage queue capacity of {} has been reached",
                self.config.capacity()
            ),
        )
    }
}

#[async_trait]
impl TokenUsageQueue for ValkeyTokenUsageQueue {
    async fn enqueue(&self, mut usage: TokenUsage) -> PlatformResult<()> {
        usage.ensure_event_ids();
        let payload = serde_json::to_string(&usage)
            .map_err(|error| ValkeyTokenUsageReply::decode_error(error.to_string()))?;
        let mut connection = self.connection().await?;
        let added: i64 = self
            .enqueue_script
            .key(self.stream_key())
            .arg(self.config.capacity())
            .arg(payload)
            .invoke_async(&mut connection)
            .await
            .map_err(|error| ValkeyTokenUsageReply::error("enqueue", error))?;
        if added == 0 {
            AuthUsageQueueMetrics::record_capacity_rejection();
            return Err(self.capacity_error());
        }
        AuthUsageQueueMetrics::record_enqueued();
        Ok(())
    }

    async fn reserve(&self, limit: usize) -> PlatformResult<Vec<TokenUsageReservation>> {
        if limit == 0 {
            return Ok(Vec::new());
        }
        self.ensure_group().await?;
        let mut connection = self.connection().await?;
        let pending = self.read_group(&mut connection, "0", limit).await?;
        if !pending.is_empty() {
            return Ok(pending);
        }
        self.read_group(&mut connection, ">", limit).await
    }

    async fn acknowledge(&self, reservation: &TokenUsageReservation) -> PlatformResult<()> {
        let mut connection = self.connection().await?;
        let _: i64 = self
            .ack_script
            .key(self.stream_key())
            .arg(GROUP)
            .arg(reservation.id())
            .invoke_async(&mut connection)
            .await
            .map_err(|error| ValkeyTokenUsageReply::error("acknowledge", error))?;
        Ok(())
    }

    async fn release(&self, reservation: &TokenUsageReservation) -> PlatformResult<()> {
        let payload = serde_json::to_string(reservation.usage())
            .map_err(|error| ValkeyTokenUsageReply::decode_error(error.to_string()))?;
        let mut connection = self.connection().await?;
        let _: i64 = self
            .release_script
            .key(self.stream_key())
            .arg(GROUP)
            .arg(reservation.id())
            .arg(payload)
            .invoke_async(&mut connection)
            .await
            .map_err(|error| ValkeyTokenUsageReply::error("release", error))?;
        Ok(())
    }
}
