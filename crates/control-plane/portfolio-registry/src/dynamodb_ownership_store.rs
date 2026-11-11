use crate::{
    ACCOUNT_KEY, ACTIVE, AccountMapping, OWNER, OwnershipCommand, OwnershipCommit, OwnershipItem,
    OwnershipRecord, OwnershipStore, OwnershipTransition, PENDING_EVENTS, VERSION,
};
use async_trait::async_trait;
use aws_sdk_dynamodb::{Client, operation::put_item::PutItemError, types::AttributeValue};
use platform_errors::{BpError, BpResult};
use platform_portfolio_ownership::{AccountKey, OwnershipEvent};

const MAX_COMMIT_ATTEMPTS: usize = 8;
pub const OWNER_INDEX: &str = "owner-index";

pub struct DynamoDbOwnershipStore {
    client: Client,
    table: String,
}

impl DynamoDbOwnershipStore {
    pub fn new(client: Client, table: impl Into<String>) -> Self {
        Self {
            client,
            table: table.into(),
        }
    }

    async fn load(&self, account_key: &str) -> BpResult<Option<OwnershipRecord>> {
        let output = self
            .client
            .get_item()
            .table_name(&self.table)
            .key(ACCOUNT_KEY, AttributeValue::S(account_key.to_string()))
            .consistent_read(true)
            .send()
            .await
            .map_err(BpError::external)?;
        output.item().map(OwnershipItem::parse).transpose()
    }
}

#[async_trait]
impl OwnershipStore for DynamoDbOwnershipStore {
    async fn commit(&self, command: OwnershipCommand) -> BpResult<OwnershipCommit> {
        let account_key = command.account.storage_key();
        for _ in 0..MAX_COMMIT_ATTEMPTS {
            let existing = self.load(&account_key).await?;
            let expected_version = existing.as_ref().map(|record| record.version);
            let (record, outcome) = match OwnershipTransition::apply(existing, &command)? {
                OwnershipTransition::Unchanged(record) => {
                    return Ok(OwnershipCommit::Unchanged(record));
                }
                OwnershipTransition::Applied(record) => {
                    let event = record.pending_events.last().cloned().ok_or_else(|| {
                        BpError::external(std::io::Error::other("missing outbox event"))
                    })?;
                    (record, OwnershipCommit::Applied(event))
                }
            };
            let mut request = self
                .client
                .put_item()
                .table_name(&self.table)
                .set_item(Some(OwnershipItem::attributes(&record)?));
            request = match expected_version {
                Some(version) => request
                    .condition_expression("#version = :expected")
                    .expression_attribute_names("#version", VERSION)
                    .expression_attribute_values(
                        ":expected",
                        AttributeValue::N(version.to_string()),
                    ),
                None => request
                    .condition_expression("attribute_not_exists(#account_key)")
                    .expression_attribute_names("#account_key", ACCOUNT_KEY),
            };
            match request.send().await {
                Ok(_) => return Ok(outcome),
                Err(error)
                    if error
                        .as_service_error()
                        .is_some_and(PutItemError::is_conditional_check_failed_exception) =>
                {
                    continue;
                }
                Err(error) => return Err(BpError::external(error)),
            }
        }
        Err(BpError::auth_invalid_field(
            "ownership",
            "version changed too often; retry the request",
        ))
    }

    async fn get(&self, account: &AccountKey) -> BpResult<Option<OwnershipRecord>> {
        self.load(&account.storage_key()).await
    }

    async fn owner_accounts(&self, owner: &str) -> BpResult<Vec<AccountMapping>> {
        let mut start_key = None;
        let mut mappings = Vec::new();
        loop {
            let output = self
                .client
                .query()
                .table_name(&self.table)
                .index_name(OWNER_INDEX)
                .key_condition_expression("#owner = :owner")
                .filter_expression("#active = :active")
                .expression_attribute_names("#owner", OWNER)
                .expression_attribute_names("#active", ACTIVE)
                .expression_attribute_values(":owner", AttributeValue::S(owner.to_string()))
                .expression_attribute_values(":active", AttributeValue::Bool(true))
                .set_exclusive_start_key(start_key)
                .send()
                .await
                .map_err(BpError::external)?;
            mappings.extend(
                output
                    .items()
                    .iter()
                    .map(OwnershipItem::parse)
                    .map(|record| record.map(|record| record.mapping()))
                    .collect::<BpResult<Vec<_>>>()?,
            );
            start_key = output.last_evaluated_key().cloned();
            if start_key.is_none() {
                break;
            }
        }
        mappings.sort_by(|left, right| left.account().cmp(right.account()));
        Ok(mappings)
    }

    async fn pending_events(&self, limit: usize) -> BpResult<Vec<OwnershipEvent>> {
        let mut events = Vec::new();
        let mut start_key = None;
        while events.len() < limit {
            let output = self
                .client
                .scan()
                .table_name(&self.table)
                .filter_expression("attribute_exists(#pending_events)")
                .expression_attribute_names("#pending_events", PENDING_EVENTS)
                .set_exclusive_start_key(start_key)
                .send()
                .await
                .map_err(BpError::external)?;
            for item in output.items() {
                if let Some(AttributeValue::S(payload)) = item.get(PENDING_EVENTS) {
                    let pending: Vec<OwnershipEvent> = serde_json::from_str(payload)?;
                    events.extend(pending);
                    if events.len() >= limit {
                        break;
                    }
                }
            }
            start_key = output.last_evaluated_key().cloned();
            if start_key.is_none() {
                break;
            }
        }
        events.truncate(limit);
        Ok(events)
    }

    async fn acknowledge(&self, event: &OwnershipEvent) -> BpResult<()> {
        let account_key = event.account().storage_key();
        for _ in 0..MAX_COMMIT_ATTEMPTS {
            let Some(mut record) = self.load(&account_key).await? else {
                return Ok(());
            };
            if !record
                .pending_events
                .iter()
                .any(|pending| pending.version() == event.version())
            {
                return Ok(());
            }
            let expected_pending = serde_json::to_string(&record.pending_events)?;
            record
                .pending_events
                .retain(|pending| pending.version() != event.version());
            let request = self
                .client
                .put_item()
                .table_name(&self.table)
                .set_item(Some(OwnershipItem::attributes(&record)?))
                .condition_expression("#version = :version AND #pending_events = :pending_events")
                .expression_attribute_names("#version", VERSION)
                .expression_attribute_names("#pending_events", PENDING_EVENTS)
                .expression_attribute_values(
                    ":version",
                    AttributeValue::N(record.version.to_string()),
                )
                .expression_attribute_values(
                    ":pending_events",
                    AttributeValue::S(expected_pending),
                );
            match request.send().await {
                Ok(_) => return Ok(()),
                Err(error)
                    if error
                        .as_service_error()
                        .is_some_and(PutItemError::is_conditional_check_failed_exception) =>
                {
                    continue;
                }
                Err(error) => return Err(BpError::external(error)),
            }
        }
        Err(BpError::auth_invalid_field(
            "outbox",
            "pending events changed too often; retry the acknowledgement",
        ))
    }
}
