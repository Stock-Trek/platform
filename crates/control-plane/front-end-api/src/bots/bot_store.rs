use crate::bots::{Bot, BotState};
use chrono::{DateTime, Utc};
use parking_lot::Mutex;

struct BotRow {
    owner: String,
    bot: Bot,
}

pub(crate) struct BotStore {
    rows: Mutex<Vec<BotRow>>,
}

impl BotStore {
    pub(crate) fn new() -> Self {
        Self {
            rows: Mutex::new(Vec::new()),
        }
    }

    pub(crate) fn insert(&self, owner: &str, bot: Bot) {
        self.rows.lock().push(BotRow {
            owner: owner.to_string(),
            bot,
        });
    }

    pub(crate) fn list(&self, owner: &str, state: Option<BotState>) -> Vec<Bot> {
        let rows = self.rows.lock();
        let mut listed: Vec<&Bot> = rows
            .iter()
            .filter(|row| row.owner == owner)
            .map(|row| &row.bot)
            .filter(|bot| match state {
                Some(state) => bot.state == state,
                None => bot.state != BotState::Archived,
            })
            .collect();
        listed.sort_by_key(|row| std::cmp::Reverse(row.created_at));
        listed.into_iter().cloned().collect()
    }

    pub(crate) fn get(&self, owner: &str, bot_id: &str) -> Option<Bot> {
        self.rows
            .lock()
            .iter()
            .find(|row| row.owner == owner && row.bot.bot_id == bot_id)
            .map(|row| row.bot.clone())
    }

    pub(crate) fn update(
        &self,
        owner: &str,
        bot_id: &str,
        now: DateTime<Utc>,
        update: impl FnOnce(Bot) -> Bot,
    ) -> Option<Bot> {
        let mut rows = self.rows.lock();
        let row = rows
            .iter_mut()
            .find(|row| row.owner == owner && row.bot.bot_id == bot_id)?;
        row.bot = update(row.bot.clone());
        row.bot.version += 1;
        row.bot.updated_at = now;
        Some(row.bot.clone())
    }

    pub(crate) fn count(&self, owner: &str) -> usize {
        self.rows
            .lock()
            .iter()
            .filter(|row| row.owner == owner && row.bot.state != BotState::Archived)
            .count()
    }

    pub(crate) fn referencing_accounts(&self, owner: &str, account_ids: &[String]) -> Vec<Bot> {
        self.rows
            .lock()
            .iter()
            .filter(|row| {
                row.owner == owner
                    && row
                        .bot
                        .account_ids
                        .iter()
                        .any(|id| account_ids.contains(id))
            })
            .map(|row| row.bot.clone())
            .collect()
    }

    pub(crate) fn pinning_build(
        &self,
        owner: &str,
        algorithm_id: &str,
        build_id: &str,
    ) -> Vec<Bot> {
        self.rows
            .lock()
            .iter()
            .filter(|row| {
                row.owner == owner
                    && row.bot.algorithm_id == algorithm_id
                    && row.bot.build_id == build_id
            })
            .map(|row| row.bot.clone())
            .collect()
    }

    pub(crate) fn all(&self, owner: &str) -> Vec<Bot> {
        self.list(owner, None)
    }
}
