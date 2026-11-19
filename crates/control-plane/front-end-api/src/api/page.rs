use crate::api::Problem;
use serde::Serialize;

const DEFAULT_LIMIT: usize = 50;
const MAX_LIMIT: usize = 200;

#[derive(Debug, Clone, Copy)]
pub(crate) struct PageQuery {
    limit: usize,
    cursor: Option<usize>,
}

impl PageQuery {
    pub(crate) fn parse(request: &crate::api::ApiRequest) -> Result<Self, Problem> {
        let limit = match request.query("limit") {
            None => DEFAULT_LIMIT,
            Some(value) => {
                let parsed = value
                    .parse::<usize>()
                    .map_err(|_| Problem::validation_failed("limit must be a positive integer"))?;
                if parsed == 0 {
                    return Err(Problem::validation_failed("limit must be at least 1"));
                }
                parsed.min(MAX_LIMIT)
            }
        };
        let cursor = match request.query("cursor") {
            None => None,
            Some(value) => Some(value.parse::<usize>().map_err(|_| {
                Problem::validation_failed("cursor must be a non-negative integer")
            })?),
        };
        Ok(Self { limit, cursor })
    }

    pub(crate) fn limit(&self) -> usize {
        self.limit
    }

    pub(crate) fn offset(&self) -> usize {
        self.cursor.unwrap_or_default()
    }
}

#[derive(Debug, Serialize)]
pub(crate) struct Page<T> {
    pub data: Vec<T>,
    #[serde(rename = "nextCursor")]
    pub next_cursor: Option<String>,
}

impl<T: Clone> Page<T> {
    pub(crate) fn of(items: Vec<T>, query: &PageQuery) -> Self {
        let offset = query.offset();
        let end = (offset + query.limit()).min(items.len());
        let data = if offset >= items.len() {
            Vec::new()
        } else {
            items[offset..end].to_vec()
        };
        let next_cursor = (end < items.len() && offset <= end).then(|| end.to_string());
        Self { data, next_cursor }
    }
}
