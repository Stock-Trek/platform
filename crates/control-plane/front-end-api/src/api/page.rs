use serde::Serialize;

const DEFAULT_LIMIT: usize = 50;
const MAX_LIMIT: usize = 200;

#[derive(Debug, Clone, Copy)]
pub struct PageQuery {
    limit: usize,
    cursor: Option<usize>,
}

impl PageQuery {
    pub fn parse(request: &crate::api::ApiRequest) -> Self {
        let limit = request
            .query("limit")
            .and_then(|value| value.parse::<usize>().ok())
            .unwrap_or(DEFAULT_LIMIT)
            .min(MAX_LIMIT);
        let cursor = request
            .query("cursor")
            .and_then(|value| value.parse::<usize>().ok());
        Self { limit, cursor }
    }

    pub fn limit(&self) -> usize {
        self.limit
    }

    pub fn offset(&self) -> usize {
        self.cursor.unwrap_or_default()
    }
}

#[derive(Debug, Serialize)]
pub struct Page<T> {
    pub data: Vec<T>,
    #[serde(rename = "nextCursor")]
    pub next_cursor: Option<String>,
}

impl<T: Clone> Page<T> {
    pub fn of(items: Vec<T>, query: &PageQuery) -> Self {
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
