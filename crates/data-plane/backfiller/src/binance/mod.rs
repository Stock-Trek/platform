pub mod event_page_source;
pub mod fetcher;
pub mod parser;
pub mod stream;

/// SSM parameter name holding the exchange API key used for historical trades.
pub const EXCHANGE_API_KEY_SECRET: &str = "BACKFILLER_EXCHANGE_API_KEY";
