use chrono::{DateTime, Utc};
use serde::Serialize;
use serde_json::Value;

pub const TICKET_TTL_SECONDS: u64 = 60;
pub const REPLAY_WINDOW: usize = 256;

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct StreamTicket {
    pub ticket: String,
    pub expires_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct GapDetail {
    pub after_sequence: u64,
    pub head_sequence: u64,
    pub reason: GapReason,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum GapReason {
    Expired,
    AheadOfHead,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ErrorDetail {
    pub code: &'static str,
    pub channel: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct Frame {
    pub channel: String,
    pub sequence: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub data: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub gap: Option<GapDetail>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<ErrorDetail>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub simulation: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub result: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub complete: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub state: Option<String>,
}

impl Frame {
    pub fn data(channel: &str, sequence: u64, data: Value) -> Self {
        Self {
            channel: channel.to_string(),
            sequence,
            data: Some(data),
            gap: None,
            error: None,
            simulation: None,
            result: None,
            complete: None,
            state: None,
        }
    }

    pub fn gap(channel: &str, detail: GapDetail) -> Self {
        Self {
            channel: channel.to_string(),
            sequence: detail.head_sequence,
            data: None,
            gap: Some(detail),
            error: None,
            simulation: None,
            result: None,
            complete: None,
            state: None,
        }
    }

    pub fn error(channel: &str, code: &'static str) -> Self {
        Self {
            channel: channel.to_string(),
            sequence: 0,
            data: None,
            gap: None,
            error: Some(ErrorDetail {
                code,
                channel: channel.to_string(),
            }),
            simulation: None,
            result: None,
            complete: None,
            state: None,
        }
    }

    pub fn backtest_result(channel: &str, sequence: u64, simulation: u64, result: Value) -> Self {
        Self {
            channel: channel.to_string(),
            sequence,
            data: None,
            gap: None,
            error: None,
            simulation: Some(simulation),
            result: Some(result),
            complete: None,
            state: None,
        }
    }

    pub fn backtest_final(channel: &str, sequence: u64, state: &str) -> Self {
        Self {
            channel: channel.to_string(),
            sequence,
            data: None,
            gap: None,
            error: None,
            simulation: None,
            result: None,
            complete: Some(true),
            state: Some(state.to_string()),
        }
    }
}
