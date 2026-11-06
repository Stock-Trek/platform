use std::fmt::{self, Display};

/// Lifecycle of a scheduled algorithm run.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RunStatus {
    Pending,
    Running,
    Completed,
    Failed,
}

impl RunStatus {
    pub fn as_str(self) -> &'static str {
        match self {
            RunStatus::Pending => "pending",
            RunStatus::Running => "running",
            RunStatus::Completed => "completed",
            RunStatus::Failed => "failed",
        }
    }

    pub fn parse(value: &str) -> Option<Self> {
        match value {
            "pending" => Some(RunStatus::Pending),
            "running" => Some(RunStatus::Running),
            "completed" => Some(RunStatus::Completed),
            "failed" => Some(RunStatus::Failed),
            _ => None,
        }
    }
}

impl Display for RunStatus {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}
