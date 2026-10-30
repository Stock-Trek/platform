use std::fmt;

/// What a shard writer does with persisted state when it starts.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum ReplayStartPolicy {
    /// Resume from a compatible checkpoint, rebuilding from the archive only
    /// when the checkpoint is missing, stale, or incompatible.
    #[default]
    Checkpoint,
    /// Ignore checkpoints and rebuild keyed state from archived raw events.
    Archive,
    /// Ignore persisted state and replay the live source from the beginning.
    Earliest,
    /// Ignore persisted state and process only messages published after start.
    Latest,
}

impl ReplayStartPolicy {
    pub const fn label(self) -> &'static str {
        match self {
            Self::Checkpoint => "checkpoint",
            Self::Archive => "archive",
            Self::Earliest => "earliest",
            Self::Latest => "latest",
        }
    }

    /// Parses the operator-facing policy name, accepting a few common aliases.
    pub fn parse(value: &str) -> Option<Self> {
        match value.trim().to_ascii_lowercase().as_str() {
            "checkpoint" | "resume" | "checkpoint-first" | "checkpoint_first" => {
                Some(Self::Checkpoint)
            }
            "archive" | "rebuild" | "rebuild-archive" | "rebuild_archive" => Some(Self::Archive),
            "earliest" | "beginning" | "start" | "all" => Some(Self::Earliest),
            "latest" | "new" | "now" => Some(Self::Latest),
            _ => None,
        }
    }
}

impl fmt::Display for ReplayStartPolicy {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.label())
    }
}
