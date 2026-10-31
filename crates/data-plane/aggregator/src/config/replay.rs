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

    /// Whether this policy can need to rebuild state from the archive.
    ///
    /// `checkpoint` falls back to the archive when a checkpoint is missing,
    /// stale, or unreadable. `archive` always rebuilds from it. `earliest` and
    /// `latest` explicitly start with empty state instead.
    pub const fn requires_archive(self) -> bool {
        matches!(self, Self::Checkpoint | Self::Archive)
    }

    /// Parses a canonical operator-facing policy name.
    ///
    /// Only the names returned by [`Self::label`] are accepted; aliases are
    /// intentionally rejected.
    pub fn parse(value: &str) -> Option<Self> {
        let value = value.trim();
        [
            Self::Checkpoint,
            Self::Archive,
            Self::Earliest,
            Self::Latest,
        ]
        .into_iter()
        .find(|policy| policy.label().eq_ignore_ascii_case(value))
    }
}

impl fmt::Display for ReplayStartPolicy {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.label())
    }
}

#[cfg(test)]
mod tests {
    use super::ReplayStartPolicy;

    #[test]
    fn parse_accepts_only_canonical_policy_labels() {
        for (value, expected) in [
            ("checkpoint", ReplayStartPolicy::Checkpoint),
            ("archive", ReplayStartPolicy::Archive),
            ("earliest", ReplayStartPolicy::Earliest),
            ("latest", ReplayStartPolicy::Latest),
            (" CHECKPOINT ", ReplayStartPolicy::Checkpoint),
        ] {
            assert_eq!(ReplayStartPolicy::parse(value), Some(expected));
        }

        for alias in [
            "resume",
            "checkpoint-first",
            "checkpoint_first",
            "rebuild",
            "rebuild-archive",
            "rebuild_archive",
            "beginning",
            "start",
            "all",
            "new",
            "now",
        ] {
            assert_eq!(
                ReplayStartPolicy::parse(alias),
                None,
                "{alias} should be rejected"
            );
        }
    }
}
