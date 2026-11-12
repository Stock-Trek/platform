#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PortfolioUpdateOutcome {
    Applied,
    IgnoredUntracked,
    IgnoredStale,
}
