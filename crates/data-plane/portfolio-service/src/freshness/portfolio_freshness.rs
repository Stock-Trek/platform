#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PortfolioFreshness {
    Missing,
    Fresh,
    Stale,
}
