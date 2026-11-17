use parking_lot::Mutex;
use platform_types::TimestampMilliseconds;
use std::time::Duration;

pub const BUDGET_WINDOW: Duration = Duration::from_secs(60 * 60);

struct WindowState {
    started_at: Option<TimestampMilliseconds>,
    commits: u64,
    bytes: u64,
}

/// Hourly commit and byte budget for maintenance commits.
///
/// The budget bounds how much metadata churn maintenance can create per hour
/// across every table and partition. A stage that cannot reserve its share of
/// the budget is blocked until the next window, which applies back-pressure
/// instead of letting maintenance outrun its limits.
pub struct MaintenanceBudget {
    max_commits: u64,
    max_bytes: u64,
    state: Mutex<WindowState>,
}

impl MaintenanceBudget {
    pub fn new(max_commits: u64, max_bytes: u64) -> Self {
        Self {
            max_commits,
            max_bytes,
            state: Mutex::new(WindowState {
                started_at: None,
                commits: 0,
                bytes: 0,
            }),
        }
    }

    pub fn max_commits(&self) -> u64 {
        self.max_commits
    }

    pub fn max_bytes(&self) -> u64 {
        self.max_bytes
    }

    /// Reserves one commit and its bytes for the current window.
    ///
    /// Returns `false` when either limit of the active window is exhausted.
    pub fn try_reserve(&self, now: TimestampMilliseconds, bytes: u64) -> bool {
        let mut state = self.state.lock();
        self.roll(&mut state, now);
        if state.commits >= self.max_commits || state.bytes.saturating_add(bytes) > self.max_bytes {
            return false;
        }
        state.commits += 1;
        state.bytes = state.bytes.saturating_add(bytes);
        true
    }

    /// Commits and bytes reserved so far in the active window.
    pub fn usage(&self, now: TimestampMilliseconds) -> (u64, u64) {
        let mut state = self.state.lock();
        self.roll(&mut state, now);
        (state.commits, state.bytes)
    }

    fn roll(&self, state: &mut WindowState, now: TimestampMilliseconds) {
        let expired = state.started_at.is_some_and(|started_at| {
            now.0.saturating_sub(started_at.0) >= BUDGET_WINDOW.as_millis() as i64
        });
        if state.started_at.is_none() || expired {
            state.started_at = Some(now);
            state.commits = 0;
            state.bytes = 0;
        }
    }
}
