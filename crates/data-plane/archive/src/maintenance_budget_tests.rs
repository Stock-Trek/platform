use super::MaintenanceBudget;
use crate::maintenance_budget::BUDGET_WINDOW;
use platform_types::TimestampMilliseconds;

fn at(hour: i64, minute: i64) -> TimestampMilliseconds {
    TimestampMilliseconds(hour * 3_600_000 + minute * 60_000)
}

fn plus(base: TimestampMilliseconds, millis: i64) -> TimestampMilliseconds {
    TimestampMilliseconds(base.0 + millis)
}

#[test]
fn reservations_within_the_window_are_accepted() {
    let budget = MaintenanceBudget::new(3, 1_000);

    assert!(budget.try_reserve(at(1, 0), 400));
    assert!(budget.try_reserve(at(1, 30), 600));
    assert_eq!(budget.usage(at(1, 40)), (2, 1_000));
}

#[test]
fn commits_beyond_the_window_limit_are_rejected() {
    let budget = MaintenanceBudget::new(2, 10_000);

    assert!(budget.try_reserve(at(1, 0), 10));
    assert!(budget.try_reserve(at(1, 10), 10));
    assert!(!budget.try_reserve(at(1, 20), 10));
    assert_eq!(budget.usage(at(1, 20)), (2, 20));
}

#[test]
fn bytes_beyond_the_window_limit_are_rejected() {
    let budget = MaintenanceBudget::new(100, 500);

    assert!(budget.try_reserve(at(1, 0), 400));
    assert!(!budget.try_reserve(at(1, 10), 200));
    assert_eq!(budget.usage(at(1, 10)), (1, 400));
}

#[test]
fn a_new_window_restores_the_budget() {
    let budget = MaintenanceBudget::new(1, 10_000);
    let start = at(1, 0);

    assert!(budget.try_reserve(start, 10_000));
    assert!(!budget.try_reserve(plus(start, 1), 10_000));
    assert!(budget.try_reserve(plus(start, BUDGET_WINDOW.as_millis() as i64), 10_000));
    assert_eq!(
        budget.usage(plus(start, BUDGET_WINDOW.as_millis() as i64)),
        (1, 10_000)
    );
}

#[test]
fn the_window_rolls_after_exactly_one_hour() {
    let budget = MaintenanceBudget::new(1, 10_000);
    let start = at(1, 0);

    assert!(budget.try_reserve(start, 10_000));
    assert!(!budget.try_reserve(plus(start, BUDGET_WINDOW.as_millis() as i64 - 1), 1));
    assert!(budget.try_reserve(plus(start, BUDGET_WINDOW.as_millis() as i64), 1));
}

#[test]
fn saturating_bytes_never_exceed_the_limit() {
    let budget = MaintenanceBudget::new(u64::MAX, 200);

    assert!(!budget.try_reserve(at(1, 0), u64::MAX));
    assert!(budget.try_reserve(at(1, 5), 150));
    assert!(!budget.try_reserve(at(1, 10), 100));
    assert_eq!(budget.usage(at(1, 10)), (1, 150));
}

#[test]
fn zero_limits_reject_every_reservation() {
    let budget = MaintenanceBudget::new(0, 0);

    assert!(!budget.try_reserve(at(1, 0), 0));
    assert_eq!(budget.usage(at(1, 0)), (0, 0));
}
