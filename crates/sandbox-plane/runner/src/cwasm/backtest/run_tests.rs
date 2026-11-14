use parking_lot::Mutex;
use platform_exit_codes::ExitCode;
use std::sync::{
    Barrier,
    atomic::{AtomicUsize, Ordering},
};

use crate::cwasm::backtest::{Backtest, MAX_SIMULATION_COUNT, WorkerLimits};

#[test]
fn simulations_respect_the_configured_concurrency_limit() {
    let active = AtomicUsize::new(0);
    let peak = AtomicUsize::new(0);
    let seen = Mutex::new(Vec::new());
    let barrier = Barrier::new(3);
    let limits = WorkerLimits::new(3);
    let results = Backtest::run_simulations_with_limits(12, 10, &limits, |index| {
        let running = active.fetch_add(1, Ordering::SeqCst) + 1;
        peak.fetch_max(running, Ordering::SeqCst);
        seen.lock().push(index);
        barrier.wait();
        active.fetch_sub(1, Ordering::SeqCst);
        Ok(())
    })
    .unwrap();
    assert_eq!(results.len(), 12);
    assert!(results.iter().all(Result::is_ok));
    assert!(peak.load(Ordering::SeqCst) <= 3);
    let mut seen = seen.into_inner();
    seen.sort_unstable();
    assert_eq!(seen, (0..12).collect::<Vec<_>>());
}

#[test]
fn oversized_simulation_counts_are_rejected() {
    let limits = WorkerLimits::new(2);
    let error =
        Backtest::run_simulations_with_limits(MAX_SIMULATION_COUNT + 1, 2, &limits, |_| Ok(()))
            .unwrap_err();
    assert_eq!(
        error.exit_code(),
        ExitCode::ServerEntryInvalidArgument.code()
    );
}

#[test]
fn worker_counts_never_exceed_the_absolute_ceiling() {
    let limits = WorkerLimits::new(u32::MAX);
    let active = AtomicUsize::new(0);
    let peak = AtomicUsize::new(0);
    let results = Backtest::run_simulations_with_limits(64, u32::MAX, &limits, |_| {
        let running = active.fetch_add(1, Ordering::SeqCst) + 1;
        peak.fetch_max(running, Ordering::SeqCst);
        active.fetch_sub(1, Ordering::SeqCst);
        Ok(())
    })
    .unwrap();
    assert_eq!(results.len(), 64);
    assert_eq!(
        limits.max_workers(),
        crate::cwasm::backtest::ABSOLUTE_MAX_WORKERS
    );
    assert!(peak.load(Ordering::SeqCst) <= crate::cwasm::backtest::ABSOLUTE_MAX_WORKERS as usize);
}

#[test]
fn a_panicking_simulation_is_reported_without_stopping_the_others() {
    let results = Backtest::run_simulations(4, 2, |index| {
        if index == 2 {
            panic!("simulation failed");
        }
        Ok(())
    })
    .unwrap();
    assert_eq!(results.iter().filter(|result| result.is_err()).count(), 1);
    assert_eq!(
        results[2].as_ref().unwrap_err().exit_code(),
        ExitCode::BotErrorPanic.code()
    );
}
