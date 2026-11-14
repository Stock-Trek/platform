use parking_lot::Mutex;
use platform_exit_codes::ExitCode;
use std::sync::{
    Barrier,
    atomic::{AtomicUsize, Ordering},
};

use crate::cwasm::backtest::BackTest;

#[test]
fn simulations_respect_the_concurrency_limit() {
    let active = AtomicUsize::new(0);
    let peak = AtomicUsize::new(0);
    let seen = Mutex::new(Vec::new());
    let barrier = Barrier::new(3);
    let results = BackTest::run_simulations(12, 3, |index| {
        let running = active.fetch_add(1, Ordering::SeqCst) + 1;
        peak.fetch_max(running, Ordering::SeqCst);
        seen.lock().push(index);
        barrier.wait();
        active.fetch_sub(1, Ordering::SeqCst);
        Ok(())
    });
    assert_eq!(results.len(), 12);
    assert!(results.iter().all(Result::is_ok));
    assert!(peak.load(Ordering::SeqCst) <= 3);
    let mut seen = seen.into_inner();
    seen.sort_unstable();
    assert_eq!(seen, (0..12).collect::<Vec<_>>());
}

#[test]
fn a_panicking_simulation_is_reported_without_stopping_the_others() {
    let results = BackTest::run_simulations(4, 2, |index| {
        if index == 2 {
            panic!("simulation failed");
        }
        Ok(())
    });
    assert_eq!(results.iter().filter(|result| result.is_err()).count(), 1);
    assert_eq!(
        results[2].as_ref().unwrap_err().exit_code(),
        ExitCode::BotErrorPanic.code()
    );
}
