use platform_config::EnvVars;

use crate::cwasm::backtest::{
    ABSOLUTE_MAX_WORKERS, DEFAULT_MAX_WORKERS, MAX_SIMULATION_COUNT, WorkerLimits,
};

#[test]
fn configuration_is_clamped_to_the_absolute_worker_ceiling() {
    let limits = WorkerLimits::new(u32::MAX);
    assert_eq!(limits.max_workers(), ABSOLUTE_MAX_WORKERS);
    assert_eq!(WorkerLimits::new(0).max_workers(), 1);
}

#[test]
fn worker_count_is_clamped_to_configuration_and_requested_value() {
    let limits = WorkerLimits::new(3);
    assert_eq!(limits.worker_count(100, 10), 3);
    assert_eq!(limits.worker_count(2, 10), 2);
    assert_eq!(limits.worker_count(100, 2), 2);
}

#[test]
fn environment_configuration_stays_below_the_absolute_ceiling() {
    let vars = EnvVars::from_map([("CWASM_INVOKER_BACKTEST_MAX_WORKERS", "6")]);
    assert_eq!(WorkerLimits::from_vars(&vars).max_workers(), 6);
    let vars = EnvVars::from_map([("CWASM_INVOKER_BACKTEST_MAX_WORKERS", "1000")]);
    assert_eq!(
        WorkerLimits::from_vars(&vars).max_workers(),
        ABSOLUTE_MAX_WORKERS
    );
    let limits = WorkerLimits::from_vars(&EnvVars::default());
    assert_eq!(limits.max_workers(), DEFAULT_MAX_WORKERS);
}

#[test]
fn request_validation_rejects_zero_and_oversized_values() {
    let limits = WorkerLimits::new(4);
    assert!(limits.validate(0, 1).is_err());
    assert!(limits.validate(1, 0).is_err());
    assert!(limits.validate(MAX_SIMULATION_COUNT + 1, 1).is_err());
    assert!(limits.validate(MAX_SIMULATION_COUNT, 1).is_ok());
}
