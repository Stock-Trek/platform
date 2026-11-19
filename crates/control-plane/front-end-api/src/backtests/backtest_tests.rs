use super::{BacktestProgress, BacktestState, BacktestStatus, BacktestStore};
use crate::streaming::ChannelRegistry;
use crate::streaming::channel_registry::SubscribeOutcome;
use chrono::Utc;
use std::sync::Arc;

const OWNER: &str = "user_7gk2";
const BACKTEST_ID: &str = "backtest_1";
const MIB: usize = 1024 * 1024;

fn status(state: BacktestState) -> BacktestStatus {
    BacktestStatus {
        backtest_id: BACKTEST_ID.to_string(),
        algorithm_id: "algorithm_1".to_string(),
        build_id: "build_1".to_string(),
        strategy_name: "momentum_v2".to_string(),
        state,
        error: None,
        seed: 7,
        progress: BacktestProgress {
            completed: 0,
            total: 2,
        },
        created_at: Utc::now(),
        started_at: None,
        completed_at: None,
        stream_sequence: 0,
    }
}

fn fixture() -> (Arc<BacktestStore>, Arc<ChannelRegistry>) {
    let registry = Arc::new(ChannelRegistry::new());
    let backtests = Arc::new(BacktestStore::new(Arc::clone(&registry)));
    backtests.insert(OWNER, status(BacktestState::Running));
    (backtests, registry)
}

#[test]
fn simulation_results_take_registry_sequences() {
    let (backtests, registry) = fixture();
    let channel = format!("backtests.{BACKTEST_ID}");
    for simulation in [1_u64, 2] {
        backtests.append_result(
            OWNER,
            BACKTEST_ID,
            simulation,
            serde_json::json!({ "pnl": simulation }),
        );
    }
    let head = registry.sequence(&channel);
    assert_eq!(head, 2);
    let (results, _, _) = backtests
        .results(OWNER, BACKTEST_ID, 0, 10)
        .expect("results");
    assert_eq!(results.len(), 2);
    assert_eq!(results[0].sequence, 1);
    assert_eq!(results[1].sequence, 2);
    let status = backtests.status(OWNER, BACKTEST_ID).expect("status");
    assert_eq!(status.stream_sequence, head);
    assert_eq!(status.progress.completed, 2);
}

#[test]
fn an_oversized_result_fails_the_backtest_with_result_too_large() {
    let (backtests, registry) = fixture();
    let channel = format!("backtests.{BACKTEST_ID}");
    backtests.append_result(OWNER, BACKTEST_ID, 1, serde_json::json!({ "pnl": 1 }));
    backtests.append_result(
        OWNER,
        BACKTEST_ID,
        2,
        serde_json::json!({ "padding": "x".repeat(2 * MIB) }),
    );
    let stored = backtests.status(OWNER, BACKTEST_ID).expect("status");
    assert_eq!(stored.state, BacktestState::Failed);
    let error = stored.error.expect("error");
    assert_eq!(error.code, "result_too_large");
    let (results, _, _) = backtests
        .results(OWNER, BACKTEST_ID, 0, 10)
        .expect("results");
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].simulation, 1);
    let replay = registry.subscribe(&channel, Some(0));
    let SubscribeOutcome::Joined { replay, .. } = replay else {
        panic!("backtest channels replay fully")
    };
    let last = replay.last().expect("final frame");
    assert_eq!(last.sequence, stored.stream_sequence);
    assert_eq!(last.complete, Some(true));
    assert_eq!(last.state.as_deref(), Some("failed"));
}

#[test]
fn reaching_a_terminal_state_publishes_a_final_frame() {
    let (backtests, registry) = fixture();
    let channel = format!("backtests.{BACKTEST_ID}");
    backtests.update(OWNER, BACKTEST_ID, |mut status| {
        status.state = BacktestState::Canceled;
        status.completed_at = Some(Utc::now());
        status
    });
    let stored = backtests.status(OWNER, BACKTEST_ID).expect("status");
    assert_eq!(stored.stream_sequence, 1);
    let replay = registry.subscribe(&channel, Some(0));
    let SubscribeOutcome::Joined { replay, .. } = replay else {
        panic!("backtest channels replay fully")
    };
    assert_eq!(replay.len(), 1);
    assert_eq!(replay[0].state.as_deref(), Some("canceled"));
    assert_eq!(replay[0].complete, Some(true));
}
