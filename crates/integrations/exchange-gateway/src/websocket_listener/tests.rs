use super::*;
use parking_lot::Mutex;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::{future::Future, task::Waker};

fn block_on<F: Future>(future: F) -> F::Output {
    let waker = Waker::noop();
    let mut context = Context::from_waker(waker);
    let mut future = Box::pin(future);
    loop {
        match future.as_mut().poll(&mut context) {
            Poll::Ready(output) => return output,
            Poll::Pending => std::thread::yield_now(),
        }
    }
}

#[test]
fn dropped_waiter_is_removed_from_listener() {
    let listener = WebsocketListener::new();
    let waiter = listener
        .waiter_for_filtered_response(Arc::new(|_| true))
        .unwrap();
    assert_eq!(listener.handlers.lock().len(), 1);
    drop(waiter);
    assert_eq!(listener.handlers.lock().len(), 0);
}

#[test]
fn dropped_waiter_does_not_swallow_response() {
    let listener = WebsocketListener::new();
    let stale = listener
        .waiter_for_filtered_response(Arc::new(|_| true))
        .unwrap();
    drop(stale);
    let fresh = listener
        .waiter_for_filtered_response(Arc::new(|_| true))
        .unwrap();
    block_on(listener.on_message(serde_json::json!({ "value": 42 }))).unwrap();
    assert_eq!(block_on(fresh).unwrap(), serde_json::json!({ "value": 42 }));
}

#[test]
fn filter_callback_does_not_hold_handlers_lock() {
    let listener = WebsocketListener::new();
    let lock_was_free = Arc::new(Mutex::new(None));
    let lock_was_free_in_filter = lock_was_free.clone();
    let probe = listener.clone();
    let _waiter = listener
        .waiter_for_filtered_response(Arc::new(move |_| {
            *lock_was_free_in_filter.lock() = Some(probe.handlers.try_lock().is_some());
            true
        }))
        .unwrap();
    block_on(listener.on_message(serde_json::json!({ "value": 42 }))).unwrap();
    assert_eq!(*lock_was_free.lock(), Some(true));
}
