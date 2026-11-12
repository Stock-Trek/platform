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

#[tokio::test]
async fn subscriber_receives_unmatched_events() {
    let listener = WebsocketListener::new();
    let mut stream = listener.subscribe();
    listener
        .on_message(serde_json::json!({ "event": "position" }))
        .await
        .unwrap();

    assert_eq!(
        stream.next().await,
        Some(serde_json::json!({ "event": "position" }))
    );
}

#[tokio::test]
async fn subscriber_receives_events_claimed_by_a_waiter() {
    let listener = WebsocketListener::new();
    let mut stream = listener.subscribe();
    let waiter = listener
        .waiter_for_filtered_response(Arc::new(|_| true))
        .unwrap();
    listener
        .on_message(serde_json::json!({ "id": 1 }))
        .await
        .unwrap();

    assert_eq!(waiter.await.unwrap(), serde_json::json!({ "id": 1 }));
    assert_eq!(stream.next().await, Some(serde_json::json!({ "id": 1 })));
}

#[test]
fn dropped_subscriber_is_removed() {
    let listener = WebsocketListener::new();
    let stream = listener.subscribe();
    assert_eq!(listener.events.subscriber_count(), 1);
    drop(stream);
    assert_eq!(listener.events.subscriber_count(), 0);
}
