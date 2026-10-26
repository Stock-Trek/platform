use std::{
    any::Any,
    cell::RefCell,
    future::{Future, poll_fn},
    panic::{AssertUnwindSafe, PanicHookInfo, catch_unwind},
    sync::Once,
    task::Poll,
};

thread_local! {
    static LAST_PANIC_MESSAGE: RefCell<Option<String>> = const { RefCell::new(None) };
}

static PANIC_HOOK: Once = Once::new();

pub(crate) struct CaughtPanic {
    payload: Box<dyn Any + Send>,
    message: String,
}

impl CaughtPanic {
    pub(crate) fn message(&self) -> &str {
        &self.message
    }

    pub(crate) fn into_payload(self) -> Box<dyn Any + Send> {
        self.payload
    }
}

pub(crate) struct PanicUtils;

impl PanicUtils {
    /// Runs `f`, converting a panic into an `Err` containing the panic payload
    pub(crate) fn catch_panic<F, R>(f: F) -> Result<R, CaughtPanic>
    where
        F: FnOnce() -> R,
    {
        Self::install_panic_hook();
        Self::clear_panic_message();
        catch_unwind(AssertUnwindSafe(f)).map_err(|payload| CaughtPanic {
            payload,
            message: Self::take_panic_message(),
        })
    }
    /// Polls `future` to completion, catching any panic raised while it is being polled and returning it as an `Err`
    pub(crate) async fn catch_panic_async<F, T>(future: F) -> Result<T, CaughtPanic>
    where
        F: Future<Output = T>,
    {
        let mut future = std::pin::pin!(AssertUnwindSafe(future));
        poll_fn(|cx| match Self::catch_panic(|| future.as_mut().poll(cx)) {
            Ok(Poll::Ready(value)) => Poll::Ready(Ok(value)),
            Ok(Poll::Pending) => Poll::Pending,
            Err(panic) => Poll::Ready(Err(panic)),
        })
        .await
    }

    fn install_panic_hook() {
        PANIC_HOOK.call_once(|| {
            let previous = std::panic::take_hook();
            std::panic::set_hook(Box::new(move |info: &PanicHookInfo<'_>| {
                let message = info.payload_as_str().unwrap_or("Unknown panic cause");
                LAST_PANIC_MESSAGE.with_borrow_mut(|slot| {
                    *slot = Some(message.to_string());
                });
                previous(info);
            }));
        });
    }

    fn clear_panic_message() {
        LAST_PANIC_MESSAGE.with_borrow_mut(|slot| *slot = None);
    }

    fn take_panic_message() -> String {
        LAST_PANIC_MESSAGE
            .with_borrow_mut(|slot| slot.take())
            .unwrap_or_else(|| "Unknown panic cause".to_string())
    }
}
