use std::{
    sync::{
        Arc, Condvar, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    thread,
    time::Duration,
};
use wasmtime::Engine;

pub(super) struct Watchdog {
    finished: Arc<(Mutex<bool>, Condvar)>,
    timed_out: Arc<AtomicBool>,
    handle: Option<thread::JoinHandle<()>>,
}

impl Watchdog {
    pub(super) fn start(engine: Engine, timeout: Duration) -> Self {
        let finished = Arc::new((Mutex::new(false), Condvar::new()));
        let timed_out = Arc::new(AtomicBool::new(false));
        let thread_finished = Arc::clone(&finished);
        let thread_timed_out = Arc::clone(&timed_out);
        let handle = thread::spawn(move || {
            let (lock, wake) = &*thread_finished;
            let finished = lock.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
            let (finished, _) = wake
                .wait_timeout_while(finished, timeout, |finished| !*finished)
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            if !*finished {
                thread_timed_out.store(true, Ordering::SeqCst);
                engine.increment_epoch();
            }
        });
        Self {
            finished,
            timed_out,
            handle: Some(handle),
        }
    }

    pub(super) fn timed_out(&self) -> bool {
        self.timed_out.load(Ordering::SeqCst)
    }
}

impl Drop for Watchdog {
    fn drop(&mut self) {
        let (lock, wake) = &*self.finished;
        *lock.lock().unwrap_or_else(|poisoned| poisoned.into_inner()) = true;
        wake.notify_one();
        if let Some(handle) = self.handle.take() {
            let _ = handle.join();
        }
    }
}
