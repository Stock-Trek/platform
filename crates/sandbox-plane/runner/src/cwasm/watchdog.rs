use parking_lot::{Condvar, Mutex};
use std::{
    sync::{
        Arc,
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
            let mut finished = lock.lock();
            wake.wait_while_for(&mut finished, |finished| !*finished, timeout);
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
        *lock.lock() = true;
        wake.notify_one();
        if let Some(handle) = self.handle.take() {
            let _ = handle.join();
        }
    }
}
