use parking_lot::Mutex;
use platform_errors::{BpError, BpResult, ConnectorError, SystemError};
use platform_types::new_types::{DurationMilliseconds, TimestampMilliseconds};
use std::{
    sync::atomic::{AtomicI64, Ordering},
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

#[derive(Debug)]
pub struct Clock {
    server_offset_millis: AtomicI64,
    last_sync: Mutex<Option<Instant>>,
}

impl Default for Clock {
    fn default() -> Self {
        Clock::new()
    }
}

impl Clock {
    pub fn new() -> Self {
        Self {
            server_offset_millis: AtomicI64::new(0),
            last_sync: Mutex::new(None),
        }
    }

    pub fn duration_since_last_sync(&self) -> BpResult<Option<Duration>> {
        let mutex = self.last_sync.lock();
        let last_sync = *mutex;
        Ok(last_sync.map(|i| i.elapsed()))
    }

    pub fn is_synced(&self) -> BpResult<bool> {
        let last_sync = self.last_sync.lock();
        Ok(last_sync.is_some())
    }
    pub fn sync(
        &self,
        server_time: TimestampMilliseconds,
        round_trip_duration: Duration,
    ) -> BpResult<()> {
        let system_time = Self::system_time()?;
        let round_trip_time = DurationMilliseconds(round_trip_duration.as_millis() as i64);
        let half_round_trip = round_trip_time.checked_div(2_i64)?;
        let system_time_estimate = system_time - half_round_trip;
        let offset_estimate = system_time_estimate - server_time;
        self.server_offset_millis
            .store(offset_estimate.0, Ordering::Relaxed);
        let mut last_sync = self.last_sync.lock();
        *last_sync = Some(Instant::now());
        Ok(())
    }
    pub fn server_time_estimate(&self) -> BpResult<TimestampMilliseconds> {
        if !self.is_synced()? {
            return Err(BpError::Connector(ConnectorError::ClockNotSynced));
        }
        self.server_time_estimate_unchecked()
    }
    pub fn server_time_estimate_unchecked(&self) -> BpResult<TimestampMilliseconds> {
        Ok(Self::system_time()? - self.server_offset())
    }

    fn system_time() -> BpResult<TimestampMilliseconds> {
        let system_time = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|_| BpError::System(SystemError::SystemTimeBeforeEpoch))?;
        Ok(TimestampMilliseconds(system_time.as_millis() as i64))
    }
    fn server_offset(&self) -> DurationMilliseconds {
        DurationMilliseconds(self.server_offset_millis.load(Ordering::Relaxed))
    }
}

#[cfg(test)]
mod tests;
