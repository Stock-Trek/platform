use serde_json::{Value, json};
use std::{
    path::Path,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
};
use wasmtime::{Caller, Memory, ResourceLimiter, StoreLimits, StoreLimitsBuilder};
use wasmtime_wasi::{DirPerms, FilePerms, WasiCtxBuilder, p1::WasiP1Ctx};

pub type Emit = Arc<dyn Fn(&str, Value) + Send + Sync>;

pub(super) struct GuestLimits {
    limits: StoreLimits,
    exceeded: Arc<AtomicBool>,
}

impl GuestLimits {
    fn new(max_memory_bytes: usize, max_table_elements: usize) -> Self {
        Self {
            limits: StoreLimitsBuilder::new()
                .memory_size(max_memory_bytes)
                .table_elements(max_table_elements)
                .instances(1)
                .memories(1)
                .tables(1)
                .build(),
            exceeded: Arc::new(AtomicBool::new(false)),
        }
    }

    pub(super) fn exceeded(&self) -> bool {
        self.exceeded.load(Ordering::SeqCst)
    }

    fn deny(&mut self) -> wasmtime::Result<bool> {
        self.exceeded.store(true, Ordering::SeqCst);
        Err(wasmtime::Error::msg("guest exceeded its resource limit"))
    }
}

impl ResourceLimiter for GuestLimits {
    fn memory_growing(
        &mut self,
        current: usize,
        desired: usize,
        maximum: Option<usize>,
    ) -> wasmtime::Result<bool> {
        if self.limits.memory_growing(current, desired, maximum)? {
            Ok(true)
        } else {
            self.deny()
        }
    }

    fn memory_grow_failed(&mut self, error: wasmtime::Error) -> wasmtime::Result<()> {
        self.exceeded.store(true, Ordering::SeqCst);
        Err(error)
    }

    fn table_growing(
        &mut self,
        current: usize,
        desired: usize,
        maximum: Option<usize>,
    ) -> wasmtime::Result<bool> {
        if self.limits.table_growing(current, desired, maximum)? {
            Ok(true)
        } else {
            self.deny()
        }
    }

    fn table_grow_failed(&mut self, error: wasmtime::Error) -> wasmtime::Result<()> {
        self.exceeded.store(true, Ordering::SeqCst);
        Err(error)
    }

    fn instances(&self) -> usize {
        self.limits.instances()
    }

    fn tables(&self) -> usize {
        self.limits.tables()
    }

    fn memories(&self) -> usize {
        self.limits.memories()
    }
}

pub(super) struct GuestState {
    wasi: WasiP1Ctx,
    emit: Emit,
    pub(super) limits: GuestLimits,
}

impl GuestState {
    pub(super) fn new(
        emit: Emit,
        working_directory: &Path,
        max_memory_bytes: usize,
        max_table_elements: usize,
    ) -> wasmtime::Result<Self> {
        let mut builder = WasiCtxBuilder::new();
        builder.stdin(std::io::empty());
        builder.stdout(std::io::stderr());
        builder.stderr(std::io::stderr());
        builder.preopened_dir(working_directory, "/", DirPerms::READ, FilePerms::READ)?;
        Ok(Self {
            wasi: builder.build_p1(),
            emit,
            limits: GuestLimits::new(max_memory_bytes, max_table_elements),
        })
    }

    pub(super) fn wasi(&mut self) -> &mut WasiP1Ctx {
        &mut self.wasi
    }

    fn emit(&self, kind: &str, payload: Value) {
        (self.emit)(kind, payload);
    }

    pub(super) fn emit_stdout(kind: &str, payload: Value) {
        println!("{}", json!({ "type": kind, "payload": payload }));
    }

    pub(super) fn queue_metadata(
        mut caller: Caller<'_, GuestState>,
        name_ptr: i32,
        name_len: i32,
        description_ptr: i32,
        description_len: i32,
    ) -> wasmtime::Result<()> {
        let memory = Self::caller_memory(&mut caller)?;
        let name = Self::guest_bytes(&caller, &memory, name_ptr, name_len)?;
        let description = Self::guest_bytes(&caller, &memory, description_ptr, description_len)?;
        let payload = json!({
            "name": String::from_utf8_lossy(name),
            "description": String::from_utf8_lossy(description),
        });
        caller.data().emit("metadata", payload);
        Ok(())
    }

    pub(super) fn queue_preferences(
        mut caller: Caller<'_, GuestState>,
        preferences_ptr: i32,
        preferences_len: i32,
    ) -> wasmtime::Result<()> {
        let memory = Self::caller_memory(&mut caller)?;
        let preferences = Self::guest_bytes(&caller, &memory, preferences_ptr, preferences_len)?;
        caller
            .data()
            .emit("preferences", json!(String::from_utf8_lossy(preferences)));
        Ok(())
    }

    pub(super) fn queue_signals(
        mut caller: Caller<'_, GuestState>,
        signals_ptr: i32,
        signals_len: i32,
    ) -> wasmtime::Result<()> {
        let memory = Self::caller_memory(&mut caller)?;
        let signals = Self::guest_bytes(&caller, &memory, signals_ptr, signals_len)?;
        caller
            .data()
            .emit("signals", json!(String::from_utf8_lossy(signals)));
        Ok(())
    }

    pub(super) fn queue_strategy(
        mut caller: Caller<'_, GuestState>,
        strategy_ptr: i32,
        strategy_len: i32,
    ) -> wasmtime::Result<()> {
        let memory = Self::caller_memory(&mut caller)?;
        let strategy = Self::guest_bytes(&caller, &memory, strategy_ptr, strategy_len)?;
        caller
            .data()
            .emit("strategy", json!(String::from_utf8_lossy(strategy)));
        Ok(())
    }

    pub(super) fn queue_action(
        mut caller: Caller<'_, GuestState>,
        action_ptr: i32,
        action_len: i32,
        recovery_policy_ptr: i32,
        recovery_policy_len: i32,
    ) -> wasmtime::Result<()> {
        let memory = Self::caller_memory(&mut caller)?;
        let action = Self::guest_bytes(&caller, &memory, action_ptr, action_len)?;
        let recovery_policy =
            Self::guest_bytes(&caller, &memory, recovery_policy_ptr, recovery_policy_len)?;
        let payload = json!({
            "action": String::from_utf8_lossy(action),
            "recoveryPolicy": String::from_utf8_lossy(recovery_policy),
        });
        caller.data().emit("action", payload);
        Ok(())
    }

    fn caller_memory(caller: &mut Caller<'_, GuestState>) -> wasmtime::Result<Memory> {
        caller
            .get_export("memory")
            .and_then(|export| export.into_memory())
            .ok_or_else(|| wasmtime::Error::msg("missing exported memory"))
    }

    fn guest_bytes<'a>(
        caller: &'a Caller<'_, GuestState>,
        memory: &Memory,
        pointer: i32,
        length: i32,
    ) -> wasmtime::Result<&'a [u8]> {
        let start = pointer as u32 as usize;
        let end = start
            .checked_add(length as u32 as usize)
            .ok_or_else(|| wasmtime::Error::msg("guest memory range overflows"))?;
        memory
            .data(caller)
            .get(start..end)
            .ok_or_else(|| wasmtime::Error::msg("guest memory access is out of bounds"))
    }
}
