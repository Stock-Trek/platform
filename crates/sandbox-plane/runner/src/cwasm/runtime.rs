use platform_exit_codes::ExitCode;
use platform_runner::command::Command;
use serde_json::{Value, json};
use std::{
    path::Path,
    sync::{
        Arc, Condvar, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    thread,
    time::Duration,
};
use wasmtime::{
    Caller, Config, Engine, Linker, Memory, Module, ResourceLimiter, Store, StoreLimits,
    StoreLimitsBuilder, TypedFunc,
};
use wasmtime_wasi::{
    DirPerms, FilePerms, I32Exit, WasiCtxBuilder,
    p1::{WasiP1Ctx, add_to_linker_sync},
};

use super::GuestWorkspace;

pub const DEFAULT_EXECUTION_TIMEOUT: Duration = Duration::from_secs(30);
pub const DEFAULT_MAX_GUEST_MEMORY_BYTES: usize = 256 * 1024 * 1024;
pub const DEFAULT_MAX_TABLE_ELEMENTS: usize = 10_000;

pub type Emit = Arc<dyn Fn(&str, Value) + Send + Sync>;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ExecutionLimits {
    pub timeout: Duration,
    pub max_memory_bytes: usize,
    pub max_table_elements: usize,
}

impl ExecutionLimits {
    pub const fn new(timeout: Duration, max_memory_bytes: usize) -> Self {
        Self::with_table_elements(timeout, max_memory_bytes, DEFAULT_MAX_TABLE_ELEMENTS)
    }

    pub const fn with_table_elements(
        timeout: Duration,
        max_memory_bytes: usize,
        max_table_elements: usize,
    ) -> Self {
        Self {
            timeout,
            max_memory_bytes,
            max_table_elements,
        }
    }
}

impl Default for ExecutionLimits {
    fn default() -> Self {
        Self::new(DEFAULT_EXECUTION_TIMEOUT, DEFAULT_MAX_GUEST_MEMORY_BYTES)
    }
}

pub fn execution_config() -> Config {
    let mut config = Config::new();
    config.epoch_interruption(true);
    config
}

enum Failure {
    NotLoaded(wasmtime::Error),
    Trapped(wasmtime::Error),
    TimedOut(Duration),
    ResourceLimitExceeded {
        max_memory_bytes: usize,
        max_table_elements: usize,
    },
}

impl Failure {
    fn exit_code(self) -> i32 {
        match self {
            Failure::NotLoaded(error) => {
                eprintln!("cwasm-invoker: failed to load cwasm artifact: {error:?}");
                ExitCode::ServerRunnerCwasmNotLoaded.code()
            }
            Failure::Trapped(error) => {
                eprintln!("cwasm-invoker: failed to run cwasm artifact: {error:?}");
                ExitCode::BotErrorPanic.code()
            }
            Failure::TimedOut(timeout) => {
                eprintln!("cwasm-invoker: guest execution exceeded {timeout:?}");
                ExitCode::BotErrorTimeout.code()
            }
            Failure::ResourceLimitExceeded {
                max_memory_bytes,
                max_table_elements,
            } => {
                eprintln!(
                    "cwasm-invoker: guest exceeded its resource limits ({max_memory_bytes} bytes of memory or {max_table_elements} table elements)"
                );
                ExitCode::BotErrorResourceLimitExceeded.code()
            }
        }
    }
}

struct GuestLimits {
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

    fn exceeded(&self) -> bool {
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

struct GuestState {
    wasi: WasiP1Ctx,
    emit: Emit,
    limits: GuestLimits,
}

impl GuestState {
    fn new(
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

    fn wasi(&mut self) -> &mut WasiP1Ctx {
        &mut self.wasi
    }

    fn emit(&self, kind: &str, payload: Value) {
        (self.emit)(kind, payload);
    }

    fn emit_stdout(kind: &str, payload: Value) {
        println!("{}", json!({ "type": kind, "payload": payload }));
    }

    fn queue_metadata(
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

    fn queue_preferences(
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

    fn queue_signals(
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

    fn queue_strategy(
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

    fn queue_action(
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

pub struct WasmRunner {
    engine: Engine,
    limits: ExecutionLimits,
}

impl WasmRunner {
    #[allow(dead_code)]
    pub fn new() -> wasmtime::Result<Self> {
        Self::with_limits(ExecutionLimits::default())
    }

    pub fn with_limits(limits: ExecutionLimits) -> wasmtime::Result<Self> {
        Ok(Self {
            engine: Engine::new(&execution_config())?,
            limits,
        })
    }

    pub fn execute_command(&self, path: &Path, command: &Command) -> i32 {
        let emit: Emit = Arc::new(GuestState::emit_stdout);
        self.execute_command_with(path, command, &emit)
    }

    pub fn execute_command_with(&self, path: &Path, command: &Command, emit: &Emit) -> i32 {
        let workspace = match GuestWorkspace::for_command(command) {
            Ok(workspace) => workspace,
            Err(code) => return code.code(),
        };
        let args = workspace.arguments_as_str();
        match self.run(path, &args, emit, workspace.path()) {
            Ok(code) => code,
            Err(failure) => failure.exit_code(),
        }
    }

    pub fn execute_arguments(
        &self,
        path: &Path,
        arguments: &[&str],
        working_directory: &Path,
        emit: &Emit,
    ) -> i32 {
        match self.run(path, arguments, emit, working_directory) {
            Ok(code) => code,
            Err(failure) => failure.exit_code(),
        }
    }

    fn run(
        &self,
        path: &Path,
        args: &[&str],
        emit: &Emit,
        working_directory: &Path,
    ) -> Result<i32, Failure> {
        let module =
            unsafe { Module::deserialize_file(&self.engine, path) }.map_err(Failure::NotLoaded)?;
        let linker = self.linker().map_err(Failure::NotLoaded)?;
        let state = GuestState::new(
            emit.clone(),
            working_directory,
            self.limits.max_memory_bytes,
            self.limits.max_table_elements,
        )
        .map_err(Failure::NotLoaded)?;
        let mut store = Store::new(&self.engine, state);
        store.limiter(|state| &mut state.limits);
        store.set_epoch_deadline(1);
        let watchdog = Watchdog::start(self.engine.clone(), self.limits.timeout);

        let result = (|| -> wasmtime::Result<i32> {
            let instance = linker.instantiate(&mut store, &module)?;
            if let Some(initialize) = instance.get_func(&mut store, "initialize") {
                initialize
                    .typed::<(), ()>(&mut store)?
                    .call(&mut store, ())?;
            }
            let alloc = instance.get_typed_func::<i32, i32>(&mut store, "alloc")?;
            let entry = instance.get_typed_func::<(i32, i32), i32>(&mut store, "entry")?;
            let memory = instance
                .get_memory(&mut store, "memory")
                .ok_or_else(|| wasmtime::Error::msg("missing exported memory"))?;
            let argv = Self::write_arguments(&memory, &mut store, &alloc, args)?;
            entry.call(&mut store, (args.len() as i32, argv as i32))
        })();

        let timed_out = watchdog.timed_out();
        let resource_limit_exceeded = store.data().limits.exceeded();
        drop(watchdog);

        match result {
            Ok(_) if resource_limit_exceeded => Err(Failure::ResourceLimitExceeded {
                max_memory_bytes: self.limits.max_memory_bytes,
                max_table_elements: self.limits.max_table_elements,
            }),
            Ok(code) => Ok(code),
            Err(_) if resource_limit_exceeded => Err(Failure::ResourceLimitExceeded {
                max_memory_bytes: self.limits.max_memory_bytes,
                max_table_elements: self.limits.max_table_elements,
            }),
            Err(_) if timed_out => Err(Failure::TimedOut(self.limits.timeout)),
            Err(error) => match error.downcast_ref::<I32Exit>() {
                Some(exit) => Ok(exit.0),
                None => Err(Failure::Trapped(error)),
            },
        }
    }

    fn linker(&self) -> wasmtime::Result<Linker<GuestState>> {
        let mut linker = Linker::new(&self.engine);
        add_to_linker_sync(&mut linker, GuestState::wasi)?;
        linker.func_wrap("env", "queue_metadata", GuestState::queue_metadata)?;
        linker.func_wrap("env", "queue_preferences", GuestState::queue_preferences)?;
        linker.func_wrap("env", "queue_signals", GuestState::queue_signals)?;
        linker.func_wrap("env", "queue_strategy", GuestState::queue_strategy)?;
        linker.func_wrap("env", "queue_action", GuestState::queue_action)?;
        Ok(linker)
    }

    fn write_arguments(
        memory: &Memory,
        store: &mut Store<GuestState>,
        alloc: &TypedFunc<i32, i32>,
        args: &[&str],
    ) -> wasmtime::Result<u32> {
        let mut size = (args.len() + 1) * size_of::<u32>();
        size += args
            .iter()
            .map(|argument| argument.len() + 1)
            .sum::<usize>();
        let size = i32::try_from(size)
            .map_err(|_| wasmtime::Error::msg("guest arguments exceed memory addressing"))?;
        let base = alloc.call(&mut *store, size)?;
        let base = u32::try_from(base)
            .map_err(|_| wasmtime::Error::msg("guest allocation returned an invalid pointer"))?;
        let mut pointers = Vec::with_capacity(args.len() + 1);
        let mut offset = base as usize + (args.len() + 1) * size_of::<u32>();
        for argument in args {
            pointers.push(
                u32::try_from(offset)
                    .map_err(|_| wasmtime::Error::msg("guest argument pointer exceeds memory"))?,
            );
            let mut bytes = Vec::with_capacity(argument.len() + 1);
            bytes.extend_from_slice(argument.as_bytes());
            bytes.push(0);
            memory.write(&mut *store, offset, &bytes)?;
            offset += bytes.len();
        }
        pointers.push(0);
        let argv: Vec<u8> = pointers
            .iter()
            .flat_map(|pointer| pointer.to_le_bytes())
            .collect();
        memory.write(&mut *store, base as usize, &argv)?;
        Ok(base)
    }
}

struct Watchdog {
    finished: Arc<(Mutex<bool>, Condvar)>,
    timed_out: Arc<AtomicBool>,
    handle: Option<thread::JoinHandle<()>>,
}

impl Watchdog {
    fn start(engine: Engine, timeout: Duration) -> Self {
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

    fn timed_out(&self) -> bool {
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
