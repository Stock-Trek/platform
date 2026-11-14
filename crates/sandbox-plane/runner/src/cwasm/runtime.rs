use crate::Command;
use platform_exit_codes::ExitCode;
use std::{path::Path, time::Duration};
use wasmtime::{Config, Engine, Linker, Memory, Module, Store, TypedFunc};
use wasmtime_wasi::{I32Exit, p1::add_to_linker_sync};

use crate::cwasm::{
    GuestWorkspace,
    guest_state::{Emit, GuestState},
    limits::ExecutionLimits,
    watchdog::Watchdog,
};

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

pub struct WasmRunner {
    engine: Engine,
    limits: ExecutionLimits,
}

impl WasmRunner {
    pub fn with_limits(limits: ExecutionLimits) -> wasmtime::Result<Self> {
        Ok(Self {
            engine: Engine::new(&Self::config())?,
            limits,
        })
    }

    pub fn config() -> Config {
        let mut config = Config::new();
        config.epoch_interruption(true);
        config
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
