use platform_exit_codes::ExitCode;
use serde_json::{Value, json};
use std::path::Path;
use std::sync::Arc;
use wasmtime::{Caller, Config, Engine, Linker, Memory, Module, Store, TypedFunc};
use wasmtime_wasi::{
    DirPerms, FilePerms, I32Exit, WasiCtxBuilder,
    p1::{WasiP1Ctx, add_to_linker_sync},
};

pub type Emit = Arc<dyn Fn(&str, Value) + Send + Sync>;

enum Failure {
    NotLoaded(wasmtime::Error),
    Trapped(wasmtime::Error),
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
        }
    }
}

struct GuestState {
    wasi: WasiP1Ctx,
    emit: Emit,
}

impl GuestState {
    fn new(emit: Emit) -> wasmtime::Result<Self> {
        let mut builder = WasiCtxBuilder::new();
        builder.inherit_stdio();
        builder.preopened_dir("/", "/", DirPerms::READ, FilePerms::READ)?;
        Ok(Self {
            wasi: builder.build_p1(),
            emit,
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
        equity_ptr: i32,
        equity_len: i32,
        symbols_ptr: i32,
        symbols_len: i32,
    ) -> wasmtime::Result<()> {
        let memory = Self::caller_memory(&mut caller)?;
        let equity = Self::guest_bytes(&caller, &memory, equity_ptr, equity_len)?;
        let symbols = Self::guest_bytes(&caller, &memory, symbols_ptr, symbols_len)?;
        let payload = json!({
            "equity": String::from_utf8_lossy(equity),
            "symbols": String::from_utf8_lossy(symbols),
        });
        caller.data().emit("preferences", payload);
        Ok(())
    }

    fn queue_signals(
        mut caller: Caller<'_, GuestState>,
        metadata_ptr: i32,
        metadata_len: i32,
        signals_ptr: i32,
        signals_len: i32,
    ) -> wasmtime::Result<()> {
        let memory = Self::caller_memory(&mut caller)?;
        let metadata = Self::guest_bytes(&caller, &memory, metadata_ptr, metadata_len)?;
        let signals = Self::guest_bytes(&caller, &memory, signals_ptr, signals_len)?;
        let payload = json!({
            "metadata": Self::metadata_json(metadata)?,
            "signals": Self::signals_json(signals)?,
        });
        caller.data().emit("signals", payload);
        Ok(())
    }

    fn queue_strategy(
        mut caller: Caller<'_, GuestState>,
        source_ptr: i32,
        source_len: i32,
    ) -> wasmtime::Result<()> {
        let memory = Self::caller_memory(&mut caller)?;
        let source = Self::guest_bytes(&caller, &memory, source_ptr, source_len)?;
        let payload = json!({ "source": String::from_utf8_lossy(source) });
        caller.data().emit("strategy", payload);
        Ok(())
    }

    fn queue_action(
        mut caller: Caller<'_, GuestState>,
        metadata_ptr: i32,
        metadata_len: i32,
        action_ptr: i32,
        action_len: i32,
    ) -> wasmtime::Result<()> {
        let memory = Self::caller_memory(&mut caller)?;
        let metadata = Self::guest_bytes(&caller, &memory, metadata_ptr, metadata_len)?;
        let action = Self::guest_bytes(&caller, &memory, action_ptr, action_len)?;
        let payload = json!({
            "metadata": Self::metadata_json(metadata)?,
            "action": Self::action_json(action)?,
        });
        caller.data().emit("action", payload);
        Ok(())
    }

    fn metadata_json(bytes: &[u8]) -> wasmtime::Result<Value> {
        serde_json::from_slice(bytes)
            .map_err(|error| wasmtime::Error::msg(format!("invalid metadata json: {error}")))
    }

    fn signals_json(bytes: &[u8]) -> wasmtime::Result<Value> {
        serde_json::from_slice(bytes)
            .map_err(|error| wasmtime::Error::msg(format!("invalid signals json: {error}")))
    }

    fn action_json(bytes: &[u8]) -> wasmtime::Result<Value> {
        serde_json::from_slice(bytes)
            .map_err(|error| wasmtime::Error::msg(format!("invalid action json: {error}")))
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
}

impl WasmRunner {
    pub fn new() -> wasmtime::Result<Self> {
        Ok(Self {
            engine: Engine::new(&Config::default())?,
        })
    }

    pub fn execute(&self, path: &Path, args: &[&str]) -> i32 {
        let emit: Emit = Arc::new(GuestState::emit_stdout);
        self.execute_with(path, args, &emit)
    }

    pub fn execute_with(&self, path: &Path, args: &[&str], emit: &Emit) -> i32 {
        match self.run(path, args, emit) {
            Ok(code) => code,
            Err(failure) => failure.exit_code(),
        }
    }

    fn run(&self, path: &Path, args: &[&str], emit: &Emit) -> Result<i32, Failure> {
        let module =
            unsafe { Module::deserialize_file(&self.engine, path) }.map_err(Failure::NotLoaded)?;
        let linker = self.linker().map_err(Failure::NotLoaded)?;
        let state = GuestState::new(emit.clone()).map_err(Failure::NotLoaded)?;
        let mut store = Store::new(&self.engine, state);
        let instance = linker
            .instantiate(&mut store, &module)
            .map_err(Failure::NotLoaded)?;
        let alloc = instance
            .get_typed_func::<i32, i32>(&mut store, "alloc")
            .map_err(Failure::NotLoaded)?;
        let entry = instance
            .get_typed_func::<(i32, i32), i32>(&mut store, "entry")
            .map_err(Failure::NotLoaded)?;
        let memory = instance
            .get_memory(&mut store, "memory")
            .ok_or_else(|| Failure::NotLoaded(wasmtime::Error::msg("missing exported memory")))?;
        let argv =
            Self::write_arguments(&memory, &mut store, &alloc, args).map_err(Failure::NotLoaded)?;
        match entry.call(&mut store, (args.len() as i32, argv as i32)) {
            Ok(code) => Ok(code),
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
