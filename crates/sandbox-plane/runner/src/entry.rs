use crate::{
    command::{Command, CommandName},
    runner::Runner,
};
use platform_exit_codes::ExitCode;
use std::{ffi::CStr, os::raw::c_char};

const USAGE: &str = "\
Usage:
  program Algorithm <strategy_name>
  program Execute <strategy_name> <strategy_context_path> <portfolio_path>
  program Metadata <strategy_name>
  program Preferences <strategy_name>
  program Signals <strategy_name> <strategy_context_path>";

pub struct Entry;

impl Entry {
    pub fn run_command(command: Command) -> i32 {
        std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| Runner::run(command)))
            .unwrap_or(ExitCode::BotErrorPanic.code())
    }

    /// # Safety
    /// This is called from the host binary which must ensure correct args
    pub unsafe fn run(argc: i32, argv: *const *const c_char) -> i32 {
        if argc < 2 {
            eprintln!("{USAGE}");
            return ExitCode::ServerEntryIncorrectArgCount.code();
        }
        let args: Vec<String> = (0..argc)
            .map(|i| {
                let arg_ptr = unsafe { *argv.offset(i as isize) };
                unsafe { CStr::from_ptr(arg_ptr).to_string_lossy().into_owned() }
            })
            .collect();
        let Some(command_name) = CommandName::parse(&args[1]) else {
            return ExitCode::ServerEntryFailedToParseCommand.code();
        };
        if args.len() != command_name.argument_count() {
            eprintln!("{USAGE}");
            return ExitCode::ServerEntryIncorrectArgCount.code();
        }
        Self::run_command(command_name.command(&args))
    }
}

/// # Safety
/// This is called from the host binary which must ensure correct args
#[unsafe(no_mangle)]
pub unsafe extern "C" fn entry(argc: i32, argv: *const *const c_char) -> i32 {
    unsafe { Entry::run(argc, argv) }
}

#[cfg(target_arch = "wasm32")]
unsafe extern "C" {
    fn __wasm_call_ctors();
}

/// Initializes the registered algorithms.
///
/// Rust's wasm linker only emits `__wasm_call_ctors` when a module needs it and
/// does not export it automatically. The host must call this once before
/// `alloc`/`entry` so that `traitreg`'s constructors run and populate the
/// algorithm registry.
#[cfg(target_arch = "wasm32")]
#[unsafe(no_mangle)]
pub extern "C" fn initialize() {
    use std::sync::atomic::{AtomicBool, Ordering};

    static INITIALIZED: AtomicBool = AtomicBool::new(false);
    if !INITIALIZED.swap(true, Ordering::SeqCst) {
        unsafe { __wasm_call_ctors() };
    }
}

#[cfg(target_arch = "wasm32")]
#[unsafe(no_mangle)]
pub extern "C" fn alloc(size: usize) -> *mut u8 {
    if size == 0 {
        return std::ptr::null_mut();
    }
    let layout = std::alloc::Layout::from_size_align(size, 16).expect("invalid alloc layout");
    unsafe { std::alloc::alloc(layout) }
}
