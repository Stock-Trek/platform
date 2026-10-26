use crate::{command::Command, runner::Runner};
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
        let command = match (args[1].as_str(), args.len()) {
            ("Algorithm", 3) => Command::Algorithm {
                strategy_name: args[2].clone(),
            },
            ("Execute", 5) => Command::Execute {
                strategy_name: args[2].clone(),
                strategy_context_path: args[3].clone(),
                portfolio_path: args[4].clone(),
            },
            ("Metadata", 3) => Command::Metadata {
                strategy_name: args[2].clone(),
            },
            ("Preferences", 3) => Command::Preferences {
                strategy_name: args[2].clone(),
            },
            ("Signals", 4) => Command::Signals {
                strategy_name: args[2].clone(),
                strategy_context_path: args[3].clone(),
            },
            ("Algorithm" | "Execute" | "Metadata" | "Preferences" | "Signals", _) => {
                eprintln!("{USAGE}");
                return ExitCode::ServerEntryIncorrectArgCount.code();
            }
            _ => return ExitCode::ServerEntryFailedToParseCommand.code(),
        };
        std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| Runner::run(command)))
            .unwrap_or(ExitCode::BotErrorPanic.code())
    }
}

/// # Safety
/// This is called from the host binary which must ensure correct args
#[unsafe(no_mangle)]
pub unsafe extern "C" fn entry(argc: i32, argv: *const *const c_char) -> i32 {
    unsafe { Entry::run(argc, argv) }
}
