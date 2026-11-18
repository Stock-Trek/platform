// TODO: these surfaces are the contract for follow-up work (WebSocket upgrade,
// exchange linking, tick dispatch, log attach); remove the allow as each comes online.
#![allow(dead_code)]

mod accounts;
mod algorithms;
mod api;
mod auth;
mod backtests;
mod bots;
mod config;
mod limits;
mod runner;
mod streaming;

pub use config::FrontEndApiConfig;
pub use runner::FrontEndApiRunner;
