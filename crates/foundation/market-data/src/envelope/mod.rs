//! JSON envelope codec for messages published on the market data subjects.

mod implementation;

#[cfg(test)]
mod tests;

pub use implementation::*;
