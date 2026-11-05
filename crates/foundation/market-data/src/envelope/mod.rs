//! JSON envelope codec for messages published on the market data subjects.

mod envelope_codec;

#[cfg(test)]
mod tests;

pub use envelope_codec::*;
