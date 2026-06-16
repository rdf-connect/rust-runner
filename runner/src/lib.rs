//! RDF-Connect Rust Runner library.
//!
//! Re-exports the public API used by integration tests and the CLI entry point.

pub mod channel;
pub mod processor;
pub mod proto;
pub mod registry;
pub mod runner;

pub use runner::run;
