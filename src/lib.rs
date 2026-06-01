//! jftermd core replay engine: sanitized, replayable terminal-stream buffering.
//!
//! I/O-free. Consumed by the daemon runtime (Plan B).

pub mod engine;
pub mod ring;
pub mod scanner;
pub mod status;
pub mod sticky;

pub use engine::ReplayEngine;
pub use status::StatusSnapshot;
