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

/// Test-support re-export so the `vt100` oracle (an integration test, which
/// can only see the public API) can drive the scanner directly.
pub mod scanner_test_support {
    pub use crate::scanner::Scanner;
}
