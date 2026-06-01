use crate::scanner::Scanner;
use crate::status::StatusSnapshot;

/// Production soft watermark for an open chunk (see spec, Buffer section).
const WATERMARK: usize = 128 * 1024;

/// Public, I/O-free core of a muxer session: feed raw output in, get a
/// faithful sanitized replay and the current shell status out.
pub struct ReplayEngine {
    scanner: Scanner,
}

impl ReplayEngine {
    pub fn new() -> Self {
        Self {
            scanner: Scanner::new(WATERMARK),
        }
    }

    /// Feed raw terminal output bytes from the PTY.
    pub fn feed(&mut self, bytes: &[u8]) {
        self.scanner.feed(bytes);
    }

    /// Full available replay (all chunks since the last purge boundary).
    pub fn replay(&self) -> Vec<u8> {
        self.scanner.replay(usize::MAX)
    }

    /// Replay capped to the most recent `want_chunks` chunks (memory/scrollback
    /// dial; see spec). Modes stay correct via the prologue even when capped.
    pub fn replay_capped(&self, want_chunks: usize) -> Vec<u8> {
        self.scanner.replay(want_chunks)
    }

    /// Current shell status for the `STATUS` wire frame.
    pub fn status(&self) -> StatusSnapshot {
        self.scanner.status()
    }
}

impl Default for ReplayEngine {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn feed_replay_status_roundtrip() {
        let mut e = ReplayEngine::new();
        e.feed(b"\x1b]133;C\x07");
        e.feed(b"building...");
        e.feed(b"\x1b]9;4;1;75\x07");
        assert_eq!(e.replay(), b"building...".to_vec());
        let s = e.status();
        assert!(s.running);
        assert_eq!(s.progress, Some(75));
    }

    #[test]
    fn replay_default_reaches_all_available_scrollback() {
        let mut e = ReplayEngine::new();
        e.feed(b"line one\r\nline two\r\n");
        let out = e.replay();
        assert!(out.windows(8).any(|w| w == b"line one"));
        assert!(out.windows(8).any(|w| w == b"line two"));
    }
}
