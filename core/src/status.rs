/// An immutable snapshot of shell status, handed to the daemon for the
/// `STATUS{running, progress}` wire frame.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StatusSnapshot {
    pub running: bool,
    /// OSC 9;4 progress percentage 0..=100, or `None` when not in a
    /// progress state.
    pub progress: Option<u8>,
}

/// Latest semantic shell status, updated as OSC 133 / 9;4 sequences arrive.
#[derive(Debug, Clone)]
pub struct StatusCache {
    running: bool,
    progress: Option<u8>,
    saw_prompt_marker: bool,
}

impl StatusCache {
    pub fn new() -> Self {
        Self {
            running: false,
            progress: None,
            saw_prompt_marker: false,
        }
    }

    pub fn set_running(&mut self, running: bool) {
        self.running = running;
    }

    pub fn set_progress(&mut self, progress: Option<u8>) {
        self.progress = progress;
    }

    pub fn set_prompt_marker_seen(&mut self) {
        self.saw_prompt_marker = true;
    }

    pub fn saw_prompt_marker(&self) -> bool {
        self.saw_prompt_marker
    }

    pub fn snapshot(&self) -> StatusSnapshot {
        StatusSnapshot {
            running: self.running,
            progress: self.progress,
        }
    }
}

impl Default for StatusCache {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults_are_idle() {
        let c = StatusCache::new();
        let s = c.snapshot();
        assert!(!s.running);
        assert_eq!(s.progress, None);
    }

    #[test]
    fn tracks_running_and_progress() {
        let mut c = StatusCache::new();
        c.set_running(true);
        c.set_progress(Some(42));
        let s = c.snapshot();
        assert!(s.running);
        assert_eq!(s.progress, Some(42));
    }

    #[test]
    fn progress_can_clear() {
        let mut c = StatusCache::new();
        c.set_progress(Some(10));
        c.set_progress(None);
        assert_eq!(c.snapshot().progress, None);
    }

    #[test]
    fn prompt_marker_latches() {
        let mut c = StatusCache::new();
        assert!(!c.saw_prompt_marker());
        c.set_prompt_marker_seen();
        assert!(c.saw_prompt_marker());
    }
}
