use std::cell::RefCell;
use std::collections::BTreeMap;

/// Visual terminal state that must be re-asserted to make a chunk
/// self-contained for replay. Updated by the scanner; serialized into each
/// chunk's prologue.
#[derive(Debug, Clone, Default)]
pub struct StickyState {
    /// Raw SGR parameters of the current graphic rendition (empty == default).
    sgr: Vec<u16>,
    /// DEC private modes currently set (code -> on). Only `true` entries are
    /// serialized; resetting removes the entry.
    dec_modes: BTreeMap<u16, bool>,
    /// DECSTBM scroll region as the 1-based (top, bottom) CSI params sent.
    scroll_region: Option<(u16, u16)>,
    /// Last OSC 7 payload (e.g. `file:///home/jfim`), opaque bytes.
    cwd: Option<Vec<u8>>,
    /// Last OSC 0/2 title payload, opaque bytes.
    title: Option<Vec<u8>>,
    /// Cached serialized prologue, rebuilt lazily and invalidated on any
    /// state mutation. Avoids rebuilding the prologue (with per-param
    /// `to_string`) on every cut/purge when the state has not changed.
    cache: RefCell<Option<Vec<u8>>>,
}

impl StickyState {
    pub fn new() -> Self {
        Self::default()
    }

    /// Drop the cached prologue; called after any state mutation.
    fn invalidate(&mut self) {
        *self.cache.get_mut() = None;
    }

    pub fn set_sgr(&mut self, params: &[u16]) {
        self.sgr.clear();
        self.sgr.extend_from_slice(params);
        self.invalidate();
    }

    pub fn set_dec_mode(&mut self, code: u16, on: bool) {
        if on {
            self.dec_modes.insert(code, true);
        } else {
            self.dec_modes.remove(&code);
        }
        self.invalidate();
    }

    pub fn set_scroll_region(&mut self, region: Option<(u16, u16)>) {
        self.scroll_region = region;
        self.invalidate();
    }

    pub fn set_cwd(&mut self, cwd: Vec<u8>) {
        self.cwd = Some(cwd);
        self.invalidate();
    }

    pub fn set_title(&mut self, title: Vec<u8>) {
        self.title = Some(title);
        self.invalidate();
    }

    /// Emit a byte string that re-establishes this state from a cold terminal.
    /// Always begins with a full SGR reset so the chunk is self-contained.
    /// The result is cached and reused until the next state mutation.
    pub fn serialize(&self) -> Vec<u8> {
        if let Some(cached) = self.cache.borrow().as_ref() {
            return cached.clone();
        }
        let out = self.build_serialized();
        *self.cache.borrow_mut() = Some(out.clone());
        out
    }

    /// Build the prologue bytes from scratch (uncached).
    fn build_serialized(&self) -> Vec<u8> {
        let mut out = Vec::new();
        out.extend_from_slice(b"\x1b[0m");

        if !self.sgr.is_empty() {
            out.extend_from_slice(b"\x1b[");
            for (i, p) in self.sgr.iter().enumerate() {
                if i > 0 {
                    out.push(b';');
                }
                out.extend_from_slice(p.to_string().as_bytes());
            }
            out.push(b'm');
        }

        // BTreeMap iterates in ascending key order -> deterministic output.
        for code in self.dec_modes.keys() {
            out.extend_from_slice(b"\x1b[?");
            out.extend_from_slice(code.to_string().as_bytes());
            out.push(b'h');
        }

        if let Some((top, bottom)) = self.scroll_region {
            out.extend_from_slice(b"\x1b[");
            out.extend_from_slice(top.to_string().as_bytes());
            out.push(b';');
            out.extend_from_slice(bottom.to_string().as_bytes());
            out.push(b'r');
        }

        if let Some(cwd) = &self.cwd {
            out.extend_from_slice(b"\x1b]7;");
            out.extend_from_slice(cwd);
            out.push(0x07);
        }

        if let Some(title) = &self.title {
            out.extend_from_slice(b"\x1b]2;");
            out.extend_from_slice(title);
            out.push(0x07);
        }

        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_state_serializes_to_a_hard_reset_only() {
        let s = StickyState::new();
        let out = s.serialize();
        assert_eq!(out, b"\x1b[0m".to_vec());
    }

    #[test]
    fn sgr_is_reasserted() {
        let mut s = StickyState::new();
        s.set_sgr(&[1, 31]); // bold, fg red
        let out = s.serialize();
        assert_eq!(out, b"\x1b[0m\x1b[1;31m".to_vec());
    }

    #[test]
    fn dec_private_modes_are_reasserted() {
        let mut s = StickyState::new();
        s.set_dec_mode(1049, true); // alt screen on
        s.set_dec_mode(2004, true); // bracketed paste on
        let out = s.serialize();
        // Modes emitted in ascending code order for determinism.
        assert_eq!(out, b"\x1b[0m\x1b[?1049h\x1b[?2004h".to_vec());
    }

    #[test]
    fn dec_mode_reset_removes_it() {
        let mut s = StickyState::new();
        s.set_dec_mode(1049, true);
        s.set_dec_mode(1049, false);
        let out = s.serialize();
        assert_eq!(out, b"\x1b[0m".to_vec());
    }

    #[test]
    fn scroll_region_and_cwd_and_title() {
        let mut s = StickyState::new();
        s.set_scroll_region(Some((2, 24))); // stored as the 1-based CSI params
        s.set_cwd(b"file:///home/jfim".to_vec());
        s.set_title(b"vim".to_vec());
        let out = s.serialize();
        assert_eq!(
            out,
            b"\x1b[0m\x1b[2;24r\x1b]7;file:///home/jfim\x07\x1b]2;vim\x07".to_vec()
        );
    }
}
