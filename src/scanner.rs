use vte::{Params, Parser, Perform};

use crate::ring::ChunkRing;
use crate::status::{StatusCache, StatusSnapshot};
use crate::sticky::StickyState;

/// What the parser did with the byte(s) just fed. Drives the outer loop's
/// span accumulation, classification, and ring cut/purge decisions.
#[derive(Debug, Clone, PartialEq, Eq)]
enum SegEvent {
    /// Still mid-sequence (ESC consumed, intermediate, param, or DCS body).
    None,
    /// A printable character reached ground state — keep verbatim.
    KeepByte,
    /// A control/sequence reached ground state and should be kept verbatim.
    KeepSeq,
    /// A sequence reached ground state and must be dropped from the ring.
    DropSeq,
    /// A full-screen clear reached ground state — purge history.
    ClearSeq,
}

/// The `vte::Perform` sink. Updates sticky/status and reports a `SegEvent`.
struct Sink {
    sticky: StickyState,
    status: StatusCache,
    event: SegEvent,
}

impl Sink {
    fn new() -> Self {
        Self {
            sticky: StickyState::new(),
            status: StatusCache::new(),
            event: SegEvent::None,
        }
    }

    fn take_event(&mut self) -> SegEvent {
        std::mem::replace(&mut self.event, SegEvent::None)
    }
}

impl Perform for Sink {
    fn print(&mut self, _c: char) {
        self.event = SegEvent::KeepByte;
    }

    fn execute(&mut self, byte: u8) {
        // C0 controls. Keep the ones that affect layout; drop the bell.
        self.event = if byte == 0x07 {
            SegEvent::DropSeq
        } else {
            SegEvent::KeepSeq
        };
    }

    fn hook(&mut self, _params: &Params, _intermediates: &[u8], _ignore: bool, _action: char) {
        self.event = SegEvent::None;
    }

    fn put(&mut self, _byte: u8) {
        self.event = SegEvent::None;
    }

    fn unhook(&mut self) {
        self.event = SegEvent::KeepSeq;
    }

    fn osc_dispatch(&mut self, params: &[&[u8]], _bell_terminated: bool) {
        let code = params.first().and_then(|p| std::str::from_utf8(p).ok());
        match code {
            Some("0") | Some("1") | Some("2") => {
                if let Some(text) = params.get(1) {
                    self.sticky.set_title(Some(text.to_vec()));
                }
                self.event = SegEvent::KeepSeq;
            }
            Some("7") => {
                if let Some(cwd) = params.get(1) {
                    self.sticky.set_cwd(Some(cwd.to_vec()));
                }
                self.event = SegEvent::KeepSeq;
            }
            Some("9") => {
                if params.get(1).is_some_and(|p| *p == b"4") {
                    let state = params.get(2).and_then(|p| std::str::from_utf8(p).ok());
                    let value = params
                        .get(3)
                        .and_then(|p| std::str::from_utf8(p).ok())
                        .and_then(|s| s.parse::<u8>().ok());
                    match state {
                        Some("0") => self.status.set_progress(None),
                        _ => self.status.set_progress(value),
                    }
                }
                self.event = SegEvent::DropSeq;
            }
            Some("133") => {
                match params.get(1).and_then(|p| p.first()) {
                    Some(b'C') => self.status.set_running(true),
                    Some(b'D') => self.status.set_running(false),
                    _ => {}
                }
                self.event = SegEvent::DropSeq;
            }
            Some("52") | Some("777") => {
                self.event = SegEvent::DropSeq;
            }
            _ => {
                self.event = SegEvent::KeepSeq;
            }
        }
    }

    fn csi_dispatch(&mut self, params: &Params, intermediates: &[u8], _ignore: bool, action: char) {
        let is_private = intermediates.first() == Some(&b'?');
        match action {
            'n' => {
                self.event = SegEvent::DropSeq;
            }
            'c' => {
                self.event = SegEvent::DropSeq;
            }
            'm' => {
                let mut flat = Vec::new();
                for sub in params.iter() {
                    for v in sub {
                        flat.push(*v);
                    }
                }
                if flat.is_empty() || flat.as_slice() == [0u16] {
                    self.sticky.set_sgr(Vec::new());
                } else {
                    self.sticky.set_sgr(flat);
                }
                self.event = SegEvent::KeepSeq;
            }
            'r' if !is_private => {
                let mut it = params.iter();
                let top = it.next().and_then(|s| s.first().copied()).unwrap_or(0);
                let bottom = it.next().and_then(|s| s.first().copied()).unwrap_or(0);
                if top == 0 && bottom == 0 {
                    self.sticky.set_scroll_region(None);
                } else {
                    self.sticky.set_scroll_region(Some((top, bottom)));
                }
                self.event = SegEvent::KeepSeq;
            }
            'h' | 'l' if is_private => {
                let on = action == 'h';
                for sub in params.iter() {
                    if let Some(code) = sub.first() {
                        self.sticky.set_dec_mode(*code, on);
                    }
                }
                self.event = SegEvent::KeepSeq;
            }
            'J' => {
                let mode = params
                    .iter()
                    .next()
                    .and_then(|s| s.first().copied())
                    .unwrap_or(0);
                self.event = if mode == 2 || mode == 3 {
                    SegEvent::ClearSeq
                } else {
                    SegEvent::KeepSeq
                };
            }
            _ => {
                self.event = SegEvent::KeepSeq;
            }
        }
    }

    fn esc_dispatch(&mut self, _intermediates: &[u8], _ignore: bool, byte: u8) {
        self.event = if byte == b'c' {
            SegEvent::ClearSeq
        } else {
            SegEvent::KeepSeq
        };
    }
}

/// Feeds a raw terminal byte stream into a sanitized, replayable ring while
/// tracking sticky state and shell status.
pub struct Scanner {
    parser: Parser,
    sink: Sink,
    ring: ChunkRing,
    /// Raw bytes of the span currently being parsed (not yet classified).
    pending: Vec<u8>,
}

impl Scanner {
    pub fn new(watermark: usize) -> Self {
        Self {
            parser: Parser::new(),
            sink: Sink::new(),
            ring: ChunkRing::new(watermark),
            pending: Vec::new(),
        }
    }

    /// Feed a chunk of raw terminal output.
    pub fn feed(&mut self, bytes: &[u8]) {
        for &b in bytes {
            self.pending.push(b);
            // Feed exactly one byte so a completing callback delimits the span.
            let Self { parser, sink, .. } = self;
            parser.advance(sink, &[b]);
            match self.sink.take_event() {
                SegEvent::None => {}
                SegEvent::KeepByte | SegEvent::KeepSeq => {
                    let span = std::mem::take(&mut self.pending);
                    self.ring.append(&span);
                    self.maybe_cut();
                }
                SegEvent::DropSeq => {
                    self.pending.clear();
                }
                SegEvent::ClearSeq => {
                    self.pending.clear();
                    let mut prologue = b"\x1b[2J\x1b[H".to_vec();
                    prologue.extend_from_slice(&self.sink.sticky.serialize());
                    self.ring.purge(prologue);
                }
            }
        }
    }

    /// Replay the most recent `want_chunks` chunks (see `ChunkRing::replay`).
    pub fn replay(&self, want_chunks: usize) -> Vec<u8> {
        self.ring.replay(want_chunks)
    }

    /// Current shell-status snapshot.
    pub fn status(&self) -> StatusSnapshot {
        self.sink.status.snapshot()
    }

    fn maybe_cut(&mut self) {
        // We are at ground state here (a span just completed). Safe to cut.
        if self.ring.should_cut() {
            let prologue = self.sink.sticky.serialize();
            self.ring.cut(prologue);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn feed(input: &[u8]) -> Scanner {
        let mut s = Scanner::new(128 * 1024);
        s.feed(input);
        s
    }

    #[test]
    fn plain_text_is_kept_verbatim() {
        let s = feed(b"hello world");
        assert_eq!(s.replay(usize::MAX), b"hello world".to_vec());
    }

    #[test]
    fn sgr_is_kept_verbatim_and_tracked() {
        let s = feed(b"\x1b[31mRED");
        assert_eq!(s.replay(usize::MAX), b"\x1b[31mRED".to_vec());
    }

    #[test]
    fn osc52_clipboard_is_dropped_from_the_ring() {
        let s = feed(b"A\x1b]52;c;aGVsbG8=\x07B");
        assert_eq!(s.replay(usize::MAX), b"AB".to_vec());
    }

    #[test]
    fn bell_is_dropped() {
        let s = feed(b"A\x07B");
        assert_eq!(s.replay(usize::MAX), b"AB".to_vec());
    }

    #[test]
    fn desktop_notification_osc9_is_dropped() {
        let s = feed(b"A\x1b]9;build done\x07B");
        assert_eq!(s.replay(usize::MAX), b"AB".to_vec());
    }

    #[test]
    fn dsr_and_da_queries_are_dropped() {
        let s = feed(b"A\x1b[6n\x1b[cB");
        assert_eq!(s.replay(usize::MAX), b"AB".to_vec());
    }

    #[test]
    fn osc9_4_progress_updates_status_and_is_dropped() {
        let s = feed(b"X\x1b]9;4;1;42\x07Y");
        assert_eq!(s.replay(usize::MAX), b"XY".to_vec());
        let snap = s.status();
        assert_eq!(snap.progress, Some(42));
    }

    #[test]
    fn osc133_prompt_updates_running_and_is_dropped() {
        let running = feed(b"\x1b]133;C\x07");
        assert_eq!(running.replay(usize::MAX), b"".to_vec());
        assert!(running.status().running);

        let done = feed(b"\x1b]133;C\x07cmd output\x1b]133;D\x07");
        assert_eq!(done.replay(usize::MAX), b"cmd output".to_vec());
        assert!(!done.status().running);
    }

    #[test]
    fn osc7_cwd_is_kept_verbatim_and_tracked_into_prologue() {
        let s = feed(b"\x1b]7;file:///tmp\x07hi");
        let mut s2 = Scanner::new(4);
        s2.feed(b"\x1b]7;file:///tmp\x07");
        s2.feed(b"aaaa");
        s2.feed(b"X");
        let out = s2.replay(1);
        assert!(
            out.windows(b"\x1b]7;file:///tmp\x07".len())
                .any(|w| w == b"\x1b]7;file:///tmp\x07")
        );
        assert!(s.replay(usize::MAX).starts_with(b"\x1b]7;file:///tmp\x07"));
    }

    #[test]
    fn clear_screen_purges_history() {
        let mut s = Scanner::new(128 * 1024);
        s.feed(b"old stuff here");
        s.feed(b"\x1b[2J");
        s.feed(b"new");
        let out = s.replay(usize::MAX);
        assert!(!out.windows(3).any(|w| w == b"old"));
        assert!(out.ends_with(b"new"));
    }

    #[test]
    fn long_single_sequence_is_not_split_across_the_watermark() {
        let mut payload = b"\x1b]52;c;".to_vec();
        payload.extend(std::iter::repeat_n(b'A', 64));
        payload.push(0x07);
        let mut s = Scanner::new(8);
        s.feed(b"start");
        s.feed(&payload);
        s.feed(b"end");
        let out = s.replay(usize::MAX);
        assert!(!out.windows(3).any(|w| w == b"AAA"));
        assert!(out.windows(3).any(|w| w == b"end"));
    }
}
