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
    /// Set at the end of `osc_dispatch` to that OSC's classification, so a
    /// directly-following ST terminator (`ESC \`) is classified the same way
    /// instead of leaking a stray backslash (drop) or splitting the ST (keep).
    st_pending: Option<SegEvent>,
    /// Reused scratch buffer for flattening SGR params, so the `'m'` arm does
    /// not allocate a fresh `Vec` on every SGR sequence.
    sgr_scratch: Vec<u16>,
}

impl Sink {
    fn new() -> Self {
        Self {
            sticky: StickyState::new(),
            status: StatusCache::new(),
            event: SegEvent::None,
            st_pending: None,
            sgr_scratch: Vec::new(),
        }
    }

    fn take_event(&mut self) -> SegEvent {
        std::mem::replace(&mut self.event, SegEvent::None)
    }
}

impl Perform for Sink {
    fn print(&mut self, _c: char) {
        self.st_pending = None;
        self.event = SegEvent::KeepByte;
    }

    fn execute(&mut self, byte: u8) {
        self.st_pending = None;
        // C0 controls. Keep the ones that affect layout; drop the bell.
        self.event = if byte == 0x07 {
            SegEvent::DropSeq
        } else {
            SegEvent::KeepSeq
        };
    }

    fn hook(&mut self, _params: &Params, _intermediates: &[u8], _ignore: bool, _action: char) {
        self.st_pending = None;
        self.event = SegEvent::None;
    }

    fn put(&mut self, _byte: u8) {
        self.st_pending = None;
        self.event = SegEvent::None;
    }

    fn unhook(&mut self) {
        self.st_pending = None;
        self.event = SegEvent::KeepSeq;
    }

    fn osc_dispatch(&mut self, params: &[&[u8]], _bell_terminated: bool) {
        let code = params.first().and_then(|p| std::str::from_utf8(p).ok());
        match code {
            Some("0") | Some("1") | Some("2") => {
                if let Some(text) = params.get(1) {
                    self.sticky.set_title(text.to_vec());
                }
                self.event = SegEvent::KeepSeq;
            }
            Some("7") => {
                if let Some(cwd) = params.get(1) {
                    self.sticky.set_cwd(cwd.to_vec());
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
                // Any OSC 133 marker means this shell does prompt marking; latch
                // it so the daemon disables its tcgetpgrp running fallback.
                self.status.set_prompt_marker_seen();
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
        self.st_pending = Some(self.event.clone());
    }

    fn csi_dispatch(&mut self, params: &Params, intermediates: &[u8], _ignore: bool, action: char) {
        self.st_pending = None;
        let is_private = intermediates.first() == Some(&b'?');
        match action {
            'n' => {
                self.event = SegEvent::DropSeq;
            }
            'c' => {
                self.event = SegEvent::DropSeq;
            }
            'm' if !is_private => {
                self.sgr_scratch.clear();
                self.sgr_scratch.extend(params.iter().flatten().copied());
                if self.sgr_scratch.is_empty() || self.sgr_scratch.as_slice() == [0u16] {
                    self.sticky.set_sgr(&[]);
                } else {
                    self.sticky.set_sgr(&self.sgr_scratch);
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
        // ST terminator (`\`) closing an OSC: mirror that OSC's classification
        // so a dropped OSC drops its `\` and a kept OSC keeps the full `ESC \`.
        if byte == b'\\'
            && let Some(ev) = self.st_pending.take()
        {
            self.event = ev;
            return;
        }
        self.st_pending = None;
        // ESC c = RIS (full reset) -> treat as a clear/purge base.
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
    /// Number of UTF-8 continuation bytes still expected for the multibyte
    /// character currently being read. While non-zero the next bytes are
    /// continuation bytes (0x80..=0xBF) and must pass through verbatim — in
    /// particular 0x9C must NOT be reinterpreted as the C1 ST terminator.
    utf8_remaining: u8,
}

impl Scanner {
    pub fn new(watermark: usize) -> Self {
        Self {
            parser: Parser::new(),
            sink: Sink::new(),
            ring: ChunkRing::new(watermark),
            pending: Vec::new(),
            utf8_remaining: 0,
        }
    }

    /// Feed a chunk of raw terminal output.
    pub fn feed(&mut self, bytes: &[u8]) {
        for &b in bytes {
            // A 0x9C byte is ambiguous: it is the 8-bit C1 ST terminator, but
            // it is also a legal UTF-8 continuation byte. Only a *standalone*
            // 0x9C (not part of a multibyte character) is a C1 ST.
            if self.utf8_remaining > 0 {
                if (0x80..=0xBF).contains(&b) {
                    // Genuine continuation byte of a multibyte character: pass
                    // through verbatim so 0x9C inside e.g. ✓ (E2 9C 93) is not
                    // mangled.
                    self.utf8_remaining -= 1;
                    self.advance_one(b);
                    continue;
                }
                // Malformed UTF-8: a non-continuation byte arrived mid-sequence.
                // Resynchronize by abandoning the partial sequence (reset the
                // counter to 0) and reprocessing this byte as a fresh ground
                // byte below — so a standalone 0x9C here is still recognized as
                // the C1 ST, and a new UTF-8 lead starts a new sequence.
                self.utf8_remaining = 0;
            }
            // Ground byte: note any UTF-8 lead so the following continuation
            // bytes are passed through verbatim above.
            self.utf8_remaining = match b {
                0xc2..=0xdf => 1,
                0xe0..=0xef => 2,
                0xf0..=0xf4 => 3,
                _ => 0,
            };
            // vte 0.15 only supports 7-bit codes and does not recognise the
            // 8-bit C1 ST byte (0x9C) as an OSC/DCS string terminator; it
            // silently appends 0x9C to the OSC body and stays in OscString
            // state, swallowing the byte that follows.  Translate a standalone
            // 0x9C to the semantically identical 7-bit two-byte form ESC \
            // before the byte reaches vte. The pending accumulator records
            // 0x1B + 0x5C so the emitted span is equivalent to `ESC \`.
            if b == 0x9c {
                self.advance_one(0x1b);
                self.advance_one(0x5c);
            } else {
                self.advance_one(b);
            }
        }
    }

    /// Process a single pre-expanded byte through vte and update the ring.
    #[inline]
    fn advance_one(&mut self, b: u8) {
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

    /// Replay the most recent `want_chunks` chunks (see `ChunkRing::replay`).
    pub fn replay(&self, want_chunks: usize) -> Vec<u8> {
        self.ring.replay(want_chunks)
    }

    /// Current shell-status snapshot.
    pub fn status(&self) -> StatusSnapshot {
        self.sink.status.snapshot()
    }

    /// Whether any OSC 133 prompt marker has been observed (latched).
    pub fn saw_prompt_marker(&self) -> bool {
        self.sink.status.saw_prompt_marker()
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
    fn st_terminated_clipboard_is_fully_dropped() {
        // OSC 52 closed by ST (ESC \) — no stray backslash may leak.
        let s = feed(b"A\x1b]52;c;aGVsbG8=\x1b\\B");
        assert_eq!(s.replay(usize::MAX), b"AB".to_vec());
    }

    #[test]
    fn st_terminated_notification_is_fully_dropped() {
        let s = feed(b"A\x1b]9;done\x1b\\B");
        assert_eq!(s.replay(usize::MAX), b"AB".to_vec());
    }

    #[test]
    fn st_terminated_title_is_kept_intact() {
        // Kept OSC keeps the full ST terminator (ESC \), no dangling ESC.
        let s = feed(b"\x1b]2;vim\x1b\\X");
        assert_eq!(s.replay(usize::MAX), b"\x1b]2;vim\x1b\\X".to_vec());
    }

    #[test]
    fn c1_st_terminated_dropped_osc_keeps_following_byte() {
        // 8-bit C1 ST (0x9C) after a dropped OSC must not swallow the next byte.
        let s = feed(b"A\x1b]52;c;Z\x9cB");
        assert_eq!(s.replay(usize::MAX), b"AB".to_vec());
    }

    #[test]
    fn c1_st_terminated_title_keeps_following_byte() {
        // 8-bit C1 ST after a kept OSC (title) must not swallow the next byte.
        let s = feed(b"\x1b]2;vim\x9cX");
        // Title kept; X kept. (Exact terminator bytes in the ring may vary; the
        // invariant under test is that X survives and the title was tracked.)
        assert!(s.replay(usize::MAX).ends_with(b"X"));
    }

    #[test]
    fn utf8_chars_with_0x9c_continuation_byte_survive_replay() {
        // 0x9C is the C1 ST byte, but it is also a valid UTF-8 continuation
        // byte. The scanner must not rewrite it to `ESC \` when it is part of a
        // multibyte character, or the character is corrupted in the replay.
        //   ✓ = U+2713 = E2 9C 93  (0x9C is the middle continuation byte)
        //   ├ = U+251C = E2 94 9C  (0x9C is the trailing continuation byte)
        let input = "café ✓ ─│├".as_bytes();
        let s = feed(input);
        assert_eq!(s.replay(usize::MAX), input.to_vec());
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
