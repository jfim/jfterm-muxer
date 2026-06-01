# jftermd Core Replay Engine — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build the pure, I/O-free heart of the JFTerm terminal multiplexer: given a raw terminal byte stream, maintain a replayable, sanitized chunk ring plus sticky terminal state and shell-status cache, and produce a faithful replay on demand.

**Architecture:** A `Scanner` feeds bytes one at a time through Alacritty's `vte` parser. A `Perform` impl updates `StickyState` (SGR / DEC modes / scroll region / charset / cwd / title) and `StatusCache` (running / progress), and classifies each completed byte-span as keep-verbatim, drop-action, or consume-to-status. Visual bytes are appended to a `ChunkRing` of growable, self-contained chunks (each prefixed with a synthesized state prologue), cut at parser ground state past a soft 128 KB watermark and purged on screen clear. Replay concatenates the first selected chunk's prologue with the data of all selected chunks. Correctness is proven by a `vt100` grid-equality oracle: feeding the original stream and the replayed stream into a reference emulator must yield identical screens.

**Tech Stack:** Rust 2021, `vte` (escape-sequence parser), `vt100` (dev-dependency, test oracle). No async, no I/O — this crate is a synchronous library consumed later by the daemon runtime (Plan B).

**Spec:** `~/projects/jfterm/docs/superpowers/specs/2026-06-01-terminal-muxer-design.md` (sections: Buffer, "Ring is replay-safe by construction"). This plan covers only the core engine; PTY, sessions, and the TLV/UDS server are Plan B.

---

## File structure

```
jfterm-muxer/
  Cargo.toml
  src/
    lib.rs          # crate root; re-exports public types
    sticky.rs       # StickyState: visual state + serialize() -> prologue bytes
    status.rs       # StatusCache: running/progress + StatusSnapshot
    ring.rs         # Chunk, ChunkRing: storage, cut, purge, replay
    scanner.rs      # Scanner + Perform impl: bytes -> ring/sticky/status
    engine.rs       # ReplayEngine: facade (feed / replay / status) for Plan B
  tests/
    oracle.rs       # vt100 grid-equality replay-fidelity tests
```

Each module has one responsibility. `ring.rs` is dumb storage (it never parses). `sticky.rs` and `status.rs` are pure state holders. `scanner.rs` is the only module that knows escape-sequence semantics. `engine.rs` is the thin public API Plan B builds on.

## Workflow

Tooling is set up in Task 0: `rustfmt`, `clippy` (as `-D warnings`), and a `justfile`. **Before every commit, run `just check && just test`** (format check + clippy + tests). Use `just fmt` to autoformat. Each task below ends in a commit, so each task is gated by a green `just check && just test`.

---

## Task 0: Scaffold the crate and pin/verify the parser API

**Files:**
- Create: `Cargo.toml`
- Create: `src/lib.rs`

- [ ] **Step 1: Initialize the crate**

Run:
```bash
cd ~/projects/jfterm-muxer
cargo init --lib --name jftermd-core --vcs none
```
Expected: creates `Cargo.toml` and `src/lib.rs`.

- [ ] **Step 2: Add dependencies and pin versions**

Run:
```bash
cargo add vte
cargo add --dev vt100
```
Expected: `cargo add` reports the resolved versions and writes them into `Cargo.toml`.

- [ ] **Step 3: Verify the `vte` Perform/advance API against the installed version**

Run:
```bash
cargo doc -p vte --no-deps 2>/dev/null; cargo tree -p vte
```
Then open the rendered docs (`target/doc/vte/trait.Perform.html`) or read the source via `cargo doc --open` and confirm these signatures, which the rest of this plan assumes:
- `vte::Parser::new() -> Parser`
- `Parser::advance<P: Perform>(&mut self, performer: &mut P, bytes: &[u8])` — accepts a byte slice. (If the installed version exposes the older per-byte `advance(&mut self, performer, byte: u8)`, the scanner still feeds one byte at a time, so call it with the single byte instead of `&[b]`. Note any difference here before proceeding.)
- `Perform` methods: `print(&mut self, c: char)`, `execute(&mut self, byte: u8)`, `hook(&mut self, params: &Params, intermediates: &[u8], ignore: bool, action: char)`, `put(&mut self, byte: u8)`, `unhook(&mut self)`, `osc_dispatch(&mut self, params: &[&[u8]], bell_terminated: bool)`, `csi_dispatch(&mut self, params: &Params, intermediates: &[u8], ignore: bool, action: char)`, `esc_dispatch(&mut self, intermediates: &[u8], ignore: bool, byte: u8)`.

Record the confirmed `vte` and `vt100` versions in a comment at the top of `Cargo.toml`.

- [ ] **Step 4: Replace `src/lib.rs` with the module skeleton**

```rust
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
```

- [ ] **Step 5: Create empty module files so the crate compiles**

```bash
: > src/sticky.rs
: > src/status.rs
: > src/ring.rs
: > src/scanner.rs
: > src/engine.rs
```
Add a temporary stub to each so `cargo build` succeeds:
```rust
// src/sticky.rs (and likewise the others, for now)
```
(Empty files are valid Rust modules; no stub content is required.)

- [ ] **Step 6: Ensure rustfmt + clippy are installed and add a rustfmt config**

Run:
```bash
rustup component add rustfmt clippy
```
Create `rustfmt.toml`:
```toml
edition = "2021"
max_width = 100
```

- [ ] **Step 7: Add a justfile**

Create `justfile`:
```just
# Default: lint + test
default: check test

# Autoformat
fmt:
    cargo fmt

# Format check + clippy as hard errors (run before every commit)
check:
    cargo fmt --check
    cargo clippy --all-targets -- -D warnings

# Run the test suite
test:
    cargo test

# Debug build
build:
    cargo build
```

- [ ] **Step 8: Verify build, lint, and tests all pass**

Run:
```bash
just build
just check
just test
```
Expected: all PASS. (At this stage there are no tests yet; `just test` reports 0 tests, which is success. `just check` must be clean — empty modules produce no clippy/format errors.)

- [ ] **Step 9: Commit**

```bash
git add Cargo.toml Cargo.lock rustfmt.toml justfile src/
git commit -m "chore: scaffold jftermd-core crate with vte + vt100, rustfmt, clippy, just"
```

---

## Task 1: StatusCache

The semantic shell-status the dot consumes: running (OSC 133) and progress (OSC 9;4). Pure holder; the scanner calls the setters.

**Files:**
- Modify: `src/status.rs`

- [ ] **Step 1: Write the failing test**

```rust
// src/status.rs

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults_are_idle() {
        let c = StatusCache::new();
        let s = c.snapshot();
        assert_eq!(s.running, false);
        assert_eq!(s.progress, None);
    }

    #[test]
    fn tracks_running_and_progress() {
        let mut c = StatusCache::new();
        c.set_running(true);
        c.set_progress(Some(42));
        let s = c.snapshot();
        assert_eq!(s.running, true);
        assert_eq!(s.progress, Some(42));
    }

    #[test]
    fn progress_can_clear() {
        let mut c = StatusCache::new();
        c.set_progress(Some(10));
        c.set_progress(None);
        assert_eq!(c.snapshot().progress, None);
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test --lib status`
Expected: FAIL with "cannot find type `StatusCache`".

- [ ] **Step 3: Write the implementation**

```rust
// src/status.rs (above the tests module)

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
}

impl StatusCache {
    pub fn new() -> Self {
        Self { running: false, progress: None }
    }

    pub fn set_running(&mut self, running: bool) {
        self.running = running;
    }

    pub fn set_progress(&mut self, progress: Option<u8>) {
        self.progress = progress;
    }

    pub fn snapshot(&self) -> StatusSnapshot {
        StatusSnapshot { running: self.running, progress: self.progress }
    }
}

impl Default for StatusCache {
    fn default() -> Self {
        Self::new()
    }
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cargo test --lib status`
Expected: PASS (3 tests).

- [ ] **Step 5: Commit**

```bash
git add src/status.rs
git commit -m "feat: StatusCache for running/progress shell status"
```

---

## Task 2: StickyState

Tracks the visual state that must be re-asserted at the start of a chunk so replay from that chunk renders correctly: SGR attributes, DEC private modes, scroll region, active charset, cursor color, cwd (OSC 7), and title (OSC 0/2). `serialize()` emits a canonical byte string that re-establishes that state from a cold terminal.

YAGNI: we track the modes that matter for fidelity and that the scanner can feed it. Cursor *position* is deliberately NOT tracked (replay starts at a purge boundary; see spec).

**Files:**
- Modify: `src/sticky.rs`

- [ ] **Step 1: Write the failing test**

```rust
// src/sticky.rs

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_state_serializes_to_a_hard_reset_only() {
        let s = StickyState::new();
        // A fresh state still emits a soft reset prologue so the chunk is
        // self-contained, but asserts no specific colors/modes.
        let out = s.serialize();
        assert_eq!(out, b"\x1b[0m".to_vec());
    }

    #[test]
    fn sgr_is_reasserted() {
        let mut s = StickyState::new();
        s.set_sgr(vec![1, 31]); // bold, fg red
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
        s.set_scroll_region(Some((2, 23)));
        s.set_cwd(Some(b"file:///home/jfim".to_vec()));
        s.set_title(Some(b"vim".to_vec()));
        let out = s.serialize();
        assert_eq!(
            out,
            b"\x1b[0m\x1b[2;24r\x1b]7;file:///home/jfim\x07\x1b]2;vim\x07".to_vec()
        );
    }
}
```

Note the scroll-region test: DECSTBM is 1-based and inclusive, so an internal `(top=2, bottom=23)` (0-based rows) serializes as `\x1b[3;24r`? **No** — to avoid an off-by-one trap, this plan stores the scroll region already in the 1-based form the terminal sent, so `(2, 23)` round-trips to `\x1b[2;24r` only if stored as `(top1=2, bottom1=24)`. To keep the test unambiguous: **store the region exactly as the CSI params received** (`top` and `bottom`, 1-based), and serialize them verbatim. Adjust the test to match that contract:

```rust
    #[test]
    fn scroll_region_and_cwd_and_title() {
        let mut s = StickyState::new();
        s.set_scroll_region(Some((2, 24))); // stored as the 1-based CSI params
        s.set_cwd(Some(b"file:///home/jfim".to_vec()));
        s.set_title(Some(b"vim".to_vec()));
        let out = s.serialize();
        assert_eq!(
            out,
            b"\x1b[0m\x1b[2;24r\x1b]7;file:///home/jfim\x07\x1b]2;vim\x07".to_vec()
        );
    }
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test --lib sticky`
Expected: FAIL with "cannot find type `StickyState`".

- [ ] **Step 3: Write the implementation**

```rust
// src/sticky.rs (above the tests module)

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
}

impl StickyState {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn set_sgr(&mut self, params: Vec<u16>) {
        self.sgr = params;
    }

    pub fn set_dec_mode(&mut self, code: u16, on: bool) {
        if on {
            self.dec_modes.insert(code, true);
        } else {
            self.dec_modes.remove(&code);
        }
    }

    pub fn set_scroll_region(&mut self, region: Option<(u16, u16)>) {
        self.scroll_region = region;
    }

    pub fn set_cwd(&mut self, cwd: Option<Vec<u8>>) {
        self.cwd = cwd;
    }

    pub fn set_title(&mut self, title: Option<Vec<u8>>) {
        self.title = title;
    }

    /// Emit a byte string that re-establishes this state from a cold terminal.
    /// Always begins with a full SGR reset so the chunk is self-contained.
    pub fn serialize(&self) -> Vec<u8> {
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
        for (code, _on) in &self.dec_modes {
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
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cargo test --lib sticky`
Expected: PASS (5 tests).

- [ ] **Step 5: Commit**

```bash
git add src/sticky.rs
git commit -m "feat: StickyState with canonical prologue serialization"
```

---

## Task 3: ChunkRing

Dumb storage. A `Chunk` is `{ prologue, data }`. The ring appends visual bytes to the open chunk, seals it (`cut`) with a fresh prologue when told, drops history (`purge`) on a clear, and replays `first_selected.prologue + concat(selected.data)`. The ring never parses; cut/purge timing comes from the scanner.

**Files:**
- Modify: `src/ring.rs`

- [ ] **Step 1: Write the failing tests**

```rust
// src/ring.rs

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn replays_open_chunk_data_with_empty_prologue() {
        let mut r = ChunkRing::new(128 * 1024);
        r.append(b"hello ");
        r.append(b"world");
        assert_eq!(r.replay(usize::MAX), b"hello world".to_vec());
    }

    #[test]
    fn cut_seals_chunk_and_starts_new_with_prologue() {
        let mut r = ChunkRing::new(128 * 1024);
        r.append(b"AAA");
        r.cut(b"\x1b[0m\x1b[31m".to_vec()); // seal chunk 0, open chunk 1 w/ prologue
        r.append(b"BBB");
        // Replay all: first selected chunk is chunk 0 (empty prologue) + all data.
        assert_eq!(r.replay(usize::MAX), b"AAABBB".to_vec());
    }

    #[test]
    fn want_chunks_caps_to_most_recent_and_uses_their_prologue() {
        let mut r = ChunkRing::new(128 * 1024);
        r.append(b"AAA");
        r.cut(b"P1".to_vec()); // chunk1 prologue = "P1"
        r.append(b"BBB");
        r.cut(b"P2".to_vec()); // chunk2 prologue = "P2"
        r.append(b"CCC");
        // want_chunks=2 selects the last two chunks (prologue "P1", data BBB+CCC).
        assert_eq!(r.replay(2), b"P1BBBCCC".to_vec());
        // want_chunks=1 selects only the last chunk (prologue "P2", data CCC).
        assert_eq!(r.replay(1), b"P2CCC".to_vec());
    }

    #[test]
    fn should_cut_reports_when_open_chunk_passes_watermark() {
        let mut r = ChunkRing::new(8); // tiny watermark for the test
        assert!(!r.should_cut());
        r.append(b"1234567"); // 7 bytes < 8
        assert!(!r.should_cut());
        r.append(b"89"); // now 9 bytes >= 8
        assert!(r.should_cut());
    }

    #[test]
    fn purge_drops_prior_chunks_and_resets_open_chunk() {
        let mut r = ChunkRing::new(128 * 1024);
        r.append(b"OLD");
        r.cut(b"P1".to_vec());
        r.append(b"MORE");
        // Clear: drop everything, reset the open chunk, install a clear prologue.
        r.purge(b"\x1bc".to_vec());
        r.append(b"NEW");
        assert_eq!(r.replay(usize::MAX), b"\x1bcNEW".to_vec());
    }

    #[test]
    fn replay_more_chunks_than_exist_returns_all() {
        let mut r = ChunkRing::new(128 * 1024);
        r.append(b"X");
        r.cut(b"P1".to_vec());
        r.append(b"Y");
        assert_eq!(r.replay(999), b"XY".to_vec());
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test --lib ring`
Expected: FAIL with "cannot find type `ChunkRing`".

- [ ] **Step 3: Write the implementation**

```rust
// src/ring.rs (above the tests module)

/// A sealed-or-open unit of replayable output. `prologue` re-establishes
/// sticky state so the chunk is self-contained; `data` is the sanitized
/// visual byte stream.
#[derive(Debug, Clone)]
struct Chunk {
    prologue: Vec<u8>,
    data: Vec<u8>,
}

impl Chunk {
    fn new(prologue: Vec<u8>) -> Self {
        Self { prologue, data: Vec::new() }
    }
}

/// A ring of growable, self-contained chunks. Storage only — it does not
/// parse. The scanner decides when to `cut` (ground state past the watermark)
/// and `purge` (screen clear).
#[derive(Debug)]
pub struct ChunkRing {
    chunks: Vec<Chunk>,
    /// Soft byte target for an open chunk before the scanner should cut it.
    watermark: usize,
}

impl ChunkRing {
    /// `watermark` is the soft cut target (128 * 1024 in production).
    pub fn new(watermark: usize) -> Self {
        Self { chunks: vec![Chunk::new(Vec::new())], watermark }
    }

    /// Append sanitized visual bytes to the currently open (last) chunk.
    pub fn append(&mut self, bytes: &[u8]) {
        self.open_mut().data.extend_from_slice(bytes);
    }

    /// True once the open chunk's data has reached the soft watermark. The
    /// scanner only acts on this at parser ground state, so a single long
    /// sequence is never split — the chunk simply grows past the watermark.
    pub fn should_cut(&self) -> bool {
        self.chunks.last().map_or(false, |c| c.data.len() >= self.watermark)
    }

    /// Seal the open chunk and open a new one carrying `prologue` (the
    /// serialized sticky state at this moment).
    pub fn cut(&mut self, prologue: Vec<u8>) {
        self.chunks.push(Chunk::new(prologue));
    }

    /// Drop all history on a screen clear: discard every chunk and start
    /// fresh with a single open chunk whose prologue is the clear sequence
    /// itself (a known-good replay base).
    pub fn purge(&mut self, clear_prologue: Vec<u8>) {
        self.chunks = vec![Chunk::new(clear_prologue)];
    }

    /// Replay the most recent `want_chunks` chunks:
    /// `first_selected.prologue + concat(selected.data)`.
    pub fn replay(&self, want_chunks: usize) -> Vec<u8> {
        let n = self.chunks.len();
        let start = n.saturating_sub(want_chunks.max(1));
        let selected = &self.chunks[start..];

        let mut out = Vec::new();
        if let Some(first) = selected.first() {
            out.extend_from_slice(&first.prologue);
        }
        for chunk in selected {
            out.extend_from_slice(&chunk.data);
        }
        out
    }

    fn open_mut(&mut self) -> &mut Chunk {
        self.chunks.last_mut().expect("ring always has an open chunk")
    }
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cargo test --lib ring`
Expected: PASS (6 tests).

- [ ] **Step 5: Commit**

```bash
git add src/ring.rs
git commit -m "feat: ChunkRing storage with cut/purge/replay"
```

---

## Task 4: Scanner — bytes to ring/sticky/status with sanitization

The only semantics-aware module. It feeds bytes one at a time through `vte::Parser`. A `Perform` impl records, for each fed byte, what the parser did (`SegEvent`), and updates `StickyState`/`StatusCache`. The `Scanner` outer loop accumulates the current raw byte span, and when a span completes it: classifies it (keep-verbatim → `ring.append`; drop-action → discard; consume-to-status → already applied), then, at ground state, cuts the ring if past the watermark and purges on a clear.

**Files:**
- Modify: `src/scanner.rs`

- [ ] **Step 1: Write the failing tests**

```rust
// src/scanner.rs

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
        // The SGR is in the data stream verbatim...
        assert_eq!(s.replay(usize::MAX), b"\x1b[31mRED".to_vec());
    }

    #[test]
    fn osc52_clipboard_is_dropped_from_the_ring() {
        // ESC ] 52 ; c ; <base64> BEL  surrounded by visible text.
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
        let s = feed(b"A\x1b[6n\x1b[cB"); // DSR cursor report request + DA
        assert_eq!(s.replay(usize::MAX), b"AB".to_vec());
    }

    #[test]
    fn osc9_4_progress_updates_status_and_is_dropped() {
        let s = feed(b"X\x1b]9;4;1;42\x07Y"); // state 1, value 42
        assert_eq!(s.replay(usize::MAX), b"XY".to_vec());
        let snap = s.status();
        assert_eq!(snap.progress, Some(42));
    }

    #[test]
    fn osc133_prompt_updates_running_and_is_dropped() {
        // OSC 133 ; C = command start (running); ; D = command done (idle).
        let running = feed(b"\x1b]133;C\x07");
        assert_eq!(running.replay(usize::MAX), b"".to_vec());
        assert_eq!(running.status().running, true);

        let done = feed(b"\x1b]133;C\x07cmd output\x1b]133;D\x07");
        assert_eq!(done.replay(usize::MAX), b"cmd output".to_vec());
        assert_eq!(done.status().running, false);
    }

    #[test]
    fn osc7_cwd_is_kept_verbatim_and_tracked_into_prologue() {
        let s = feed(b"\x1b]7;file:///tmp\x07hi");
        // After a cut, the prologue must re-assert cwd. Force a cut by using a
        // tiny watermark scanner instead:
        let mut s2 = Scanner::new(4);
        s2.feed(b"\x1b]7;file:///tmp\x07");
        s2.feed(b"aaaa"); // pushes open chunk past the 4-byte watermark
        s2.feed(b"X"); // next ground state triggers a cut
        // Replaying only the last chunk must still restore cwd via the prologue.
        let out = s2.replay(1);
        assert!(out.windows(b"\x1b]7;file:///tmp\x07".len())
            .any(|w| w == b"\x1b]7;file:///tmp\x07"));
        // And the verbatim OSC 7 is present in full replay too.
        assert!(s.replay(usize::MAX).starts_with(b"\x1b]7;file:///tmp\x07"));
    }

    #[test]
    fn clear_screen_purges_history() {
        let mut s = Scanner::new(128 * 1024);
        s.feed(b"old stuff here");
        s.feed(b"\x1b[2J"); // ED 2: clear entire screen
        s.feed(b"new");
        // History before the clear is gone; replay starts at the clear.
        let out = s.replay(usize::MAX);
        assert!(!out.windows(3).any(|w| w == b"old"));
        assert!(out.ends_with(b"new"));
    }

    #[test]
    fn long_single_sequence_is_not_split_across_the_watermark() {
        // A huge OSC 52 payload exceeds the watermark but must be dropped as a
        // single unit (and never appear), proving the cut waits for ground state.
        let mut payload = b"\x1b]52;c;".to_vec();
        payload.extend(std::iter::repeat(b'A').take(64));
        payload.push(0x07);
        let mut s = Scanner::new(8); // tiny watermark
        s.feed(b"start");
        s.feed(&payload);
        s.feed(b"end");
        let out = s.replay(usize::MAX);
        assert!(!out.windows(3).any(|w| w == b"AAA")); // payload never stored
        assert!(out.windows(3).any(|w| w == b"end"));
    }
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cargo test --lib scanner`
Expected: FAIL with "cannot find type `Scanner`".

- [ ] **Step 3: Write the implementation**

```rust
// src/scanner.rs (above the tests module)

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
    /// A control/sequence reached ground state and should be kept verbatim
    /// (e.g. CR, LF, TAB, SGR, OSC 7, title, scroll region, cursor moves).
    KeepSeq,
    /// A sequence reached ground state and must be dropped from the ring
    /// (action with side effects, or status already consumed).
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
        self.event = if byte == 0x07 { SegEvent::DropSeq } else { SegEvent::KeepSeq };
    }

    fn hook(&mut self, _params: &Params, _intermediates: &[u8], _ignore: bool, _action: char) {
        // DCS start (e.g. Sixel). Mid-sequence until unhook.
        self.event = SegEvent::None;
    }

    fn put(&mut self, _byte: u8) {
        self.event = SegEvent::None;
    }

    fn unhook(&mut self) {
        // DCS complete. Keep verbatim (harmless if VTE ignores it).
        self.event = SegEvent::KeepSeq;
    }

    fn osc_dispatch(&mut self, params: &[&[u8]], _bell_terminated: bool) {
        let code = params.first().and_then(|p| std::str::from_utf8(p).ok());
        match code {
            Some("0") | Some("1") | Some("2") => {
                // Window/icon title. Track for the prologue; keep verbatim.
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
                // OSC 9;4 = progress (consume to status). Bare OSC 9 =
                // desktop notification (drop). Either way, drop from ring.
                if params.get(1).map(|p| *p == b"4").unwrap_or(false) {
                    // params: ["9", "4", <state>, <value?>]
                    let state = params.get(2).and_then(|p| std::str::from_utf8(p).ok());
                    let value = params
                        .get(3)
                        .and_then(|p| std::str::from_utf8(p).ok())
                        .and_then(|s| s.parse::<u8>().ok());
                    match state {
                        Some("0") => self.status.set_progress(None), // hidden
                        _ => self.status.set_progress(value),
                    }
                }
                self.event = SegEvent::DropSeq;
            }
            Some("133") => {
                // Shell-integration prompt marks. C = command running,
                // D = command finished. Consume to status; drop from ring.
                match params.get(1).and_then(|p| p.first()) {
                    Some(b'C') => self.status.set_running(true),
                    Some(b'D') => self.status.set_running(false),
                    _ => {}
                }
                self.event = SegEvent::DropSeq;
            }
            Some("52") | Some("777") => {
                // Clipboard / extended notifications. Action — drop.
                self.event = SegEvent::DropSeq;
            }
            _ => {
                // Unknown OSC: keep verbatim (conservative for fidelity).
                self.event = SegEvent::KeepSeq;
            }
        }
    }

    fn csi_dispatch(&mut self, params: &Params, intermediates: &[u8], _ignore: bool, action: char) {
        let is_private = intermediates.first() == Some(&b'?');
        match action {
            // Device Status Report / cursor position report request: a query
            // that would make the emulator answer up the input stream. Drop.
            'n' => {
                self.event = SegEvent::DropSeq;
                return;
            }
            // Primary/secondary Device Attributes query. Drop.
            'c' => {
                self.event = SegEvent::DropSeq;
                return;
            }
            // SGR — track current rendition for the prologue.
            'm' => {
                let mut flat = Vec::new();
                for sub in params.iter() {
                    for v in sub {
                        flat.push(*v);
                    }
                }
                if flat.is_empty() || flat == [0] {
                    self.sticky.set_sgr(Vec::new());
                } else {
                    self.sticky.set_sgr(flat);
                }
                self.event = SegEvent::KeepSeq;
                return;
            }
            // DECSTBM scroll region.
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
                return;
            }
            // DEC private mode set/reset.
            'h' | 'l' if is_private => {
                let on = action == 'h';
                for sub in params.iter() {
                    if let Some(code) = sub.first() {
                        self.sticky.set_dec_mode(*code, on);
                    }
                }
                self.event = SegEvent::KeepSeq;
                return;
            }
            // Erase in Display with parameter 2 or 3 = full clear -> purge.
            'J' => {
                let mode = params.iter().next().and_then(|s| s.first().copied()).unwrap_or(0);
                self.event = if mode == 2 || mode == 3 {
                    SegEvent::ClearSeq
                } else {
                    SegEvent::KeepSeq
                };
                return;
            }
            _ => {
                self.event = SegEvent::KeepSeq;
            }
        }
    }

    fn esc_dispatch(&mut self, _intermediates: &[u8], _ignore: bool, byte: u8) {
        // ESC c = RIS (full reset) -> treat as a clear/purge base.
        self.event = if byte == b'c' { SegEvent::ClearSeq } else { SegEvent::KeepSeq };
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
            // (If the installed vte exposes per-byte `advance`, call
            // `self.parser.advance(&mut self.sink, b)` instead.)
            self.parser.advance(&mut self.sink, &[b]);
            match self.sink.take_event() {
                SegEvent::None => { /* still mid-sequence; keep accumulating */ }
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
                    // Use the clear sequence itself as the new chunk's prologue
                    // base, then re-assert sticky state on top of it.
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
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cargo test --lib scanner`
Expected: PASS (all scanner tests). If a test about OSC parameter splitting fails, re-check against the installed `vte`'s `osc_dispatch` parameter slicing (Task 0, Step 3) — some versions include the numeric code as `params[0]` exactly as assumed here; adjust indices if it differs, and note the difference in a code comment.

- [ ] **Step 5: Commit**

```bash
git add src/scanner.rs
git commit -m "feat: Scanner with sanitizing classification and sticky/status tracking"
```

---

## Task 5: ReplayEngine facade

The thin public API Plan B's session layer will hold. Wraps `Scanner` and exposes exactly `feed` / `replay` / `status`, plus a production constructor with the 128 KB watermark.

**Files:**
- Modify: `src/engine.rs`

- [ ] **Step 1: Write the failing test**

```rust
// src/engine.rs

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
        assert_eq!(s.running, true);
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
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test --lib engine`
Expected: FAIL with "cannot find type `ReplayEngine`".

- [ ] **Step 3: Write the implementation**

```rust
// src/engine.rs (above the tests module)

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
        Self { scanner: Scanner::new(WATERMARK) }
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
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cargo test --lib engine`
Expected: PASS (2 tests).

- [ ] **Step 5: Commit**

```bash
git add src/engine.rs
git commit -m "feat: ReplayEngine public facade"
```

---

## Task 6: vt100 grid-equality replay oracle

The capstone correctness test. For a battery of byte streams, feed the **original** into one `vt100::Parser` and the **replay** into a fresh one, and assert the rendered screens are identical. This proves the state prologue plus sanitization reproduce the exact visible screen — the central risk of the whole buffer design. Sanitized-away actions (clipboard, bells, queries) don't affect the rendered grid, so equality holds.

**Files:**
- Create: `tests/oracle.rs`

- [ ] **Step 1: Confirm the vt100 API**

Open `target/doc/vt100/struct.Parser.html` (from `cargo doc -p vt100 --no-deps`) and confirm:
- `vt100::Parser::new(rows: u16, cols: u16, scrollback: usize) -> Parser`
- `Parser::process(&mut self, bytes: &[u8])`
- `Parser::screen(&self) -> &vt100::Screen`
- `Screen::contents_formatted(&self) -> Vec<u8>` and `Screen::cursor_position(&self) -> (u16, u16)`

If method names differ in the installed version, adjust the helper below and note it in a comment.

- [ ] **Step 2: Write the failing oracle test**

```rust
// tests/oracle.rs

use jftermd_core::scanner_test_support::Scanner; // re-export added in Step 4

/// Render a byte stream into a fixed-size vt100 screen and return a stable
/// representation (formatted contents + cursor) for equality comparison.
fn render(bytes: &[u8]) -> (Vec<u8>, (u16, u16)) {
    let mut p = vt100::Parser::new(24, 80, 0);
    p.process(bytes);
    (p.screen().contents_formatted(), p.screen().cursor_position())
}

/// Assert that replaying `input` reproduces the same screen as `input` itself.
fn assert_replay_faithful(input: &[u8]) {
    let mut s = Scanner::new(128 * 1024);
    s.feed(input);
    let replay = s.replay(usize::MAX);

    let original = render(input);
    let replayed = render(&replay);
    assert_eq!(
        original, replayed,
        "replay diverged from original\ninput={:?}\nreplay={:?}",
        String::from_utf8_lossy(input),
        String::from_utf8_lossy(&replay),
    );
}

#[test]
fn plain_text() {
    assert_replay_faithful(b"the quick brown fox\r\njumps over\r\n");
}

#[test]
fn colored_and_styled_text() {
    assert_replay_faithful(b"\x1b[1;31mred bold\x1b[0m normal \x1b[4munder\x1b[0m");
}

#[test]
fn cursor_movement_and_overwrite() {
    assert_replay_faithful(b"hello\x1b[1;1Hxxxxx\x1b[2;1Hworld");
}

#[test]
fn scroll_region_and_newlines() {
    assert_replay_faithful(b"\x1b[2;23r\x1b[5;1Hline\r\nanother\r\nthird\r\n");
}

#[test]
fn clipboard_write_does_not_affect_screen() {
    // OSC 52 is stripped from replay but never affected the grid anyway.
    assert_replay_faithful(b"before\x1b]52;c;c2VjcmV0\x07after");
}

#[test]
fn bell_and_queries_do_not_affect_screen() {
    assert_replay_faithful(b"a\x07b\x1b[6nc\x1b[cd");
}

#[test]
fn title_and_cwd_then_text() {
    assert_replay_faithful(b"\x1b]2;mytitle\x07\x1b]7;file:///tmp\x07content here");
}

#[test]
fn clear_then_redraw() {
    assert_replay_faithful(b"garbage to be cleared\x1b[2J\x1b[Hfresh screen content");
}

#[test]
fn capped_replay_keeps_bottom_screen_faithful() {
    // With a tiny watermark, force many chunks; a capped replay of the last
    // chunk must still render the most recent screen state correctly because
    // the prologue re-asserts modes. We compare only the final cursor + the
    // last line's contents, which the bottom chunk fully determines.
    let mut s = Scanner::new(16);
    let input = b"\x1b[32mAAAAAAAA\r\nBBBBBBBB\r\nCCCCCCCC\r\n\x1b[0mDDDD";
    s.feed(input);
    let capped = s.replay(1);

    let mut p = vt100::Parser::new(24, 80, 0);
    p.process(&capped);
    // The capped replay must at least render the final visible line.
    let contents = p.screen().contents();
    assert!(contents.contains("DDDD"), "capped replay lost the final line: {contents:?}");
}
```

- [ ] **Step 3: Run the oracle test to verify it fails to compile**

Run: `cargo test --test oracle`
Expected: FAIL — `scanner_test_support` is not yet exported.

- [ ] **Step 4: Export the Scanner for integration tests**

Add to `src/lib.rs`:
```rust
/// Test-support re-export so the `vt100` oracle (an integration test, which
/// can only see the public API) can drive the scanner directly.
pub mod scanner_test_support {
    pub use crate::scanner::Scanner;
}
```
And make `Scanner` public if it is not already — confirm `pub struct Scanner` in `src/scanner.rs` (it is, per Task 4).

- [ ] **Step 5: Run the oracle test to verify it passes**

Run: `cargo test --test oracle`
Expected: PASS (9 tests). If `colored_and_styled_text` or `scroll_region_and_newlines` diverge, the gap is almost always a missing sticky-state field in the prologue (Task 2) or a misclassified CSI in the scanner (Task 4) — fix there, not in the oracle. The oracle is the spec; treat divergence as a real bug in the engine.

- [ ] **Step 6: Run the full suite and commit**

Run: `cargo test`
Expected: PASS (all unit + oracle tests).

```bash
git add src/lib.rs tests/oracle.rs
git commit -m "test: vt100 grid-equality replay-fidelity oracle"
```

---

## Self-review notes (for the implementer)

- **Spec coverage:** chunk ring with soft-watermark ground-state cut (Task 3 `should_cut` + Task 4 `maybe_cut`), per-chunk synthesized prologue (Task 2 + Task 4 cut), purge-on-clear (Task 4 `ClearSeq`), replay = first prologue + concat data (Task 3 `replay`), replay-safe sanitization of OSC 52 / 9 / 777 / BEL / DSR / DA (Task 4), status cache for running/progress (Task 1, wired in Task 4), `want_chunks` cap (Task 3 + Task 5 `replay_capped`), and the vt100 fidelity oracle (Task 6). The TLV protocol, PTY, sessions, and UDS server are intentionally out of scope (Plan B).
- **Known API risk:** `vte` and `vt100` signatures are verified in Task 0 Step 3 and Task 6 Step 1. If they differ, adjust at those points before building on them — do not guess.
- **Cursor position is intentionally untracked** in `StickyState`; replay starts at a purge boundary (full `replay()`), and the capped-replay test only asserts the bottom-screen invariant the spec guarantees.

---

## Execution Handoff

Plan complete. After this core lands and `cargo test` is green, Plan B (daemon runtime: PTY/`forkpty`, session lifecycle, tokio UDS server with the TLV protocol, registry, and self-spawn/daemonization) builds directly on `ReplayEngine`.
