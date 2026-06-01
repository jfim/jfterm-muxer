# jftermd Daemon B1 — Protocol Codec + PTY + Session Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build the testable, socket-free foundation of the `jftermd` daemon: a TLV wire-protocol codec, a synchronous `forkpty` PTY wrapper, and a `Session` that binds a PTY to the existing `ReplayEngine`.

**Architecture:** Restructure the repo into a cargo workspace — `core/` keeps the pure, async-free `jftermd-core` replay engine; a new `jftermd/` crate holds the daemon runtime. B1 ships three modules in `jftermd/`: `protocol` (pure TLV framing + JSON control payloads), `pty` (sync `nix::pty::forkpty` wrapper — write/drain/resize/reap), and `session` (`Pty` + `ReplayEngine` + lifecycle + attach-replay producer). No sockets, no tokio — that is B2.

**Tech Stack:** Rust 1.96, edition 2024, cargo workspace. New deps: `nix` (forkpty/ioctl/signals/wait), `libc` (TIOCSWINSZ), `serde` + `serde_json` (control-frame payloads). Existing: `vte` (core), `vt100` (core test oracle).

---

## Environment (read before every task)

- `cargo`, `rustc`, `just` live in `~/.cargo/bin` and are **NOT on the default PATH**. Shell state does not persist between tool calls, so prefix **every** cargo/just invocation with `source "$HOME/.cargo/env" &&`.
- Every commit is gated on a green `just check && just test`. `just check` = `cargo fmt --check` + `cargo clippy --all-targets -- -D warnings` (clippy warnings are hard errors). `just test` = `cargo test` (whole workspace).
- Expect clippy to demand idiomatic rewrites (`is_some_and`, `assert!(x)` over `assert_eq!(x, true)`, multi-line struct literals from rustfmt, `#[derive(Default)]` or a `Default` impl whenever you write a `new()` with no args). Apply them — they are behaviour-preserving.
- **Crate-API drift on `nix`:** this plan targets `nix` 0.29.x. `cargo add` may pin a different minor. If a signature differs (notably: `forkpty`'s return-struct field names; whether `read`/`write`/`fcntl` take `impl AsFd` vs a `RawFd`; `waitpid`/`killpg` arg types), **adjust the call to match the pinned version — the logic is unchanged.** Record the exact pinned version as a comment in `jftermd/Cargo.toml`.
- Editor "unresolved import / file not found for module" diagnostics often lag behind subagent edits. Trust `cargo build` / `just check`, not stale diagnostics.

---

## File structure (end state of B1)

```
jfterm-muxer/
  Cargo.toml            # [workspace] manifest (members = core, jftermd)
  rustfmt.toml          # unchanged, workspace-wide
  justfile              # unchanged (cargo commands act on the workspace)
  core/
    Cargo.toml          # package jftermd-core (moved, unchanged content)
    src/                # moved verbatim from ./src
    tests/oracle.rs     # moved verbatim from ./tests
  jftermd/
    Cargo.toml          # package jftermd (lib only in B1; bin lands in B2)
    src/
      lib.rs            # pub mod protocol; pub mod pty; pub mod session;
      protocol.rs       # TLV framing + FrameDecoder + serde JSON payloads
      pty.rs            # sync forkpty wrapper
      session.rs        # Session: Pty + ReplayEngine + lifecycle + attach replay
```

Each module is registered in `lib.rs` by the task that creates it. `core/`'s contents are moved with `git mv` to preserve history; its package name stays `jftermd-core`, so `core/tests/oracle.rs`'s `use jftermd_core::…` keeps working.

---

## Task 0: Restructure into a cargo workspace

**Files:**
- Create dir: `core/`, move `src/` → `core/src/`, `tests/` → `core/tests/`
- Create: `core/Cargo.toml`
- Modify: `Cargo.toml` (root → workspace manifest)
- Create: `jftermd/Cargo.toml`, `jftermd/src/lib.rs`

- [ ] **Step 1: Move the core crate into `core/` (preserve history)**

```bash
source "$HOME/.cargo/env"
mkdir -p core
git mv src core/src
git mv tests core/tests
```

- [ ] **Step 2: Create `core/Cargo.toml`**

```toml
# Pinned dependency versions (verified 2026-06-01):
#   vte  = 0.15.0  — Parser::new() -> Parser, advance(&mut self, performer: &mut P, bytes: &[u8])
#   vt100 = 0.16.2  — dev dependency for integration tests
[package]
name = "jftermd-core"
version = "0.1.0"
edition = "2024"

[dependencies]
vte = "0.15.0"

[dev-dependencies]
vt100 = "0.16.2"
```

- [ ] **Step 3: Replace root `Cargo.toml` with a workspace manifest**

```toml
[workspace]
resolver = "3"
members = ["core", "jftermd"]
```

- [ ] **Step 4: Create the `jftermd` crate stub**

`jftermd/Cargo.toml`:

```toml
[package]
name = "jftermd"
version = "0.1.0"
edition = "2024"

[dependencies]
jftermd-core = { path = "../core" }
```

`jftermd/src/lib.rs`:

```rust
//! jftermd daemon runtime (Plan B).
//!
//! B1 lands the socket-free foundation: the wire protocol codec, the PTY
//! wrapper, and the Session that binds a PTY to the replay engine. The tokio
//! UDS server and daemonization land in B2.
```

- [ ] **Step 5: Verify the workspace builds and core tests still pass**

Run: `source "$HOME/.cargo/env" && just check && just test`
Expected: PASS — all pre-existing core tests (41) green, clippy/fmt clean. The `jftermd` lib compiles as an empty crate.

- [ ] **Step 6: Commit**

```bash
source "$HOME/.cargo/env"
git add Cargo.toml Cargo.lock core jftermd
git commit -m "refactor: restructure into cargo workspace (core + jftermd)"
```

---

## Task 1: TLV frame codec — `FrameType`, `Frame`, encode

**Files:**
- Create: `jftermd/src/protocol.rs`
- Modify: `jftermd/src/lib.rs`

- [ ] **Step 1: Register the module**

Add to `jftermd/src/lib.rs` (after the doc comment):

```rust
pub mod protocol;
```

- [ ] **Step 2: Write the failing test**

Create `jftermd/src/protocol.rs`:

```rust
//! TLV wire protocol: `[u8 type][u32 len big-endian][value]`.
//!
//! Hot frames (`Data`/`Input`) carry raw terminal bytes; control frames carry
//! a small JSON value. Pure and I/O-free — the socket layer (B2) owns transport.

/// Protocol version carried in `Hello`; a mismatch is rejected by the server.
pub const PROTO_VERSION: u32 = 1;

/// Hard cap on a single frame's value length. Guards against a malformed or
/// hostile length header forcing a giant allocation.
pub const MAX_FRAME_LEN: u32 = 16 * 1024 * 1024;

/// Every wire message is one of these. Discriminants are the on-wire type byte.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum FrameType {
    // Control connection
    Hello = 1,
    HelloOk = 2,
    List = 3,
    Sessions = 4,
    // Session connection (client -> daemon)
    AttachOrOpen = 5,
    Input = 6,
    Resize = 7,
    Close = 8,
    // Session connection (daemon -> client)
    Data = 9,
    Status = 10,
    Exit = 11,
}

impl FrameType {
    fn from_u8(b: u8) -> Option<Self> {
        Some(match b {
            1 => Self::Hello,
            2 => Self::HelloOk,
            3 => Self::List,
            4 => Self::Sessions,
            5 => Self::AttachOrOpen,
            6 => Self::Input,
            7 => Self::Resize,
            8 => Self::Close,
            9 => Self::Data,
            10 => Self::Status,
            11 => Self::Exit,
            _ => return None,
        })
    }
}

/// One decoded frame: a type and its raw value bytes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Frame {
    pub ty: FrameType,
    pub payload: Vec<u8>,
}

impl Frame {
    /// Build a frame from a type and raw value bytes.
    pub fn new(ty: FrameType, payload: Vec<u8>) -> Self {
        Self { ty, payload }
    }

    /// Serialize to the wire: `[type][len:u32 BE][value]`.
    pub fn encode(&self) -> Vec<u8> {
        let len = self.payload.len();
        let mut out = Vec::with_capacity(5 + len);
        out.push(self.ty as u8);
        out.extend_from_slice(&(len as u32).to_be_bytes());
        out.extend_from_slice(&self.payload);
        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn encode_data_frame_layout() {
        let f = Frame::new(FrameType::Data, b"hi".to_vec());
        // type=9, len=2 big-endian, then payload
        assert_eq!(f.encode(), vec![9, 0, 0, 0, 2, b'h', b'i']);
    }

    #[test]
    fn encode_empty_payload() {
        let f = Frame::new(FrameType::List, Vec::new());
        assert_eq!(f.encode(), vec![3, 0, 0, 0, 0]);
    }
}
```

- [ ] **Step 3: Run the tests — verify they pass**

Run: `source "$HOME/.cargo/env" && cargo test -p jftermd protocol::`
Expected: PASS (`encode_data_frame_layout`, `encode_empty_payload`).

- [ ] **Step 4: Commit**

```bash
source "$HOME/.cargo/env"
git add jftermd/src/protocol.rs jftermd/src/lib.rs
git commit -m "feat(protocol): FrameType + TLV frame encode"
```

---

## Task 2: Streaming frame decoder (partial-frame safe)

**Files:**
- Modify: `jftermd/src/protocol.rs`

- [ ] **Step 1: Write the failing tests**

Append to `jftermd/src/protocol.rs` (above the `#[cfg(test)]` module add the impl; here is the impl plus new tests). First add the error type and decoder **before** the test module:

```rust
/// Errors from decoding a frame stream. Either closes the connection (B2).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ProtocolError {
    /// Type byte did not map to a known `FrameType`.
    UnknownType(u8),
    /// Declared value length exceeded `MAX_FRAME_LEN`.
    FrameTooLarge(u32),
}

impl std::fmt::Display for ProtocolError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::UnknownType(b) => write!(f, "unknown frame type byte {b}"),
            Self::FrameTooLarge(n) => write!(f, "frame length {n} exceeds cap"),
        }
    }
}

impl std::error::Error for ProtocolError {}

/// Accumulates bytes off a socket and yields whole `Frame`s. Handles frames
/// split across reads and multiple frames in one read.
#[derive(Debug, Default)]
pub struct FrameDecoder {
    buf: Vec<u8>,
}

impl FrameDecoder {
    pub fn new() -> Self {
        Self::default()
    }

    /// Append freshly-read bytes to the internal buffer.
    pub fn push(&mut self, bytes: &[u8]) {
        self.buf.extend_from_slice(bytes);
    }

    /// Pop the next complete frame, or `Ok(None)` if more bytes are needed.
    /// A malformed type byte or oversized length is a hard `Err`.
    pub fn next_frame(&mut self) -> Result<Option<Frame>, ProtocolError> {
        if self.buf.len() < 5 {
            return Ok(None);
        }
        let ty_byte = self.buf[0];
        let len = u32::from_be_bytes([self.buf[1], self.buf[2], self.buf[3], self.buf[4]]);
        if len > MAX_FRAME_LEN {
            return Err(ProtocolError::FrameTooLarge(len));
        }
        let ty = FrameType::from_u8(ty_byte).ok_or(ProtocolError::UnknownType(ty_byte))?;
        let total = 5 + len as usize;
        if self.buf.len() < total {
            return Ok(None);
        }
        let payload = self.buf[5..total].to_vec();
        self.buf.drain(..total);
        Ok(Some(Frame::new(ty, payload)))
    }
}
```

Then add these tests inside the existing `#[cfg(test)] mod tests`:

```rust
    #[test]
    fn decode_round_trip() {
        let f = Frame::new(FrameType::Data, b"hello".to_vec());
        let mut d = FrameDecoder::new();
        d.push(&f.encode());
        assert_eq!(d.next_frame().unwrap(), Some(f));
        assert_eq!(d.next_frame().unwrap(), None);
    }

    #[test]
    fn decode_partial_then_complete() {
        let wire = Frame::new(FrameType::Input, b"abc".to_vec()).encode();
        let mut d = FrameDecoder::new();
        d.push(&wire[..3]); // header not even complete
        assert_eq!(d.next_frame().unwrap(), None);
        d.push(&wire[3..]);
        assert_eq!(
            d.next_frame().unwrap(),
            Some(Frame::new(FrameType::Input, b"abc".to_vec()))
        );
    }

    #[test]
    fn decode_two_frames_in_one_push() {
        let mut wire = Frame::new(FrameType::Status, b"x".to_vec()).encode();
        wire.extend(Frame::new(FrameType::Exit, b"y".to_vec()).encode());
        let mut d = FrameDecoder::new();
        d.push(&wire);
        assert_eq!(d.next_frame().unwrap().unwrap().ty, FrameType::Status);
        assert_eq!(d.next_frame().unwrap().unwrap().ty, FrameType::Exit);
        assert_eq!(d.next_frame().unwrap(), None);
    }

    #[test]
    fn decode_unknown_type_errors() {
        let mut d = FrameDecoder::new();
        d.push(&[200, 0, 0, 0, 0]);
        assert_eq!(d.next_frame(), Err(ProtocolError::UnknownType(200)));
    }

    #[test]
    fn decode_oversized_length_errors_without_alloc() {
        let mut d = FrameDecoder::new();
        d.push(&[9, 0xFF, 0xFF, 0xFF, 0xFF]); // len ~4 GiB
        assert_eq!(d.next_frame(), Err(ProtocolError::FrameTooLarge(u32::MAX)));
    }
```

- [ ] **Step 2: Run the tests — verify they pass**

Run: `source "$HOME/.cargo/env" && cargo test -p jftermd protocol::`
Expected: PASS (all encode + decode tests).

- [ ] **Step 3: Commit**

```bash
source "$HOME/.cargo/env"
git add jftermd/src/protocol.rs
git commit -m "feat(protocol): streaming FrameDecoder with partial-frame + bounds handling"
```

---

## Task 3: JSON control payloads (serde structs + frame helpers)

**Files:**
- Modify: `jftermd/Cargo.toml`, `jftermd/src/protocol.rs`

- [ ] **Step 1: Add serde deps (pin versions)**

```bash
source "$HOME/.cargo/env"
cd jftermd
cargo add serde --features derive
cargo add serde_json
cd ..
```

Expected: `jftermd/Cargo.toml` now lists `serde` (with `derive`) and `serde_json` at concrete versions.

- [ ] **Step 2: Write the failing tests + payload types**

Add to `jftermd/src/protocol.rs`, **before** the test module, a `use` and the payload structs + frame helpers:

```rust
use serde::{Deserialize, Serialize};

/// `Hello` / `HelloOk` value.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Hello {
    pub proto_version: u32,
    pub daemon_version: String,
}

/// One entry in the `Sessions` reply to `List`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SessionInfo {
    pub session_id: String,
    pub argv: Vec<String>,
    pub cwd: String,
    pub running: bool,
    pub has_client: bool,
    pub created_at: u64,
}

/// `AttachOrOpen` value — binds a session connection; opens if unknown.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AttachOrOpen {
    pub session_id: String,
    pub cwd: String,
    pub argv: Vec<String>,
    /// `0` = full available scrollback (last purge boundary); else cap.
    pub want_chunks: usize,
    pub cols: u16,
    pub rows: u16,
}

/// `Resize` value.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct Resize {
    pub cols: u16,
    pub rows: u16,
}

/// `Status` value — semantic dot state.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct StatusMsg {
    pub running: bool,
    pub progress: Option<u8>,
}

/// `Exit` value — shell child exited.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExitMsg {
    pub status: i32,
}

impl Frame {
    /// Build a control frame whose value is the JSON encoding of `value`.
    pub fn control<T: Serialize>(ty: FrameType, value: &T) -> serde_json::Result<Self> {
        Ok(Self::new(ty, serde_json::to_vec(value)?))
    }

    /// Decode this frame's value as JSON into `T`.
    pub fn json<T: for<'de> Deserialize<'de>>(&self) -> serde_json::Result<T> {
        serde_json::from_slice(&self.payload)
    }
}
```

Add these tests to the `#[cfg(test)] mod tests`:

```rust
    #[test]
    fn attach_or_open_round_trips_through_a_frame() {
        let msg = AttachOrOpen {
            session_id: "tab-1".into(),
            cwd: "/home/u".into(),
            argv: vec!["bash".into(), "-l".into()],
            want_chunks: 0,
            cols: 80,
            rows: 24,
        };
        let frame = Frame::control(FrameType::AttachOrOpen, &msg).unwrap();
        assert_eq!(frame.ty, FrameType::AttachOrOpen);
        let back: AttachOrOpen = frame.json().unwrap();
        assert_eq!(back, msg);
    }

    #[test]
    fn status_round_trips_both_progress_states() {
        for progress in [None, Some(0u8), Some(75u8), Some(100u8)] {
            let msg = StatusMsg { running: true, progress };
            let frame = Frame::control(FrameType::Status, &msg).unwrap();
            let back: StatusMsg = frame.json().unwrap();
            assert_eq!(back, msg);
        }
    }

    #[test]
    fn hello_round_trips() {
        let msg = Hello { proto_version: PROTO_VERSION, daemon_version: "0.1.0".into() };
        let frame = Frame::control(FrameType::Hello, &msg).unwrap();
        let back: Hello = frame.json().unwrap();
        assert_eq!(back, msg);
    }
```

- [ ] **Step 3: Run the tests — verify they pass**

Run: `source "$HOME/.cargo/env" && cargo test -p jftermd protocol::`
Expected: PASS (round-trip tests for AttachOrOpen, Status, Hello, plus the earlier framing tests).

- [ ] **Step 4: Lint + full suite, then commit**

```bash
source "$HOME/.cargo/env"
just check && just test
git add jftermd/Cargo.toml jftermd/src/protocol.rs Cargo.lock
git commit -m "feat(protocol): serde JSON control payloads + Frame::control/json helpers"
```

---

## Task 4: PTY wrapper — `forkpty`, spawn, drain

**Files:**
- Modify: `jftermd/Cargo.toml`, `jftermd/src/lib.rs`
- Create: `jftermd/src/pty.rs`

> **Reviewer note:** this task is flagged for independent review (fd ownership / non-blocking semantics / EOF vs EAGAIN / no fd leaks). The `nix` API may have drifted from 0.29.x — adapt signatures to the pinned version, logic unchanged.

- [ ] **Step 1: Add nix + libc deps (pin versions)**

```bash
source "$HOME/.cargo/env"
cd jftermd
cargo add nix --features term,process,signal,fcntl,ioctl,fs
cargo add libc
cd ..
```

Record the pinned `nix` version as a comment at the top of `jftermd/Cargo.toml` (e.g. `# nix = 0.29.x`). If a needed symbol turns out to be behind a feature not listed above, add that feature and note it.

- [ ] **Step 2: Register the module**

Add to `jftermd/src/lib.rs`:

```rust
pub mod pty;
```

- [ ] **Step 3: Write `pty.rs` with the spawn + drain test**

Create `jftermd/src/pty.rs`:

```rust
//! Synchronous PTY wrapper around `nix::pty::forkpty`. The async loop (B2)
//! drives `drain`/`write_input` off `AsyncFd`; this layer is deliberately
//! blocking-fd-free (master is non-blocking) and testable with real shells.

use std::ffi::CString;
use std::io;
use std::os::fd::{AsRawFd, OwnedFd};
use std::path::Path;

use nix::fcntl::{FcntlArg, OFlag, fcntl};
use nix::pty::{ForkptyResult, Winsize, forkpty};
use nix::sys::signal::{Signal, killpg};
use nix::sys::wait::{WaitPidFlag, WaitStatus, waitpid};
use nix::unistd::{ForkResult, Pid, chdir, execvpe, read, write};

// TIOCSWINSZ ioctl for resizing the master after fork.
nix::ioctl_write_ptr_bad!(tiocswinsz, libc::TIOCSWINSZ, Winsize);

/// Build a `Winsize` from columns/rows (pixel sizes left zero).
pub fn winsize(cols: u16, rows: u16) -> Winsize {
    Winsize { ws_row: rows, ws_col: cols, ws_xpixel: 0, ws_ypixel: 0 }
}

/// Result of draining all currently-available PTY output.
#[derive(Debug, Default)]
pub struct DrainResult {
    /// Bytes read this drain (may be empty if nothing was ready).
    pub bytes: Vec<u8>,
    /// True once the master hit EOF (slave fully closed → child gone).
    pub eof: bool,
}

/// An owned PTY master + its shell child process.
#[derive(Debug)]
pub struct Pty {
    master: OwnedFd,
    child: Pid,
}

impl Pty {
    /// `forkpty` a shell: `argv[0]` is exec'd via PATH, cwd set, `TERM`
    /// forced to `xterm-256color`, master left non-blocking.
    pub fn spawn(argv: &[String], cwd: &Path, ws: Winsize) -> io::Result<Self> {
        // Build argv/env/cwd C strings in the PARENT — the child path between
        // fork and exec must use only async-signal-safe calls (no allocation,
        // no env mutation), so everything is prepared up front.
        let argv_c: Vec<CString> = argv
            .iter()
            .map(|s| CString::new(s.as_bytes()))
            .collect::<Result<_, _>>()
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e))?;
        if argv_c.is_empty() {
            return Err(io::Error::new(io::ErrorKind::InvalidInput, "empty argv"));
        }
        let mut env_c: Vec<CString> = std::env::vars()
            .filter(|(k, _)| k != "TERM")
            .map(|(k, v)| CString::new(format!("{k}={v}")))
            .collect::<Result<_, _>>()
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e))?;
        env_c.push(CString::new("TERM=xterm-256color").unwrap());
        let cwd_c = CString::new(cwd.as_os_str().as_encoded_bytes())
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e))?;

        // SAFETY: child path touches only async-signal-safe syscalls.
        let res = unsafe { forkpty(Some(&ws), None) }.map_err(io_err)?;
        match res {
            ForkptyResult::Parent { master, child } => {
                set_nonblocking(&master)?;
                Ok(Self { master, child })
            }
            ForkptyResult::Child => {
                let _ = chdir(cwd_c.as_c_str());
                let _ = execvpe(&argv_c[0], &argv_c, &env_c);
                // exec failed; child must not return into Rust.
                unsafe { libc::_exit(127) };
            }
        }
    }

    /// Raw master fd, for `AsyncFd` registration in B2.
    pub fn master_raw_fd(&self) -> i32 {
        self.master.as_raw_fd()
    }

    /// Write keystroke bytes to the shell. Returns bytes written.
    pub fn write_input(&self, data: &[u8]) -> io::Result<usize> {
        write(&self.master, data).map_err(io_err)
    }

    /// Read all currently-available output until EAGAIN or EOF.
    pub fn drain(&mut self) -> io::Result<DrainResult> {
        let mut out = DrainResult::default();
        let mut buf = [0u8; 65536];
        loop {
            match read(&self.master, &mut buf) {
                Ok(0) => {
                    out.eof = true;
                    return Ok(out);
                }
                Ok(n) => out.bytes.extend_from_slice(&buf[..n]),
                Err(nix::errno::Errno::EAGAIN) => return Ok(out),
                Err(nix::errno::Errno::EINTR) => continue,
                Err(e) => return Err(io_err(e)),
            }
        }
    }

    /// Apply a new winsize and SIGWINCH the child's process group.
    pub fn resize(&self, ws: Winsize) -> io::Result<()> {
        // SAFETY: master is a valid fd; ws outlives the call.
        unsafe { tiocswinsz(self.master.as_raw_fd(), &ws) }.map_err(io_err)?;
        self.sigwinch()
    }

    /// SIGWINCH the child's process group (it is its own group leader after
    /// forkpty's setsid, so the pgid equals the child pid).
    pub fn sigwinch(&self) -> io::Result<()> {
        killpg(self.child, Signal::SIGWINCH).map_err(io_err)
    }

    /// SIGHUP the child's process group (Close).
    pub fn hangup(&self) -> io::Result<()> {
        killpg(self.child, Signal::SIGHUP).map_err(io_err)
    }

    /// Non-blocking reap. `Some(status)` once reaped (128+sig for signals),
    /// `None` while still running.
    pub fn try_reap(&self) -> io::Result<Option<i32>> {
        match waitpid(self.child, Some(WaitPidFlag::WNOHANG)) {
            Ok(WaitStatus::Exited(_, code)) => Ok(Some(code)),
            Ok(WaitStatus::Signaled(_, sig, _)) => Ok(Some(128 + sig as i32)),
            Ok(_) => Ok(None),
            Err(nix::errno::Errno::ECHILD) => Ok(Some(0)), // already reaped
            Err(e) => Err(io_err(e)),
        }
    }
}

fn set_nonblocking(fd: &OwnedFd) -> io::Result<()> {
    fcntl(fd, FcntlArg::F_SETFL(OFlag::O_NONBLOCK)).map_err(io_err)?;
    Ok(())
}

fn io_err(e: nix::errno::Errno) -> io::Error {
    io::Error::from_raw_os_error(e as i32)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{Duration, Instant};

    /// Drain repeatedly (with short sleeps) until `needle` appears or timeout.
    fn read_until(pty: &mut Pty, needle: &[u8], timeout: Duration) -> Vec<u8> {
        let mut acc = Vec::new();
        let start = Instant::now();
        while start.elapsed() < timeout {
            let d = pty.drain().expect("drain");
            acc.extend_from_slice(&d.bytes);
            if acc.windows(needle.len()).any(|w| w == needle) {
                return acc;
            }
            std::thread::sleep(Duration::from_millis(10));
        }
        acc
    }

    fn argv(parts: &[&str]) -> Vec<String> {
        parts.iter().map(|s| s.to_string()).collect()
    }

    #[test]
    fn echoes_term_to_xterm_256color() {
        let mut pty = Pty::spawn(
            &argv(&["sh", "-c", "printf %s \"$TERM\""]),
            Path::new("/"),
            winsize(80, 24),
        )
        .expect("spawn");
        let out = read_until(&mut pty, b"xterm-256color", Duration::from_secs(3));
        assert!(
            out.windows(14).any(|w| w == b"xterm-256color"),
            "TERM not set; got {:?}",
            String::from_utf8_lossy(&out)
        );
    }

    #[test]
    fn write_input_reaches_the_shell() {
        let mut pty = Pty::spawn(&argv(&["cat"]), Path::new("/"), winsize(80, 24)).expect("spawn");
        pty.write_input(b"ping\n").expect("write");
        let out = read_until(&mut pty, b"ping", Duration::from_secs(3));
        assert!(out.windows(4).any(|w| w == b"ping"));
    }

    #[test]
    fn child_exit_is_reaped_with_status() {
        let mut pty =
            Pty::spawn(&argv(&["sh", "-c", "exit 7"]), Path::new("/"), winsize(80, 24)).expect("spawn");
        let start = Instant::now();
        loop {
            let d = pty.drain().expect("drain");
            if d.eof {
                break;
            }
            assert!(start.elapsed() < Duration::from_secs(3), "no EOF");
            std::thread::sleep(Duration::from_millis(10));
        }
        // After EOF the child should reap with code 7.
        let mut status = None;
        let start = Instant::now();
        while start.elapsed() < Duration::from_secs(3) {
            if let Some(s) = pty.try_reap().expect("reap") {
                status = Some(s);
                break;
            }
            std::thread::sleep(Duration::from_millis(10));
        }
        assert_eq!(status, Some(7));
    }

    #[test]
    fn resize_does_not_error() {
        let pty = Pty::spawn(&argv(&["cat"]), Path::new("/"), winsize(80, 24)).expect("spawn");
        pty.resize(winsize(120, 40)).expect("resize");
        pty.hangup().expect("hangup");
    }
}
```

- [ ] **Step 4: Run the tests — verify they pass (resolve nix API drift here)**

Run: `source "$HOME/.cargo/env" && cargo test -p jftermd pty::`
Expected: PASS (all four). If compilation fails on a `nix` signature, adapt it to the pinned version (see Environment note) — do not change the logic. If `as_encoded_bytes` is unavailable on the toolchain, use `cwd.to_str()`-based conversion instead.

- [ ] **Step 5: Lint + full suite, then commit**

```bash
source "$HOME/.cargo/env"
just check && just test
git add jftermd/Cargo.toml jftermd/src/pty.rs jftermd/src/lib.rs Cargo.lock
git commit -m "feat(pty): sync forkpty wrapper (spawn/drain/write/resize/reap)"
```

---

## Task 5: `Session` — bind PTY to ReplayEngine + lifecycle

**Files:**
- Modify: `jftermd/src/lib.rs`
- Create: `jftermd/src/session.rs`

> **Reviewer note:** verify the drain→feed ordering (live bytes forwarded verbatim AND fed to the engine), the Dead-transition + reap on EOF, and that `replay_for_attach` maps `want_chunks == 0` to full replay.

- [ ] **Step 1: Register the module**

Add to `jftermd/src/lib.rs`:

```rust
pub mod session;
```

- [ ] **Step 2: Write `session.rs` with tests**

Create `jftermd/src/session.rs`:

```rust
//! A daemon `Session`: one shell PTY plus the replayable engine and lifecycle.
//! Synchronous and socket-free — the B2 server drives `drain` off the loop and
//! frames the outputs onto the wire.

use std::io;
use std::path::PathBuf;
use std::time::{SystemTime, UNIX_EPOCH};

use jftermd_core::{ReplayEngine, StatusSnapshot};

use crate::protocol::SessionInfo;
use crate::pty::{Pty, Winsize, winsize};

/// Whether the shell child is still alive.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Lifecycle {
    Live,
    Dead { status: i32 },
}

/// What a `drain` produced, for the server to act on.
#[derive(Debug, Default)]
pub struct DrainOutcome {
    /// Raw live bytes to forward to an attached client as `Data`.
    pub data: Vec<u8>,
    /// Status snapshot after feeding (for `Status` frames).
    pub status: StatusSnapshot,
    /// `Some(code)` if the child exited during this drain.
    pub exit: Option<i32>,
}

/// The replay handshake payload for an attaching client.
#[derive(Debug)]
pub struct AttachReplay {
    /// Prologue + sanitized data (one or more `Data` frames' worth).
    pub data: Vec<u8>,
    /// Current status, sent as a `Status` frame after the data.
    pub status: StatusSnapshot,
}

/// One shell session.
#[derive(Debug)]
pub struct Session {
    id: String,
    argv: Vec<String>,
    cwd: PathBuf,
    pty: Pty,
    engine: ReplayEngine,
    size: Winsize,
    created_at: u64,
    lifecycle: Lifecycle,
}

impl Session {
    /// Open a fresh session: forkpty the shell and start with an empty engine.
    pub fn open(
        id: impl Into<String>,
        argv: Vec<String>,
        cwd: impl Into<PathBuf>,
        cols: u16,
        rows: u16,
    ) -> io::Result<Self> {
        let cwd = cwd.into();
        let size = winsize(cols, rows);
        let pty = Pty::spawn(&argv, &cwd, size)?;
        Ok(Self {
            id: id.into(),
            argv,
            cwd,
            pty,
            engine: ReplayEngine::new(),
            size,
            created_at: now_secs(),
            lifecycle: Lifecycle::Live,
        })
    }

    pub fn id(&self) -> &str {
        &self.id
    }

    pub fn lifecycle(&self) -> Lifecycle {
        self.lifecycle
    }

    pub fn is_dead(&self) -> bool {
        matches!(self.lifecycle, Lifecycle::Dead { .. })
    }

    /// Raw master fd for `AsyncFd` in B2.
    pub fn master_raw_fd(&self) -> i32 {
        self.pty.master_raw_fd()
    }

    /// Drain the PTY: forward live bytes, feed the engine, detect exit.
    pub fn drain(&mut self) -> io::Result<DrainOutcome> {
        let drained = self.pty.drain()?;
        if !drained.bytes.is_empty() {
            self.engine.feed(&drained.bytes);
        }
        let mut exit = None;
        if drained.eof && !self.is_dead() {
            let status = self.pty.try_reap()?.unwrap_or(0);
            self.lifecycle = Lifecycle::Dead { status };
            exit = Some(status);
        }
        Ok(DrainOutcome {
            data: drained.bytes,
            status: self.engine.status(),
            exit,
        })
    }

    /// Forward keystrokes to the shell (no-op once dead).
    pub fn write_input(&self, data: &[u8]) -> io::Result<()> {
        if self.is_dead() {
            return Ok(());
        }
        self.pty.write_input(data)?;
        Ok(())
    }

    /// Update the stored winsize and SIGWINCH the shell.
    pub fn resize(&mut self, cols: u16, rows: u16) -> io::Result<()> {
        self.size = winsize(cols, rows);
        if self.is_dead() {
            return Ok(());
        }
        self.pty.resize(self.size)
    }

    /// Produce the attach handshake replay. `want_chunks == 0` → full available
    /// scrollback (last purge boundary); otherwise cap to the most recent N.
    pub fn replay_for_attach(&self, want_chunks: usize) -> AttachReplay {
        let data = if want_chunks == 0 {
            self.engine.replay()
        } else {
            self.engine.replay_capped(want_chunks)
        };
        AttachReplay { data, status: self.engine.status() }
    }

    pub fn status(&self) -> StatusSnapshot {
        self.engine.status()
    }

    /// Kill the shell (SIGHUP), reap, and mark dead. Idempotent.
    pub fn close(&mut self) -> io::Result<i32> {
        if let Lifecycle::Dead { status } = self.lifecycle {
            return Ok(status);
        }
        self.pty.hangup()?;
        // Drain any final output into the engine before reaping.
        let _ = self.pty.drain();
        let mut status = 0;
        for _ in 0..200 {
            if let Some(s) = self.pty.try_reap()? {
                status = s;
                break;
            }
            std::thread::sleep(std::time::Duration::from_millis(5));
        }
        self.lifecycle = Lifecycle::Dead { status };
        Ok(status)
    }

    /// Snapshot for the `List`/`Sessions` control reply.
    pub fn info(&self, has_client: bool) -> SessionInfo {
        SessionInfo {
            session_id: self.id.clone(),
            argv: self.argv.clone(),
            cwd: self.cwd.to_string_lossy().into_owned(),
            running: !self.is_dead(),
            has_client,
            created_at: self.created_at,
        }
    }
}

fn now_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{Duration, Instant};

    fn argv(parts: &[&str]) -> Vec<String> {
        parts.iter().map(|s| s.to_string()).collect()
    }

    /// Drain until `needle` is in the accumulated replay or timeout.
    fn drain_until(s: &mut Session, needle: &[u8], timeout: Duration) {
        let start = Instant::now();
        while start.elapsed() < timeout {
            let _ = s.drain().expect("drain");
            if s.replay_for_attach(0)
                .data
                .windows(needle.len())
                .any(|w| w == needle)
            {
                return;
            }
            std::thread::sleep(Duration::from_millis(10));
        }
    }

    #[test]
    fn open_drains_into_replay_before_attach() {
        let mut s = Session::open("t1", argv(&["sh", "-c", "echo hello; sleep 0.2"]), "/", 80, 24)
            .expect("open");
        drain_until(&mut s, b"hello", Duration::from_secs(3));
        let replay = s.replay_for_attach(0);
        assert!(replay.data.windows(5).any(|w| w == b"hello"));
    }

    #[test]
    fn input_round_trips() {
        let mut s = Session::open("t2", argv(&["cat"]), "/", 80, 24).expect("open");
        s.write_input(b"ping\n").expect("input");
        drain_until(&mut s, b"ping", Duration::from_secs(3));
        assert!(s.replay_for_attach(0).data.windows(4).any(|w| w == b"ping"));
    }

    #[test]
    fn shell_exit_marks_dead_and_keeps_replay() {
        let mut s = Session::open("t3", argv(&["sh", "-c", "echo bye; exit 3"]), "/", 80, 24)
            .expect("open");
        let mut exit = None;
        let start = Instant::now();
        while start.elapsed() < Duration::from_secs(3) {
            let out = s.drain().expect("drain");
            if let Some(code) = out.exit {
                exit = Some(code);
                break;
            }
            std::thread::sleep(Duration::from_millis(10));
        }
        assert_eq!(exit, Some(3));
        assert!(s.is_dead());
        // Replay survives death.
        assert!(s.replay_for_attach(0).data.windows(3).any(|w| w == b"bye"));
    }

    #[test]
    fn resize_and_info_reflect_state() {
        let mut s = Session::open("t4", argv(&["cat"]), "/home", 80, 24).expect("open");
        s.resize(120, 40).expect("resize");
        let info = s.info(true);
        assert_eq!(info.session_id, "t4");
        assert_eq!(info.argv, argv(&["cat"]));
        assert_eq!(info.cwd, "/home");
        assert!(info.running);
        assert!(info.has_client);
    }

    #[test]
    fn close_kills_and_reaps() {
        let mut s = Session::open("t5", argv(&["cat"]), "/", 80, 24).expect("open");
        let status = s.close().expect("close");
        assert!(s.is_dead());
        // SIGHUP-terminated `cat` reaps as 128+1 (SIGHUP) or 0 if it raced.
        assert!(status == 129 || status == 0, "unexpected close status {status}");
    }
}
```

- [ ] **Step 3: Run the tests — verify they pass**

Run: `source "$HOME/.cargo/env" && cargo test -p jftermd session::`
Expected: PASS (all five). If `close_kills_and_reaps` flakes on the status code, keep the assertion permissive as written (race between hangup and reap).

- [ ] **Step 4: Lint + full suite, then commit**

```bash
source "$HOME/.cargo/env"
just check && just test
git add jftermd/src/session.rs jftermd/src/lib.rs
git commit -m "feat(session): Session binds PTY to ReplayEngine with lifecycle + attach replay"
```

---

## Self-review (completed by plan author)

**Spec coverage (B1 scope only — sockets/daemonize are B2):**
- TLV framing `[u8 type][u32 len][value]` (spec ~166–170) → Tasks 1–2.
- `PROTO_VERSION`, Hello/HelloOk, List/Sessions, AttachOrOpen, Input, Resize, Close, Data, Status, Exit message set (spec ~181–198) → `FrameType` (Task 1) + serde payloads (Task 3).
- Hot frames carry raw bytes; control frames carry JSON (spec ~172–174) → `Frame::new` raw vs `Frame::control`/`json` (Tasks 1, 3).
- Malformed-frame / oversized-length handling (spec ~276) → `ProtocolError` (Task 2).
- forkpty + `TERM=xterm-256color` + winsize + non-blocking master + write/drain/resize/SIGWINCH/kill/reap (spec ~89–95, plan B1) → Task 4.
- Session owns Pty + ReplayEngine + lifecycle (Live/Dead) + winsize + attach-replay producer (prologue+data+STATUS) + drain/write_input/resize/replay_for_attach/status/close/on_child_exit (spec ~84–118, ~200–211) → Task 5. (`on_child_exit` is folded into `drain`'s EOF→Dead transition, returning `exit`.)
- want_chunks default = full available scrollback (spec ~145–148) → `want_chunks == 0` maps to `engine.replay()` (Task 5).

**Out of B1 scope (deferred to B2, intentionally absent):** UnixListener/server routing, AsyncFd, SIGCHLD reaping via tokio::signal, bounded out-queue/backpressure, most-recent-wins takeover, dead-session grace timer, registry, daemonize (setsid/double-fork/flock/stale-socket), main.rs/CLI/tracing. The B1 modules expose the sync hooks (`master_raw_fd`, `drain`, `try_reap`, `info`) the server will drive.

**Placeholder scan:** none — every code step is complete. The only deliberate "adapt at execution" points are `nix` signature drift (Task 4) and the `serde`/`nix`/`libc` version pins resolved by `cargo add` — both are verification steps, not unwritten logic.

**Type consistency:** `FrameType` variants, `Frame::new`/`control`/`json`, `ProtocolError`, `SessionInfo`, `StatusMsg`, `AttachOrOpen`, `Pty::{spawn,drain,write_input,resize,sigwinch,hangup,try_reap,master_raw_fd}`, `DrainResult{bytes,eof}`, `winsize`, `Winsize`, `Session::{open,drain,write_input,resize,replay_for_attach,status,close,info,is_dead,master_raw_fd}`, `DrainOutcome{data,status,exit}`, `AttachReplay{data,status}`, `Lifecycle::{Live,Dead{status}}` are used consistently across tasks. `Session` consumes the core API exactly as exported: `ReplayEngine::{new,feed,replay,replay_capped,status}` and `StatusSnapshot`.
