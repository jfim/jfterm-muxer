# jftermd Daemon B2 — UDS Server + Daemonization Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build the tokio Unix-domain-socket server and self-spawning daemonization on top of the B1 `Session`/`Pty`/`protocol` foundation, so JFTerm shells outlive the GTK client and reattach with full replay.

**Architecture:** A per-session **actor task** owns its sync `Session`, drives `session.drain()` off an `AsyncFd` on the PTY master, and forwards `DATA`/`STATUS`/`EXIT` to at most one attached client through a **bounded mpsc out-queue** (overflow → drop client, never stall the shell). A `Registry` (`Arc<Mutex<HashMap<id, cmd_tx>>>`) does race-free check-and-insert so `ATTACH_OR_OPEN` attaches-or-opens atomically; the blocking `forkpty`/`close` reaps run via `spawn_blocking`. The accept loop branches a connection on its first frame: `HELLO` → control connection (`HELLO_OK`/`LIST`→`SESSIONS`), `ATTACH_OR_OPEN` → session connection (reader task → commands, writer task → socket). Daemonization is `setsid` + double-fork with an `flock` lockfile + atomic `bind()` (stale-socket unlink on `ECONNREFUSED`) to resolve spawn races; the daemon self-exits a short grace period after its last session ends.

**Tech Stack:** Rust 2024, `tokio` (rt-multi-thread, net, io-util, sync, time, signal, macros), `nix` 0.31 (`flock`, `fork`, `setsid`), `tracing` + `tracing-subscriber`, building on `jftermd-core` + the B1 `jftermd` lib modules (`protocol`, `pty`, `session`).

**Reaping ownership (deliberate decision — read before Task 5/6):** Child reaping stays owned by `Session::drain()`/`Session::close()`, keyed on PTY-master EOF (`EIO`), exactly as B1 built it. We do **not** install a `tokio::signal` SIGCHLD reaper that calls `waitpid`. Reason: the B1 review (follow-up #2) showed a separate SIGCHLD reaper races `Pty::try_reap`'s `ECHILD → Some(0)` mapping and would fabricate a bogus exit-0, masking the real status. EOF-driven reaping is also more correct for the grandchild-holds-the-pty case (the session stays `Live` while something still owns its terminal). This is a conscious deviation from the spec's "tokio::signal for SIGCHLD reaping" phrasing; flag it to the user at plan review. The `signal` feature is still pulled in for graceful SIGTERM/SIGINT shutdown only.

---

## File Structure

- `jftermd/Cargo.toml` — add `tokio`, `tracing`, `tracing-subscriber`; add `[[bin]] name = "jftermd"`; add dev-dep `tempfile`.
- `jftermd/src/lib.rs` — register new modules: `socket`, `registry`, `server`, `daemonize`.
- `jftermd/src/protocol.rs` — fold in B1 follow-up #3 (encoder `debug_assert!` on `MAX_FRAME_LEN`); add a `frame_data` chunk helper.
- `jftermd/src/socket.rs` (new) — socket-path resolution + runtime-dir/socket permission helpers (pure-ish; unit-tested with an overridden `XDG_RUNTIME_DIR`).
- `jftermd/src/registry.rs` (new) — `SessionCommand`, `AttachRequest`, `SessionHandle`, `Registry` (race-free map + empty-`Notify`).
- `jftermd/src/server.rs` (new) — `run(listener, opts)`, the accept loop, control-connection handler, session-connection binding, the per-session actor `session_task`, reader/writer tasks, the empty-registry self-exit watcher.
- `jftermd/src/daemonize.rs` (new) — `flock` lockfile, atomic `bind()` with stale-socket cleanup, `setsid` + double-fork.
- `jftermd/src/main.rs` (new) — thin CLI entry (`--foreground`, `--socket <path>`), tracing init, daemonize-or-foreground dispatch.
- `jftermd/tests/integration.rs` (new) — real-socket, real-shell lifecycle/concurrency tests with an in-process server harness.

---

## Environment note for every task

`cargo`/`just` are **not on PATH**. Every shell step must begin with `source "$HOME/.cargo/env"`. Shell state does not persist between tool calls. The commit gate for every task is `just check && just test` (clippy is `-D warnings`; apply its idiomatic rewrites — they are behavior-preserving). Stale rust-analyzer "file not found for module" diagnostics fire right after a new module file is created — trust `cargo build`/`just check`, not the editor.

---

### Task 0: Workspace deps, bin target, protocol follow-ups

**Files:**
- Modify: `jftermd/Cargo.toml`
- Modify: `jftermd/src/protocol.rs` (encoder bound + `frame_data` helper)
- Modify: `jftermd/src/lib.rs` (declare new modules as they land; for now just keep compiling)

- [ ] **Step 1: Add dependencies and bin target**

Run (records exact resolved versions per the project convention):

```bash
source "$HOME/.cargo/env"
cd jftermd
cargo add tokio --features rt-multi-thread,net,io-util,sync,time,signal,macros
cargo add tracing
cargo add tracing-subscriber --features fmt,env-filter
cargo add --dev tempfile
```

Then edit `jftermd/Cargo.toml` to add the bin target after `[dependencies]` (the lib stays the default `src/lib.rs`):

```toml
[[bin]]
name = "jftermd"
path = "src/main.rs"
```

- [ ] **Step 2: Add a `main.rs` stub so the bin target compiles**

Create `jftermd/src/main.rs`:

```rust
//! Placeholder entry point; real CLI + daemonize dispatch lands in Task 9.
fn main() {
    eprintln!("jftermd: not yet wired up");
    std::process::exit(1);
}
```

- [ ] **Step 3: Write the failing test for the encoder bound + data chunker**

Append to the `tests` module in `jftermd/src/protocol.rs`:

```rust
#[test]
fn frame_data_splits_oversized_payload_into_max_sized_frames() {
    // One byte over the cap must become two DATA frames, none exceeding the cap.
    let big = vec![0xABu8; MAX_FRAME_LEN as usize + 1];
    let frames = frame_data(&big);
    assert_eq!(frames.len(), 2);
    assert_eq!(frames[0].payload.len(), MAX_FRAME_LEN as usize);
    assert_eq!(frames[1].payload.len(), 1);
    assert!(frames.iter().all(|f| f.ty == FrameType::Data));
    // Reassembled payload is byte-identical to the input.
    let mut joined = Vec::new();
    for f in &frames {
        joined.extend_from_slice(&f.payload);
    }
    assert_eq!(joined, big);
}

#[test]
fn frame_data_empty_input_yields_no_frames() {
    assert!(frame_data(&[]).is_empty());
}
```

- [ ] **Step 4: Run it and confirm it fails**

Run: `source "$HOME/.cargo/env" && cargo test -p jftermd frame_data 2>&1 | tail -20`
Expected: FAIL — `cannot find function frame_data in this scope`.

- [ ] **Step 5: Implement the encoder bound and `frame_data`**

In `jftermd/src/protocol.rs`, in `Frame::encode`, add the symmetry assert as the first line of the method body (the decoder already rejects oversized lengths; this catches an oversized *encode* in debug builds):

```rust
    pub fn encode(&self) -> Vec<u8> {
        debug_assert!(
            self.payload.len() <= MAX_FRAME_LEN as usize,
            "frame payload {} exceeds MAX_FRAME_LEN",
            self.payload.len()
        );
        let len = self.payload.len();
```

Add a free function near the bottom of the file (before `#[cfg(test)]`):

```rust
/// Split raw output bytes into one or more `Data` frames, each at most
/// `MAX_FRAME_LEN` bytes, so a large replay never trips the frame cap.
pub fn frame_data(bytes: &[u8]) -> Vec<Frame> {
    bytes
        .chunks(MAX_FRAME_LEN as usize)
        .map(|c| Frame::new(FrameType::Data, c.to_vec()))
        .collect()
}
```

- [ ] **Step 6: Run tests + check**

Run: `source "$HOME/.cargo/env" && just check && just test 2>&1 | tail -25`
Expected: PASS — all prior tests plus the two new `frame_data` tests; clippy/fmt clean. (`cargo build` must produce the `jftermd` bin without error.)

- [ ] **Step 7: Commit**

```bash
source "$HOME/.cargo/env"
git add jftermd/Cargo.toml jftermd/src/protocol.rs jftermd/src/main.rs Cargo.lock
git commit -m "build(jftermd): add tokio/tracing deps, bin target, frame_data + encode bound"
```

---

### Task 1: Socket path + runtime-dir/permission helpers

**Files:**
- Create: `jftermd/src/socket.rs`
- Modify: `jftermd/src/lib.rs` (add `pub mod socket;`)

- [ ] **Step 1: Declare the module**

In `jftermd/src/lib.rs` add (keep modules alphabetical-ish; order is not significant):

```rust
pub mod socket;
```

- [ ] **Step 2: Write the failing tests**

Create `jftermd/src/socket.rs`:

```rust
//! Socket-path resolution and the 0700 runtime dir / 0600 socket perms.
//! `$XDG_RUNTIME_DIR/jfterm/muxer.sock`, falling back to `/tmp/jfterm-<uid>`.

use std::io;
use std::os::unix::fs::{DirBuilderExt, PermissionsExt};
use std::path::{Path, PathBuf};

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn socket_path_uses_xdg_runtime_dir_when_set() {
        let dir = tempfile::tempdir().unwrap();
        let p = socket_path_in(Some(dir.path().as_os_str()), 1000);
        assert_eq!(p, dir.path().join("jfterm").join("muxer.sock"));
    }

    #[test]
    fn socket_path_falls_back_to_tmp_with_uid_when_unset() {
        let p = socket_path_in(None, 4242);
        assert_eq!(p, PathBuf::from("/tmp/jfterm-4242").join("muxer.sock"));
    }

    #[test]
    fn ensure_socket_dir_creates_0700_directory() {
        let base = tempfile::tempdir().unwrap();
        let sock = base.path().join("jfterm").join("muxer.sock");
        ensure_socket_dir(&sock).unwrap();
        let dir = sock.parent().unwrap();
        let mode = std::fs::metadata(dir).unwrap().permissions().mode() & 0o777;
        assert_eq!(mode, 0o700);
        // Idempotent: a second call on an existing dir succeeds.
        ensure_socket_dir(&sock).unwrap();
    }

    #[test]
    fn restrict_socket_perms_sets_0600() {
        let base = tempfile::tempdir().unwrap();
        let f = base.path().join("muxer.sock");
        std::fs::write(&f, b"").unwrap();
        restrict_socket_perms(&f).unwrap();
        let mode = std::fs::metadata(&f).unwrap().permissions().mode() & 0o777;
        assert_eq!(mode, 0o600);
    }
}
```

- [ ] **Step 3: Run it and confirm it fails**

Run: `source "$HOME/.cargo/env" && cargo test -p jftermd socket:: 2>&1 | tail -20`
Expected: FAIL — `cannot find function socket_path_in` etc.

- [ ] **Step 4: Implement the helpers**

Add above the `#[cfg(test)]` block in `jftermd/src/socket.rs`:

```rust
use std::ffi::OsStr;

/// Resolve the socket path: `$XDG_RUNTIME_DIR/jfterm/muxer.sock`, else
/// `/tmp/jfterm-<uid>/muxer.sock`. `xdg` and `uid` are injected for testing.
pub fn socket_path_in(xdg: Option<&OsStr>, uid: u32) -> PathBuf {
    let base = match xdg {
        Some(x) if !x.is_empty() => PathBuf::from(x).join("jfterm"),
        _ => PathBuf::from(format!("/tmp/jfterm-{uid}")),
    };
    base.join("muxer.sock")
}

/// The real default socket path for this process.
pub fn default_socket_path() -> PathBuf {
    let xdg = std::env::var_os("XDG_RUNTIME_DIR");
    // SAFETY: getuid is always safe.
    let uid = unsafe { libc::getuid() };
    socket_path_in(xdg.as_deref(), uid)
}

/// Create the socket's parent dir at mode 0700 (idempotent).
pub fn ensure_socket_dir(sock: &Path) -> io::Result<()> {
    let dir = sock
        .parent()
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "socket path has no parent"))?;
    if dir.exists() {
        return Ok(());
    }
    std::fs::DirBuilder::new()
        .recursive(true)
        .mode(0o700)
        .create(dir)
}

/// Tighten an already-bound socket file to 0600.
pub fn restrict_socket_perms(sock: &Path) -> io::Result<()> {
    std::fs::set_permissions(sock, std::fs::Permissions::from_mode(0o600))
}
```

- [ ] **Step 5: Run tests + check**

Run: `source "$HOME/.cargo/env" && just check && just test 2>&1 | tail -25`
Expected: PASS — the four `socket::` tests plus all prior tests; clippy/fmt clean.

- [ ] **Step 6: Commit**

```bash
source "$HOME/.cargo/env"
git add jftermd/src/socket.rs jftermd/src/lib.rs
git commit -m "feat(socket): runtime-dir/socket path resolution with 0700/0600 perms"
```

---

### Task 2: Registry — session map, commands, empty-notify

**Files:**
- Create: `jftermd/src/registry.rs`
- Modify: `jftermd/src/lib.rs` (add `pub mod registry;`)

This is pure coordination state (no I/O). The `Registry` does a **synchronous** check-and-insert under a `std::sync::Mutex` so `ATTACH_OR_OPEN` is race-free without holding a lock across an `.await`.

- [ ] **Step 1: Declare the module**

In `jftermd/src/lib.rs` add `pub mod registry;`.

- [ ] **Step 2: Write the failing tests**

Create `jftermd/src/registry.rs`:

```rust
//! The session registry: `session_id -> cmd_tx` with a race-free
//! check-and-insert (so ATTACH_OR_OPEN attaches-or-opens atomically) and an
//! empty-notify the self-exit watcher waits on.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use tokio::sync::{Notify, mpsc, oneshot};

use crate::protocol::SessionInfo;

/// A command delivered to a session's actor task.
#[derive(Debug)]
pub enum SessionCommand {
    /// Raw keystroke bytes from the client (`INPUT`).
    Input(Vec<u8>),
    /// Client resize (`RESIZE`).
    Resize { cols: u16, rows: u16 },
    /// Kill the shell and drop the session (`CLOSE`).
    Close,
    /// A (re)attach: most-recent-wins takeover of the session's single client.
    Attach(AttachRequest),
    /// Snapshot request for the control connection's `LIST`.
    Info(oneshot::Sender<SessionInfo>),
}

/// Everything the actor needs to bind a newly-attached client.
#[derive(Debug)]
pub struct AttachRequest {
    pub want_chunks: usize,
    pub cols: u16,
    pub rows: u16,
    /// The client's bounded out-queue; the actor pushes `DATA`/`STATUS`/`EXIT`
    /// frames here and drops it (forced detach) on overflow.
    pub out_tx: mpsc::Sender<crate::protocol::Frame>,
}

/// Registry-side handle to one session actor.
#[derive(Debug, Clone)]
pub struct SessionHandle {
    pub cmd_tx: mpsc::Sender<SessionCommand>,
}

/// The shared session table.
#[derive(Debug, Default)]
pub struct Registry {
    sessions: Mutex<HashMap<String, SessionHandle>>,
    /// Pulsed whenever a session is removed, so the self-exit watcher rechecks.
    ended: Notify,
}

/// Outcome of `attach_or_create`: whether the caller must spawn the actor.
pub enum Bind {
    /// Session already existed; here is its command channel.
    Existing(mpsc::Sender<SessionCommand>),
    /// Session was newly inserted; the caller must spawn the actor that reads
    /// `cmd_rx`. The handle is already in the table (race-free).
    Created {
        cmd_tx: mpsc::Sender<SessionCommand>,
        cmd_rx: mpsc::Receiver<SessionCommand>,
    },
}

impl Registry {
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    /// Race-free: if `id` exists return its channel, else insert a fresh handle
    /// and return the receiver for the caller to spawn an actor around. The
    /// whole check-and-insert happens under the lock with no `.await`.
    pub fn attach_or_create(&self, id: &str) -> Bind {
        let mut map = self.sessions.lock().unwrap();
        if let Some(h) = map.get(id) {
            return Bind::Existing(h.cmd_tx.clone());
        }
        let (cmd_tx, cmd_rx) = mpsc::channel(256);
        map.insert(id.to_string(), SessionHandle { cmd_tx: cmd_tx.clone() });
        Bind::Created { cmd_tx, cmd_rx }
    }

    /// Look up an existing session's channel (no creation).
    pub fn get(&self, id: &str) -> Option<mpsc::Sender<SessionCommand>> {
        self.sessions.lock().unwrap().get(id).map(|h| h.cmd_tx.clone())
    }

    /// Remove a session (its actor is ending) and pulse the empty-notify.
    pub fn remove(&self, id: &str) {
        self.sessions.lock().unwrap().remove(id);
        self.ended.notify_one();
    }

    /// All `(id, cmd_tx)` pairs, for the control connection's `LIST`.
    pub fn handles(&self) -> Vec<(String, mpsc::Sender<SessionCommand>)> {
        self.sessions
            .lock()
            .unwrap()
            .iter()
            .map(|(k, v)| (k.clone(), v.cmd_tx.clone()))
            .collect()
    }

    pub fn is_empty(&self) -> bool {
        self.sessions.lock().unwrap().is_empty()
    }

    /// Wait until the next session removal.
    pub async fn wait_for_change(&self) {
        self.ended.notified().await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn create_then_get_then_remove() {
        let reg = Registry::new();
        assert!(reg.is_empty());
        let bind = reg.attach_or_create("s1");
        assert!(matches!(bind, Bind::Created { .. }));
        assert!(!reg.is_empty());
        assert!(reg.get("s1").is_some());
        assert_eq!(reg.handles().len(), 1);
        reg.remove("s1");
        assert!(reg.is_empty());
        assert!(reg.get("s1").is_none());
    }

    #[test]
    fn second_attach_or_create_returns_existing() {
        let reg = Registry::new();
        let first = reg.attach_or_create("s1");
        let (keep_tx, _keep_rx) = match first {
            Bind::Created { cmd_tx, cmd_rx } => (cmd_tx, cmd_rx),
            _ => panic!("expected Created"),
        };
        // Keep the receiver alive so the channel isn't closed.
        let second = reg.attach_or_create("s1");
        match second {
            Bind::Existing(tx) => assert!(tx.same_channel(&keep_tx)),
            _ => panic!("expected Existing on second attach"),
        }
    }

    #[tokio::test]
    async fn wait_for_change_wakes_on_remove() {
        let reg = Registry::new();
        let _ = reg.attach_or_create("s1");
        let reg2 = reg.clone();
        let waiter = tokio::spawn(async move { reg2.wait_for_change().await });
        // Give the waiter a beat to park on the notify.
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        reg.remove("s1");
        tokio::time::timeout(std::time::Duration::from_secs(1), waiter)
            .await
            .expect("waiter should wake")
            .unwrap();
    }
}
```

- [ ] **Step 3: Run it and confirm it fails**

Run: `source "$HOME/.cargo/env" && cargo test -p jftermd registry:: 2>&1 | tail -20`
Expected: FAIL — module/type not found until implemented (the impl is in the same file, so the failure here is the compile error that the file is brand new and `lib.rs` may not yet reference it; after Step 1 it compiles and the tests pass). If it already passes, that's fine — proceed.

- [ ] **Step 4: Verify the suite is green**

Run: `source "$HOME/.cargo/env" && just check && just test 2>&1 | tail -25`
Expected: PASS — the three `registry::` tests plus all prior tests; clippy/fmt clean. Note `same_channel` is stable on `mpsc::Sender`.

- [ ] **Step 5: Commit**

```bash
source "$HOME/.cargo/env"
git add jftermd/src/registry.rs jftermd/src/lib.rs
git commit -m "feat(registry): race-free session map with commands and empty-notify"
```

---

### Task 3: Session actor task (the concurrency heart) — RISKY, independent review

**Files:**
- Create: `jftermd/src/server.rs` (this task adds `ServerOpts`, the `MasterFd` wrapper, the forwarding helpers, and `actor_loop`; later tasks add the accept loop / connection handlers to the same file)
- Modify: `jftermd/src/lib.rs` (add `pub mod server;`)

**Design contract for this task (independent review must verify all of these):**
- The actor owns one sync `Session`. It drives `session.drain()` off `AsyncFd<MasterFd>` (READABLE). After every drain it calls `guard.clear_ready()` because `drain()` reads to `EAGAIN`/EOF itself — without the manual clear, `readable()` busy-spins.
- The readable branch is **disabled once the session is dead** (`if !session.is_dead()`), since a dead PTY produces no more output.
- At most one client. Forwarding uses **`try_send` on a bounded queue**; `Full` or `Closed` → set `client = None` (forced detach / lazy detach). The shell is **never** stalled: `drain()` runs on every readable wake regardless of whether a client is attached.
- `select!` is `biased` with commands first so `Close`/`Attach` are never starved by a chatty shell.
- Blocking calls (`Session::close`) run via `spawn_blocking` (B1 follow-up #1/#4). `Session::open`'s blocking spawn is handled by the wrapper added in Task 5, not here.
- Child reaping is **not** done via SIGCHLD here; `drain()`/`close()` own it (see the plan's reaping decision).
- On exit detected during drain: forward `EXIT`, arm the `dead_grace` timer, keep the session in the registry (dead-session retention). On grace elapse with no reattach → end actor → `registry.remove`. On reattach to a dead session → queue replay (which already contains the final output) + `STATUS` + `EXIT`, then end.

- [ ] **Step 1: Declare the module**

In `jftermd/src/lib.rs` add `pub mod server;`.

- [ ] **Step 2: Write the failing actor test**

Create `jftermd/src/server.rs` with **only** the test module first (the impl lands in Step 4):

```rust
//! tokio UDS server: accept loop, control + session connections, and the
//! per-session actor that bridges a sync `Session` onto the async loop.

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::{Frame, FrameType};
    use crate::registry::{AttachRequest, Registry, SessionCommand};
    use crate::session::Session;
    use std::time::Duration;
    use tokio::sync::mpsc;

    fn contains(hay: &[u8], needle: &[u8]) -> bool {
        hay.windows(needle.len()).any(|w| w == needle)
    }

    /// Pull frames off the client queue, accumulating DATA payloads, until the
    /// needle appears or the deadline passes.
    async fn collect_data_until(
        rx: &mut mpsc::Receiver<Frame>,
        needle: &[u8],
        timeout: Duration,
    ) -> Vec<u8> {
        let mut acc = Vec::new();
        let deadline = tokio::time::Instant::now() + timeout;
        loop {
            match tokio::time::timeout_at(deadline, rx.recv()).await {
                Ok(Some(f)) => {
                    if f.ty == FrameType::Data {
                        acc.extend_from_slice(&f.payload);
                    }
                    if contains(&acc, needle) {
                        return acc;
                    }
                }
                Ok(None) | Err(_) => return acc,
            }
        }
    }

    fn argv(parts: &[&str]) -> Vec<String> {
        parts.iter().map(|s| s.to_string()).collect()
    }

    #[tokio::test]
    async fn actor_attaches_forwards_input_and_closes() {
        let reg = Registry::new();
        let opts = ServerOpts::default();
        let session = tokio::task::spawn_blocking(|| {
            Session::open("a1", argv(&["sh", "-c", "echo HELLO; exec cat"]), "/", 80, 24)
        })
        .await
        .unwrap()
        .expect("open");

        let (cmd_tx, cmd_rx) = mpsc::channel(16);
        let task = tokio::spawn(actor_loop(session, "a1".into(), cmd_rx, reg.clone(), opts.clone()));

        let (out_tx, mut out_rx) = mpsc::channel(opts.out_queue);
        cmd_tx
            .send(SessionCommand::Attach(AttachRequest {
                want_chunks: 0,
                cols: 80,
                rows: 24,
                out_tx,
            }))
            .await
            .unwrap();

        let got = collect_data_until(&mut out_rx, b"HELLO", Duration::from_secs(3)).await;
        assert!(contains(&got, b"HELLO"), "no HELLO in replay/live output");

        cmd_tx.send(SessionCommand::Input(b"PING\n".to_vec())).await.unwrap();
        let got = collect_data_until(&mut out_rx, b"PING", Duration::from_secs(3)).await;
        assert!(contains(&got, b"PING"), "input did not round-trip");

        cmd_tx.send(SessionCommand::Close).await.unwrap();
        tokio::time::timeout(Duration::from_secs(3), task)
            .await
            .expect("actor should end after Close")
            .unwrap();
        assert!(reg.is_empty(), "registry should be empty after Close");
    }

    #[tokio::test]
    async fn dead_session_is_retained_then_reattach_replays_and_exits() {
        let reg = Registry::new();
        let opts = ServerOpts {
            dead_grace: Duration::from_secs(5),
            ..ServerOpts::default()
        };
        let session = tokio::task::spawn_blocking(|| {
            Session::open("d1", argv(&["sh", "-c", "echo BYE; exit 3"]), "/", 80, 24)
        })
        .await
        .unwrap()
        .expect("open");
        let (cmd_tx, cmd_rx) = mpsc::channel(16);
        let task = tokio::spawn(actor_loop(session, "d1".into(), cmd_rx, reg.clone(), opts));

        // No client attached while it runs/exits -> session goes dead, retained.
        tokio::time::sleep(Duration::from_millis(300)).await;
        assert!(!reg.is_empty(), "dead session must be retained for reattach");

        // Reattach: expect BYE in the replay and an EXIT{status:3}.
        let (out_tx, mut out_rx) = mpsc::channel(1024);
        cmd_tx
            .send(SessionCommand::Attach(AttachRequest { want_chunks: 0, cols: 80, rows: 24, out_tx }))
            .await
            .unwrap();

        let mut saw_bye = false;
        let mut exit_code = None;
        let deadline = tokio::time::Instant::now() + Duration::from_secs(3);
        while let Ok(Some(f)) = tokio::time::timeout_at(deadline, out_rx.recv()).await {
            match f.ty {
                FrameType::Data if contains(&f.payload, b"BYE") => saw_bye = true,
                FrameType::Exit => {
                    let m: crate::protocol::ExitMsg = f.json().unwrap();
                    exit_code = Some(m.status);
                }
                _ => {}
            }
        }
        assert!(saw_bye, "dead reattach must replay final output");
        assert_eq!(exit_code, Some(3));
        tokio::time::timeout(Duration::from_secs(3), task)
            .await
            .expect("actor should end after dead reattach")
            .unwrap();
        assert!(reg.is_empty());
    }
}
```

- [ ] **Step 3: Run it and confirm it fails**

Run: `source "$HOME/.cargo/env" && cargo test -p jftermd server:: 2>&1 | tail -25`
Expected: FAIL — `cannot find function actor_loop` / `ServerOpts` not found.

- [ ] **Step 4: Implement `ServerOpts`, helpers, and `actor_loop`**

Insert above the `#[cfg(test)]` module in `jftermd/src/server.rs`:

```rust
use std::os::fd::{AsRawFd, RawFd};
use std::sync::Arc;
use std::time::Duration;

use jftermd_core::StatusSnapshot;
use tokio::io::Interest;
use tokio::io::unix::AsyncFd;
use tokio::sync::mpsc;
use tokio::time::{Instant, sleep_until};

use crate::protocol::{ExitMsg, Frame, FrameType, StatusMsg, frame_data};
use crate::registry::{AttachRequest, Registry, SessionCommand};
use crate::session::{Lifecycle, Session};

/// Tunable runtime knobs (overridable in tests; CLI sets them in Task 9).
#[derive(Debug, Clone)]
pub struct ServerOpts {
    /// How long a dead session is retained awaiting a reattach.
    pub dead_grace: Duration,
    /// How long after the last session ends before the daemon self-exits.
    pub exit_grace: Duration,
    /// Bounded per-client out-queue depth; overflow forces a detach.
    pub out_queue: usize,
}

impl Default for ServerOpts {
    fn default() -> Self {
        Self {
            dead_grace: Duration::from_secs(30),
            exit_grace: Duration::from_secs(5),
            out_queue: 1024,
        }
    }
}

/// `AsRawFd` shim so `AsyncFd` can watch the PTY master without owning it
/// (the `Session` keeps the real `OwnedFd`; dropping `AsyncFd` only
/// deregisters from epoll, it does not close the fd).
struct MasterFd(RawFd);
impl AsRawFd for MasterFd {
    fn as_raw_fd(&self) -> RawFd {
        self.0
    }
}

fn status_frame(s: StatusSnapshot) -> Frame {
    Frame::control(
        FrameType::Status,
        &StatusMsg {
            running: s.running,
            progress: s.progress,
        },
    )
    .expect("StatusMsg always serializes")
}

fn exit_frame(code: i32) -> Frame {
    Frame::control(FrameType::Exit, &ExitMsg { status: code }).expect("ExitMsg always serializes")
}

/// Push one frame to the attached client; on a full or closed queue, drop the
/// client (forced/lazy detach). The shell is never stalled by this.
fn push_or_drop(client: &mut Option<mpsc::Sender<Frame>>, frame: Frame) {
    let drop_it = match client.as_ref() {
        Some(tx) => match tx.try_send(frame) {
            Ok(()) => false,
            Err(mpsc::error::TrySendError::Full(_)) => true,
            Err(mpsc::error::TrySendError::Closed(_)) => true,
        },
        None => false,
    };
    if drop_it {
        *client = None;
    }
}

fn forward_data(client: &mut Option<mpsc::Sender<Frame>>, data: &[u8]) {
    for f in frame_data(data) {
        push_or_drop(client, f);
    }
}

/// Bind a (re)attaching client: most-recent-wins takeover, replay handshake.
///
/// Note: `Session::resize` both sets the winsize and SIGWINCHes the shell, so
/// this collapses handshake steps 1 ("set winsize") and 5 ("SIGWINCH") into one
/// call issued before the replay. That is behaviorally equivalent: the client
/// applies the replay DATA/STATUS frames in order, and the shell's
/// SIGWINCH-triggered repaint arrives afterward as ordinary live DATA.
fn attach_client(
    session: &mut Session,
    client: &mut Option<mpsc::Sender<Frame>>,
    req: AttachRequest,
) {
    let _ = session.resize(req.cols, req.rows);
    let replay = session.replay_for_attach(req.want_chunks);
    for f in frame_data(&replay.data) {
        let _ = req.out_tx.try_send(f);
    }
    let _ = req.out_tx.try_send(status_frame(replay.status));
    if let Lifecycle::Dead { status } = session.lifecycle() {
        let _ = req.out_tx.try_send(exit_frame(status));
    }
    // Replacing the Option drops the previous out_tx -> old writer task ends ->
    // old client socket closes (takeover detach).
    *client = Some(req.out_tx);
}

/// The per-session actor: owns the `Session`, drains the PTY, and forwards
/// frames to at most one client. Returns when the session ends; always removes
/// itself from the registry on the way out.
pub(crate) async fn actor_loop(
    mut session: Session,
    id: String,
    mut cmd_rx: mpsc::Receiver<SessionCommand>,
    registry: Arc<Registry>,
    opts: ServerOpts,
) {
    let async_fd = match AsyncFd::with_interest(MasterFd(session.master_raw_fd()), Interest::READABLE)
    {
        Ok(fd) => fd,
        Err(e) => {
            tracing::error!(%id, error = %e, "AsyncFd registration failed");
            registry.remove(&id);
            return;
        }
    };

    let mut client: Option<mpsc::Sender<Frame>> = None;
    // Set only after the child exits (dead-session retention deadline). `Instant`
    // is `Copy`, so the timer arm captures it by value — no borrow conflict with
    // the readable arm that reassigns it.
    let mut grace_deadline: Option<Instant> = None;

    loop {
        tokio::select! {
            biased;

            cmd = cmd_rx.recv() => {
                match cmd {
                    None => break,
                    Some(SessionCommand::Input(bytes)) => {
                        let _ = session.write_input(&bytes);
                    }
                    Some(SessionCommand::Resize { cols, rows }) => {
                        let _ = session.resize(cols, rows);
                    }
                    Some(SessionCommand::Info(reply)) => {
                        let _ = reply.send(session.info(client.is_some()));
                    }
                    Some(SessionCommand::Close) => {
                        session = tokio::task::spawn_blocking(move || {
                            let _ = session.close();
                            session
                        })
                        .await
                        .expect("close task panicked");
                        let code = match session.lifecycle() {
                            Lifecycle::Dead { status } => status,
                            Lifecycle::Live => 0,
                        };
                        if let Some(tx) = client.take() {
                            let _ = tx.try_send(exit_frame(code));
                        }
                        break;
                    }
                    Some(SessionCommand::Attach(req)) => {
                        attach_client(&mut session, &mut client, req);
                        if session.is_dead() {
                            // Final output + EXIT already queued; flush by ending.
                            break;
                        }
                    }
                }
            }

            guard = async_fd.readable(), if !session.is_dead() => {
                let mut guard = match guard {
                    Ok(g) => g,
                    Err(e) => {
                        tracing::error!(%id, error = %e, "readable() failed");
                        break;
                    }
                };
                match session.drain() {
                    Ok(outcome) => {
                        guard.clear_ready();
                        if !outcome.data.is_empty() {
                            forward_data(&mut client, &outcome.data);
                        }
                        push_or_drop(&mut client, status_frame(outcome.status));
                        if let Some(code) = outcome.exit {
                            push_or_drop(&mut client, exit_frame(code));
                            grace_deadline = Some(Instant::now() + opts.dead_grace);
                        }
                    }
                    Err(e) => {
                        tracing::error!(%id, error = %e, "drain failed");
                        break;
                    }
                }
            }

            () = async {
                match grace_deadline {
                    Some(d) => sleep_until(d).await,
                    None => std::future::pending::<()>().await,
                }
            }, if grace_deadline.is_some() => {
                break;
            }
        }
    }

    registry.remove(&id);
}
```

- [ ] **Step 5: Run the actor tests + full check**

Run: `source "$HOME/.cargo/env" && just check && just test 2>&1 | tail -30`
Expected: PASS — both `server::` actor tests plus all prior tests; clippy/fmt clean. If clippy flags the `() = async {…}` arm or the `match … { Ok(()) => false … }`, apply its idiomatic rewrite (behavior-preserving).

- [ ] **Step 6: Commit**

```bash
source "$HOME/.cargo/env"
git add jftermd/src/server.rs jftermd/src/lib.rs
git commit -m "feat(server): per-session actor — AsyncFd drain, bounded forward, dead retention"
```

**Independent review after this task** (opus, fresh context): audit `actor_loop` for the `clear_ready`/busy-spin contract, backpressure correctness (no path that stalls the shell), the dead-session retention/grace state machine, takeover via `Option` replacement, and that no blocking call runs on the loop thread. Provide the reviewer the full `server.rs` diff and `session.rs`/`pty.rs` for context.

---

### Task 4: Accept loop + connection dispatch + control connection (HELLO/LIST)

**Files:**
- Modify: `jftermd/src/server.rs` (add `run`, `dispatch_connection`, `read_one_frame`, `handle_control`, `collect_sessions`, and small io-error adapters)
- Create: `jftermd/tests/integration.rs` (shared harness + control-connection tests)

The first frame on a connection classifies it: `HELLO` → control connection; `ATTACH_OR_OPEN` → session connection (bound in Task 5). A proto-version mismatch is rejected by closing without `HELLO_OK`.

- [ ] **Step 1: Write the failing integration tests + harness**

Create `jftermd/tests/integration.rs`:

```rust
//! Real-socket, real-shell integration tests against an in-process server.

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use jftermd::protocol::{Frame, FrameDecoder, FrameType, Hello, SessionInfo, PROTO_VERSION};
use jftermd::registry::Registry;
use jftermd::server::{run, ServerOpts};

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{UnixListener, UnixStream};

/// In-process server bound to a throwaway socket under a temp dir.
struct Harness {
    sock: PathBuf,
    registry: Arc<Registry>,
    _tmp: tempfile::TempDir,
    _server: tokio::task::JoinHandle<()>,
}

impl Harness {
    async fn start(opts: ServerOpts) -> Self {
        let tmp = tempfile::tempdir().unwrap();
        let sock = tmp.path().join("muxer.sock");
        let listener = UnixListener::bind(&sock).unwrap();
        let registry = Registry::new();
        let server = tokio::spawn(run(listener, registry.clone(), opts));
        Self {
            sock,
            registry,
            _tmp: tmp,
            _server: server,
        }
    }

    async fn connect(&self) -> Conn {
        Conn {
            stream: UnixStream::connect(&self.sock).await.unwrap(),
            dec: FrameDecoder::new(),
        }
    }
}

/// A minimal framed client over one UnixStream.
struct Conn {
    stream: UnixStream,
    dec: FrameDecoder,
}

impl Conn {
    async fn send(&mut self, frame: &Frame) {
        self.stream.write_all(&frame.encode()).await.unwrap();
    }

    /// Read the next whole frame, or `None` on EOF / timeout.
    async fn recv(&mut self, timeout: Duration) -> Option<Frame> {
        let deadline = tokio::time::Instant::now() + timeout;
        let mut buf = [0u8; 4096];
        loop {
            if let Some(f) = self.dec.next_frame().unwrap() {
                return Some(f);
            }
            match tokio::time::timeout_at(deadline, self.stream.read(&mut buf)).await {
                Ok(Ok(0)) | Err(_) => return None,
                Ok(Ok(n)) => self.dec.push(&buf[..n]),
                Ok(Err(_)) => return None,
            }
        }
    }
}

fn hello(version: u32) -> Frame {
    Frame::control(
        FrameType::Hello,
        &Hello {
            proto_version: version,
            daemon_version: "test".into(),
        },
    )
    .unwrap()
}

#[tokio::test]
async fn hello_handshake_returns_hello_ok() {
    let h = Harness::start(ServerOpts::default()).await;
    let mut c = h.connect().await;
    c.send(&hello(PROTO_VERSION)).await;
    let reply = c.recv(Duration::from_secs(2)).await.expect("HELLO_OK");
    assert_eq!(reply.ty, FrameType::HelloOk);
    let ok: Hello = reply.json().unwrap();
    assert_eq!(ok.proto_version, PROTO_VERSION);
}

#[tokio::test]
async fn proto_mismatch_is_rejected_by_closing() {
    let h = Harness::start(ServerOpts::default()).await;
    let mut c = h.connect().await;
    c.send(&hello(PROTO_VERSION + 99)).await;
    // No HELLO_OK; the connection is closed.
    assert!(c.recv(Duration::from_secs(1)).await.is_none());
}

#[tokio::test]
async fn list_on_empty_daemon_returns_no_sessions() {
    let h = Harness::start(ServerOpts::default()).await;
    let mut c = h.connect().await;
    c.send(&hello(PROTO_VERSION)).await;
    let _ = c.recv(Duration::from_secs(2)).await.expect("HELLO_OK");
    c.send(&Frame::new(FrameType::List, Vec::new())).await;
    let reply = c.recv(Duration::from_secs(2)).await.expect("SESSIONS");
    assert_eq!(reply.ty, FrameType::Sessions);
    let sessions: Vec<SessionInfo> = reply.json().unwrap();
    assert!(sessions.is_empty());
    // keep registry referenced so it isn't dropped early
    assert!(h.registry.is_empty());
}
```

- [ ] **Step 2: Run it and confirm it fails**

Run: `source "$HOME/.cargo/env" && cargo test -p jftermd --test integration 2>&1 | tail -25`
Expected: FAIL — `run` not found / unresolved imports (`run`, `ServerOpts` exist but `run` is not yet defined).

- [ ] **Step 3: Implement the accept loop, dispatch, and control handler**

Add to the top-of-file `use` block in `jftermd/src/server.rs`:

```rust
use std::io;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::unix::{OwnedReadHalf, OwnedWriteHalf};
use tokio::net::{UnixListener, UnixStream};
use tokio::sync::oneshot;

use crate::protocol::{FrameDecoder, Hello, ProtocolError, SessionInfo, PROTO_VERSION};
```

Then add these functions above the `#[cfg(test)]` module:

```rust
fn proto_io(e: ProtocolError) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, e)
}

fn json_io(e: serde_json::Error) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, e)
}

/// Read frames off `rh` until one whole frame is available, or EOF (`None`).
async fn read_one_frame(
    rh: &mut OwnedReadHalf,
    dec: &mut FrameDecoder,
) -> io::Result<Option<Frame>> {
    let mut buf = [0u8; 4096];
    loop {
        if let Some(f) = dec.next_frame().map_err(proto_io)? {
            return Ok(Some(f));
        }
        let n = rh.read(&mut buf).await?;
        if n == 0 {
            return Ok(None);
        }
        dec.push(&buf[..n]);
    }
}

/// Accept connections forever, one task per connection.
pub async fn run(listener: UnixListener, registry: Arc<Registry>, opts: ServerOpts) {
    loop {
        match listener.accept().await {
            Ok((stream, _addr)) => {
                let reg = registry.clone();
                let opts = opts.clone();
                tokio::spawn(async move {
                    if let Err(e) = dispatch_connection(stream, reg, opts).await {
                        tracing::debug!(error = %e, "connection ended");
                    }
                });
            }
            Err(e) => {
                tracing::error!(error = %e, "accept failed");
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        }
    }
}

/// Classify a connection by its first frame.
async fn dispatch_connection(
    stream: UnixStream,
    registry: Arc<Registry>,
    opts: ServerOpts,
) -> io::Result<()> {
    let (mut rh, wh) = stream.into_split();
    let mut dec = FrameDecoder::new();
    let first = match read_one_frame(&mut rh, &mut dec).await? {
        Some(f) => f,
        None => return Ok(()),
    };
    match first.ty {
        FrameType::Hello => handle_control(first, rh, wh, registry, dec).await,
        FrameType::AttachOrOpen => {
            // Session binding lands in Task 5; until then, close politely.
            tracing::warn!("session binding not yet implemented");
            Ok(())
        }
        other => {
            tracing::warn!(?other, "unexpected first frame; closing");
            Ok(())
        }
    }
}

/// Control connection: HELLO_OK handshake, then LIST -> SESSIONS.
async fn handle_control(
    hello_frame: Frame,
    mut rh: OwnedReadHalf,
    mut wh: OwnedWriteHalf,
    registry: Arc<Registry>,
    mut dec: FrameDecoder,
) -> io::Result<()> {
    let hello: Hello = hello_frame.json().map_err(json_io)?;
    if hello.proto_version != PROTO_VERSION {
        tracing::warn!(
            got = hello.proto_version,
            want = PROTO_VERSION,
            "proto mismatch; rejecting"
        );
        return Ok(());
    }
    let ok = Frame::control(
        FrameType::HelloOk,
        &Hello {
            proto_version: PROTO_VERSION,
            daemon_version: env!("CARGO_PKG_VERSION").to_string(),
        },
    )
    .map_err(json_io)?;
    wh.write_all(&ok.encode()).await?;

    loop {
        let frame = match read_one_frame(&mut rh, &mut dec).await? {
            Some(f) => f,
            None => return Ok(()),
        };
        match frame.ty {
            FrameType::List => {
                let sessions = collect_sessions(&registry).await;
                let reply = Frame::control(FrameType::Sessions, &sessions).map_err(json_io)?;
                wh.write_all(&reply.encode()).await?;
            }
            other => {
                tracing::warn!(?other, "unexpected control frame; ignoring");
            }
        }
    }
}

/// Ask every live session actor for its `SessionInfo` (bounded by a timeout).
async fn collect_sessions(registry: &Registry) -> Vec<SessionInfo> {
    let handles = registry.handles();
    let mut out = Vec::with_capacity(handles.len());
    for (_id, tx) in handles {
        let (reply_tx, reply_rx) = oneshot::channel();
        if tx.send(SessionCommand::Info(reply_tx)).await.is_ok() {
            if let Ok(Ok(info)) = tokio::time::timeout(Duration::from_secs(1), reply_rx).await {
                out.push(info);
            }
        }
    }
    out
}
```

- [ ] **Step 4: Run tests + check**

Run: `source "$HOME/.cargo/env" && just check && just test 2>&1 | tail -30`
Expected: PASS — the three `integration` control tests plus all prior tests; clippy/fmt clean. If clippy wants the two nested `if let` collapsed in `collect_sessions`, apply `let chain` / `&&` as it suggests.

- [ ] **Step 5: Commit**

```bash
source "$HOME/.cargo/env"
git add jftermd/src/server.rs jftermd/tests/integration.rs
git commit -m "feat(server): accept loop, connection dispatch, control HELLO/LIST"
```

---

### Task 5: Session connection binding + reader/writer tasks — RISKY, independent review

**Files:**
- Modify: `jftermd/src/server.rs` (replace the stub `AttachOrOpen` arm; add `handle_session`, `session_task`, `reader_task`, `writer_task`)
- Modify: `jftermd/tests/integration.rs` (lifecycle tests + helpers)

**Design contract (independent review must verify):**
- `ATTACH_OR_OPEN` binding is race-free via `Registry::attach_or_create` (synchronous check-and-insert). On `Created`, the caller spawns `session_task`, which runs `Session::open` via `spawn_blocking` (B1 follow-up #1) before entering `actor_loop`; on open failure the task removes itself from the registry.
- One connection = a `reader_task` (socket → `Input`/`Resize`/`Close` commands; malformed frame → close) + a `writer_task` (out-queue → socket). The two are lifetime-coupled: when either ends, the other is aborted, fully tearing down the connection.
- **Detach is lazy and takeover-safe:** a closed socket is *not* signaled to the actor. The actor drops a client only when `try_send` on the *current* `out_tx` returns `Closed`/`Full`. Because takeover already replaced `out_tx`, a stale close from the old connection can never drop the new client.
- **Takeover:** a second `ATTACH_OR_OPEN` for a live id sends `Attach`, which replaces the actor's `out_tx`; the old `out_tx` drops → old `writer_task` ends → old socket shuts down → old client sees EOF → old `reader_task` aborted. `CLOSE` is never sent on detach/takeover, so the shell is not killed.

- [ ] **Step 1: Write the failing lifecycle tests + helpers**

Append to `jftermd/tests/integration.rs`. First extend the imports line for protocol to include `AttachOrOpen`:

```rust
use jftermd::protocol::{
    AttachOrOpen, Frame, FrameDecoder, FrameType, Hello, SessionInfo, PROTO_VERSION,
};
```

Then append these helpers and tests:

```rust
fn argv(parts: &[&str]) -> Vec<String> {
    parts.iter().map(|s| s.to_string()).collect()
}

fn attach_or_open(id: &str, argv: Vec<String>, want_chunks: usize, cols: u16, rows: u16) -> Frame {
    Frame::control(
        FrameType::AttachOrOpen,
        &AttachOrOpen {
            session_id: id.into(),
            cwd: "/".into(),
            argv,
            want_chunks,
            cols,
            rows,
        },
    )
    .unwrap()
}

fn contains(hay: &[u8], needle: &[u8]) -> bool {
    hay.windows(needle.len()).any(|w| w == needle)
}

/// Accumulate DATA payloads until `needle` appears or the deadline passes.
async fn recv_data_until(c: &mut Conn, needle: &[u8], timeout: Duration) -> Vec<u8> {
    let mut acc = Vec::new();
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        if remaining.is_zero() {
            return acc;
        }
        match c.recv(remaining).await {
            Some(f) if f.ty == FrameType::Data => {
                acc.extend_from_slice(&f.payload);
                if contains(&acc, needle) {
                    return acc;
                }
            }
            Some(_) => {}
            None => return acc,
        }
    }
}

async fn wait_until<F: Fn() -> bool>(timeout: Duration, cond: F) {
    let deadline = tokio::time::Instant::now() + timeout;
    while tokio::time::Instant::now() < deadline {
        if cond() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

#[tokio::test]
async fn open_drains_then_reattach_replays_scrollback() {
    let h = Harness::start(ServerOpts::default()).await;
    let mut c1 = h.connect().await;
    c1.send(&attach_or_open("s1", argv(&["sh", "-c", "echo MARKER; exec cat"]), 0, 80, 24))
        .await;
    let got = recv_data_until(&mut c1, b"MARKER", Duration::from_secs(3)).await;
    assert!(contains(&got, b"MARKER"), "first client missed live output");
    drop(c1);
    tokio::time::sleep(Duration::from_millis(150)).await;

    let mut c2 = h.connect().await;
    c2.send(&attach_or_open("s1", argv(&["true"]), 0, 80, 24)).await;
    let got = recv_data_until(&mut c2, b"MARKER", Duration::from_secs(3)).await;
    assert!(contains(&got, b"MARKER"), "reattach did not replay scrollback");
}

#[tokio::test]
async fn input_reaches_shell_and_output_returns() {
    let h = Harness::start(ServerOpts::default()).await;
    let mut c = h.connect().await;
    c.send(&attach_or_open("s2", argv(&["cat"]), 0, 80, 24)).await;
    c.send(&Frame::new(FrameType::Input, b"PINGPONG\n".to_vec())).await;
    let got = recv_data_until(&mut c, b"PINGPONG", Duration::from_secs(3)).await;
    assert!(contains(&got, b"PINGPONG"));
}

#[tokio::test]
async fn close_kills_and_drops_session() {
    let h = Harness::start(ServerOpts::default()).await;
    let mut c = h.connect().await;
    c.send(&attach_or_open("s3", argv(&["cat"]), 0, 80, 24)).await;
    wait_until(Duration::from_secs(3), || !h.registry.is_empty()).await;
    assert!(!h.registry.is_empty());
    c.send(&Frame::new(FrameType::Close, Vec::new())).await;
    wait_until(Duration::from_secs(3), || h.registry.is_empty()).await;
    assert!(h.registry.is_empty(), "CLOSE should drop the session");
}

#[tokio::test]
async fn socket_drop_detaches_but_keeps_shell_running() {
    let h = Harness::start(ServerOpts::default()).await;
    let mut c = h.connect().await;
    c.send(&attach_or_open("s4", argv(&["sh", "-c", "echo START; exec cat"]), 0, 80, 24))
        .await;
    let _ = recv_data_until(&mut c, b"START", Duration::from_secs(3)).await;
    drop(c);
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert!(!h.registry.is_empty(), "detach must not kill the shell");

    let mut c2 = h.connect().await;
    c2.send(&attach_or_open("s4", argv(&["true"]), 0, 80, 24)).await;
    let got = recv_data_until(&mut c2, b"START", Duration::from_secs(3)).await;
    assert!(contains(&got, b"START"));
}

#[tokio::test]
async fn second_attach_takes_over_and_kicks_first() {
    let h = Harness::start(ServerOpts::default()).await;
    let mut c1 = h.connect().await;
    c1.send(&attach_or_open("s5", argv(&["sh", "-c", "echo HI; exec cat"]), 0, 80, 24))
        .await;
    let _ = recv_data_until(&mut c1, b"HI", Duration::from_secs(3)).await;

    let mut c2 = h.connect().await;
    c2.send(&attach_or_open("s5", argv(&["true"]), 0, 80, 24)).await;
    let _ = recv_data_until(&mut c2, b"HI", Duration::from_secs(3)).await;

    assert!(
        c1.recv(Duration::from_secs(3)).await.is_none(),
        "old client must be kicked on takeover"
    );
    c2.send(&Frame::new(FrameType::Input, b"AFTER\n".to_vec())).await;
    let got = recv_data_until(&mut c2, b"AFTER", Duration::from_secs(3)).await;
    assert!(contains(&got, b"AFTER"));
}
```

- [ ] **Step 2: Run it and confirm it fails**

Run: `source "$HOME/.cargo/env" && cargo test -p jftermd --test integration open_drains 2>&1 | tail -25`
Expected: FAIL — the new tests hang/timeout or assert false because the `AttachOrOpen` arm is still the Task-4 stub that closes the connection.

- [ ] **Step 3: Implement session binding + reader/writer + open wrapper**

Extend the `server.rs` imports:

```rust
use crate::protocol::{AttachOrOpen, Resize};
use crate::registry::Bind;
use crate::session::Session;
```

(Adjust the existing `use crate::session::{Lifecycle, Session};` to keep both: `use crate::session::{Lifecycle, Session};` — do not duplicate the `Session` import.)

Replace the stub `AttachOrOpen` arm in `dispatch_connection` with:

```rust
        FrameType::AttachOrOpen => handle_session(first, rh, wh, registry, opts).await,
```

Add these functions above the `#[cfg(test)]` module:

```rust
/// Bind a session connection: attach-or-open, then run reader+writer until the
/// socket (or a takeover) tears the connection down. Never sends CLOSE on plain
/// detach — only an explicit `CLOSE` frame kills the shell.
async fn handle_session(
    first: Frame,
    rh: OwnedReadHalf,
    wh: OwnedWriteHalf,
    registry: Arc<Registry>,
    opts: ServerOpts,
) -> io::Result<()> {
    let req: AttachOrOpen = first.json().map_err(json_io)?;
    let cmd_tx = match registry.attach_or_create(&req.session_id) {
        Bind::Existing(tx) => tx,
        Bind::Created { cmd_tx, cmd_rx } => {
            tokio::spawn(session_task(
                req.session_id.clone(),
                req.argv.clone(),
                req.cwd.clone(),
                req.cols,
                req.rows,
                cmd_rx,
                registry.clone(),
                opts.clone(),
            ));
            cmd_tx
        }
    };

    let (out_tx, out_rx) = mpsc::channel::<Frame>(opts.out_queue);
    let mut writer = tokio::spawn(writer_task(out_rx, wh));

    let attach = AttachRequest {
        want_chunks: req.want_chunks,
        cols: req.cols,
        rows: req.rows,
        out_tx,
    };
    if cmd_tx.send(SessionCommand::Attach(attach)).await.is_err() {
        // Actor ended (e.g. open failed) before we could attach.
        writer.abort();
        return Ok(());
    }

    let mut reader = tokio::spawn(reader_task(rh, cmd_tx));
    // Lifetime-couple the halves: when one ends, tear the whole connection down.
    tokio::select! {
        _ = &mut writer => { reader.abort(); }
        _ = &mut reader => { writer.abort(); }
    }
    Ok(())
}

/// Open the PTY off the event loop (B1 follow-up #1: `Pty::spawn` blocks), then
/// run the actor. Removes itself from the registry if the open fails.
async fn session_task(
    id: String,
    argv: Vec<String>,
    cwd: String,
    cols: u16,
    rows: u16,
    cmd_rx: mpsc::Receiver<SessionCommand>,
    registry: Arc<Registry>,
    opts: ServerOpts,
) {
    let open_id = id.clone();
    let opened =
        tokio::task::spawn_blocking(move || Session::open(open_id, argv, cwd, cols, rows)).await;
    let session = match opened {
        Ok(Ok(s)) => s,
        Ok(Err(e)) => {
            tracing::error!(%id, error = %e, "Session::open failed");
            registry.remove(&id);
            return;
        }
        Err(e) => {
            tracing::error!(%id, error = %e, "open task panicked");
            registry.remove(&id);
            return;
        }
    };
    actor_loop(session, id, cmd_rx, registry, opts).await;
}

/// Socket -> commands. Malformed frame closes the connection (detach).
async fn reader_task(mut rh: OwnedReadHalf, cmd_tx: mpsc::Sender<SessionCommand>) {
    let mut dec = FrameDecoder::new();
    let mut buf = [0u8; 65536];
    'outer: loop {
        let n = match rh.read(&mut buf).await {
            Ok(0) | Err(_) => break,
            Ok(n) => n,
        };
        dec.push(&buf[..n]);
        loop {
            match dec.next_frame() {
                Ok(Some(frame)) => match frame.ty {
                    FrameType::Input => {
                        if cmd_tx.send(SessionCommand::Input(frame.payload)).await.is_err() {
                            break 'outer;
                        }
                    }
                    FrameType::Resize => {
                        if let Ok(r) = frame.json::<Resize>() {
                            if cmd_tx
                                .send(SessionCommand::Resize {
                                    cols: r.cols,
                                    rows: r.rows,
                                })
                                .await
                                .is_err()
                            {
                                break 'outer;
                            }
                        }
                    }
                    FrameType::Close => {
                        let _ = cmd_tx.send(SessionCommand::Close).await;
                        break 'outer;
                    }
                    other => tracing::warn!(?other, "unexpected session frame; ignoring"),
                },
                Ok(None) => break,
                Err(e) => {
                    tracing::warn!(error = %e, "malformed frame; closing session conn");
                    break 'outer;
                }
            }
        }
    }
}

/// Out-queue -> socket. Ends when the queue closes (detach/takeover) or a write
/// fails (client gone); shuts the write half so the peer sees EOF.
async fn writer_task(mut out_rx: mpsc::Receiver<Frame>, mut wh: OwnedWriteHalf) {
    while let Some(frame) = out_rx.recv().await {
        if wh.write_all(&frame.encode()).await.is_err() {
            break;
        }
    }
    let _ = wh.shutdown().await;
}
```

- [ ] **Step 4: Run tests + check**

Run: `source "$HOME/.cargo/env" && just check && just test 2>&1 | tail -30`
Expected: PASS — all five lifecycle integration tests plus prior tests; clippy/fmt clean. (Real shells are spawned; allow a few seconds.)

- [ ] **Step 5: Commit**

```bash
source "$HOME/.cargo/env"
git add jftermd/src/server.rs jftermd/tests/integration.rs
git commit -m "feat(server): session connection binding, reader/writer, attach/open/takeover"
```

**Independent review after this task** (opus, fresh context): audit `handle_session`/`reader_task`/`writer_task` for the lazy-detach takeover-safety argument, partial-frame routing, malformed-frame handling, the reader/writer lifetime coupling (no leaked tasks), and the open-failure registry cleanup. Provide the full `server.rs` and `registry.rs`.

---

### Task 6: Backpressure + dead-while-detached integration coverage

**Files:**
- Modify: `jftermd/tests/integration.rs` (two socket-level tests)

These validate the actor's backpressure and dead-retention behavior (built in Task 3) over real sockets. No new production code is expected; if a test fails, the bug is in `actor_loop` — fix it there.

- [ ] **Step 1: Add `ExitMsg` to the integration imports**

```rust
use jftermd::protocol::{
    AttachOrOpen, ExitMsg, Frame, FrameDecoder, FrameType, Hello, SessionInfo, PROTO_VERSION,
};
```

- [ ] **Step 2: Write the tests**

Append to `jftermd/tests/integration.rs`:

```rust
#[tokio::test]
async fn backpressure_drops_stalled_client_without_disturbing_shell() {
    // Tiny out-queue so a non-reading client overflows quickly.
    let opts = ServerOpts {
        out_queue: 4,
        ..ServerOpts::default()
    };
    let h = Harness::start(opts).await;

    let mut c1 = h.connect().await;
    c1.send(&attach_or_open(
        "bp",
        argv(&["sh", "-c", "yes BLAH | head -c 2000000; exec cat"]),
        0,
        80,
        24,
    ))
    .await;

    // Do NOT read c1: the bounded out-queue overflows and the daemon drops it.
    tokio::time::sleep(Duration::from_millis(600)).await;
    assert!(!h.registry.is_empty(), "backpressure must not kill the shell");

    // c1 is force-detached: draining it reaches EOF.
    let mut hit_eof = false;
    for _ in 0..100000 {
        if c1.recv(Duration::from_secs(2)).await.is_none() {
            hit_eof = true;
            break;
        }
    }
    assert!(hit_eof, "stalled client should be force-detached (EOF)");

    // The shell stays responsive for a fresh client (cap replay to keep it fast).
    let mut c2 = h.connect().await;
    c2.send(&attach_or_open("bp", argv(&["true"]), 1, 80, 24)).await;
    c2.send(&Frame::new(FrameType::Input, b"ALIVE\n".to_vec())).await;
    let got = recv_data_until(&mut c2, b"ALIVE", Duration::from_secs(3)).await;
    assert!(contains(&got, b"ALIVE"), "shell unresponsive after backpressure drop");
}

#[tokio::test]
async fn shell_exit_while_detached_retains_dead_session_then_replays_exit() {
    let h = Harness::start(ServerOpts {
        dead_grace: Duration::from_secs(10),
        ..ServerOpts::default()
    })
    .await;

    let mut c = h.connect().await;
    c.send(&attach_or_open(
        "dead",
        argv(&["sh", "-c", "sleep 0.3; echo LASTLINE; exit 7"]),
        0,
        80,
        24,
    ))
    .await;
    // Detach BEFORE the shell exits.
    drop(c);
    tokio::time::sleep(Duration::from_millis(700)).await;
    assert!(!h.registry.is_empty(), "dead session must be retained");

    // Reattach: replay final output + EXIT{7}, then the session is dropped.
    let mut c2 = h.connect().await;
    c2.send(&attach_or_open("dead", argv(&["true"]), 0, 80, 24)).await;
    let mut saw_last = false;
    let mut exit_code = None;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(3);
    loop {
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        if remaining.is_zero() {
            break;
        }
        match c2.recv(remaining).await {
            Some(f) if f.ty == FrameType::Data && contains(&f.payload, b"LASTLINE") => {
                saw_last = true;
            }
            Some(f) if f.ty == FrameType::Exit => {
                let m: ExitMsg = f.json().unwrap();
                exit_code = Some(m.status);
                break;
            }
            Some(_) => {}
            None => break,
        }
    }
    assert!(saw_last, "reattach must replay final output");
    assert_eq!(exit_code, Some(7));
    wait_until(Duration::from_secs(3), || h.registry.is_empty()).await;
    assert!(h.registry.is_empty(), "dead session dropped after reattach replay");
}
```

- [ ] **Step 3: Run tests + check**

Run: `source "$HOME/.cargo/env" && just check && just test 2>&1 | tail -30`
Expected: PASS — both new tests plus all prior tests; clippy/fmt clean.

- [ ] **Step 4: Commit**

```bash
source "$HOME/.cargo/env"
git add jftermd/tests/integration.rs
git commit -m "test(server): backpressure drop + dead-while-detached reattach coverage"
```

---

### Task 7: Idle self-exit watcher

**Files:**
- Modify: `jftermd/src/registry.rs` (pulse the change-notify on insert too)
- Modify: `jftermd/src/server.rs` (add `wait_until_idle`)

The daemon exits a short grace after its **last** session ends — but must not exit on the just-started empty state, so the watcher first waits for the first session to appear. `main.rs` (Task 9) selects this future against `run`.

- [ ] **Step 1: Pulse the notify on insert**

In `jftermd/src/registry.rs`, in `attach_or_create`'s `Created` branch, pulse after inserting so the watcher wakes when the first session appears:

```rust
        let (cmd_tx, cmd_rx) = mpsc::channel(256);
        map.insert(id.to_string(), SessionHandle { cmd_tx: cmd_tx.clone() });
        self.ended.notify_one();
        Bind::Created { cmd_tx, cmd_rx }
```

- [ ] **Step 2: Write the failing watcher tests**

Append to the `tests` module in `jftermd/src/server.rs`:

```rust
    #[tokio::test]
    async fn idle_watcher_waits_for_first_session_then_resolves_when_empty() {
        let reg = Registry::new();
        let watcher = tokio::spawn(wait_until_idle(reg.clone(), Duration::from_millis(50)));
        // Empty at start: must NOT resolve (a freshly-spawned daemon stays up).
        tokio::time::sleep(Duration::from_millis(200)).await;
        assert!(!watcher.is_finished(), "must not exit before any session exists");
        // A session appears, then ends -> watcher resolves after the grace.
        let _ = reg.attach_or_create("s1");
        tokio::time::sleep(Duration::from_millis(20)).await;
        reg.remove("s1");
        tokio::time::timeout(Duration::from_secs(2), watcher)
            .await
            .expect("watcher should resolve once idle")
            .unwrap();
    }

    #[tokio::test]
    async fn idle_watcher_does_not_resolve_while_a_session_lives() {
        let reg = Registry::new();
        let _keep = reg.attach_or_create("s1");
        let watcher = tokio::spawn(wait_until_idle(reg.clone(), Duration::from_millis(50)));
        tokio::time::sleep(Duration::from_millis(300)).await;
        assert!(!watcher.is_finished(), "must stay up while a session lives");
        reg.remove("s1");
        tokio::time::timeout(Duration::from_secs(2), watcher)
            .await
            .expect("resolves after removal")
            .unwrap();
    }
```

Add `use crate::registry::Registry;` is already present in the test module via Task 3? The Task-3 test module imports `Registry`. Keep it. `Duration` is imported there too.

- [ ] **Step 3: Run it and confirm it fails**

Run: `source "$HOME/.cargo/env" && cargo test -p jftermd idle_watcher 2>&1 | tail -20`
Expected: FAIL — `cannot find function wait_until_idle`.

- [ ] **Step 4: Implement `wait_until_idle`**

Add above the `#[cfg(test)]` module in `jftermd/src/server.rs`:

```rust
/// Resolve when the daemon should self-exit: after the first session has
/// existed and the registry has then stayed empty for `grace`. A
/// freshly-started (never-populated) registry never resolves.
pub async fn wait_until_idle(registry: Arc<Registry>, grace: Duration) {
    while registry.is_empty() {
        registry.wait_for_change().await;
    }
    loop {
        if registry.is_empty() {
            tokio::time::sleep(grace).await;
            // Re-check authoritatively: a session may have appeared during the grace.
            if registry.is_empty() {
                return;
            }
        }
        registry.wait_for_change().await;
    }
}
```

- [ ] **Step 5: Run tests + check**

Run: `source "$HOME/.cargo/env" && just check && just test 2>&1 | tail -30`
Expected: PASS — both watcher tests plus all prior; clippy/fmt clean.

- [ ] **Step 6: Commit**

```bash
source "$HOME/.cargo/env"
git add jftermd/src/registry.rs jftermd/src/server.rs
git commit -m "feat(server): idle self-exit watcher (exit grace after last session)"
```

---

### Task 8: Daemonization — flock lock, atomic bind, double-fork — RISKY, independent review

**Files:**
- Create: `jftermd/src/daemonize.rs`
- Modify: `jftermd/src/lib.rs` (add `pub mod daemonize;`)

**Design contract (independent review must verify):**
- Spawn race resolves to **one** daemon via a non-blocking `flock` on a lockfile: the winner binds the socket; a loser gets `AlreadyRunning` and exits so its client connects to the winner.
- Holding the lock means we are authoritative, so any existing socket file is **stale** (prior holder died) → unlink + rebind. (Client-side `connect → ECONNREFUSED → unlink + respawn` lives in JFTerm, out of scope here.)
- `daemonize()` is `setsid` + double-fork (so we can never reacquire a controlling terminal), `chdir("/")`, and std fds redirected to `/dev/null`. The held `Flock` must outlive the process (returned to `main`, kept on the stack).

- [ ] **Step 1: Declare the module**

In `jftermd/src/lib.rs` add `pub mod daemonize;`.

- [ ] **Step 2: Write the failing tests**

Create `jftermd/src/daemonize.rs` with the test module first:

```rust
//! Self-spawn support: an `flock` lockfile + atomic socket bind (stale-socket
//! cleanup) resolves spawn races to one daemon; `daemonize()` double-forks.

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn second_acquire_reports_already_running() {
        let dir = tempfile::tempdir().unwrap();
        let sock = dir.path().join("muxer.sock");
        let lock = dir.path().join("muxer.lock");
        let first = acquire_daemon(&sock, &lock).unwrap();
        assert!(matches!(first, Acquire::Bound { .. }));
        let second = acquire_daemon(&sock, &lock).unwrap();
        assert!(matches!(second, Acquire::AlreadyRunning));
        drop(first);
    }

    #[test]
    fn stale_socket_file_is_unlinked_and_rebound() {
        let dir = tempfile::tempdir().unwrap();
        let sock = dir.path().join("muxer.sock");
        let lock = dir.path().join("muxer.lock");
        std::fs::write(&sock, b"stale").unwrap();
        let acq = acquire_daemon(&sock, &lock).unwrap();
        assert!(matches!(acq, Acquire::Bound { .. }));
        // It is now a real socket a client can connect to.
        assert!(std::os::unix::net::UnixStream::connect(&sock).is_ok());
        drop(acq);
    }

    #[test]
    fn lock_releases_on_drop_allowing_reacquire() {
        let dir = tempfile::tempdir().unwrap();
        let sock = dir.path().join("muxer.sock");
        let lock = dir.path().join("muxer.lock");
        let first = acquire_daemon(&sock, &lock).unwrap();
        assert!(matches!(first, Acquire::Bound { .. }));
        drop(first); // releases flock + closes listener
        // A new acquire after release must bind again (stale socket unlinked).
        let again = acquire_daemon(&sock, &lock).unwrap();
        assert!(matches!(again, Acquire::Bound { .. }));
    }
}
```

- [ ] **Step 3: Run it and confirm it fails**

Run: `source "$HOME/.cargo/env" && cargo test -p jftermd daemonize:: 2>&1 | tail -20`
Expected: FAIL — `acquire_daemon` / `Acquire` not found.

- [ ] **Step 4: Implement flock + bind + double-fork**

Add above the `#[cfg(test)]` module in `jftermd/src/daemonize.rs`:

```rust
use std::fs::{File, OpenOptions};
use std::io::{self, ErrorKind};
use std::os::unix::fs::OpenOptionsExt;
use std::os::unix::net::UnixListener;
use std::path::Path;

use nix::fcntl::{Flock, FlockArg};

fn errno_io(e: nix::errno::Errno) -> io::Error {
    io::Error::from_raw_os_error(e as i32)
}

/// Outcome of trying to become the daemon.
pub enum Acquire {
    /// We won: hold the lock (must stay alive) and the bound listener.
    Bound { lock: Flock<File>, listener: UnixListener },
    /// Another daemon already holds the lock; the caller should exit.
    AlreadyRunning,
}

/// Try to take the exclusive lockfile without blocking.
fn try_flock(lock_path: &Path) -> io::Result<Option<Flock<File>>> {
    let file = OpenOptions::new()
        .create(true)
        .read(true)
        .write(true)
        .mode(0o600)
        .open(lock_path)?;
    match Flock::lock(file, FlockArg::LockExclusiveNonblock) {
        Ok(flock) => Ok(Some(flock)),
        // EWOULDBLOCK == EAGAIN on Linux; matching one covers both.
        Err((_f, nix::errno::Errno::EAGAIN)) => Ok(None),
        Err((_f, e)) => Err(errno_io(e)),
    }
}

/// Resolve the spawn race and bind the socket. Holding the lock makes us
/// authoritative, so a leftover socket file is stale → unlink + rebind.
pub fn acquire_daemon(sock_path: &Path, lock_path: &Path) -> io::Result<Acquire> {
    let lock = match try_flock(lock_path)? {
        Some(l) => l,
        None => return Ok(Acquire::AlreadyRunning),
    };
    let listener = match UnixListener::bind(sock_path) {
        Ok(l) => l,
        Err(e) if e.kind() == ErrorKind::AddrInUse => {
            std::fs::remove_file(sock_path).ok();
            UnixListener::bind(sock_path)?
        }
        Err(e) => return Err(e),
    };
    Ok(Acquire::Bound { lock, listener })
}

/// `setsid` + double-fork into a daemon; `chdir("/")`; std fds → `/dev/null`.
pub fn daemonize() -> io::Result<()> {
    use nix::unistd::{ForkResult, chdir, fork, setsid};

    // SAFETY: between fork and the next exec/_exit we touch only
    // async-signal-safe calls; the parent simply exits.
    match unsafe { fork() }.map_err(errno_io)? {
        ForkResult::Parent { .. } => std::process::exit(0),
        ForkResult::Child => {}
    }
    setsid().map_err(errno_io)?;
    match unsafe { fork() }.map_err(errno_io)? {
        ForkResult::Parent { .. } => std::process::exit(0),
        ForkResult::Child => {}
    }
    chdir("/").map_err(errno_io)?;
    redirect_std_to_devnull()?;
    Ok(())
}

fn redirect_std_to_devnull() -> io::Result<()> {
    use std::os::fd::AsRawFd;
    let devnull = OpenOptions::new().read(true).write(true).open("/dev/null")?;
    let fd = devnull.as_raw_fd();
    // SAFETY: dup2 onto the three std fds; fd is a valid open /dev/null.
    for target in 0..=2 {
        if unsafe { libc::dup2(fd, target) } < 0 {
            return Err(io::Error::last_os_error());
        }
    }
    Ok(())
}
```

- [ ] **Step 5: Run tests + check**

Run: `source "$HOME/.cargo/env" && just check && just test 2>&1 | tail -30`
Expected: PASS — the three `daemonize::` tests plus all prior; clippy/fmt clean. (`daemonize()` itself is exercised by the Task 9 subprocess smoke test.)

- [ ] **Step 6: Commit**

```bash
source "$HOME/.cargo/env"
git add jftermd/src/daemonize.rs jftermd/src/lib.rs
git commit -m "feat(daemonize): flock spawn-race resolution, stale-socket rebind, double-fork"
```

**Independent review after this task** (opus, fresh context): audit `daemonize.rs` for the flock OFD semantics (per-`open` lock conflict), the stale-socket unlink/rebind path, the double-fork + `setsid` ordering, fd redirection, and that the returned `Flock` is held for the process lifetime by `main`.

---

### Task 9: `main.rs` wiring — CLI, tracing, daemonize/foreground, end-to-end smoke

**Files:**
- Modify: `jftermd/src/main.rs` (replace the Task-0 stub)
- Modify: `jftermd/tests/integration.rs` (subprocess smoke test of the real binary)

`main` parses `--foreground`/`--socket`, inits tracing, ensures the socket dir, daemonizes unless `--foreground`, resolves the spawn race, binds, runs the server until idle or signalled, and unlinks the socket on exit.

- [ ] **Step 1: Write the failing smoke test**

Append to `jftermd/tests/integration.rs`:

```rust
#[tokio::test]
async fn binary_foreground_serves_a_session_then_self_exits_when_idle() {
    let tmp = tempfile::tempdir().unwrap();
    let sock = tmp.path().join("muxer.sock");
    let bin = env!("CARGO_BIN_EXE_jftermd");

    let mut child = std::process::Command::new(bin)
        .arg("--foreground")
        .arg("--socket")
        .arg(&sock)
        .spawn()
        .expect("spawn jftermd");

    // Wait for the socket to appear.
    wait_until(Duration::from_secs(5), || sock.exists()).await;

    let mut c = Conn {
        stream: UnixStream::connect(&sock).await.expect("connect"),
        dec: FrameDecoder::new(),
    };
    c.send(&attach_or_open("smoke", argv(&["sh", "-c", "echo SMOKED; exec cat"]), 0, 80, 24))
        .await;
    let got = recv_data_until(&mut c, b"SMOKED", Duration::from_secs(5)).await;
    assert!(contains(&got, b"SMOKED"), "binary did not serve session output");

    c.send(&Frame::new(FrameType::Close, Vec::new())).await;
    drop(c);

    // Idle after the last session ends -> the daemon self-exits within exit_grace.
    let mut exited = None;
    for _ in 0..150 {
        if let Some(status) = child.try_wait().expect("try_wait") {
            exited = Some(status);
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    match exited {
        Some(status) => assert!(status.success(), "daemon exited non-zero: {status:?}"),
        None => {
            let _ = child.kill();
            panic!("daemon did not self-exit when idle");
        }
    }
}
```

- [ ] **Step 2: Run it and confirm it fails**

Run: `source "$HOME/.cargo/env" && cargo test -p jftermd --test integration binary_foreground 2>&1 | tail -25`
Expected: FAIL — the stub `main` prints "not yet wired up" and exits 1, so the socket never appears.

- [ ] **Step 3: Implement `main.rs`**

Replace the entire contents of `jftermd/src/main.rs`:

```rust
//! jftermd entry: parse CLI, init tracing, daemonize-or-foreground, run server.

use std::path::PathBuf;
use std::process::ExitCode;

use jftermd::daemonize::{self, Acquire};
use jftermd::registry::Registry;
use jftermd::server::{ServerOpts, run, wait_until_idle};
use jftermd::socket;

struct Args {
    foreground: bool,
    socket: Option<PathBuf>,
}

fn parse_args() -> Args {
    let mut args = Args {
        foreground: false,
        socket: None,
    };
    let mut it = std::env::args().skip(1);
    while let Some(arg) = it.next() {
        match arg.as_str() {
            "--foreground" | "-f" => args.foreground = true,
            "--socket" => args.socket = it.next().map(PathBuf::from),
            other => eprintln!("jftermd: ignoring unknown argument {other}"),
        }
    }
    args
}

fn init_tracing() {
    use tracing_subscriber::{EnvFilter, fmt};
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    let _ = fmt()
        .with_env_filter(filter)
        .with_writer(std::io::stderr)
        .try_init();
}

async fn shutdown_signal() {
    use tokio::signal::unix::{SignalKind, signal};
    let mut term = signal(SignalKind::terminate()).expect("install SIGTERM handler");
    let mut intr = signal(SignalKind::interrupt()).expect("install SIGINT handler");
    tokio::select! {
        _ = term.recv() => {}
        _ = intr.recv() => {}
    }
}

fn main() -> ExitCode {
    let args = parse_args();
    init_tracing();

    let sock = args.socket.unwrap_or_else(socket::default_socket_path);
    if let Err(e) = socket::ensure_socket_dir(&sock) {
        eprintln!("jftermd: cannot create socket dir: {e}");
        return ExitCode::FAILURE;
    }
    let lock = sock.with_extension("lock");

    if !args.foreground {
        if let Err(e) = daemonize::daemonize() {
            eprintln!("jftermd: daemonize failed: {e}");
            return ExitCode::FAILURE;
        }
    }

    let acq = match daemonize::acquire_daemon(&sock, &lock) {
        Ok(a) => a,
        Err(e) => {
            tracing::error!(error = %e, "failed to acquire daemon lock/socket");
            return ExitCode::FAILURE;
        }
    };
    let (listener, _lock) = match acq {
        Acquire::Bound { listener, lock } => (listener, lock),
        Acquire::AlreadyRunning => {
            tracing::info!("another daemon already running; exiting");
            return ExitCode::SUCCESS;
        }
    };

    if let Err(e) = socket::restrict_socket_perms(&sock) {
        tracing::warn!(error = %e, "could not chmod socket to 0600");
    }
    if let Err(e) = listener.set_nonblocking(true) {
        tracing::error!(error = %e, "could not set listener non-blocking");
        return ExitCode::FAILURE;
    }

    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("build tokio runtime");

    rt.block_on(async move {
        let registry = Registry::new();
        let opts = ServerOpts::default();
        let listener =
            tokio::net::UnixListener::from_std(listener).expect("adopt std listener into tokio");
        tokio::select! {
            _ = run(listener, registry.clone(), opts.clone()) => {}
            _ = wait_until_idle(registry.clone(), opts.exit_grace) => {
                tracing::info!("idle; shutting down");
            }
            _ = shutdown_signal() => {
                tracing::info!("signal received; shutting down");
            }
        }
    });

    let _ = std::fs::remove_file(&sock);
    // `_lock` (the held Flock) drops here, releasing the lockfile.
    ExitCode::SUCCESS
}
```

- [ ] **Step 4: Run tests + check**

Run: `source "$HOME/.cargo/env" && just check && just test 2>&1 | tail -30`
Expected: PASS — the subprocess smoke test plus all prior tests; clippy/fmt clean. The smoke test may take ~6-8s (idle exit grace is 5s).

- [ ] **Step 5: Manual sanity (optional but recommended)**

```bash
source "$HOME/.cargo/env"
cargo build -p jftermd
XDG_RUNTIME_DIR=/tmp/jf-manual ./target/debug/jftermd --foreground &
sleep 1
ls -la /tmp/jf-manual/jfterm/   # expect muxer.sock at 0600, dir 0700
kill %1 2>/dev/null; rm -rf /tmp/jf-manual
```

- [ ] **Step 6: Commit**

```bash
source "$HOME/.cargo/env"
git add jftermd/src/main.rs jftermd/tests/integration.rs
git commit -m "feat(jftermd): main entry — CLI, tracing, daemonize/foreground, idle exit"
```

---

## Final verification (run before finishing the branch)

- [ ] Full green gate from a clean build:

```bash
source "$HOME/.cargo/env"
cargo clean -p jftermd
just check && just test 2>&1 | tail -40
```

Expected: `just check` clean (fmt + clippy `-D warnings`); all core, oracle, jftermd unit, and integration tests pass.

- [ ] Confirm no `target/`, `continue.md`, or `.swp` artifacts are staged in any commit on this branch (`git log --stat origin/master..HEAD | grep -E 'continue|\.swp|target/'` should be empty).

---

## Spec coverage self-check (writing-plans self-review)

- Wire protocol HELLO/HELLO_OK/LIST/SESSIONS → Task 4; ATTACH_OR_OPEN/INPUT/RESIZE/CLOSE/DATA/STATUS/EXIT → Tasks 3 & 5.
- Attach handshake (winsize, prologue+data, STATUS, SIGWINCH, live flow) → `attach_client` (Task 3); note the documented step-1/step-5 collapse.
- Backpressure (bounded out-queue, drop client, shell unaffected) → Task 3 + Task 6 test.
- Most-recent-wins takeover → Task 5 (`handle_session` + actor `Attach`), tested Task 5.
- Detach without kill (socket drop) → Task 5 lazy-detach, tested Task 5.
- Dead-session retention + reattach replays final output + EXIT, then drop → Task 3 actor + Task 6 test.
- Daemon exits after last session (grace) → Task 7 + Task 9.
- Spawn race → one daemon; stale socket cleanup → Task 8.
- Socket security (0700 dir / 0600 socket) → Task 1 + Task 9.
- Malformed frame closes connection; proto mismatch rejected → Task 4 (control) + Task 5 (`reader_task`).
- **Deliberate deviation:** no `tokio::signal` SIGCHLD reaper — reaping stays in `drain()`/`close()` (see the reaping decision at the top). Flag at plan review.
- **B1 follow-ups folded in:** #1 `spawn_blocking` for `Session::open` (Task 5) and `Session::close` (Task 3); #3 encoder `debug_assert` + `frame_data` (Task 0); #2 resolved by not adding a second reaper; #4 `close()` blocking spin handled via `spawn_blocking` (Task 3).

---

## Execution notes (for the controller)

- Branch `daemon-b2` is already created. Execute via **superpowers:subagent-driven-development**: fresh subagent per task, sonnet for mechanical tasks (0, 1, 2, 6, 7), opus for the risky ones (3, 5, 8). Pass the FULL task text in each dispatch; always include the cargo PATH gotcha (`source "$HOME/.cargo/env"`).
- **Independent opus review** after Tasks 3, 5, 8 (and a whole-crate diff review at the end), per the markers above. Trust-but-verify every subagent: run `just check && cargo test` yourself and read the key diffs before marking a task done.
- Finish with **superpowers:finishing-a-development-branch** (merge to `master`, re-run the gate on the merged result, delete the branch). There is a pending `claude-notify` to ping the user when the long work finishes.
