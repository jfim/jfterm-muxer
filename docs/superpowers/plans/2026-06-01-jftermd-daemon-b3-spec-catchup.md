# jftermd Daemon B3 — spec catch-up (CLOSE escalation + status fallback) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Bring `jftermd` up to the revised muxer spec: `CLOSE{grace_ms}` with a daemon-owned SIGHUP→SIGKILL escalation, and a `tcgetpgrp` `running` fallback for shells without OSC 133 prompt marking. (Delta 1, `COLORTERM=truecolor`, is already landed.)

**Architecture:** Both deltas extend the per-session actor (`server.rs::actor_loop`). **Delta 2** replaces the blocking `Session::close()` call with a non-blocking, EOF-driven close: on `CLOSE{grace_ms}` the actor SIGHUPs the shell group, marks the session `closing`, and arms a kill-deadline; it keeps draining (reaping via PTY EOF as usual), and if the child is still alive at the deadline it escalates to `killpg(SIGKILL)`. The session is **kept in the registry until it is actually reaped** (so the daemon's idle self-exit can't fire mid-reap and orphan the child). **Delta 3** adds an attach-gated poll timer: while a client is attached and no OSC 133 marker has been seen, the actor polls `tcgetpgrp(master) != shell_pgid` and merges that into the `running` it sends; the first OSC 133 marker (any subtype) permanently disables the poll, after which the engine's `running` is authoritative.

**Tech Stack:** Rust 2024, `tokio`, `nix` 0.31 (`killpg`, `tcgetpgrp`), building on the merged B1/B2 `jftermd` + `jftermd-core`.

**Design decisions baked in (do not relitigate):**
- **Reaping stays EOF-driven** (no SIGCHLD reaper). The escalation trigger is "the session is not yet `Dead` at the kill-deadline"; the action is `killpg(SIGKILL)` the whole group (which also cleans up a grandchild holding the PTY). This keeps reaping single-owner and per-session, preserving EXIT-status correctness.
- **Keep-until-`Dead`** for CLOSE: the session stays in the registry until reaped, so `wait_until_idle` waits for in-flight reapers for free. (Restart always mints a new `session_id`, so the old key staying briefly occupied never collides.)
- **`has_prompt_marking()` is an engine accessor**, not a field on `StatusSnapshot` — so the STATUS de-dup (`last_status != …`) stays uncontaminated. The latch fires on the **first OSC 133 of any subtype**.

---

## File Structure

- `jftermd/src/protocol.rs` — add `CloseMsg { grace_ms: u32 }`.
- `jftermd/src/pty.rs` — add `kill()` (SIGKILL killpg) and a `foreground_busy()` (`tcgetpgrp(master) != child`).
- `jftermd/src/session.rs` — add `hangup()`, `kill()`, `poll_running()`, `has_prompt_marking()` passthroughs.
- `jftermd/src/registry.rs` — `SessionCommand::Close` gains `{ grace_ms: u32 }`.
- `jftermd/src/server.rs` — actor: CLOSE escalation state machine (Delta 2) + attach-gated poll timer & effective-`running` merge (Delta 3); `ServerOpts` gains `status_poll`.
- `jftermd/tests/integration.rs` — CLOSE escalation tests + status-fallback test.
- `core/src/status.rs` — `StatusCache` latch (`saw_prompt_marker`).
- `core/src/scanner.rs` — set the latch on any OSC 133; expose `saw_prompt_marker()`.
- `core/src/engine.rs` — `ReplayEngine::has_prompt_marking()`.

## Environment note (every task)
`cargo`/`just` are NOT on PATH — prefix every shell command with `source "$HOME/.cargo/env" && …` (re-source each call). Commit gate: `just check && just test` (clippy `-D warnings`; apply idiomatic rewrites). Ignore stale rust-analyzer "file not found for module" diagnostics. Real-shell tests can flake under full-parallel `just test`; if one fails, re-run it in isolation before treating it as real.

---

### Task 1: CLOSE wire + signal primitives (additive, stays green)

**Files:** `jftermd/src/protocol.rs`, `jftermd/src/pty.rs`, `jftermd/src/session.rs`.

This task is purely additive — it adds `CloseMsg`, `Pty::kill`, and `Session::hangup`/`Session::kill` without touching `SessionCommand` or the actor, so the whole suite stays green. (The actor rework that *uses* these lands in Task 2.)

- [ ] **Step 1: `CloseMsg` + round-trip test**

In `jftermd/src/protocol.rs`, add next to the other payload structs (e.g. after `Resize`):

```rust
/// `Close` value — SIGHUP the shell, then SIGKILL after `grace_ms` if still
/// alive. `grace_ms == 0` = SIGHUP only (no escalation).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct CloseMsg {
    pub grace_ms: u32,
}
```

Append to the `#[cfg(test)] mod tests` block:

```rust
#[test]
fn close_msg_round_trips() {
    for grace_ms in [0u32, 1500, u32::MAX] {
        let msg = CloseMsg { grace_ms };
        let frame = Frame::control(FrameType::Close, &msg).unwrap();
        assert_eq!(frame.ty, FrameType::Close);
        assert_eq!(frame.json::<CloseMsg>().unwrap(), msg);
    }
}
```

- [ ] **Step 2: `Pty::kill` + test**

In `jftermd/src/pty.rs`, add a method to `impl Pty` next to `hangup`:

```rust
    /// SIGKILL the child's process group (CLOSE escalation).
    pub fn kill(&self) -> io::Result<()> {
        killpg(self.child, Signal::SIGKILL).map_err(io_err)
    }
```

(`killpg`, `Signal`, and `io_err` are already imported.) Append to `pty.rs`'s `#[cfg(test)] mod tests`:

```rust
    #[test]
    fn kill_terminates_and_reaps_as_sigkill() {
        let mut pty =
            Pty::spawn(&argv(&["cat"]), Path::new("/"), winsize(80, 24)).expect("spawn");
        pty.kill().expect("kill");
        // Drain to EOF, then reap: SIGKILL surfaces as 128 + 9 = 137.
        let start = Instant::now();
        loop {
            let d = pty.drain().expect("drain");
            if d.eof {
                break;
            }
            assert!(start.elapsed() < Duration::from_secs(3), "no EOF after SIGKILL");
            std::thread::sleep(Duration::from_millis(10));
        }
        let mut status = None;
        let start = Instant::now();
        while start.elapsed() < Duration::from_secs(3) {
            if let Some(s) = pty.try_reap().expect("reap") {
                status = Some(s);
                break;
            }
            std::thread::sleep(Duration::from_millis(10));
        }
        assert_eq!(status, Some(137));
    }
```

- [ ] **Step 3: `Session::hangup` / `Session::kill`**

In `jftermd/src/session.rs`, add to `impl Session` (near `close`):

```rust
    /// SIGHUP the shell's process group (no-op once dead).
    pub fn hangup(&self) -> io::Result<()> {
        if self.is_dead() {
            return Ok(());
        }
        self.pty.hangup()
    }

    /// SIGKILL the shell's process group (no-op once dead).
    pub fn kill(&self) -> io::Result<()> {
        if self.is_dead() {
            return Ok(());
        }
        self.pty.kill()
    }
```

- [ ] **Step 4: Gate + commit**

Run: `source "$HOME/.cargo/env" && just check && just test 2>&1 | tail -20` — all green (new `close_msg_round_trips` + `kill_terminates_and_reaps_as_sigkill`).

```bash
source "$HOME/.cargo/env"
git add jftermd/src/protocol.rs jftermd/src/pty.rs jftermd/src/session.rs
git commit -m "feat(close): add CloseMsg, Pty::kill, Session::hangup/kill (Delta 2 prep)"
```

---

### Task 2: CLOSE escalation in the actor — RISKY, independent review

**Files:** `jftermd/src/registry.rs`, `jftermd/src/server.rs`, `jftermd/tests/integration.rs`.

Reworks the actor's CLOSE handling from a blocking `Session::close()` to a non-blocking, EOF-driven escalation. **Design contract (reviewer verifies):** on `CLOSE{grace_ms}` the actor SIGHUPs the group, sets `closing = true`, and (if `grace_ms > 0`) arms a kill-deadline; it keeps draining; on the drain that detects the child's exit it forwards `EXIT` and **breaks** (→ `registry.remove`); if the kill-deadline fires while the child is still alive it `killpg(SIGKILL)`s the group once. No blocking call runs on the loop. The session stays in the registry until reaped (idle self-exit waits for it). Reaping stays EOF-driven — no SIGCHLD.

- [ ] **Step 1: `SessionCommand::Close` carries `grace_ms`**

In `jftermd/src/registry.rs`, change the `Close` variant:

```rust
    /// Kill the shell and drop the session (`CLOSE`): SIGHUP, then SIGKILL after
    /// `grace_ms` if still alive (`grace_ms == 0` = SIGHUP only).
    Close { grace_ms: u32 },
```

- [ ] **Step 2: Parse `CloseMsg` in `reader_task`**

In `jftermd/src/server.rs`, add `CloseMsg` to the protocol import, and change the `FrameType::Close` arm of `reader_task`:

```rust
                    FrameType::Close => {
                        // Empty or malformed payload defaults to grace_ms = 0.
                        let grace_ms = frame.json::<CloseMsg>().map(|m| m.grace_ms).unwrap_or(0);
                        let _ = cmd_tx.send(SessionCommand::Close { grace_ms }).await;
                        break 'outer;
                    }
```

- [ ] **Step 3: Rework the actor Close handling**

In `jftermd/src/server.rs::actor_loop`, add two state vars next to `grace_deadline`:

```rust
    // Set once a CLOSE arrives; the next exit-detecting drain ends the actor
    // (instead of arming dead-session retention).
    let mut closing = false;
    // Set on CLOSE{grace_ms>0}: SIGKILL-escalation deadline.
    let mut kill_deadline: Option<Instant> = None;
```

Replace the entire `Some(SessionCommand::Close) => { … }` arm with:

```rust
                    Some(SessionCommand::Close { grace_ms }) => {
                        // Force-close: SIGHUP the group now (non-blocking); if it
                        // is still alive at the deadline, escalate to SIGKILL.
                        // Reaping stays EOF-driven (the readable arm); we keep the
                        // session in the registry until reaped so idle self-exit
                        // can't fire mid-reap and orphan the child.
                        let _ = session.hangup();
                        closing = true;
                        if grace_ms > 0 {
                            kill_deadline =
                                Some(Instant::now() + Duration::from_millis(grace_ms as u64));
                        }
                        if session.is_dead() {
                            // Raced (already gone): tell any client and end now.
                            let code = match session.lifecycle() {
                                Lifecycle::Dead { status } => status,
                                Lifecycle::Live => 0,
                            };
                            if let Some(tx) = client.take() {
                                let _ = tx.try_send(exit_frame(code));
                            }
                            break;
                        }
                    }
```

In the readable arm, change the exit handling so a `closing` session ends instead of entering dead-retention:

```rust
                        if let Some(code) = outcome.exit {
                            push_or_drop(&mut client, exit_frame(code));
                            if closing {
                                break; // reaped after CLOSE -> end + registry.remove
                            }
                            grace_deadline = Some(Instant::now() + opts.dead_grace);
                        }
```

Add a fourth `select!` arm (after the existing `grace_deadline` arm) for the kill-deadline:

```rust
            () = async {
                match kill_deadline {
                    Some(d) => sleep_until(d).await,
                    None => std::future::pending::<()>().await,
                }
            }, if kill_deadline.is_some() => {
                // Grace elapsed; if still not reaped, force-kill the whole group.
                // Fire once — the readable arm then sees EOF -> reap -> break.
                if !session.is_dead() {
                    let _ = session.kill();
                }
                kill_deadline = None;
            }
```

- [ ] **Step 4: Update the actor unit test for the new `Close` shape**

In `server.rs`'s `#[cfg(test)] mod tests`, in `actor_attaches_forwards_input_and_closes`, change:

```rust
        cmd_tx.send(SessionCommand::Close { grace_ms: 0 }).await.unwrap();
```

- [ ] **Step 5: Integration tests (escalation + no-escalation)**

In `jftermd/tests/integration.rs`, add `CloseMsg` to the `jftermd::protocol` import, and add a helper + two tests:

```rust
fn close_frame(grace_ms: u32) -> Frame {
    Frame::control(FrameType::Close, &CloseMsg { grace_ms }).unwrap()
}

#[tokio::test]
async fn close_grace_zero_reaps_without_escalation() {
    let h = Harness::start(ServerOpts::default()).await;
    let mut c = h.connect().await;
    c.send(&attach_or_open("cz", argv(&["cat"]), 0, 80, 24)).await;
    wait_until(Duration::from_secs(3), || !h.registry.is_empty()).await;
    c.send(&close_frame(0)).await; // SIGHUP only; cat dies on SIGHUP
    wait_until(Duration::from_secs(3), || h.registry.is_empty()).await;
    assert!(h.registry.is_empty(), "CLOSE{{0}} should reap a SIGHUP-dying shell");
}

#[tokio::test]
async fn close_escalates_to_sigkill_when_child_ignores_sighup() {
    let h = Harness::start(ServerOpts::default()).await;
    let mut c = h.connect().await;
    // Ignores SIGHUP and runs forever -> only SIGKILL can end it.
    c.send(&attach_or_open(
        "esc",
        argv(&["sh", "-c", "trap \"\" HUP; while :; do sleep 1; done"]),
        0,
        80,
        24,
    ))
    .await;
    wait_until(Duration::from_secs(3), || !h.registry.is_empty()).await;
    c.send(&close_frame(300)).await;
    // Without escalation this never empties; with it, ~300ms later.
    wait_until(Duration::from_secs(4), || h.registry.is_empty()).await;
    assert!(
        h.registry.is_empty(),
        "SIGKILL escalation should reap a HUP-ignoring child"
    );
}
```

(The existing `close_kills_and_drops_session` test sends an empty `Close` frame; the new reader parses that as `grace_ms = 0`, so it keeps passing unchanged.)

- [ ] **Step 6: Gate + commit**

Run: `source "$HOME/.cargo/env" && just check && just test 2>&1 | tail -30` — all green. If a real-shell test flakes under parallel load, re-run it in isolation to confirm before treating it as a real failure.

```bash
source "$HOME/.cargo/env"
git add jftermd/src/registry.rs jftermd/src/server.rs jftermd/tests/integration.rs
git commit -m "feat(server): CLOSE{grace_ms} with EOF-driven SIGKILL escalation (Delta 2)"
```

**Independent review after this task** (opus, fresh context): audit the CLOSE state machine in `actor_loop` — that no blocking call runs on the loop, the `closing`/`kill_deadline` interaction with the readable and grace arms is race-free, the escalation fires exactly once and only when still alive, EXIT is delivered before the break, and the keep-until-`Dead` registry behavior is correct for idle self-exit. Provide the full `server.rs` + `session.rs`/`pty.rs`.

---

### Task 3: OSC 133 latch in the core engine (Delta 3 part 1)

**Files:** `core/src/status.rs`, `core/src/scanner.rs`, `core/src/engine.rs`.

Exposes "has this shell ever emitted an OSC 133 prompt marker" so the daemon can permanently disable its `running` poll once prompt marking appears. Pure, fully unit-testable.

- [ ] **Step 1: `StatusCache` latch + test**

In `core/src/status.rs`, add a field to `StatusCache` and two methods. Change the struct:

```rust
pub struct StatusCache {
    running: bool,
    progress: Option<u8>,
    saw_prompt_marker: bool,
}
```

`new()` initializes it false:

```rust
    pub fn new() -> Self {
        Self {
            running: false,
            progress: None,
            saw_prompt_marker: false,
        }
    }
```

Add the methods inside `impl StatusCache`:

```rust
    pub fn set_prompt_marker_seen(&mut self) {
        self.saw_prompt_marker = true;
    }

    pub fn saw_prompt_marker(&self) -> bool {
        self.saw_prompt_marker
    }
```

Append to `status.rs`'s `#[cfg(test)] mod tests`:

```rust
    #[test]
    fn prompt_marker_latches() {
        let mut c = StatusCache::new();
        assert!(!c.saw_prompt_marker());
        c.set_prompt_marker_seen();
        assert!(c.saw_prompt_marker());
    }
```

- [ ] **Step 2: Latch on any OSC 133 in the scanner + accessor**

In `core/src/scanner.rs`, in the `Some("133")` OSC arm, latch the marker before the C/D match (so *any* subtype counts):

```rust
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
```

Add an accessor on `Scanner` next to `pub fn status(&self)`:

```rust
    /// Whether any OSC 133 prompt marker has been observed (latched).
    pub fn saw_prompt_marker(&self) -> bool {
        self.sink.status.saw_prompt_marker()
    }
```

- [ ] **Step 3: `ReplayEngine::has_prompt_marking` + tests**

In `core/src/engine.rs`, add to `impl ReplayEngine`:

```rust
    /// Whether the shell uses OSC 133 prompt marking (latched on the first
    /// marker of any subtype). Until true, the daemon supplies `running` from a
    /// tcgetpgrp poll.
    pub fn has_prompt_marking(&self) -> bool {
        self.scanner.saw_prompt_marker()
    }
```

Append to `engine.rs`'s `#[cfg(test)] mod tests`:

```rust
    #[test]
    fn has_prompt_marking_latches_on_first_osc133_any_subtype() {
        let mut e = ReplayEngine::new();
        assert!(!e.has_prompt_marking());
        // 133;A (prompt start) is not C/D but must still latch.
        e.feed(b"\x1b]133;A\x07");
        assert!(e.has_prompt_marking());
    }

    #[test]
    fn no_prompt_marking_without_osc133() {
        let mut e = ReplayEngine::new();
        e.feed(b"plain output\r\n");
        assert!(!e.has_prompt_marking());
    }
```

- [ ] **Step 4: Gate + commit**

Run: `source "$HOME/.cargo/env" && just check && just test 2>&1 | tail -20` — all green.

```bash
source "$HOME/.cargo/env"
git add core/src/status.rs core/src/scanner.rs core/src/engine.rs
git commit -m "feat(core): latch OSC 133 prompt marking, expose has_prompt_marking (Delta 3)"
```

---

### Task 4: tcgetpgrp running fallback in the daemon — RISKY, independent review

**Files:** `jftermd/src/pty.rs`, `jftermd/src/session.rs`, `jftermd/src/server.rs`, `jftermd/tests/integration.rs`.

Adds the attach-gated `tcgetpgrp` poll and the effective-`running` merge. **Design contract (reviewer verifies):** the poll runs only while a client is attached, the session is live, and `has_prompt_marking()` is false; the effective `running` is `has_marking ? engine.running : poll_running`; `progress` is always the engine's; the STATUS de-dup (`last_status`) applies to the effective snapshot; both the drain path and the poll path push through the same merge; the poll permanently stops once a 133 marker latches.

- [ ] **Step 1: `Pty::foreground_busy` + unit test**

In `jftermd/src/pty.rs`, add `tcgetpgrp` to the `nix::unistd` import, and a method to `impl Pty`:

```rust
    /// True when a foreground process group other than the shell itself owns the
    /// terminal — i.e. a command is running. The `running` fallback for shells
    /// without OSC 133. (Requires an interactive, job-control shell to ever be
    /// true; a non-job-control `sh -c` keeps everything in the leader's group.)
    pub fn foreground_busy(&self) -> io::Result<bool> {
        let fg = tcgetpgrp(&self.master).map_err(io_err)?;
        Ok(fg != self.child)
    }
```

Append to `pty.rs`'s tests (deterministic half — a shell sitting at its prompt is *not* busy):

```rust
    #[test]
    fn foreground_not_busy_at_prompt() {
        let mut pty = Pty::spawn(
            &argv(&["bash", "--norc", "-i"]),
            Path::new("/"),
            winsize(80, 24),
        )
        .expect("spawn");
        // Let bash take the terminal and print its prompt.
        let _ = read_until(&mut pty, b"$", Duration::from_secs(3));
        std::thread::sleep(Duration::from_millis(200));
        assert!(
            !pty.foreground_busy().expect("tcgetpgrp"),
            "a shell at its prompt should not be foreground-busy"
        );
        pty.hangup().ok();
    }
```

- [ ] **Step 2: `Session` passthroughs**

In `jftermd/src/session.rs`, add to `impl Session`:

```rust
    /// `running` fallback via tcgetpgrp (false once dead).
    pub fn poll_running(&self) -> bool {
        if self.is_dead() {
            return false;
        }
        self.pty.foreground_busy().unwrap_or(false)
    }

    /// Whether the shell uses OSC 133 prompt marking (engine latch).
    pub fn has_prompt_marking(&self) -> bool {
        self.engine.has_prompt_marking()
    }
```

- [ ] **Step 3: `ServerOpts.status_poll` + pure merge helper + test**

In `jftermd/src/server.rs`, add a field to `ServerOpts`:

```rust
    /// Interval for the tcgetpgrp `running` fallback poll (non-OSC-133 shells).
    pub status_poll: Duration,
```

and in its `Default`:

```rust
            status_poll: Duration::from_millis(300),
```

Add a pure merge helper above the actor:

```rust
/// Effective `running`: the engine's value once the shell has shown OSC 133
/// prompt marking, otherwise the tcgetpgrp poll value.
fn merge_running(has_marking: bool, engine_running: bool, poll_running: bool) -> bool {
    if has_marking {
        engine_running
    } else {
        poll_running
    }
}

/// Build the STATUS snapshot the client should see: merged `running`, engine
/// `progress`.
fn effective_status(session: &Session, poll_running: bool) -> StatusSnapshot {
    let snap = session.status();
    StatusSnapshot {
        running: merge_running(session.has_prompt_marking(), snap.running, poll_running),
        progress: snap.progress,
    }
}
```

Append to `server.rs`'s `#[cfg(test)] mod tests`:

```rust
    #[test]
    fn merge_running_prefers_engine_when_marked_else_poll() {
        assert!(super::merge_running(true, true, false)); // marked -> engine
        assert!(!super::merge_running(true, false, true)); // marked -> engine
        assert!(super::merge_running(false, false, true)); // unmarked -> poll
        assert!(!super::merge_running(false, true, false)); // unmarked -> poll
    }
```

- [ ] **Step 4: Wire the poll into the actor**

In `actor_loop`, add poll state after `last_status`:

```rust
    // Latest tcgetpgrp fallback result (used until OSC 133 prompt marking shows).
    let mut poll_running = false;
    let mut poll = tokio::time::interval(opts.status_poll);
    poll.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
```

In the `Attach` arm, refresh the poll value before recording `last_status`, so the at-attach STATUS is accurate for a non-133 shell. Replace the body of the `Attach` arm's post-`attach_client` lines with:

```rust
                    Some(SessionCommand::Attach(req)) => {
                        if !session.has_prompt_marking() {
                            poll_running = session.poll_running();
                        }
                        attach_client(&mut session, &mut client, poll_running, req);
                        last_status = Some(effective_status(&session, poll_running));
                        if session.is_dead() {
                            break;
                        }
                    }
```

Update `attach_client` to take `poll_running` and send the effective status:

```rust
fn attach_client(
    session: &mut Session,
    client: &mut Option<mpsc::Sender<Frame>>,
    poll_running: bool,
    req: AttachRequest,
) {
    let _ = session.resize(req.cols, req.rows);
    let replay = session.replay_for_attach(req.want_chunks);
    for f in frame_data(&replay.data) {
        let _ = req.out_tx.try_send(f);
    }
    let _ = req.out_tx.try_send(status_frame(effective_status(session, poll_running)));
    if let Lifecycle::Dead { status } = session.lifecycle() {
        let _ = req.out_tx.try_send(exit_frame(status));
    }
    *client = Some(req.out_tx);
}
```

In the readable (drain) arm, replace the STATUS push with the effective merge:

```rust
                        let eff = effective_status(&session, poll_running);
                        if last_status != Some(eff) {
                            push_or_drop(&mut client, status_frame(eff));
                            last_status = Some(eff);
                        }
```

Add the poll `select!` arm (after the kill-deadline arm), attach-gated and disabled once 133 latches:

```rust
            _ = poll.tick(), if client.is_some()
                && !session.is_dead()
                && !session.has_prompt_marking() =>
            {
                poll_running = session.poll_running();
                let eff = effective_status(&session, poll_running);
                if last_status != Some(eff) {
                    push_or_drop(&mut client, status_frame(eff));
                    last_status = Some(eff);
                }
            }
```

- [ ] **Step 5: Integration test (real interactive shell)**

In `jftermd/tests/integration.rs`, add `StatusMsg` to the `jftermd::protocol` import, and a helper + test:

```rust
async fn recv_status_until(c: &mut Conn, want_running: bool, timeout: Duration) -> bool {
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        if remaining.is_zero() {
            return false;
        }
        match c.recv(remaining).await {
            Some(f) if f.ty == FrameType::Status => {
                if let Ok(m) = f.json::<StatusMsg>() {
                    if m.running == want_running {
                        return true;
                    }
                }
            }
            Some(_) => {}
            None => return false,
        }
    }
}

#[tokio::test]
async fn running_falls_back_to_tcgetpgrp_without_osc133() {
    let opts = ServerOpts {
        status_poll: Duration::from_millis(100),
        ..ServerOpts::default()
    };
    let h = Harness::start(opts).await;
    let mut c = h.connect().await;
    // bash --norc -i: job control on, no OSC 133 -> the poll drives `running`.
    c.send(&attach_or_open("poll", argv(&["bash", "--norc", "-i"]), 0, 80, 24))
        .await;
    // Drain the prompt, then run a foreground command.
    let _ = recv_data_until(&mut c, b"$", Duration::from_secs(3)).await;
    c.send(&Frame::new(FrameType::Input, b"sleep 0.6\n".to_vec())).await;
    assert!(
        recv_status_until(&mut c, true, Duration::from_secs(3)).await,
        "fallback should report running=true while `sleep` is foreground"
    );
    assert!(
        recv_status_until(&mut c, false, Duration::from_secs(3)).await,
        "fallback should report running=false back at the prompt"
    );
}
```

(This test needs an interactive, job-control `bash`; it is the real end-to-end check. The load-bearing correctness is covered by the pure `merge_running` test and the core latch tests, so if the environment lacks job-control bash the executor should flag it rather than weaken the logic.)

- [ ] **Step 6: Gate + commit**

Run: `source "$HOME/.cargo/env" && just check && just test 2>&1 | tail -30` — all green. Re-run any flaky real-shell test in isolation before treating it as a real failure.

```bash
source "$HOME/.cargo/env"
git add jftermd/src/pty.rs jftermd/src/session.rs jftermd/src/server.rs jftermd/tests/integration.rs
git commit -m "feat(server): attach-gated tcgetpgrp running fallback + effective merge (Delta 3)"
```

**Independent review after this task** (opus, fresh context): audit the poll arm gating (attach + live + no-133), the effective-`running` merge across the attach/drain/poll paths, the de-dup correctness, that the poll truly pauses while detached and stops permanently after 133, and that the interval's missed-tick behavior can't busy-spin. Provide the full `server.rs` + `session.rs`/`pty.rs`.

---

## Final verification & self-check

- [ ] Clean gate: `source "$HOME/.cargo/env" && cargo clean -p jftermd && just check && just test`.
- [ ] Spec coverage: Delta 1 (COLORTERM, already landed) ✓; Delta 2 (`CLOSE{grace_ms}` + EOF-driven SIGKILL escalation, keep-until-`Dead`) → Tasks 1–2; Delta 3 (`has_prompt_marking` latch + attach-gated tcgetpgrp fallback + effective merge) → Tasks 3–4.
- [ ] Finish with **superpowers:finishing-a-development-branch** (merge `daemon-b3` to `master`, re-verify on the merged result, delete the branch).

## Execution notes
- Branch `daemon-b3` exists; Delta 1 is already committed there.
- Subagent-driven: sonnet for Tasks 1 & 3 (mechanical/additive), opus for Tasks 2 & 4 (actor-loop reworks). Independent opus review after Tasks 2 and 4, plus a whole-crate diff review at the end. Always pass the cargo PATH gotcha.
- Trust-but-verify every subagent: run `just check && cargo test` yourself and read the diffs before marking a task done.
