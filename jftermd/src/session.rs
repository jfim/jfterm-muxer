//! A daemon `Session`: one shell PTY plus the replayable engine and lifecycle.
//! Synchronous and socket-free — the B2 server drives `drain` off the loop and
//! frames the outputs onto the wire.

use std::io;
use std::path::PathBuf;
use std::time::{SystemTime, UNIX_EPOCH};

use jftermd_core::{ReplayEngine, StatusSnapshot};
use nix::pty::Winsize;

use crate::protocol::SessionInfo;
use crate::pty::{Pty, winsize};

/// Whether the shell child is still alive.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Lifecycle {
    Live,
    Dead { status: i32 },
}

/// What a `drain` produced, for the server to act on.
#[derive(Debug)]
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
            // EOF means the shell child is terminating; spin the reap until a
            // terminal WaitStatus is observed so the real exit code is reported
            // rather than fabricating 0 from a transient StillAlive/ECHILD.
            // `None` only when the child was already reaped elsewhere (e.g. the
            // Pty Drop reaper raced this path); we surface 0 in that single
            // genuinely-unrecoverable case rather than blocking forever.
            let status = self.pty.reap_blocking()?.unwrap_or(0);
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
    #[must_use]
    pub fn replay_for_attach(&self, want_chunks: usize) -> AttachReplay {
        let data = if want_chunks == 0 {
            self.engine.replay()
        } else {
            self.engine.replay_capped(want_chunks)
        };
        AttachReplay {
            data,
            status: self.engine.status(),
        }
    }

    pub fn status(&self) -> StatusSnapshot {
        self.engine.status()
    }

    /// `running` fallback via tcgetpgrp (false once dead).
    #[must_use]
    pub fn poll_running(&self) -> bool {
        if self.is_dead() {
            return false;
        }
        self.pty.foreground_busy().unwrap_or(false)
    }

    /// Effective/merged `running`, matching the semantics the server uses for
    /// STATUS frames: the engine's OSC 133 value once prompt marking has been
    /// seen, otherwise the tcgetpgrp poll fallback. A dead session is never
    /// running.
    pub fn effective_running(&self) -> bool {
        if self.is_dead() {
            return false;
        }
        if self.has_prompt_marking() {
            self.engine.status().running
        } else {
            self.poll_running()
        }
    }

    /// Whether the shell uses OSC 133 prompt marking (engine latch).
    pub fn has_prompt_marking(&self) -> bool {
        self.engine.has_prompt_marking()
    }

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

    /// Snapshot for the `List`/`Sessions` control reply.
    #[must_use]
    pub fn info(&self, has_client: bool) -> SessionInfo {
        SessionInfo {
            session_id: self.id.clone(),
            argv: self.argv.clone(),
            cwd: self.cwd.to_string_lossy().into_owned(),
            running: self.effective_running(),
            has_client,
            created_at: self.created_at,
        }
    }
}

fn now_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock before UNIX_EPOCH")
        .as_secs()
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
        let mut s = Session::open(
            "t1",
            argv(&["sh", "-c", "echo hello; sleep 0.2"]),
            "/",
            80,
            24,
        )
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
        // `info.running` now reflects effective/merged running semantics, not
        // mere liveness: `cat` is exec'd directly as the PTY session leader
        // (no job-control shell, no OSC 133), so the foreground process group
        // equals the leader and the tcgetpgrp fallback reports not-running.
        assert!(!info.running);
        assert!(info.has_client);
    }

    #[test]
    fn draining_osc133_latches_has_prompt_marking() {
        // A shell that emits an OSC 133 marker should latch prompt marking once
        // its output is drained into the engine — which is what permanently
        // disables the daemon's tcgetpgrp `running` fallback. (The 133 sequence
        // itself is stripped from the replay, so we poll `has_prompt_marking`.)
        let mut s = Session::open(
            "t133",
            argv(&["sh", "-c", "printf '\\033]133;D\\007'; sleep 0.3"]),
            "/",
            80,
            24,
        )
        .expect("open");
        assert!(!s.has_prompt_marking());
        let start = Instant::now();
        while start.elapsed() < Duration::from_secs(3) {
            let _ = s.drain().expect("drain");
            if s.has_prompt_marking() {
                break;
            }
            std::thread::sleep(Duration::from_millis(10));
        }
        assert!(
            s.has_prompt_marking(),
            "draining OSC 133 output should latch prompt marking (engine becomes authoritative)"
        );
    }
}
