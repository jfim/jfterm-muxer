//! Synchronous PTY wrapper around `nix::pty::forkpty`. The async loop (B2)
//! drives `drain`/`write_input` off `AsyncFd`; this layer is deliberately
//! blocking-fd-free (master is non-blocking) and testable with real shells.

use std::ffi::CString;
use std::io;
use std::os::fd::{AsRawFd, OwnedFd};
use std::path::Path;
use std::sync::Mutex;

use nix::fcntl::{FcntlArg, FdFlag, OFlag, fcntl};
use nix::pty::{ForkptyResult, Winsize, forkpty};
use nix::sys::signal::{Signal, killpg};
use nix::sys::wait::{WaitPidFlag, WaitStatus, waitpid};
use nix::unistd::{Pid, getpgid, read, tcgetpgrp, write};

// TIOCSWINSZ ioctl for resizing the master after fork.
nix::ioctl_write_ptr_bad!(tiocswinsz, libc::TIOCSWINSZ, Winsize);

/// Build a `Winsize` from columns/rows (pixel sizes left zero).
pub fn winsize(cols: u16, rows: u16) -> Winsize {
    Winsize {
        ws_row: rows,
        ws_col: cols,
        ws_xpixel: 0,
        ws_ypixel: 0,
    }
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
    ///
    /// B2 note: this blocks briefly (a bounded pgid-readiness spin, ~1ms
    /// typical, 200ms worst case) so it MUST NOT be called directly on the
    /// tokio event-loop thread — wrap it in `spawn_blocking` or redesign the
    /// process-group barrier to be non-blocking.
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
        // Force the emulator-capability vars; drop any inherited values so they
        // can't override what JFTerm's terminal actually supports.
        let mut env_c: Vec<CString> = std::env::vars()
            .filter(|(k, _)| k != "TERM" && k != "COLORTERM")
            .map(|(k, v)| CString::new(format!("{k}={v}")))
            .collect::<Result<_, _>>()
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e))?;
        env_c.push(CString::new("TERM=xterm-256color").unwrap());
        env_c.push(CString::new("COLORTERM=truecolor").unwrap());
        let cwd_c = CString::new(cwd.as_os_str().as_encoded_bytes())
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e))?;

        // Build the NULL-terminated argv/envp pointer arrays in the PARENT so the
        // child does ZERO heap allocation between fork and exec. `nix::execvpe`
        // builds these arrays *inside the child* (`Vec::collect`), and `malloc` is
        // not async-signal-safe: if another thread held the allocator lock at fork
        // time the child would deadlock before exec. (SPAWN_LOCK below serializes
        // the fork window but not other threads' allocations, so it does not cover
        // this — calling raw `libc::execvpe` with parent-built arrays does.)
        let argv_ptrs: Vec<*const libc::c_char> = argv_c
            .iter()
            .map(|s| s.as_ptr())
            .chain(std::iter::once(std::ptr::null()))
            .collect();
        let env_ptrs: Vec<*const libc::c_char> = env_c
            .iter()
            .map(|s| s.as_ptr())
            .chain(std::iter::once(std::ptr::null()))
            .collect();

        // Serialize forkpty across threads. `forkpty` opens the slave, forks,
        // and closes the slave in the parent — none of those fds carry
        // O_CLOEXEC. If another thread forks during that window its child
        // inherits this session's slave fd and holds it open, so when our shell
        // exits the master never reaches EOF and the exit is never detected.
        // The lock confines the slave to one spawn at a time.
        static SPAWN_LOCK: Mutex<()> = Mutex::new(());
        let res = {
            let _guard = SPAWN_LOCK.lock().unwrap_or_else(|e| e.into_inner());
            // SAFETY: child path touches only async-signal-safe syscalls.
            unsafe { forkpty(Some(&ws), None) }.map_err(io_err)?
        };
        match res {
            ForkptyResult::Parent { master, child } => {
                set_nonblocking(&master)?;
                // Close-on-exec so a later concurrent spawn's child does not
                // inherit (and hold open) this master across its own exec. The
                // SPAWN_LOCK above already prevents the slave from leaking; this
                // keeps the long-lived master from leaking into siblings too.
                set_cloexec(&master)?;
                let pty = Self { master, child };
                pty.await_own_process_group();
                Ok(pty)
            }
            ForkptyResult::Child => {
                // SAFETY: async-signal-safe libc calls only; the CStrings and
                // pointer arrays were built in the parent and remain valid here.
                unsafe {
                    libc::chdir(cwd_c.as_ptr());
                    libc::execvpe(argv_c[0].as_ptr(), argv_ptrs.as_ptr(), env_ptrs.as_ptr());
                    // exec failed; the child must not unwind back into Rust.
                    libc::_exit(127);
                }
            }
        }
    }

    /// Briefly wait for the child to become its own process-group leader.
    ///
    /// `forkpty` returns in the parent right after `fork`, but the child only
    /// becomes a session/group leader once it runs `setsid` (via `login_tty`).
    /// Until then the child's pgid is still the parent's group, so `killpg`
    /// against the child pid would hit ESRCH. Spin briefly so the documented
    /// "pgid == child pid" invariant holds before `spawn` returns; if the child
    /// dies first, signaling will simply no-op later.
    fn await_own_process_group(&self) {
        for _ in 0..200 {
            match getpgid(Some(self.child)) {
                Ok(pgid) if pgid == self.child => return,
                // Child gone already; nothing to wait for.
                Err(nix::errno::Errno::ESRCH) => return,
                _ => std::thread::sleep(std::time::Duration::from_millis(1)),
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
                // On Linux, once the slave side fully closes (child gone), a
                // read on the PTY master reports EIO rather than a clean 0-byte
                // EOF — treat it as EOF so callers can reap the child.
                Err(nix::errno::Errno::EIO) => {
                    out.eof = true;
                    return Ok(out);
                }
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

    /// True when a foreground process group other than the shell itself owns the
    /// terminal — i.e. a command is running. The `running` fallback for shells
    /// without OSC 133. (Only ever true under an interactive, job-control shell;
    /// a non-job-control `sh -c` keeps everything in the leader's group.)
    pub fn foreground_busy(&self) -> io::Result<bool> {
        let fg = tcgetpgrp(&self.master).map_err(io_err)?;
        Ok(fg != self.child)
    }

    /// SIGHUP the child's process group (Close).
    pub fn hangup(&self) -> io::Result<()> {
        killpg(self.child, Signal::SIGHUP).map_err(io_err)
    }

    /// SIGKILL the child's process group (CLOSE escalation).
    pub fn kill(&self) -> io::Result<()> {
        killpg(self.child, Signal::SIGKILL).map_err(io_err)
    }

    /// Non-blocking reap. `Some(status)` once reaped (128+sig for signals),
    /// `None` while still running OR if the status is not yet observable.
    ///
    /// Does NOT fabricate an exit code: a non-terminal `WaitStatus`
    /// (`StillAlive`/`Stopped`/`Continued`) and a transient `ECHILD` both map
    /// to `None` so the caller can retry until a real terminal status is seen,
    /// rather than coercing a lost code to 0.
    pub fn try_reap(&self) -> io::Result<Option<i32>> {
        match waitpid(self.child, Some(WaitPidFlag::WNOHANG)) {
            Ok(WaitStatus::Exited(_, code)) => Ok(Some(code)),
            Ok(WaitStatus::Signaled(_, sig, _)) => Ok(Some(128 + sig as i32)),
            Ok(_) => Ok(None),
            // No child to wait for yet (e.g. status not reapable this instant);
            // do not invent a 0 — let the caller spin for the real status.
            Err(nix::errno::Errno::ECHILD) => Ok(None),
            Err(e) => Err(io_err(e)),
        }
    }

    /// Blocking reap that spins until a terminal `WaitStatus` is observed,
    /// returning the true exit code (128+sig for signals). Used at EOF, where
    /// the child is known to be terminating, so the real code is never lost to
    /// a transient non-terminal wait result. Bounded so a wedged reap cannot
    /// hang the actor forever; returns `None` only if no terminal status is
    /// observable within the bound (child already reaped elsewhere).
    pub fn reap_blocking(&self) -> io::Result<Option<i32>> {
        // ~1s worst case (200 * 5ms): the child has already closed its PTY
        // slave (that is what produced EOF), so the kernel transition to a
        // terminal wait state is imminent and this spins only a few times.
        for _ in 0..200 {
            match waitpid(self.child, Some(WaitPidFlag::WNOHANG)) {
                Ok(WaitStatus::Exited(_, code)) => return Ok(Some(code)),
                Ok(WaitStatus::Signaled(_, sig, _)) => return Ok(Some(128 + sig as i32)),
                // Not yet terminal (StillAlive/Stopped/Continued): keep waiting
                // for the real code instead of coercing to 0.
                Ok(_) => std::thread::sleep(std::time::Duration::from_millis(5)),
                // Already reaped elsewhere: the real code is genuinely
                // unrecoverable, but we still must not fabricate one.
                Err(nix::errno::Errno::ECHILD) => return Ok(None),
                Err(e) => return Err(io_err(e)),
            }
        }
        Ok(None)
    }
}

impl Drop for Pty {
    /// Best-effort kill/reap on drop. Kill/reap policy: SIGKILL the child's
    /// process group, then `waitpid` (blocking, but the group was just killed)
    /// to clear the zombie. All errors are ignored — the child may already be
    /// reaped (ECHILD) or gone (ESRCH); dropping must never panic or block
    /// indefinitely. This guarantees no orphaned shell process group is left
    /// behind when a `Pty` (and its owning `Session`) is dropped on any path.
    fn drop(&mut self) {
        let _ = killpg(self.child, Signal::SIGKILL);
        let _ = waitpid(self.child, None);
    }
}

fn set_nonblocking(fd: &OwnedFd) -> io::Result<()> {
    fcntl(fd, FcntlArg::F_SETFL(OFlag::O_NONBLOCK)).map_err(io_err)?;
    Ok(())
}

fn set_cloexec(fd: &OwnedFd) -> io::Result<()> {
    fcntl(fd, FcntlArg::F_SETFD(FdFlag::FD_CLOEXEC)).map_err(io_err)?;
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
    fn echoes_colorterm_truecolor() {
        let mut pty = Pty::spawn(
            &argv(&["sh", "-c", "printf %s \"$COLORTERM\""]),
            Path::new("/"),
            winsize(80, 24),
        )
        .expect("spawn");
        let out = read_until(&mut pty, b"truecolor", Duration::from_secs(3));
        assert!(
            out.windows(9).any(|w| w == b"truecolor"),
            "COLORTERM not set; got {:?}",
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
        let mut pty = Pty::spawn(
            &argv(&["sh", "-c", "exit 7"]),
            Path::new("/"),
            winsize(80, 24),
        )
        .expect("spawn");
        let start = Instant::now();
        loop {
            let d = pty.drain().expect("drain");
            if d.eof {
                break;
            }
            assert!(start.elapsed() < Duration::from_secs(3), "no EOF");
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
        assert_eq!(status, Some(7));
    }

    #[test]
    fn resize_does_not_error() {
        let pty = Pty::spawn(&argv(&["cat"]), Path::new("/"), winsize(80, 24)).expect("spawn");
        pty.resize(winsize(120, 40)).expect("resize");
        pty.hangup().expect("hangup");
    }

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

    #[test]
    fn kill_terminates_and_reaps_as_sigkill() {
        let mut pty = Pty::spawn(&argv(&["cat"]), Path::new("/"), winsize(80, 24)).expect("spawn");
        pty.kill().expect("kill");
        // Drain to EOF, then reap: SIGKILL surfaces as 128 + 9 = 137.
        let start = Instant::now();
        loop {
            let d = pty.drain().expect("drain");
            if d.eof {
                break;
            }
            assert!(
                start.elapsed() < Duration::from_secs(3),
                "no EOF after SIGKILL"
            );
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
}
