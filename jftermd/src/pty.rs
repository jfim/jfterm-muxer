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
use nix::unistd::{Pid, chdir, execvpe, getpgid, read, write};

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
                let pty = Self { master, child };
                pty.await_own_process_group();
                Ok(pty)
            }
            ForkptyResult::Child => {
                let _ = chdir(cwd_c.as_c_str());
                let _ = execvpe(&argv_c[0], &argv_c, &env_c);
                // exec failed; child must not return into Rust.
                unsafe { libc::_exit(127) };
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
}
