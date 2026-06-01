//! Self-spawn support: an `flock` lockfile + atomic socket bind (stale-socket
//! cleanup) resolves spawn races to one daemon; `daemonize()` double-forks.

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
// `main.rs` (Task 9) consumes `lock`/`listener`; until then only tests read
// them via `matches!`, so allow dead_code here rather than dropping the fields.
#[allow(dead_code)]
pub enum Acquire {
    /// We won: hold the lock (must stay alive) and the bound listener.
    Bound {
        lock: Flock<File>,
        listener: UnixListener,
    },
    /// Another daemon already holds the lock; the caller should exit.
    AlreadyRunning,
}

/// Try to take the exclusive lockfile without blocking.
fn try_flock(lock_path: &Path) -> io::Result<Option<Flock<File>>> {
    let file = OpenOptions::new()
        .create(true)
        .read(true)
        .write(true)
        // A lockfile must never be truncated: its content is irrelevant and
        // truncation would needlessly race other openers. Default is no-trunc.
        .truncate(false)
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
/// authoritative, so a leftover socket file is stale -> unlink + rebind.
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

/// `setsid` + double-fork into a daemon; `chdir("/")`; std fds -> `/dev/null`.
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
    let devnull = OpenOptions::new()
        .read(true)
        .write(true)
        .open("/dev/null")?;
    let fd = devnull.as_raw_fd();
    // SAFETY: dup2 onto the three std fds; fd is a valid open /dev/null.
    for target in 0..=2 {
        if unsafe { libc::dup2(fd, target) } < 0 {
            return Err(io::Error::last_os_error());
        }
    }
    Ok(())
}

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
        drop(first);
        let again = acquire_daemon(&sock, &lock).unwrap();
        assert!(matches!(again, Acquire::Bound { .. }));
    }
}
