//! Socket-path resolution and the 0700 runtime dir / 0600 socket perms.
//! `$XDG_RUNTIME_DIR/jfterm/muxer.sock`, falling back to `/tmp/jfterm-<uid>`.

use std::ffi::OsStr;
use std::io;
use std::os::unix::fs::{DirBuilderExt, PermissionsExt};
use std::path::{Path, PathBuf};

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

/// Create the socket's parent dir at mode 0700 (idempotent), refusing any
/// pre-existing path that is not a real directory owned by us at mode 0700.
///
/// On an existing path we `lstat` (never follow a symlink) and require:
/// a real directory (not a symlink or other file type), owned by the current
/// uid, with mode exactly 0700. Otherwise we refuse rather than trust it.
pub fn ensure_socket_dir(sock: &Path) -> io::Result<()> {
    let dir = sock
        .parent()
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "socket path has no parent"))?;

    // `symlink_metadata` is lstat: it does not follow a final symlink, so a
    // symlink planted at `dir` is rejected instead of silently followed.
    match std::fs::symlink_metadata(dir) {
        Ok(md) => {
            validate_socket_dir(&md)?;
            Ok(())
        }
        Err(e) if e.kind() == io::ErrorKind::NotFound => std::fs::DirBuilder::new()
            .recursive(true)
            .mode(0o700)
            .create(dir),
        Err(e) => Err(e),
    }
}

/// Require that an existing socket-dir is a real directory, owned by the
/// current uid, with mode exactly 0700. These are fixed, standard values.
fn validate_socket_dir(md: &std::fs::Metadata) -> io::Result<()> {
    use std::os::unix::fs::MetadataExt;
    if !md.file_type().is_dir() {
        return Err(io::Error::new(
            io::ErrorKind::AlreadyExists,
            "socket dir path exists but is not a directory",
        ));
    }
    // SAFETY: getuid is always safe.
    let uid = unsafe { libc::getuid() };
    if md.uid() != uid {
        return Err(io::Error::new(
            io::ErrorKind::PermissionDenied,
            "socket dir is not owned by the current user",
        ));
    }
    if md.mode() & 0o777 != 0o700 {
        return Err(io::Error::new(
            io::ErrorKind::PermissionDenied,
            "socket dir mode is not 0700",
        ));
    }
    Ok(())
}

/// Tighten an already-bound socket file to 0600.
pub fn restrict_socket_perms(sock: &Path) -> io::Result<()> {
    std::fs::set_permissions(sock, std::fs::Permissions::from_mode(0o600))
}

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
        ensure_socket_dir(&sock).unwrap();
    }

    #[test]
    fn ensure_socket_dir_refuses_symlink() {
        let base = tempfile::tempdir().unwrap();
        let real = base.path().join("real");
        std::fs::create_dir(&real).unwrap();
        let link = base.path().join("jfterm");
        std::os::unix::fs::symlink(&real, &link).unwrap();
        let sock = link.join("muxer.sock");
        // lstat sees the symlink (not a directory) and refuses.
        assert!(ensure_socket_dir(&sock).is_err());
    }

    #[test]
    fn ensure_socket_dir_refuses_loose_mode() {
        let base = tempfile::tempdir().unwrap();
        let dir = base.path().join("jfterm");
        std::fs::DirBuilder::new().mode(0o755).create(&dir).unwrap();
        let sock = dir.join("muxer.sock");
        assert!(ensure_socket_dir(&sock).is_err());
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
