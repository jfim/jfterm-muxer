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

    // Daemonize BEFORE building the tokio runtime (fork carries only this thread).
    if !args.foreground
        && let Err(e) = daemonize::daemonize()
    {
        eprintln!("jftermd: daemonize failed: {e}");
        return ExitCode::FAILURE;
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
        // The umask(0o077) set in daemonize() already closes the bind->chmod
        // window, but a chmod failure means we cannot prove the socket is 0600,
        // so abort rather than serve a possibly world-accessible socket.
        tracing::error!(error = %e, "could not chmod socket to 0600; aborting");
        let _ = std::fs::remove_file(&sock);
        return ExitCode::FAILURE;
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
