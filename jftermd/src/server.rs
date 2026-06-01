//! tokio UDS server: accept loop, control + session connections, and the
//! per-session actor that bridges a sync `Session` onto the async loop.
//!
//! The actor and its helpers are currently driven only by the in-crate tests;
//! the accept loop that wires them into a running daemon lands in a later task,
//! so the public surface is allowed to be unused for now. TODO(Task 5): remove
//! this module-wide allow once `handle_session` wires `actor_loop` in.
#![allow(dead_code)]

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

/// Tunable runtime knobs (overridable in tests; CLI sets them later).
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
    // The caller (accept loop / tests) inserts the registry handle via
    // `attach_or_create` before spawning us; the actor only ever `remove`s
    // itself on the way out, so insert/remove ownership stays with the caller.
    let async_fd =
        match AsyncFd::with_interest(MasterFd(session.master_raw_fd()), Interest::READABLE) {
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::{Frame, FrameType};
    use crate::registry::{AttachRequest, Bind, Registry, SessionCommand};
    use crate::session::Session;
    use std::time::Duration;
    use tokio::sync::mpsc;

    fn contains(hay: &[u8], needle: &[u8]) -> bool {
        hay.windows(needle.len()).any(|w| w == needle)
    }

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
            Session::open(
                "a1",
                argv(&["sh", "-c", "echo HELLO; exec cat"]),
                "/",
                80,
                24,
            )
        })
        .await
        .unwrap()
        .expect("open");

        // Register through the registry exactly as the production caller does:
        // caller inserts via `attach_or_create`, keeps `cmd_tx`, hands `cmd_rx`
        // to the actor. Makes dead-session retention observable on `reg`.
        let (cmd_tx, cmd_rx) = match reg.attach_or_create("a1") {
            Bind::Created { cmd_tx, cmd_rx } => (cmd_tx, cmd_rx),
            Bind::Existing(_) => panic!("fresh id should be Created"),
        };
        let task = tokio::spawn(actor_loop(
            session,
            "a1".into(),
            cmd_rx,
            reg.clone(),
            opts.clone(),
        ));

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

        cmd_tx
            .send(SessionCommand::Input(b"PING\n".to_vec()))
            .await
            .unwrap();
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
        let (cmd_tx, cmd_rx) = match reg.attach_or_create("d1") {
            Bind::Created { cmd_tx, cmd_rx } => (cmd_tx, cmd_rx),
            Bind::Existing(_) => panic!("fresh id should be Created"),
        };
        let task = tokio::spawn(actor_loop(session, "d1".into(), cmd_rx, reg.clone(), opts));

        tokio::time::sleep(Duration::from_millis(300)).await;
        assert!(
            !reg.is_empty(),
            "dead session must be retained for reattach"
        );

        let (out_tx, mut out_rx) = mpsc::channel(1024);
        cmd_tx
            .send(SessionCommand::Attach(AttachRequest {
                want_chunks: 0,
                cols: 80,
                rows: 24,
                out_tx,
            }))
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
