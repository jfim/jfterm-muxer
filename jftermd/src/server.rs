//! tokio UDS server: accept loop, control + session connections, and the
//! per-session actor that bridges a sync `Session` onto the async loop.
//!
//! The actor and its helpers are driven by the accept loop, which classifies
//! each connection by its first frame and binds session connections through
//! `handle_session`.

use std::io;
use std::os::fd::{AsRawFd, RawFd};
use std::sync::Arc;
use std::time::Duration;

use jftermd_core::StatusSnapshot;
use tokio::io::unix::AsyncFd;
use tokio::io::{AsyncReadExt, AsyncWriteExt, Interest};
use tokio::net::unix::{OwnedReadHalf, OwnedWriteHalf};
use tokio::net::{UnixListener, UnixStream};
use tokio::sync::{mpsc, oneshot};
use tokio::time::{Instant, sleep_until};

use crate::protocol::{
    AttachOrOpen, CloseMsg, ExitMsg, Frame, FrameDecoder, FrameType, Hello, PROTO_VERSION,
    Resize, SessionInfo, StatusMsg, frame_data,
};
use crate::registry::{AttachRequest, Bind, Registry, SessionCommand};
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
    /// Interval for the tcgetpgrp `running` fallback poll (non-OSC-133 shells).
    pub status_poll: Duration,
}

impl Default for ServerOpts {
    fn default() -> Self {
        Self {
            dead_grace: Duration::from_secs(30),
            exit_grace: Duration::from_secs(5),
            out_queue: 1024,
            status_poll: Duration::from_millis(300),
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
    poll_running: bool,
    req: AttachRequest,
) {
    let _ = session.resize(req.cols, req.rows);
    let replay = session.replay_for_attach(req.want_chunks);
    for f in frame_data(&replay.data) {
        let _ = req.out_tx.try_send(f);
    }
    let _ = req
        .out_tx
        .try_send(status_frame(effective_status(session, poll_running)));
    if let Lifecycle::Dead { status } = session.lifecycle() {
        let _ = req.out_tx.try_send(exit_frame(status));
    }
    // Replacing the Option drops the previous out_tx -> old writer task ends ->
    // old client socket closes (takeover detach).
    *client = Some(req.out_tx);
}

/// Read frames off `rh` until one whole frame is available, or EOF (`None`).
async fn read_one_frame(
    rh: &mut OwnedReadHalf,
    dec: &mut FrameDecoder,
) -> io::Result<Option<Frame>> {
    let mut buf = [0u8; 4096];
    loop {
        if let Some(f) = dec.next_frame()? {
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
        FrameType::AttachOrOpen => handle_session(first, rh, wh, registry, dec, opts).await,
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
    let hello: Hello = hello_frame.json()?;
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
    )?;
    wh.write_all(&ok.encode()).await?;

    loop {
        let frame = match read_one_frame(&mut rh, &mut dec).await? {
            Some(f) => f,
            None => return Ok(()),
        };
        match frame.ty {
            FrameType::List => {
                let sessions = collect_sessions(&registry).await;
                let reply = Frame::control(FrameType::Sessions, &sessions)?;
                wh.write_all(&reply.encode()).await?;
            }
            other => {
                tracing::warn!(?other, "unexpected control frame; ignoring");
            }
        }
    }
}

/// Ask every live session actor for its `SessionInfo`, concurrently.
///
/// Dispatch every `Info` request first, collect the reply receivers, then await
/// them all under a single overall timeout. The actors answer in parallel, so
/// LIST latency is bounded by one timeout rather than scaling at 1s * N as it
/// did when each actor was queried serially with its own per-actor timeout.
async fn collect_sessions(registry: &Registry) -> Vec<SessionInfo> {
    let handles = registry.handles();
    let mut rxs = Vec::with_capacity(handles.len());
    for (_id, tx) in handles {
        let (reply_tx, reply_rx) = oneshot::channel();
        if tx.send(SessionCommand::Info(reply_tx)).await.is_ok() {
            rxs.push(reply_rx);
        }
    }
    let mut out = Vec::with_capacity(rxs.len());
    let collect = async {
        for rx in rxs {
            if let Ok(info) = rx.await {
                out.push(info);
            }
        }
    };
    // Single shared deadline across all replies (not per-actor).
    let _ = tokio::time::timeout(Duration::from_secs(1), collect).await;
    out
}

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
    // Last status snapshot pushed to the *current* client, so live drains only
    // emit a STATUS frame when it actually changes. Suppressing redundant
    // STATUS frames keeps a takeover clean: without it, a no-op STATUS pushed
    // just before the takeover would linger in the old client's socket buffer
    // and defeat the "next recv is EOF" contract.
    let mut last_status: Option<StatusSnapshot> = None;
    // Latest tcgetpgrp fallback result (used until OSC 133 prompt marking shows).
    let mut poll_running = false;
    let mut poll = tokio::time::interval(opts.status_poll);
    poll.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    // Set only after the child exits (dead-session retention deadline). `Instant`
    // is `Copy`, so the timer arm captures it by value — no borrow conflict with
    // the readable arm that reassigns it.
    let mut grace_deadline: Option<Instant> = None;
    // Set once a CLOSE arrives; the next exit-detecting drain ends the actor
    // (instead of arming dead-session retention).
    let mut closing = false;
    // Set on CLOSE{grace_ms>0}: SIGKILL-escalation deadline.
    let mut kill_deadline: Option<Instant> = None;

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
                    Some(SessionCommand::Attach(req)) => {
                        if !session.has_prompt_marking() {
                            poll_running = session.poll_running();
                        }
                        attach_client(&mut session, &mut client, poll_running, req);
                        // The replay already carried a fresh STATUS to the new
                        // client; record it so live drains don't re-send it.
                        last_status = Some(effective_status(&session, poll_running));
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
                        // When detached, forward_data/push_or_drop below are
                        // no-ops, so skip recomputing the merged status entirely.
                        if client.is_some() {
                            // Refresh the tcgetpgrp fallback here so a non-OSC-133
                            // shell reports current `running` on every live drain
                            // (e.g. right after a reattach) instead of a value left
                            // stale since the last poll tick / attach.
                            if !session.has_prompt_marking() && !session.is_dead() {
                                poll_running = session.poll_running();
                            }
                            let eff = effective_status(&session, poll_running);
                            if last_status != Some(eff) {
                                push_or_drop(&mut client, status_frame(eff));
                                last_status = Some(eff);
                            }
                        }
                        if let Some(code) = outcome.exit {
                            push_or_drop(&mut client, exit_frame(code));
                            if closing {
                                break; // reaped after CLOSE -> end + registry.remove
                            }
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

            // Attach-gated `running` fallback for shells without OSC 133. This is
            // best-effort: a foreground command shorter than `status_poll` can
            // start and finish between ticks and never be seen as running.
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
        }
    }

    registry.remove(&id);
}

/// Bind a session connection: attach-or-open, then run reader+writer until the
/// socket (or a takeover) tears the connection down. Never sends CLOSE on plain
/// detach — only an explicit `CLOSE` frame kills the shell.
async fn handle_session(
    first: Frame,
    rh: OwnedReadHalf,
    wh: OwnedWriteHalf,
    registry: Arc<Registry>,
    dec: FrameDecoder,
    opts: ServerOpts,
) -> io::Result<()> {
    let req: AttachOrOpen = first.json()?;
    let cmd_tx = match registry.attach_or_create(&req.session_id) {
        Bind::Existing(tx) => tx,
        Bind::Created { cmd_tx, cmd_rx } => {
            tokio::spawn(session_task(
                req.clone(),
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

    let mut reader = tokio::spawn(reader_task(rh, dec, cmd_tx));
    // Lifetime-couple the halves: when one ends, tear the whole connection down.
    tokio::select! {
        _ = &mut writer => { reader.abort(); }
        _ = &mut reader => { writer.abort(); }
    }
    Ok(())
}

/// Open the PTY off the event loop (`Pty::spawn` blocks), then run the actor.
/// Removes itself from the registry if the open fails.
async fn session_task(
    spec: AttachOrOpen,
    cmd_rx: mpsc::Receiver<SessionCommand>,
    registry: Arc<Registry>,
    opts: ServerOpts,
) {
    let id = spec.session_id.clone();
    let open_id = spec.session_id;
    let AttachOrOpen {
        argv,
        cwd,
        cols,
        rows,
        ..
    } = spec;
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
///
/// `dec` carries any bytes already buffered past the first `ATTACH_OR_OPEN`
/// frame (a fast client may pipeline `INPUT`/`RESIZE` right behind it), so we
/// drain it before the first socket read to avoid losing those frames.
async fn reader_task(
    mut rh: OwnedReadHalf,
    mut dec: FrameDecoder,
    cmd_tx: mpsc::Sender<SessionCommand>,
) {
    let mut buf = [0u8; 65536];
    let mut have_bytes = true;
    'outer: loop {
        if !have_bytes {
            let n = match rh.read(&mut buf).await {
                Ok(0) | Err(_) => break,
                Ok(n) => n,
            };
            dec.push(&buf[..n]);
        }
        have_bytes = false;
        loop {
            match dec.next_frame() {
                Ok(Some(frame)) => match frame.ty {
                    FrameType::Input => {
                        if cmd_tx
                            .send(SessionCommand::Input(frame.payload))
                            .await
                            .is_err()
                        {
                            break 'outer;
                        }
                    }
                    FrameType::Resize => {
                        if let Ok(r) = frame.json::<Resize>()
                            && cmd_tx
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
                    FrameType::Close => {
                        // Empty or malformed payload defaults to grace_ms = 0.
                        let grace_ms = frame.json::<CloseMsg>().map(|m| m.grace_ms).unwrap_or(0);
                        let _ = cmd_tx.send(SessionCommand::Close { grace_ms }).await;
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

    #[test]
    fn merge_running_prefers_engine_when_marked_else_poll() {
        assert!(super::merge_running(true, true, false)); // marked -> engine
        assert!(!super::merge_running(true, false, true)); // marked -> engine
        assert!(super::merge_running(false, false, true)); // unmarked -> poll
        assert!(!super::merge_running(false, true, false)); // unmarked -> poll
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

        cmd_tx
            .send(SessionCommand::Close { grace_ms: 0 })
            .await
            .unwrap();
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

    #[tokio::test]
    async fn idle_watcher_waits_for_first_session_then_resolves_when_empty() {
        let reg = Registry::new();
        let watcher = tokio::spawn(wait_until_idle(reg.clone(), Duration::from_millis(50)));
        // Empty at start: must NOT resolve (a freshly-spawned daemon stays up).
        tokio::time::sleep(Duration::from_millis(200)).await;
        assert!(
            !watcher.is_finished(),
            "must not exit before any session exists"
        );
        // A session appears, then ends -> watcher resolves after the grace.
        let _keep = reg.attach_or_create("s1");
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
}
