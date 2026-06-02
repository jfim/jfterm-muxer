//! Real-socket, real-shell integration tests against an in-process server.

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use jftermd::protocol::{
    AttachOrOpen, CloseMsg, ExitMsg, Frame, FrameDecoder, FrameType, Hello, PROTO_VERSION,
    SessionInfo, StatusMsg,
};
use jftermd::registry::Registry;
use jftermd::server::{ServerOpts, run};

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
    assert!(h.registry.is_empty());
}

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

async fn recv_status_until(c: &mut Conn, want_running: bool, timeout: Duration) -> bool {
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        if remaining.is_zero() {
            return false;
        }
        match c.recv(remaining).await {
            Some(f) if f.ty == FrameType::Status => {
                if let Ok(m) = f.json::<StatusMsg>()
                    && m.running == want_running
                {
                    return true;
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
    c.send(&attach_or_open(
        "poll",
        argv(&["bash", "--norc", "-i"]),
        0,
        80,
        24,
    ))
    .await;
    let _ = recv_data_until(&mut c, b"$", Duration::from_secs(3)).await;
    c.send(&Frame::new(FrameType::Input, b"sleep 0.6\n".to_vec()))
        .await;
    assert!(
        recv_status_until(&mut c, true, Duration::from_secs(3)).await,
        "fallback should report running=true while `sleep` is foreground"
    );
    assert!(
        recv_status_until(&mut c, false, Duration::from_secs(3)).await,
        "fallback should report running=false back at the prompt"
    );
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

fn close_frame(grace_ms: u32) -> Frame {
    Frame::control(FrameType::Close, &CloseMsg { grace_ms }).unwrap()
}

#[tokio::test]
async fn close_grace_zero_reaps_without_escalation() {
    let h = Harness::start(ServerOpts::default()).await;
    let mut c = h.connect().await;
    c.send(&attach_or_open("cz", argv(&["cat"]), 0, 80, 24))
        .await;
    wait_until(Duration::from_secs(3), || !h.registry.is_empty()).await;
    c.send(&close_frame(0)).await; // SIGHUP only; cat dies on SIGHUP
    wait_until(Duration::from_secs(3), || h.registry.is_empty()).await;
    assert!(
        h.registry.is_empty(),
        "CLOSE{{0}} should reap a SIGHUP-dying shell"
    );
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

#[tokio::test]
async fn open_drains_then_reattach_replays_scrollback() {
    let h = Harness::start(ServerOpts::default()).await;
    let mut c1 = h.connect().await;
    c1.send(&attach_or_open(
        "s1",
        argv(&["sh", "-c", "echo MARKER; exec cat"]),
        0,
        80,
        24,
    ))
    .await;
    let got = recv_data_until(&mut c1, b"MARKER", Duration::from_secs(3)).await;
    assert!(contains(&got, b"MARKER"), "first client missed live output");
    drop(c1);
    tokio::time::sleep(Duration::from_millis(150)).await;

    let mut c2 = h.connect().await;
    c2.send(&attach_or_open("s1", argv(&["true"]), 0, 80, 24))
        .await;
    let got = recv_data_until(&mut c2, b"MARKER", Duration::from_secs(3)).await;
    assert!(
        contains(&got, b"MARKER"),
        "reattach did not replay scrollback"
    );
}

#[tokio::test]
async fn input_reaches_shell_and_output_returns() {
    let h = Harness::start(ServerOpts::default()).await;
    let mut c = h.connect().await;
    c.send(&attach_or_open("s2", argv(&["cat"]), 0, 80, 24))
        .await;
    c.send(&Frame::new(FrameType::Input, b"PINGPONG\n".to_vec()))
        .await;
    let got = recv_data_until(&mut c, b"PINGPONG", Duration::from_secs(3)).await;
    assert!(contains(&got, b"PINGPONG"));
}

#[tokio::test]
async fn close_kills_and_drops_session() {
    let h = Harness::start(ServerOpts::default()).await;
    let mut c = h.connect().await;
    c.send(&attach_or_open("s3", argv(&["cat"]), 0, 80, 24))
        .await;
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
    c.send(&attach_or_open(
        "s4",
        argv(&["sh", "-c", "echo START; exec cat"]),
        0,
        80,
        24,
    ))
    .await;
    let _ = recv_data_until(&mut c, b"START", Duration::from_secs(3)).await;
    drop(c);
    // Detach keeps the shell alive, so the session must remain in the registry.
    wait_until(Duration::from_secs(3), || !h.registry.is_empty()).await;
    assert!(!h.registry.is_empty(), "detach must not kill the shell");

    let mut c2 = h.connect().await;
    c2.send(&attach_or_open("s4", argv(&["true"]), 0, 80, 24))
        .await;
    let got = recv_data_until(&mut c2, b"START", Duration::from_secs(3)).await;
    assert!(contains(&got, b"START"));
}

#[tokio::test]
async fn second_attach_takes_over_and_kicks_first() {
    let h = Harness::start(ServerOpts::default()).await;
    let mut c1 = h.connect().await;
    c1.send(&attach_or_open(
        "s5",
        argv(&["sh", "-c", "echo HI; exec cat"]),
        0,
        80,
        24,
    ))
    .await;
    let _ = recv_data_until(&mut c1, b"HI", Duration::from_secs(3)).await;

    let mut c2 = h.connect().await;
    c2.send(&attach_or_open("s5", argv(&["true"]), 0, 80, 24))
        .await;
    let _ = recv_data_until(&mut c2, b"HI", Duration::from_secs(3)).await;

    assert!(
        c1.recv(Duration::from_secs(3)).await.is_none(),
        "old client must be kicked on takeover"
    );
    c2.send(&Frame::new(FrameType::Input, b"AFTER\n".to_vec()))
        .await;
    let got = recv_data_until(&mut c2, b"AFTER", Duration::from_secs(3)).await;
    assert!(contains(&got, b"AFTER"));
}

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
    // The session is created only once the accept loop has processed the first
    // frame, so confirm it exists before asserting the shell survives the drop.
    wait_until(Duration::from_secs(3), || !h.registry.is_empty()).await;
    assert!(
        !h.registry.is_empty(),
        "backpressure must not kill the shell"
    );

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
    c2.send(&attach_or_open("bp", argv(&["true"]), 1, 80, 24))
        .await;
    c2.send(&Frame::new(FrameType::Input, b"ALIVE\n".to_vec()))
        .await;
    let got = recv_data_until(&mut c2, b"ALIVE", Duration::from_secs(3)).await;
    assert!(
        contains(&got, b"ALIVE"),
        "shell unresponsive after backpressure drop"
    );
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
    // The session is created only once the accept loop has processed the first
    // frame; confirm it exists before detaching so the drop can't race creation.
    // (No output is emitted until the shell's 0.3s sleep elapses, so the registry
    // is the only creation signal available here.)
    wait_until(Duration::from_secs(3), || !h.registry.is_empty()).await;
    // Detach BEFORE the shell exits (it sleeps 0.3s first).
    drop(c);
    // Let the shell exit *while detached* so reattach exercises the dead-session
    // replay path; the dead session is retained for dead_grace (10s). With
    // creation already confirmed, this only needs to cover the shell's own
    // wall-clock 0.3s sleep, so it is robust to scheduler jitter.
    tokio::time::sleep(Duration::from_millis(700)).await;
    assert!(!h.registry.is_empty(), "dead session must be retained");

    // Reattach: replay final output + EXIT{7}, then the session is dropped.
    let mut c2 = h.connect().await;
    c2.send(&attach_or_open("dead", argv(&["true"]), 0, 80, 24))
        .await;
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
    assert!(
        h.registry.is_empty(),
        "dead session dropped after reattach replay"
    );
}

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
    c.send(&attach_or_open(
        "smoke",
        argv(&["sh", "-c", "echo SMOKED; exec cat"]),
        0,
        80,
        24,
    ))
    .await;
    let got = recv_data_until(&mut c, b"SMOKED", Duration::from_secs(5)).await;
    assert!(
        contains(&got, b"SMOKED"),
        "binary did not serve session output"
    );

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
