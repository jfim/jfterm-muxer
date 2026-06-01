//! Real-socket, real-shell integration tests against an in-process server.

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use jftermd::protocol::{Frame, FrameDecoder, FrameType, Hello, PROTO_VERSION, SessionInfo};
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
