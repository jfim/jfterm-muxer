//! TLV wire protocol: `[u8 type][u32 len big-endian][value]`.
//!
//! Hot frames (`Data`/`Input`) carry raw terminal bytes; control frames carry
//! a small JSON value. Pure and I/O-free — the socket layer (B2) owns transport.

use serde::{Deserialize, Serialize};

/// Protocol version carried in `Hello`; a mismatch is rejected by the server.
pub const PROTO_VERSION: u32 = 1;

/// Hard cap on a single frame's value length. Guards against a malformed or
/// hostile length header forcing a giant allocation.
pub const MAX_FRAME_LEN: u32 = 16 * 1024 * 1024;

/// Every wire message is one of these. Discriminants are the on-wire type byte.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum FrameType {
    // Control connection
    Hello = 1,
    HelloOk = 2,
    List = 3,
    Sessions = 4,
    // Session connection (client -> daemon)
    AttachOrOpen = 5,
    Input = 6,
    Resize = 7,
    Close = 8,
    // Session connection (daemon -> client)
    Data = 9,
    Status = 10,
    Exit = 11,
}

impl FrameType {
    fn from_u8(b: u8) -> Option<Self> {
        Some(match b {
            1 => Self::Hello,
            2 => Self::HelloOk,
            3 => Self::List,
            4 => Self::Sessions,
            5 => Self::AttachOrOpen,
            6 => Self::Input,
            7 => Self::Resize,
            8 => Self::Close,
            9 => Self::Data,
            10 => Self::Status,
            11 => Self::Exit,
            _ => return None,
        })
    }
}

/// One decoded frame: a type and its raw value bytes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Frame {
    pub ty: FrameType,
    pub payload: Vec<u8>,
}

impl Frame {
    /// Build a frame from a type and raw value bytes.
    pub fn new(ty: FrameType, payload: Vec<u8>) -> Self {
        Self { ty, payload }
    }

    /// Serialize to the wire: `[type][len:u32 BE][value]`.
    pub fn encode(&self) -> Vec<u8> {
        debug_assert!(
            self.payload.len() <= MAX_FRAME_LEN as usize,
            "frame payload {} exceeds MAX_FRAME_LEN",
            self.payload.len()
        );
        let len = self.payload.len();
        let mut out = Vec::with_capacity(5 + len);
        out.push(self.ty as u8);
        out.extend_from_slice(&(len as u32).to_be_bytes());
        out.extend_from_slice(&self.payload);
        out
    }
}

/// Errors from decoding a frame stream. Either closes the connection (B2).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ProtocolError {
    /// Type byte did not map to a known `FrameType`.
    UnknownType(u8),
    /// Declared value length exceeded `MAX_FRAME_LEN`.
    FrameTooLarge(u32),
}

impl std::fmt::Display for ProtocolError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::UnknownType(b) => write!(f, "unknown frame type byte {b}"),
            Self::FrameTooLarge(n) => write!(f, "frame length {n} exceeds cap"),
        }
    }
}

impl std::error::Error for ProtocolError {}

/// Compact the read buffer once consumed bytes pass this threshold, so a long
/// run of small frames neither grows the buffer unbounded nor pays an O(n)
/// shift per frame.
const COMPACT_THRESHOLD: usize = 64 * 1024;

impl From<ProtocolError> for std::io::Error {
    fn from(e: ProtocolError) -> Self {
        std::io::Error::new(std::io::ErrorKind::InvalidData, e)
    }
}

/// Accumulates bytes off a socket and yields whole `Frame`s. Handles frames
/// split across reads and multiple frames in one read.
#[derive(Debug, Default)]
pub struct FrameDecoder {
    buf: Vec<u8>,
    /// Offset of the first unconsumed byte in `buf`. Consumed frames advance
    /// this cursor instead of draining the front (which would be O(n) per
    /// frame); `buf` is compacted only once `pos` crosses `COMPACT_THRESHOLD`.
    pos: usize,
}

impl FrameDecoder {
    pub fn new() -> Self {
        Self::default()
    }

    /// Append freshly-read bytes to the internal buffer.
    pub fn push(&mut self, bytes: &[u8]) {
        // Drop already-consumed bytes before appending so the buffer does not
        // grow without bound across many push/next_frame cycles.
        self.compact();
        self.buf.extend_from_slice(bytes);
    }

    /// Drop consumed bytes from the front of `buf` and reset the cursor.
    fn compact(&mut self) {
        if self.pos == 0 {
            return;
        }
        self.buf.drain(..self.pos);
        self.pos = 0;
    }

    /// Pop the next complete frame, or `Ok(None)` if more bytes are needed.
    /// A malformed type byte or oversized length is a hard `Err`.
    pub fn next_frame(&mut self) -> Result<Option<Frame>, ProtocolError> {
        let avail = &self.buf[self.pos..];
        if avail.len() < 5 {
            return Ok(None);
        }
        let ty_byte = avail[0];
        let len = u32::from_be_bytes([avail[1], avail[2], avail[3], avail[4]]);
        if len > MAX_FRAME_LEN {
            return Err(ProtocolError::FrameTooLarge(len));
        }
        let ty = FrameType::from_u8(ty_byte).ok_or(ProtocolError::UnknownType(ty_byte))?;
        let total = 5 + len as usize;
        if avail.len() < total {
            return Ok(None);
        }
        let payload = avail[5..total].to_vec();
        self.pos += total;
        // Reclaim space eagerly once enough has been consumed.
        if self.pos >= COMPACT_THRESHOLD {
            self.compact();
        }
        Ok(Some(Frame::new(ty, payload)))
    }
}

/// `Hello` / `HelloOk` value.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Hello {
    pub proto_version: u32,
    pub daemon_version: String,
}

/// One entry in the `Sessions` reply to `List`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SessionInfo {
    pub session_id: String,
    pub argv: Vec<String>,
    pub cwd: String,
    pub running: bool,
    pub has_client: bool,
    pub created_at: u64,
}

/// `AttachOrOpen` value — binds a session connection; opens if unknown.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AttachOrOpen {
    pub session_id: String,
    pub cwd: String,
    pub argv: Vec<String>,
    /// `0` = full available scrollback (last purge boundary); else cap.
    pub want_chunks: usize,
    pub cols: u16,
    pub rows: u16,
}

/// `Resize` value.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct Resize {
    pub cols: u16,
    pub rows: u16,
}

/// `Close` value — SIGHUP the shell, then SIGKILL after `grace_ms` if still
/// alive. `grace_ms == 0` = SIGHUP only (no escalation).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct CloseMsg {
    pub grace_ms: u32,
}

/// `Status` value — semantic dot state.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct StatusMsg {
    pub running: bool,
    pub progress: Option<u8>,
}

/// `Exit` value — shell child exited.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExitMsg {
    pub status: i32,
}

impl Frame {
    /// Build a control frame whose value is the JSON encoding of `value`.
    pub fn control<T: Serialize>(ty: FrameType, value: &T) -> serde_json::Result<Self> {
        Ok(Self::new(ty, serde_json::to_vec(value)?))
    }

    /// Decode this frame's value as JSON into `T`.
    pub fn json<T: for<'de> Deserialize<'de>>(&self) -> serde_json::Result<T> {
        serde_json::from_slice(&self.payload)
    }
}

/// Split raw output bytes into one or more `Data` frames, each at most
/// `MAX_FRAME_LEN` bytes, so a large replay never trips the frame cap.
pub fn frame_data(bytes: &[u8]) -> Vec<Frame> {
    bytes
        .chunks(MAX_FRAME_LEN as usize)
        .map(|c| Frame::new(FrameType::Data, c.to_vec()))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn encode_data_frame_layout() {
        let f = Frame::new(FrameType::Data, b"hi".to_vec());
        // type=9, len=2 big-endian, then payload
        assert_eq!(f.encode(), vec![9, 0, 0, 0, 2, b'h', b'i']);
    }

    #[test]
    fn encode_empty_payload() {
        let f = Frame::new(FrameType::List, Vec::new());
        assert_eq!(f.encode(), vec![3, 0, 0, 0, 0]);
    }

    #[test]
    fn decode_round_trip() {
        let f = Frame::new(FrameType::Data, b"hello".to_vec());
        let mut d = FrameDecoder::new();
        d.push(&f.encode());
        assert_eq!(d.next_frame().unwrap(), Some(f));
        assert_eq!(d.next_frame().unwrap(), None);
    }

    #[test]
    fn decode_partial_then_complete() {
        let wire = Frame::new(FrameType::Input, b"abc".to_vec()).encode();
        let mut d = FrameDecoder::new();
        d.push(&wire[..3]); // header not even complete
        assert_eq!(d.next_frame().unwrap(), None);
        d.push(&wire[3..]);
        assert_eq!(
            d.next_frame().unwrap(),
            Some(Frame::new(FrameType::Input, b"abc".to_vec()))
        );
    }

    #[test]
    fn decode_two_frames_in_one_push() {
        let mut wire = Frame::new(FrameType::Status, b"x".to_vec()).encode();
        wire.extend(Frame::new(FrameType::Exit, b"y".to_vec()).encode());
        let mut d = FrameDecoder::new();
        d.push(&wire);
        assert_eq!(d.next_frame().unwrap().unwrap().ty, FrameType::Status);
        assert_eq!(d.next_frame().unwrap().unwrap().ty, FrameType::Exit);
        assert_eq!(d.next_frame().unwrap(), None);
    }

    #[test]
    fn decode_unknown_type_errors() {
        let mut d = FrameDecoder::new();
        d.push(&[200, 0, 0, 0, 0]);
        assert_eq!(d.next_frame(), Err(ProtocolError::UnknownType(200)));
    }

    #[test]
    fn decode_oversized_length_errors_without_alloc() {
        let mut d = FrameDecoder::new();
        d.push(&[9, 0xFF, 0xFF, 0xFF, 0xFF]); // len ~4 GiB
        assert_eq!(d.next_frame(), Err(ProtocolError::FrameTooLarge(u32::MAX)));
    }

    #[test]
    fn attach_or_open_round_trips_through_a_frame() {
        let msg = AttachOrOpen {
            session_id: "tab-1".into(),
            cwd: "/home/u".into(),
            argv: vec!["bash".into(), "-l".into()],
            want_chunks: 0,
            cols: 80,
            rows: 24,
        };
        let frame = Frame::control(FrameType::AttachOrOpen, &msg).unwrap();
        assert_eq!(frame.ty, FrameType::AttachOrOpen);
        let back: AttachOrOpen = frame.json().unwrap();
        assert_eq!(back, msg);
    }

    #[test]
    fn status_round_trips_both_progress_states() {
        for progress in [None, Some(0u8), Some(75u8), Some(100u8)] {
            let msg = StatusMsg {
                running: true,
                progress,
            };
            let frame = Frame::control(FrameType::Status, &msg).unwrap();
            let back: StatusMsg = frame.json().unwrap();
            assert_eq!(back, msg);
        }
    }

    #[test]
    fn hello_round_trips() {
        let msg = Hello {
            proto_version: PROTO_VERSION,
            daemon_version: "0.1.0".into(),
        };
        let frame = Frame::control(FrameType::Hello, &msg).unwrap();
        let back: Hello = frame.json().unwrap();
        assert_eq!(back, msg);
    }

    #[test]
    fn frame_data_splits_oversized_payload_into_max_sized_frames() {
        let big = vec![0xABu8; MAX_FRAME_LEN as usize + 1];
        let frames = frame_data(&big);
        assert_eq!(frames.len(), 2);
        assert_eq!(frames[0].payload.len(), MAX_FRAME_LEN as usize);
        assert_eq!(frames[1].payload.len(), 1);
        assert!(frames.iter().all(|f| f.ty == FrameType::Data));
        let mut joined = Vec::new();
        for f in &frames {
            joined.extend_from_slice(&f.payload);
        }
        assert_eq!(joined, big);
    }

    #[test]
    fn frame_data_empty_input_yields_no_frames() {
        assert!(frame_data(&[]).is_empty());
    }

    #[test]
    fn close_msg_round_trips() {
        for grace_ms in [0u32, 1500, u32::MAX] {
            let msg = CloseMsg { grace_ms };
            let frame = Frame::control(FrameType::Close, &msg).unwrap();
            assert_eq!(frame.ty, FrameType::Close);
            assert_eq!(frame.json::<CloseMsg>().unwrap(), msg);
        }
    }
}
