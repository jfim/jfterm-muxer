//! TLV wire protocol: `[u8 type][u32 len big-endian][value]`.
//!
//! Hot frames (`Data`/`Input`) carry raw terminal bytes; control frames carry
//! a small JSON value. Pure and I/O-free — the socket layer (B2) owns transport.

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

/// Accumulates bytes off a socket and yields whole `Frame`s. Handles frames
/// split across reads and multiple frames in one read.
#[derive(Debug, Default)]
pub struct FrameDecoder {
    buf: Vec<u8>,
}

impl FrameDecoder {
    pub fn new() -> Self {
        Self::default()
    }

    /// Append freshly-read bytes to the internal buffer.
    pub fn push(&mut self, bytes: &[u8]) {
        self.buf.extend_from_slice(bytes);
    }

    /// Pop the next complete frame, or `Ok(None)` if more bytes are needed.
    /// A malformed type byte or oversized length is a hard `Err`.
    pub fn next_frame(&mut self) -> Result<Option<Frame>, ProtocolError> {
        if self.buf.len() < 5 {
            return Ok(None);
        }
        let ty_byte = self.buf[0];
        let len = u32::from_be_bytes([self.buf[1], self.buf[2], self.buf[3], self.buf[4]]);
        if len > MAX_FRAME_LEN {
            return Err(ProtocolError::FrameTooLarge(len));
        }
        let ty = FrameType::from_u8(ty_byte).ok_or(ProtocolError::UnknownType(ty_byte))?;
        let total = 5 + len as usize;
        if self.buf.len() < total {
            return Ok(None);
        }
        let payload = self.buf[5..total].to_vec();
        self.buf.drain(..total);
        Ok(Some(Frame::new(ty, payload)))
    }
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
}
