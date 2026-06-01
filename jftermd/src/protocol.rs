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
    // used by the FrameDecoder in the next task
    #[allow(dead_code)]
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
}
