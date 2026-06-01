//! jftermd daemon runtime (Plan B).
//!
//! B1 lands the socket-free foundation: the wire protocol codec, the PTY
//! wrapper, and the Session that binds a PTY to the replay engine. The tokio
//! UDS server and daemonization land in B2.

pub mod protocol;
pub mod pty;
pub mod session;
pub mod socket;
