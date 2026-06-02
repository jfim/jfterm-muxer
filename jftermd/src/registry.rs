//! The session registry: `session_id -> cmd_tx` with a race-free
//! check-and-insert (so ATTACH_OR_OPEN attaches-or-opens atomically) and an
//! empty-notify the self-exit watcher waits on.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use tokio::sync::{Notify, mpsc, oneshot};

use crate::protocol::SessionInfo;

/// A command delivered to a session's actor task.
#[derive(Debug)]
pub enum SessionCommand {
    /// Raw keystroke bytes from the client (`INPUT`).
    Input(Vec<u8>),
    /// Client resize (`RESIZE`).
    Resize { cols: u16, rows: u16 },
    /// Kill the shell and drop the session (`CLOSE`): SIGHUP, then SIGKILL after
    /// `grace_ms` if still alive (`grace_ms == 0` = SIGHUP only).
    Close { grace_ms: u32 },
    /// A (re)attach: most-recent-wins takeover of the session's single client.
    Attach(AttachRequest),
    /// Snapshot request for the control connection's `LIST`.
    Info(oneshot::Sender<SessionInfo>),
}

/// Everything the actor needs to bind a newly-attached client.
#[derive(Debug)]
pub struct AttachRequest {
    pub want_chunks: usize,
    pub cols: u16,
    pub rows: u16,
    /// The client's bounded out-queue; the actor pushes `DATA`/`STATUS`/`EXIT`
    /// frames here and drops it (forced detach) on overflow.
    pub out_tx: mpsc::Sender<crate::protocol::Frame>,
}

/// Registry-side handle to one session actor.
#[derive(Debug, Clone)]
pub struct SessionHandle {
    pub cmd_tx: mpsc::Sender<SessionCommand>,
}

/// The shared session table.
#[derive(Debug, Default)]
pub struct Registry {
    sessions: Mutex<HashMap<String, SessionHandle>>,
    /// Pulsed whenever the session set changes, so the self-exit watcher rechecks.
    ended: Notify,
}

/// Outcome of `attach_or_create`: whether the caller must spawn the actor.
pub enum Bind {
    /// Session already existed; here is its command channel.
    Existing(mpsc::Sender<SessionCommand>),
    /// Session was newly inserted; the caller must spawn the actor that reads
    /// `cmd_rx`. The handle is already in the table (race-free).
    Created {
        cmd_tx: mpsc::Sender<SessionCommand>,
        cmd_rx: mpsc::Receiver<SessionCommand>,
    },
}

impl Registry {
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    /// Race-free: if `id` exists return its channel, else insert a fresh handle
    /// and return the receiver for the caller to spawn an actor around. The
    /// whole check-and-insert happens under the lock with no `.await`.
    pub fn attach_or_create(&self, id: &str) -> Bind {
        let mut map = self.sessions.lock().unwrap();
        if let Some(h) = map.get(id) {
            return Bind::Existing(h.cmd_tx.clone());
        }
        let (cmd_tx, cmd_rx) = mpsc::channel(256);
        map.insert(
            id.to_string(),
            SessionHandle {
                cmd_tx: cmd_tx.clone(),
            },
        );
        self.ended.notify_one();
        Bind::Created { cmd_tx, cmd_rx }
    }

    /// Look up an existing session's channel (no creation).
    pub fn get(&self, id: &str) -> Option<mpsc::Sender<SessionCommand>> {
        self.sessions
            .lock()
            .unwrap()
            .get(id)
            .map(|h| h.cmd_tx.clone())
    }

    /// Remove a session (its actor is ending) and pulse the empty-notify.
    pub fn remove(&self, id: &str) {
        self.sessions.lock().unwrap().remove(id);
        self.ended.notify_one();
    }

    /// All `(id, cmd_tx)` pairs, for the control connection's `LIST`.
    pub fn handles(&self) -> Vec<(String, mpsc::Sender<SessionCommand>)> {
        self.sessions
            .lock()
            .unwrap()
            .iter()
            .map(|(k, v)| (k.clone(), v.cmd_tx.clone()))
            .collect()
    }

    pub fn is_empty(&self) -> bool {
        self.sessions.lock().unwrap().is_empty()
    }

    /// Wait until the next session-set change.
    pub async fn wait_for_change(&self) {
        self.ended.notified().await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn create_then_get_then_remove() {
        let reg = Registry::new();
        assert!(reg.is_empty());
        let bind = reg.attach_or_create("s1");
        assert!(matches!(bind, Bind::Created { .. }));
        assert!(!reg.is_empty());
        assert!(reg.get("s1").is_some());
        assert_eq!(reg.handles().len(), 1);
        reg.remove("s1");
        assert!(reg.is_empty());
        assert!(reg.get("s1").is_none());
    }

    #[test]
    fn second_attach_or_create_returns_existing() {
        let reg = Registry::new();
        let first = reg.attach_or_create("s1");
        let (keep_tx, _keep_rx) = match first {
            Bind::Created { cmd_tx, cmd_rx } => (cmd_tx, cmd_rx),
            _ => panic!("expected Created"),
        };
        let second = reg.attach_or_create("s1");
        match second {
            Bind::Existing(tx) => assert!(tx.same_channel(&keep_tx)),
            _ => panic!("expected Existing on second attach"),
        }
    }

    #[tokio::test]
    async fn wait_for_change_wakes_on_remove() {
        let reg = Registry::new();
        let _keep = reg.attach_or_create("s1");
        let reg2 = reg.clone();
        let waiter = tokio::spawn(async move { reg2.wait_for_change().await });
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        reg.remove("s1");
        tokio::time::timeout(std::time::Duration::from_secs(1), waiter)
            .await
            .expect("waiter should wake")
            .unwrap();
    }
}
