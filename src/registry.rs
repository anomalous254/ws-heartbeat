use crate::session::HeartbeatSession;
use dashmap::DashMap;
use std::hash::Hash;
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::time;
use tracing::{debug, warn};

struct HeartbeatEntry<S> {
    session: S,
    last_pong_ms: AtomicI64,
}

/// Tracks many sessions and sweeps them from one background task.
#[derive(Clone)]
pub struct HeartbeatRegistry<K, S>
where
    K: Eq + Hash + Clone + Send + Sync + 'static,
    S: HeartbeatSession,
{
    inner: Arc<DashMap<K, HeartbeatEntry<S>>>,
}

impl<K, S> Default for HeartbeatRegistry<K, S>
where
    K: Eq + Hash + Clone + Send + Sync + std::fmt::Display + 'static,
    S: HeartbeatSession,
{
    fn default() -> Self {
        Self::new()
    }
}

impl<K, S> HeartbeatRegistry<K, S>
where
    K: Eq + Hash + Clone + Send + Sync + std::fmt::Display + 'static,
    S: HeartbeatSession,
{
    pub fn new() -> Self {
        Self { inner: Arc::new(DashMap::new()) }
    }

    /// Call once when a connection is established.
    pub fn register(&self, session_id: K, session: S) {
        self.inner.insert(
            session_id,
            HeartbeatEntry { session, last_pong_ms: AtomicI64::new(now_ms()) },
        );
    }

    /// Call whenever a Pong (or any sign of life) arrives.
    pub fn touch(&self, session_id: &K) {
        if let Some(entry) = self.inner.get(session_id) {
            entry.last_pong_ms.store(now_ms(), Ordering::Relaxed);
        }
    }

    /// Call when the connection ends, for any reason.
    pub fn remove(&self, session_id: &K) {
        self.inner.remove(session_id);
    }

    /// How many sessions are currently tracked.
    pub fn len(&self) -> usize {
        self.inner.len()
    }

    pub fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }

    /// Start the single background sweeper. Call ONCE at startup,
    /// from inside a Tokio runtime.
    pub fn spawn_sweeper(&self, interval: Duration, timeout: Duration) {
        let registry = self.clone();
        tokio::spawn(async move {
            let mut ticker = time::interval(interval);
            let timeout_ms = timeout.as_millis() as i64;
            loop {
                ticker.tick().await;
                let now = now_ms();
                let mut stale = Vec::new();

                for entry in registry.inner.iter() {
                    let last = entry.value().last_pong_ms.load(Ordering::Relaxed);
                    if now - last > timeout_ms {
                        stale.push(entry.key().clone());
                    } else {
                        let session = entry.value().session.clone();
                        tokio::spawn(session.send_ping());
                    }
                }

                for session_id in stale {
                    if let Some((_, entry)) = registry.inner.remove(&session_id) {
                        warn!(%session_id, "heartbeat timeout, force-closing");
                        entry.session.close_session().await;
                    }
                }

                debug!(active = registry.inner.len(), "heartbeat sweep complete");
            }
        });
    }
}

fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64
}