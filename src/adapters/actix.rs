use crate::HeartbeatSession;
use async_trait::async_trait;

#[derive(Clone)]
pub struct ActixSession(pub actix_ws::Session);

#[async_trait]
impl HeartbeatSession for ActixSession {
    async fn send_ping(mut self) {
        let _ = self.0.ping(b"").await;
    }

    async fn close_session(self) {
        let _ = self.0.close(None).await;
    }
}
