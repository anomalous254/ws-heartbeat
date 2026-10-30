use crate::session::HeartbeatSession;
use async_trait::async_trait;
use axum::extract::ws::Message;
use tokio::sync::mpsc;

/// Wraps the channel that feeds your socket's writer task.
#[derive(Clone)]
pub struct AxumSession(pub mpsc::Sender<Message>);

#[async_trait]
impl HeartbeatSession for AxumSession {
    async fn send_ping(self) {
        let _ = self.0.send(Message::Ping(Vec::new().into())).await;
    }

    async fn close_session(self) {
        let _ = self.0.send(Message::Close(None)).await;
    }
}