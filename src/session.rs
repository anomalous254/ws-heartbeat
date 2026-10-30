use async_trait::async_trait;

/// Anything that can be pinged and closed can be tracked.
#[async_trait]
pub trait HeartbeatSession: Clone + Send + Sync + 'static {
    /// Send a ping to the client.
    async fn send_ping(self);

    /// Close the connection.
    async fn close_session(self);
}