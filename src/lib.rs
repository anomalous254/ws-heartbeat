//! # ws-heartbeat
//!
//! Framework-agnostic WebSocket heartbeat tracking.
//! One background sweeper pings every session and closes the dead ones.
//!
//! Enable an adapter with a feature: `actix` or `axum`.


mod registry;
mod session;

pub mod adapters;
pub mod prelude;

pub use registry::HeartbeatRegistry;
pub use session::HeartbeatSession;
