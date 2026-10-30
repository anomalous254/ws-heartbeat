//! Convenience re-exports.
//!
//! ```ignore
//! use ws_heartbeat::prelude::*;
//! ```

pub use crate::registry::HeartbeatRegistry;
pub use crate::session::HeartbeatSession;

#[cfg(feature = "actix")]
pub use crate::adapters::actix::ActixSession;

#[cfg(feature = "axum")]
pub use crate::adapters::axum::AxumSession;