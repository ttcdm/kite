//! Kite's message contracts: envelopes, schemas, configuration and the wire protocol.
//! No I/O here beyond reading kite.toml; the broker lives in `kite-core`.

pub mod board;
pub mod config;
pub mod envelope;
pub mod error;
pub mod schema;
pub mod wire;

pub use config::Config;
pub use envelope::{Draft, Envelope, Kind};
pub use error::{ErrorCode, KiteError, KiteResult};
pub use schema::{ContractKind, Registry, SchemaRef};
