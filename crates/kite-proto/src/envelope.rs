use crate::schema::SchemaRef;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use ulid::Ulid;

/// The four things a message can be.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Kind {
    /// Expects exactly one `response` or `error` back.
    Request,
    /// The successful answer to a request.
    Response,
    /// The failed answer to a request, with a `kite.error@1` payload.
    Error,
    /// One-way; nothing comes back.
    Event,
}

/// A message after the broker accepted it. Every field except `payload` is
/// stamped or checked by the broker, so a recipient can trust all of them.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Envelope {
    pub id: Ulid,
    pub from: String,
    pub to: String,
    pub kind: Kind,
    pub schema: SchemaRef,
    /// For `response` and `error`: the id of the request being answered.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub correlation_id: Option<Ulid>,
    /// Position of this message among those `from` has sent to `to`, starting at 1.
    pub seq: u64,
    /// Broker logical clock: a total order over every message the broker accepted.
    pub lc: u64,
    /// Unix milliseconds at which the broker accepted the message.
    pub sent_at_ms: u64,
    /// For requests: Unix milliseconds after which the broker answers with `timeout`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub deadline_ms: Option<u64>,
    /// How many times this message was handed out before and not acknowledged.
    #[serde(default)]
    pub redelivered: u32,
    pub payload: Value,
}

/// What an agent submits. The broker fills in everything else.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Draft {
    /// Optional idempotency key. Sending the same id twice delivers once.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub id: Option<Ulid>,
    pub to: String,
    pub schema: SchemaRef,
    /// Optional. If given it must match the schema's kind; if omitted it is taken from the schema.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub kind: Option<Kind>,
    pub payload: Value,
    /// Requests only: how long the recipient has to answer.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub timeout_ms: Option<u64>,
    /// Requests only: the sender is waiting on the answer. Blocking requests
    /// are checked for wait cycles (deadlocks) before they are accepted.
    #[serde(default)]
    pub blocking: bool,
}
