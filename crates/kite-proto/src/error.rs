use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::fmt;

/// Every way the broker can refuse or fail an operation. The code is the
/// stable, machine-readable part; agents branch on it, humans read `message`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ErrorCode {
    /// The frame was not valid JSON or did not match the wire protocol.
    InvalidFrame,
    /// The client speaks a different protocol version.
    ProtocolMismatch,
    /// The operation needs a registered agent and this connection has none.
    NotRegistered,
    /// This connection already registered an agent.
    AlreadyRegistered,
    /// Another live connection is already this agent.
    AlreadyConnected,
    /// The agent name is not declared in kite.toml.
    UnknownAgent,
    /// The schema reference is not declared in kite.toml.
    UnknownSchema,
    /// No route allows this sender to send this schema to this recipient.
    RouteDenied,
    /// The payload does not match the schema.
    SchemaViolation,
    /// The message kind does not match the schema's declared kind.
    WrongKind,
    /// The recipient's mailbox is full. Retry later.
    Busy,
    /// Sending this blocking request would close a cycle of agents waiting on each other.
    Deadlock,
    /// The request's deadline passed before a response arrived.
    Timeout,
    /// The request was redelivered too many times because its recipient kept disconnecting.
    PeerDown,
    /// The requester cancelled the request.
    Cancelled,
    /// No open request has this id.
    NoSuchRequest,
    /// The request is addressed to a different agent.
    NotYourRequest,
    /// This agent already has a recv or await waiting.
    WaitInProgress,
    /// The resource is leased by another agent.
    LeaseConflict,
    /// The agent does not hold a lease on this resource.
    LeaseNotHeld,
    /// No card has this id.
    NoSuchCard,
    /// The card's current state does not allow this move, or its exit criteria are not met.
    TransitionRefused,
    /// This agent may not make this change to the card.
    NotPermitted,
    /// Another active card already works in this worktree, or the card id is taken.
    CardConflict,
    /// A field is outside its allowed range.
    InvalidArgument,
    /// The broker is shutting down.
    ShuttingDown,
    /// The broker hit an internal error.
    Internal,
}

impl ErrorCode {
    pub fn as_str(self) -> &'static str {
        match self {
            ErrorCode::InvalidFrame => "invalid_frame",
            ErrorCode::ProtocolMismatch => "protocol_mismatch",
            ErrorCode::NotRegistered => "not_registered",
            ErrorCode::AlreadyRegistered => "already_registered",
            ErrorCode::AlreadyConnected => "already_connected",
            ErrorCode::UnknownAgent => "unknown_agent",
            ErrorCode::UnknownSchema => "unknown_schema",
            ErrorCode::RouteDenied => "route_denied",
            ErrorCode::SchemaViolation => "schema_violation",
            ErrorCode::WrongKind => "wrong_kind",
            ErrorCode::Busy => "busy",
            ErrorCode::Deadlock => "deadlock",
            ErrorCode::Timeout => "timeout",
            ErrorCode::PeerDown => "peer_down",
            ErrorCode::Cancelled => "cancelled",
            ErrorCode::NoSuchRequest => "no_such_request",
            ErrorCode::NotYourRequest => "not_your_request",
            ErrorCode::WaitInProgress => "wait_in_progress",
            ErrorCode::LeaseConflict => "lease_conflict",
            ErrorCode::LeaseNotHeld => "lease_not_held",
            ErrorCode::NoSuchCard => "no_such_card",
            ErrorCode::TransitionRefused => "transition_refused",
            ErrorCode::NotPermitted => "not_permitted",
            ErrorCode::CardConflict => "card_conflict",
            ErrorCode::InvalidArgument => "invalid_argument",
            ErrorCode::ShuttingDown => "shutting_down",
            ErrorCode::Internal => "internal",
        }
    }
}

impl fmt::Display for ErrorCode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// The error body sent on the wire and carried inside `kite.error@1` envelopes.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, thiserror::Error)]
#[error("{code}: {message}")]
pub struct KiteError {
    pub code: ErrorCode,
    pub message: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub detail: Option<Value>,
}

impl KiteError {
    pub fn new(code: ErrorCode, message: impl Into<String>) -> Self {
        KiteError { code, message: message.into(), detail: None }
    }

    pub fn with_detail(mut self, detail: Value) -> Self {
        self.detail = Some(detail);
        self
    }
}

pub type KiteResult<T> = Result<T, KiteError>;
