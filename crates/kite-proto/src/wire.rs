//! The broker's socket protocol: one JSON object per line in each direction.
//! A client may have many frames in flight; replies carry the frame's `id`.

use crate::envelope::{Draft, Envelope};
use crate::error::KiteError;
use crate::schema::SchemaRef;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use ulid::Ulid;

pub const PROTOCOL: &str = "kite/1";
/// Longest frame the broker reads; longer lines are rejected and the connection closed.
pub const MAX_FRAME_BYTES: usize = 1 << 20;
/// Longest a single recv or await may wait.
pub const MAX_WAIT_MS: u64 = 300_000;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ClientFrame {
    pub id: u64,
    #[serde(flatten)]
    pub op: Op,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "op", rename_all = "snake_case")]
pub enum Op {
    /// Binds this connection to a declared agent. One live connection per agent.
    Register {
        agent: String,
        protocol: String,
    },
    /// Submits a request or event.
    Send {
        draft: Draft,
    },
    /// Waits up to `wait_ms` for messages and hands out at most `max`.
    /// Calling `recv` again acknowledges everything the previous call handed out.
    Recv {
        #[serde(default)]
        wait_ms: u64,
        #[serde(default = "default_max")]
        max: usize,
    },
    /// Acknowledges handed-out messages without receiving more.
    Ack {
        ids: Vec<Ulid>,
    },
    /// Waits up to `wait_ms` for the answer to one request this agent sent.
    Await {
        request_id: Ulid,
        #[serde(default)]
        wait_ms: u64,
    },
    /// Answers a request this agent received. Exactly one of `result` or `error`.
    Respond {
        request_id: Ulid,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        result: Option<Value>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        error: Option<AgentError>,
    },
    /// Withdraws a request this agent sent.
    Cancel {
        request_id: Ulid,
    },
    /// Takes or renews an exclusive lease.
    Lease {
        resource: String,
        ttl_ms: u64,
    },
    Release {
        resource: String,
    },
    /// Who holds a resource. Allowed without registering.
    LeaseCheck {
        resource: String,
    },
    /// Broker state, plus full detail for one agent if `agent` is given.
    /// Allowed without registering.
    Status {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        agent: Option<String>,
    },
    /// The contracts that apply to this agent: what it may send to whom, what it may receive.
    Contracts,
    /// Agent Kanban. `get` and `list` are allowed without registering.
    Card {
        card: crate::board::CardOp,
    },
}

fn default_max() -> usize {
    16
}

/// A responder's refusal. `code` is the responder's own, for example `cannot_build`;
/// it is carried in a `kite.error@1` envelope.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AgentError {
    pub code: String,
    pub message: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub detail: Option<Value>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ServerFrame {
    pub id: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ok: Option<Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<KiteError>,
    /// Messages waiting in this agent's mailbox after the operation.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub inbox: Option<usize>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RegisterOk {
    pub agent: String,
    pub protocol: String,
    pub inbox: usize,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SendOk {
    pub id: Ulid,
    pub kind: crate::envelope::Kind,
    pub seq: u64,
    pub lc: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub deadline_ms: Option<u64>,
    /// True when the id was already accepted; nothing new was delivered.
    pub duplicate: bool,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RecvOk {
    pub messages: Vec<Envelope>,
    /// Messages still waiting after this batch.
    pub remaining: usize,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "status", rename_all = "snake_case")]
pub enum AwaitOk {
    /// The answer: an envelope of kind `response` or `error`.
    Done { response: Envelope },
    /// No answer yet; the request is still open.
    Pending { request_id: Ulid },
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct LeaseInfo {
    pub resource: String,
    pub holder: String,
    pub expires_at_ms: u64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct LeaseCheckOk {
    pub resource: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub lease: Option<LeaseInfo>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ProcessState {
    Starting,
    Running,
    /// Exited and waiting out the backoff before the next start.
    Restarting,
    /// Exited and the restart policy says not to start it again.
    Exited,
    /// Exited after using up `max_restarts`.
    GaveUp,
}

/// The OS process behind an agent, when `kite up` launched it.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ProcessStatus {
    pub state: ProcessState,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pid: Option<u32>,
    pub restarts: u32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_exit: Option<i32>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AgentStatus {
    pub name: String,
    pub connected: bool,
    /// Messages queued and not yet handed out.
    pub mailbox: usize,
    /// Messages handed out and not yet acknowledged.
    pub in_flight: usize,
    /// Open requests this agent sent.
    pub awaiting: usize,
    /// Open requests addressed to this agent.
    pub owes: usize,
    pub leases: usize,
    /// Cards this agent owns that are not merged or archived.
    pub cards: usize,
    /// Unix ms of this agent's last operation: its heartbeat.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_seen_ms: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub process: Option<ProcessStatus>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct MessageSummary {
    pub id: Ulid,
    pub from: String,
    pub kind: crate::envelope::Kind,
    pub schema: SchemaRef,
    pub lc: u64,
    pub redelivered: u32,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RequestSummary {
    pub id: Ulid,
    pub from: String,
    pub to: String,
    pub schema: SchemaRef,
    pub deadline_ms: u64,
    pub blocking: bool,
}

/// Everything the broker knows about one agent.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AgentDetail {
    pub name: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    pub connected: bool,
    /// The agent has a recv waiting for mail.
    pub in_recv: bool,
    /// Requests the agent has an await waiting on.
    pub in_await: Vec<Ulid>,
    pub mailbox: Vec<MessageSummary>,
    pub in_flight: Vec<MessageSummary>,
    pub awaiting: Vec<RequestSummary>,
    pub owes: Vec<RequestSummary>,
    pub leases: Vec<LeaseInfo>,
    /// Ids of the cards this agent owns that are not merged or archived.
    pub cards: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_seen_ms: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub process: Option<ProcessStatus>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct StatusOk {
    pub protocol: String,
    pub lc: u64,
    pub agents: Vec<AgentStatus>,
    pub leases: Vec<LeaseInfo>,
    /// Blocking waits: `[from, to]` pairs.
    pub waits: Vec<(String, String)>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub detail: Option<AgentDetail>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ContractView {
    pub schema: SchemaRef,
    pub kind: crate::schema::ContractKind,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub response: Option<SchemaRef>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub response_schema: Option<Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    pub json_schema: Value,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RouteView {
    pub peer: String,
    pub schemas: Vec<SchemaRef>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ContractsOk {
    pub agent: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    /// Who this agent may send to, and with which schemas.
    pub may_send: Vec<RouteView>,
    /// Who may send to this agent, and with which schemas.
    pub may_receive: Vec<RouteView>,
    /// Every contract named above.
    pub contracts: Vec<ContractView>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn frames_round_trip_with_flattened_op() {
        let f: ClientFrame = serde_json::from_value(json!({"id": 7, "op": "recv", "wait_ms": 100})).unwrap();
        assert_eq!(f, ClientFrame { id: 7, op: Op::Recv { wait_ms: 100, max: 16 } });
        let back = serde_json::to_value(&f).unwrap();
        assert_eq!(back["op"], "recv");
        let status: ClientFrame = serde_json::from_value(json!({"id": 1, "op": "status"})).unwrap();
        assert_eq!(status.op, Op::Status { agent: None });
    }

    #[test]
    fn unknown_ops_are_rejected() {
        assert!(serde_json::from_value::<ClientFrame>(json!({"id": 1, "op": "shout"})).is_err());
    }
}
