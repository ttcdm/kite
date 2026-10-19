//! `kite mcp`: a Model Context Protocol server over stdio (newline-delimited
//! JSON-RPC 2.0). Any MCP host can run it; the host's agent then talks to the
//! broker through the `kite_*` tools. stdout carries only protocol frames;
//! diagnostics go to stderr.

use crate::paths::{Paths, resource_key};
use kite_core::Client;
use kite_proto::board::CardOp;
use kite_proto::wire::{AgentError, MAX_WAIT_MS};
use kite_proto::{Draft, ErrorCode, KiteError, KiteResult, SchemaRef};
use serde_json::{Value, json};
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::sync::{Mutex, mpsc};
use ulid::Ulid;

const SUPPORTED: &[&str] = &["2025-06-18", "2025-03-26", "2024-11-05"];
const STATES: [&str; 7] = ["backlog", "ready", "running", "review", "blocked", "merged", "archived"];
/// Hosts commonly cut tool calls off around a minute, so no tool waits longer than this.
const MAX_TOOL_WAIT_MS: u64 = 25_000;

const INSTRUCTIONS: &str = "You are one agent in a Kite team. Every message goes through a broker that \
enforces typed contracts. Call kite_contracts first: it lists who you may message, with which schemas, \
and the JSON Schema each payload must match. Use kite_request when you need an answer and kite_send for \
events. Check kite_recv for work; every request you receive must be answered with kite_respond (a result \
matching its response schema, or an error). A kite.cancel@1 event means stop working on that request. \
Take a kite_lease on a file before editing it and kite_release it when done. Work items live on the board \
(kite_board): start only cards assigned to you, record evidence with kite_card_evidence, set a handoff \
note, and move the card with kite_card_move; a refusal says which exit criterion is missing. Errors carry a code and a \
detail object saying exactly what to fix.";

struct State {
    agent: Option<String>,
    paths: Paths,
    client: Mutex<Option<Arc<Client>>>,
}

impl State {
    /// The registered client, connecting and registering on first use and
    /// again after the broker restarts.
    async fn client(&self) -> KiteResult<Arc<Client>> {
        let mut slot = self.client.lock().await;
        if let Some(c) = slot.as_ref() {
            return Ok(c.clone());
        }
        let agent = self.agent.as_deref().ok_or_else(|| {
            KiteError::new(
                ErrorCode::NotRegistered,
                "no agent name: start with `kite mcp --agent <name>` or set KITE_AGENT",
            )
        })?;
        let c = Client::connect(&self.paths.socket)
            .await
            .map_err(|e| KiteError::new(ErrorCode::ShuttingDown, e.to_string()))?;
        c.register(agent).await?;
        let c = Arc::new(c);
        *slot = Some(c.clone());
        Ok(c)
    }

    async fn forget(&self) {
        *self.client.lock().await = None;
    }
}

fn tool(name: &str, description: &str, properties: Value, required: &[&str]) -> Value {
    json!({
        "name": name,
        "description": description,
        "inputSchema": { "type": "object", "properties": properties, "required": required, "additionalProperties": false },
    })
}

pub fn tools() -> Vec<Value> {
    let wait = json!({ "type": "integer", "minimum": 0, "maximum": MAX_TOOL_WAIT_MS, "description": "How long to wait, in ms." });
    let rid = json!({ "type": "string", "description": "The request's id." });
    vec![
        tool(
            "kite_contracts",
            "Who you are, who you may message with which schemas, and the JSON Schema of every payload. Call this first.",
            json!({}),
            &[],
        ),
        tool(
            "kite_send",
            "Send an event, or a request whose answer you will collect later with kite_await. Returns the message id.",
            json!({
                "to": { "type": "string" },
                "schema": { "type": "string", "description": "name@version, from kite_contracts" },
                "payload": { "description": "Must match the schema." },
                "timeout_ms": { "type": "integer", "minimum": 1, "description": "Requests only." },
                "id": { "type": "string", "description": "Optional idempotency key (ULID); resending it delivers once." },
            }),
            &["to", "schema", "payload"],
        ),
        tool(
            "kite_request",
            "Send a request and wait for its answer. If it is not answered within wait_ms you get status=pending and a request_id for kite_await.",
            json!({
                "to": { "type": "string" },
                "schema": { "type": "string" },
                "payload": {},
                "timeout_ms": { "type": "integer", "minimum": 1, "description": "How long the recipient has to answer." },
                "wait_ms": wait,
            }),
            &["to", "schema", "payload"],
        ),
        tool(
            "kite_await",
            "Wait for the answer to a request you sent.",
            json!({ "request_id": rid, "wait_ms": wait }),
            &["request_id"],
        ),
        tool(
            "kite_recv",
            "Get messages addressed to you. Calling it again acknowledges the previous batch.",
            json!({ "wait_ms": wait, "max": { "type": "integer", "minimum": 1, "maximum": 64 } }),
            &[],
        ),
        tool(
            "kite_respond",
            "Answer a request you received: either result (matching its response schema) or error.",
            json!({
                "request_id": rid,
                "result": {},
                "error": {
                    "type": "object",
                    "properties": { "code": { "type": "string" }, "message": { "type": "string" }, "detail": {} },
                    "required": ["code", "message"],
                },
            }),
            &["request_id"],
        ),
        tool("kite_cancel", "Withdraw a request you sent.", json!({ "request_id": rid }), &["request_id"]),
        tool(
            "kite_lease",
            "Take or renew an exclusive lease, usually on a file path, before changing it.",
            json!({ "resource": { "type": "string" }, "ttl_ms": { "type": "integer", "minimum": 1 } }),
            &["resource"],
        ),
        tool("kite_release", "Release a lease you hold.", json!({ "resource": { "type": "string" } }), &["resource"]),
        tool(
            "kite_status",
            "Every agent's state; with agent, that agent's queued messages, open requests and leases.",
            json!({ "agent": { "type": "string" } }),
            &[],
        ),
        tool(
            "kite_board",
            "The team's work items (cards): all of them, one by id, or filtered by state or owner.",
            json!({
                "id": { "type": "string" },
                "state": { "type": "string", "enum": STATES },
                "owner": { "type": "string" },
            }),
            &[],
        ),
        tool(
            "kite_card_create",
            "Propose a work item. It starts in backlog; a manager shapes and assigns it.",
            json!({
                "title": { "type": "string" },
                "acceptance": { "type": "array", "items": { "type": "string" } },
                "merge_gate": { "type": "string", "description": "The condition for merging, in words." },
                "gates": { "type": "array", "items": { "type": "string" }, "description": "Checks that must pass in order." },
                "owner": { "type": "string" },
                "branch": { "type": "string" },
                "worktree": { "type": "string", "description": "Directory relative to the project root." },
                "id": { "type": "string" },
            }),
            &["title"],
        ),
        tool(
            "kite_card_update",
            "Change a card's fields. Set handoff to the path of your handoff note before moving to review.",
            json!({
                "id": { "type": "string" },
                "title": { "type": "string" },
                "acceptance": { "type": "array", "items": { "type": "string" } },
                "merge_gate": { "type": "string" },
                "gates": { "type": "array", "items": { "type": "string" } },
                "owner": { "type": "string" },
                "branch": { "type": "string" },
                "worktree": { "type": "string" },
                "handoff": { "type": "string" },
            }),
            &["id"],
        ),
        tool(
            "kite_card_move",
            "Move a card to another column. Refused, with the reason, until the column's exit criteria are met.",
            json!({
                "id": { "type": "string" },
                "to": { "type": "string", "enum": STATES },
                "blocker": {
                    "type": "object",
                    "properties": { "reason": { "type": "string" }, "owner": { "type": "string" }, "next_action": { "type": "string" } },
                    "required": ["reason", "owner", "next_action"],
                },
                "reason": { "type": "string", "description": "Required to archive." },
                "merge_ref": { "type": "string", "description": "Required to merge: the commit or PR." },
            }),
            &["id", "to"],
        ),
        tool(
            "kite_card_evidence",
            "Attach evidence to a card (test run, diff, screenshot, log, review). With gate and pass it records that gate's result.",
            json!({
                "id": { "type": "string" },
                "kind": { "type": "string" },
                "reference": { "type": "string", "description": "Path, URL, commit or command." },
                "note": { "type": "string" },
                "gate": { "type": "string" },
                "pass": { "type": "boolean" },
            }),
            &["id", "kind", "reference"],
        ),
    ]
}

fn arg<'a>(args: &'a Value, key: &str) -> KiteResult<&'a Value> {
    args.get(key).ok_or_else(|| KiteError::new(ErrorCode::InvalidArgument, format!("missing argument {key}")))
}

fn arg_str<'a>(args: &'a Value, key: &str) -> KiteResult<&'a str> {
    arg(args, key)?
        .as_str()
        .ok_or_else(|| KiteError::new(ErrorCode::InvalidArgument, format!("{key} must be a string")))
}

fn arg_ulid(args: &Value, key: &str) -> KiteResult<Ulid> {
    arg_str(args, key)?
        .parse()
        .map_err(|_| KiteError::new(ErrorCode::InvalidArgument, format!("{key} is not a valid id")))
}

fn wait_of(args: &Value, default: u64) -> u64 {
    args.get("wait_ms").and_then(Value::as_u64).unwrap_or(default).min(MAX_TOOL_WAIT_MS)
}

fn draft(args: &Value) -> KiteResult<Draft> {
    let schema: SchemaRef = arg_str(args, "schema")?.parse()?;
    Ok(Draft {
        id: match args.get("id").and_then(Value::as_str) {
            Some(s) => Some(s.parse().map_err(|_| KiteError::new(ErrorCode::InvalidArgument, "id is not a ULID"))?),
            None => None,
        },
        to: arg_str(args, "to")?.to_string(),
        schema,
        kind: None,
        payload: arg(args, "payload")?.clone(),
        timeout_ms: args.get("timeout_ms").and_then(Value::as_u64),
        blocking: false,
    })
}

fn parse<T: serde::de::DeserializeOwned>(v: Value, what: &str) -> KiteResult<T> {
    serde_json::from_value(v).map_err(|e| KiteError::new(ErrorCode::InvalidArgument, format!("bad {what}: {e}")))
}

/// Builds a card operation from tool arguments that mirror its fields.
fn card_op(action: &str, mut args: Value) -> KiteResult<CardOp> {
    if let Value::Object(m) = &mut args {
        m.insert("action".into(), json!(action));
    }
    parse(args, action)
}

fn value<T: serde::Serialize>(v: T) -> Value {
    serde_json::to_value(v).expect("wire types serialize")
}

async fn call_tool(state: &State, name: &str, args: &Value) -> KiteResult<Value> {
    if name == "kite_status" || name == "kite_board" {
        // Reads need no identity; fall back to an unregistered connection.
        let c = match state.client().await {
            Ok(c) => c,
            Err(_) => Arc::new(
                Client::connect(&state.paths.socket)
                    .await
                    .map_err(|e| KiteError::new(ErrorCode::ShuttingDown, e.to_string()))?,
            ),
        };
        if name == "kite_status" {
            return Ok(value(c.status(args.get("agent").and_then(Value::as_str)).await?));
        }
        let op = match args.get("id").and_then(Value::as_str) {
            Some(id) => CardOp::Get { id: id.to_string() },
            None => card_op("list", args.clone())?,
        };
        return c.card(op).await;
    }
    let c = state.client().await?;
    let mut v = match name {
        "kite_contracts" => value(c.contracts().await?),
        "kite_send" => value(c.send(draft(args)?).await?),
        "kite_request" => {
            let (sent, got) = c.request(draft(args)?, Duration::from_millis(wait_of(args, 20_000))).await?;
            json!({ "request_id": sent.id, "answer": got })
        }
        "kite_await" => value(c.await_answer(arg_ulid(args, "request_id")?, wait_of(args, 20_000)).await?),
        "kite_recv" => {
            let max = args.get("max").and_then(Value::as_u64).unwrap_or(10).clamp(1, 64) as usize;
            value(c.recv(wait_of(args, 10_000).min(MAX_WAIT_MS), max).await?)
        }
        "kite_respond" => {
            let rid = arg_ulid(args, "request_id")?;
            match (args.get("result"), args.get("error")) {
                (Some(r), None) => c.respond(rid, r.clone()).await?,
                (None, Some(e)) => {
                    let e: AgentError = serde_json::from_value(e.clone()).map_err(|e| {
                        KiteError::new(ErrorCode::InvalidArgument, format!("error must be {{code, message}}: {e}"))
                    })?;
                    c.respond_error(rid, e).await?
                }
                _ => return Err(KiteError::new(ErrorCode::InvalidArgument, "give exactly one of result or error")),
            }
        }
        "kite_cancel" => c.cancel(arg_ulid(args, "request_id")?).await?,
        "kite_lease" => {
            let ttl = args.get("ttl_ms").and_then(Value::as_u64).unwrap_or(600_000);
            let r = resource_key(&state.paths.root, arg_str(args, "resource")?);
            value(c.lease(&r, ttl).await?)
        }
        "kite_card_create" => c.card(CardOp::Create { card: parse(args.clone(), "card")? }).await?,
        "kite_card_update" => {
            let mut patch = args.clone();
            let id = patch.as_object_mut().and_then(|m| m.remove("id")).and_then(|v| v.as_str().map(str::to_string));
            let id = id.ok_or_else(|| KiteError::new(ErrorCode::InvalidArgument, "missing argument id"))?;
            c.card(CardOp::Update { id, patch: parse(patch, "patch")? }).await?
        }
        "kite_card_move" => c.card(card_op("move", args.clone())?).await?,
        "kite_card_evidence" => c.card(card_op("evidence", args.clone())?).await?,
        "kite_release" => c.release(&resource_key(&state.paths.root, arg_str(args, "resource")?)).await?,
        other => return Err(KiteError::new(ErrorCode::InvalidArgument, format!("unknown tool {other}"))),
    };
    match &mut v {
        Value::Object(m) => {
            m.insert("inbox".into(), json!(c.inbox()));
        }
        _ => v = json!({ "result": v, "inbox": c.inbox() }),
    }
    Ok(v)
}

async fn handle(state: &State, msg: Value) -> Option<Value> {
    let id = msg.get("id").cloned()?; // notifications get no reply
    let method = msg.get("method").and_then(Value::as_str).unwrap_or_default();
    let params = msg.get("params").cloned().unwrap_or(Value::Null);
    let result = match method {
        "initialize" => {
            let asked = params.get("protocolVersion").and_then(Value::as_str).unwrap_or("");
            let version = if SUPPORTED.contains(&asked) { asked } else { SUPPORTED[0] };
            json!({
                "protocolVersion": version,
                "capabilities": { "tools": { "listChanged": false } },
                "serverInfo": { "name": "kite", "version": env!("CARGO_PKG_VERSION") },
                "instructions": INSTRUCTIONS,
            })
        }
        "ping" => json!({}),
        "tools/list" => json!({ "tools": tools() }),
        "tools/call" => {
            let name = params.get("name").and_then(Value::as_str).unwrap_or_default();
            let args = params.get("arguments").cloned().unwrap_or_else(|| json!({}));
            match call_tool(state, name, &args).await {
                Ok(v) => json!({
                    "content": [{ "type": "text", "text": serde_json::to_string_pretty(&v).expect("serializes") }],
                    "structuredContent": v,
                    "isError": false,
                }),
                Err(e) => {
                    if e.code == ErrorCode::ShuttingDown {
                        state.forget().await;
                    }
                    let detail = e.detail.as_ref().map(|d| serde_json::to_string_pretty(d).expect("serializes"));
                    json!({
                        "content": [{ "type": "text", "text": format!("{}: {}\n{}", e.code, e.message, detail.unwrap_or_default()) }],
                        "structuredContent": { "error": value(&e) },
                        "isError": true,
                    })
                }
            }
        }
        _ => {
            return Some(json!({
                "jsonrpc": "2.0", "id": id,
                "error": { "code": -32601, "message": format!("method not found: {method}") },
            }));
        }
    };
    Some(json!({ "jsonrpc": "2.0", "id": id, "result": result }))
}

/// Serves MCP on stdin/stdout until stdin closes. Requests run concurrently,
/// so a host's parallel tool calls do not queue behind a long recv.
pub async fn serve(agent: Option<String>, paths: Paths) -> anyhow::Result<()> {
    let state = Arc::new(State { agent, paths, client: Mutex::new(None) });
    let (out, mut out_rx) = mpsc::channel::<Value>(64);
    let writer = tokio::spawn(async move {
        let mut stdout = tokio::io::stdout();
        while let Some(v) = out_rx.recv().await {
            let mut line = serde_json::to_vec(&v).expect("serializes");
            line.push(b'\n');
            if stdout.write_all(&line).await.is_err() || stdout.flush().await.is_err() {
                break;
            }
        }
    });
    let mut tasks = tokio::task::JoinSet::new();
    let mut lines = BufReader::new(tokio::io::stdin()).lines();
    while let Some(line) = lines.next_line().await? {
        if line.trim().is_empty() {
            continue;
        }
        let msg: Value = match serde_json::from_str(&line) {
            Ok(v) => v,
            Err(e) => {
                let err = json!({ "jsonrpc": "2.0", "id": null, "error": { "code": -32700, "message": format!("parse error: {e}") } });
                let _ = out.send(err).await;
                continue;
            }
        };
        let (state, out) = (state.clone(), out.clone());
        tasks.spawn(async move {
            if let Some(reply) = handle(&state, msg).await {
                let _ = out.send(reply).await;
            }
        });
    }
    // stdin closed: let calls in flight answer, then exit.
    while tasks.join_next().await.is_some() {}
    drop(out);
    let _ = writer.await;
    Ok(())
}
