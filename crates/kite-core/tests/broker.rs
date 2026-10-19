//! Broker behaviour through its public handle, with a real ledger on disk.
//! Timeouts use tokio's paused clock, so no assertion depends on wall time.

use kite_core::broker::{BrokerHandle, ConnId, Reply};
use kite_core::{Broker, Ledger, ledger};
use kite_proto::wire::*;
use kite_proto::*;
use serde::de::DeserializeOwned;
use serde_json::{Value, json};
use std::sync::Arc;
use tempfile::TempDir;

const CONFIG: &str = r#"
[broker]
mailbox_capacity = 4
require_tokens = false
max_redeliveries = 1

[schemas."task.do@1"]
kind = "request"
response = "task.done@1"
schema = { type = "object", required = ["goal"], properties = { goal = { type = "string" } } }

[schemas."task.done@1"]
kind = "response"
schema = { type = "object", required = ["ok"], properties = { ok = { type = "boolean" } } }

[schemas."note@1"]
kind = "event"
schema = { type = "object" }

[agents.lead]
description = "hands out work"
[agents.worker]
[agents.other]

[[routes]]
from = "lead"
to = ["worker", "other"]
schemas = ["task.do@1", "note@1"]

[[routes]]
from = "worker"
to = "lead"
schemas = ["task.do@1", "note@1"]

[board]
managers = ["lead"]
default_gates = ["tests"]
"#;

struct Env {
    h: BrokerHandle,
    dir: TempDir,
    task: tokio::task::JoinHandle<std::io::Result<()>>,
}

fn start() -> Env {
    start_in(tempfile::tempdir().unwrap())
}

/// Starts a broker on an existing project directory, continuing its ledger.
fn start_in(dir: TempDir) -> Env {
    start_with(dir, CONFIG)
}

fn start_with(dir: TempDir, config: &str) -> Env {
    let config = Arc::new(Config::parse(config, dir.path()).unwrap());
    let ledger = Ledger::open(&config.broker.ledger, config.broker.fsync).unwrap();
    let (h, task) = Broker::spawn(config, ledger);
    Env { h, dir, task }
}

impl Env {
    async fn agent(&self, name: &str) -> ConnId {
        let c = self.h.connect();
        let r = self.h.call(c, Op::Register { agent: name.into(), protocol: PROTOCOL.into(), token: None }).await;
        r.result.unwrap();
        c
    }

    /// Stops the broker, verifies the ledger chain and returns its events.
    async fn stop(self) -> Vec<Value> {
        self.stop_keep().await.1
    }

    /// Like `stop`, but hands back the project directory for a restart.
    async fn stop_keep(self) -> (TempDir, Vec<Value>) {
        self.h.shutdown().await;
        self.task.await.unwrap().unwrap();
        let path = self.dir.path().join(".kite/ledger.jsonl");
        ledger::verify(&path).unwrap();
        let events = std::fs::read_to_string(&path)
            .unwrap()
            .lines()
            .map(|l| serde_json::from_str::<Value>(l).unwrap()["event"].clone())
            .collect();
        (self.dir, events)
    }
}

fn ok<T: DeserializeOwned>(r: Reply) -> T {
    serde_json::from_value(r.result.unwrap()).unwrap()
}

fn code(r: Reply) -> ErrorCode {
    r.result.unwrap_err().code
}

fn draft(to: &str, schema: &str, payload: Value) -> Draft {
    Draft {
        id: None,
        to: to.into(),
        schema: schema.parse().unwrap(),
        kind: None,
        payload,
        timeout_ms: None,
        blocking: false,
    }
}

fn send(d: Draft) -> Op {
    Op::Send { draft: d }
}

fn recv(wait_ms: u64) -> Op {
    Op::Recv { wait_ms, max: 16 }
}

fn answer(id: ulid::Ulid, result: Value) -> Op {
    Op::Respond { request_id: id, result: Some(result), error: None }
}

#[tokio::test]
async fn registration_is_strict() {
    let e = start();
    let c = e.h.connect();
    let r = e.h.call(c, Op::Register { agent: "ghost".into(), protocol: PROTOCOL.into(), token: None }).await;
    assert_eq!(code(r), ErrorCode::UnknownAgent);
    let r = e.h.call(c, Op::Register { agent: "lead".into(), protocol: "kite/0".into(), token: None }).await;
    assert_eq!(code(r), ErrorCode::ProtocolMismatch);
    assert_eq!(code(e.h.call(c, recv(0)).await), ErrorCode::NotRegistered);
    e.agent("lead").await;
    let c2 = e.h.connect();
    let r = e.h.call(c2, Op::Register { agent: "lead".into(), protocol: PROTOCOL.into(), token: None }).await;
    assert_eq!(code(r), ErrorCode::AlreadyConnected);
    // Read-only operations work without registering.
    let s: StatusOk = ok(e.h.call(c2, Op::Status { agent: None }).await);
    assert_eq!(s.agents.len(), 3);
    e.stop().await;
}

#[tokio::test]
async fn routes_schemas_and_kinds_are_enforced() {
    let e = start();
    let worker = e.agent("worker").await;

    let err = e.h.call(worker, send(draft("other", "note@1", json!({})))).await.result.unwrap_err();
    assert_eq!(err.code, ErrorCode::RouteDenied);
    assert_eq!(err.detail.unwrap()["allowed"], json!([]));

    let err = e.h.call(worker, send(draft("lead", "task.do@1", json!({"goal": 3})))).await.result.unwrap_err();
    assert_eq!(err.code, ErrorCode::SchemaViolation);
    assert_eq!(err.detail.unwrap()["violations"][0]["path"], "/goal");

    let r = e.h.call(worker, send(draft("lead", "task.done@1", json!({"ok": true})))).await;
    assert_eq!(code(r), ErrorCode::WrongKind);

    let mut d = draft("lead", "note@1", json!({}));
    d.kind = Some(Kind::Request);
    assert_eq!(code(e.h.call(worker, send(d)).await), ErrorCode::WrongKind);

    let r = e.h.call(worker, send(draft("lead", "nope@1", json!({})))).await;
    assert_eq!(code(r), ErrorCode::UnknownSchema);

    let mut d = draft("lead", "note@1", json!({}));
    d.timeout_ms = Some(10);
    assert_eq!(code(e.h.call(worker, send(d)).await), ErrorCode::InvalidArgument);

    let events = e.stop().await;
    assert_eq!(events.iter().filter(|ev| ev["type"] == "refused").count(), 6);
}

#[tokio::test]
async fn a_request_gets_exactly_one_checked_answer() {
    let e = start();
    let lead = e.agent("lead").await;
    let worker = e.agent("worker").await;
    let other = e.agent("other").await;

    let mut d = draft("worker", "task.do@1", json!({"goal": "build"}));
    d.blocking = true;
    let sent: SendOk = ok(e.h.call(lead, send(d)).await);
    assert_eq!((sent.kind, sent.seq), (Kind::Request, 1));

    let got: RecvOk = ok(e.h.call(worker, recv(0)).await);
    let req = &got.messages[0];
    assert_eq!((req.id, req.from.as_str(), req.kind), (sent.id, "lead", Kind::Request));
    assert!(req.deadline_ms.is_some());

    let status: StatusOk = ok(e.h.call(lead, Op::Status { agent: None }).await);
    assert_eq!(status.waits, vec![("lead".to_string(), "worker".to_string())]);

    assert_eq!(code(e.h.call(worker, answer(sent.id, json!({"ok": "yes"}))).await), ErrorCode::SchemaViolation);
    assert_eq!(code(e.h.call(other, answer(sent.id, json!({"ok": true}))).await), ErrorCode::NotYourRequest);
    let both = Op::Respond {
        request_id: sent.id,
        result: Some(json!({"ok": true})),
        error: Some(AgentError { code: "x".into(), message: "y".into(), detail: None }),
    };
    assert_eq!(code(e.h.call(worker, both).await), ErrorCode::InvalidArgument);

    e.h.call(worker, answer(sent.id, json!({"ok": true}))).await.result.unwrap();
    let again = e.h.call(worker, answer(sent.id, json!({"ok": true}))).await.result.unwrap_err();
    assert_eq!(again.code, ErrorCode::NoSuchRequest);
    assert_eq!(again.detail.unwrap()["closed"], "answered");

    let AwaitOk::Done { response } = ok(e.h.call(lead, Op::Await { request_id: sent.id, wait_ms: 0 }).await) else {
        panic!("expected an answer");
    };
    assert_eq!(response.kind, Kind::Response);
    assert_eq!(response.correlation_id, Some(sent.id));
    assert_eq!(response.schema.to_string(), "task.done@1");
    assert!(response.lc > req.lc);

    let status: StatusOk = ok(e.h.call(lead, Op::Status { agent: None }).await);
    assert!(status.waits.is_empty());
    e.stop().await;
}

#[tokio::test]
async fn responders_can_refuse_with_their_own_error_code() {
    let e = start();
    let lead = e.agent("lead").await;
    let worker = e.agent("worker").await;
    let sent: SendOk = ok(e.h.call(lead, send(draft("worker", "task.do@1", json!({"goal": "x"})))).await);
    let refuse = Op::Respond {
        request_id: sent.id,
        result: None,
        error: Some(AgentError { code: "cannot_build".into(), message: "no compiler".into(), detail: None }),
    };
    e.h.call(worker, refuse).await.result.unwrap();
    let AwaitOk::Done { response } = ok(e.h.call(lead, Op::Await { request_id: sent.id, wait_ms: 0 }).await) else {
        panic!("expected an answer");
    };
    assert_eq!(response.kind, Kind::Error);
    assert_eq!(response.payload["code"], "cannot_build");
    e.stop().await;
}

#[tokio::test(start_paused = true)]
async fn a_missed_deadline_answers_with_timeout_and_withdraws_the_request() {
    let e = start();
    let lead = e.agent("lead").await;
    let worker = e.agent("worker").await;
    let mut d = draft("worker", "task.do@1", json!({"goal": "slow"}));
    d.timeout_ms = Some(1_000);
    let sent: SendOk = ok(e.h.call(lead, send(d)).await);

    // The await outlives the deadline, so the broker's timeout is what wakes it.
    let AwaitOk::Done { response } = ok(e.h.call(lead, Op::Await { request_id: sent.id, wait_ms: 60_000 }).await)
    else {
        panic!("expected the timeout answer");
    };
    assert_eq!((response.kind, response.from.as_str()), (Kind::Error, "kite"));
    assert_eq!(response.payload["code"], "timeout");

    // The worker never picked the request up, so it is withdrawn rather than cancelled.
    let got: RecvOk = ok(e.h.call(worker, recv(0)).await);
    assert!(got.messages.is_empty());
    e.stop().await;
}

#[tokio::test(start_paused = true)]
async fn a_held_request_that_times_out_sends_the_holder_a_cancel() {
    let e = start();
    let lead = e.agent("lead").await;
    let worker = e.agent("worker").await;
    let mut d = draft("worker", "task.do@1", json!({"goal": "slow"}));
    d.timeout_ms = Some(1_000);
    let sent: SendOk = ok(e.h.call(lead, send(d)).await);
    let got: RecvOk = ok(e.h.call(worker, recv(0)).await);
    assert_eq!(got.messages.len(), 1);

    let got: RecvOk = ok(e.h.call(worker, recv(60_000)).await);
    let cancel = &got.messages[0];
    assert_eq!(cancel.schema.to_string(), "kite.cancel@1");
    assert_eq!(cancel.payload, json!({"request_id": sent.id.to_string(), "reason": "timeout"}));
    let err = e.h.call(worker, answer(sent.id, json!({"ok": true}))).await.result.unwrap_err();
    assert_eq!(err.detail.unwrap()["closed"], "timeout");
    e.stop().await;
}

#[tokio::test]
async fn cancelling_wakes_the_requesters_await() {
    let e = start();
    let lead = e.agent("lead").await;
    e.agent("worker").await;
    let sent: SendOk = ok(e.h.call(lead, send(draft("worker", "task.do@1", json!({"goal": "x"})))).await);
    let waiting = e.h.submit(lead, Op::Await { request_id: sent.id, wait_ms: 300_000 }).await;
    e.h.call(lead, Op::Cancel { request_id: sent.id }).await.result.unwrap();
    assert_eq!(code(waiting.await.unwrap()), ErrorCode::Cancelled);
    e.stop().await;
}

#[tokio::test]
async fn a_blocking_request_that_closes_a_wait_cycle_is_refused() {
    let e = start();
    let lead = e.agent("lead").await;
    let worker = e.agent("worker").await;
    let mut d = draft("worker", "task.do@1", json!({"goal": "a"}));
    d.blocking = true;
    e.h.call(lead, send(d)).await.result.unwrap();

    let mut back = draft("lead", "task.do@1", json!({"goal": "b"}));
    back.blocking = true;
    let err = e.h.call(worker, send(back.clone())).await.result.unwrap_err();
    assert_eq!(err.code, ErrorCode::Deadlock);
    assert_eq!(err.detail.unwrap()["cycle"], json!(["worker", "lead", "worker"]));

    // The same request without blocking is not a wait, so it is allowed.
    back.blocking = false;
    e.h.call(worker, send(back)).await.result.unwrap();
    let events = e.stop().await;
    assert!(events.iter().any(|ev| ev["type"] == "deadlock_prevented"));
}

#[tokio::test]
async fn a_full_mailbox_refuses_new_work_but_not_answers() {
    let e = start();
    let lead = e.agent("lead").await;
    let worker = e.agent("worker").await;
    for _ in 0..4 {
        e.h.call(worker, send(draft("lead", "note@1", json!({})))).await.result.unwrap();
    }
    let full = e.h.call(worker, send(draft("lead", "note@1", json!({})))).await.result.unwrap_err();
    assert_eq!(full.code, ErrorCode::Busy);
    assert_eq!(full.detail.unwrap()["capacity"], 4);

    // The answer to lead's own request still gets through its full mailbox.
    let sent: SendOk = ok(e.h.call(lead, send(draft("worker", "task.do@1", json!({"goal": "x"})))).await);
    e.h.call(worker, answer(sent.id, json!({"ok": true}))).await.result.unwrap();
    let status: StatusOk = ok(e.h.call(lead, Op::Status { agent: None }).await);
    assert_eq!(status.agents.iter().find(|a| a.name == "lead").unwrap().mailbox, 5);
    e.stop().await;
}

#[tokio::test]
async fn resending_the_same_id_delivers_once() {
    let e = start();
    let lead = e.agent("lead").await;
    let worker = e.agent("worker").await;
    let mut d = draft("lead", "note@1", json!({"n": 1}));
    d.id = Some(ulid::Ulid::generate());
    let first: SendOk = ok(e.h.call(worker, send(d.clone())).await);
    let second: SendOk = ok(e.h.call(worker, send(d)).await);
    assert!(!first.duplicate && second.duplicate);
    assert_eq!((first.id, first.seq), (second.id, second.seq));
    let got: RecvOk = ok(e.h.call(lead, recv(0)).await);
    assert_eq!(got.messages.len(), 1);
    e.stop().await;
}

#[tokio::test]
async fn messages_from_one_sender_arrive_in_order() {
    let e = start();
    let lead = e.agent("lead").await;
    let worker = e.agent("worker").await;
    let mut seen = Vec::new();
    for i in 0..12 {
        e.h.call(worker, send(draft("lead", "note@1", json!({"i": i})))).await.result.unwrap();
        if i % 3 == 2 {
            let got: RecvOk = ok(e.h.call(lead, recv(0)).await);
            // Handed-out messages count against the mailbox until acknowledged.
            let ids = got.messages.iter().map(|m| m.id).collect();
            e.h.call(lead, Op::Ack { ids }).await.result.unwrap();
            seen.extend(got.messages);
        }
    }
    let order: Vec<u64> = seen.iter().map(|m| m.payload["i"].as_u64().unwrap()).collect();
    assert_eq!(order, (0..12).collect::<Vec<_>>());
    let seqs: Vec<u64> = seen.iter().map(|m| m.seq).collect();
    assert_eq!(seqs, (1..=12).collect::<Vec<_>>());
    assert!(seen.windows(2).all(|w| w[0].lc < w[1].lc));
    e.stop().await;
}

#[tokio::test]
async fn a_waiting_recv_is_woken_by_a_send() {
    let e = start();
    let lead = e.agent("lead").await;
    let worker = e.agent("worker").await;
    let pending = e.h.submit(lead, recv(300_000)).await;
    assert_eq!(code(e.h.call(lead, recv(0)).await), ErrorCode::WaitInProgress);
    e.h.call(worker, send(draft("lead", "note@1", json!({"hi": 1})))).await.result.unwrap();
    let got: RecvOk = ok(pending.await.unwrap());
    assert_eq!(got.messages[0].payload["hi"], 1);
    let acked = e.h.call(lead, Op::Ack { ids: vec![got.messages[0].id] }).await.result.unwrap();
    assert_eq!(acked["acked"], 1);
    e.stop().await;
}

#[tokio::test]
async fn an_unacknowledged_message_is_redelivered_then_dead_lettered() {
    let e = start();
    let lead = e.agent("lead").await;
    let sent: SendOk = ok(e.h.call(lead, send(draft("worker", "task.do@1", json!({"goal": "x"})))).await);

    let w1 = e.agent("worker").await;
    let got: RecvOk = ok(e.h.call(w1, recv(0)).await);
    assert_eq!(got.messages[0].redelivered, 0);
    e.h.disconnect(w1).await;

    let w2 = e.agent("worker").await;
    let got: RecvOk = ok(e.h.call(w2, recv(0)).await);
    assert_eq!((got.messages[0].id, got.messages[0].redelivered), (sent.id, 1));
    e.h.disconnect(w2).await;

    // max_redeliveries = 1, so the second loss gives up and tells the requester.
    let AwaitOk::Done { response } = ok(e.h.call(lead, Op::Await { request_id: sent.id, wait_ms: 0 }).await) else {
        panic!("expected peer_down");
    };
    assert_eq!(response.payload["code"], "peer_down");
    e.stop().await;
}

#[tokio::test(start_paused = true)]
async fn leases_are_exclusive_expire_and_die_with_their_holder() {
    let e = start();
    let lead = e.agent("lead").await;
    let worker = e.agent("worker").await;
    let lease = |r: &str, ttl| Op::Lease { resource: r.into(), ttl_ms: ttl };
    e.h.call(lead, lease("src/a.rs", 1_000)).await.result.unwrap();
    let conflict = e.h.call(worker, lease("src/a.rs", 1_000)).await.result.unwrap_err();
    assert_eq!(conflict.code, ErrorCode::LeaseConflict);
    assert_eq!(conflict.detail.unwrap()["holder"], "lead");
    assert_eq!(code(e.h.call(worker, Op::Release { resource: "src/a.rs".into() }).await), ErrorCode::LeaseNotHeld);

    // Let the lease run out: a parked recv lets the paused clock advance.
    let _: RecvOk = ok(e.h.call(worker, recv(2_000)).await);
    e.h.call(worker, lease("src/a.rs", 1_000)).await.result.unwrap();

    e.h.disconnect(worker).await;
    let check: LeaseCheckOk = ok(e.h.call(lead, Op::LeaseCheck { resource: "src/a.rs".into() }).await);
    assert_eq!(check.lease, None);
    let events = e.stop().await;
    assert!(events.iter().any(|ev| ev["type"] == "lease_expired"));
}

#[tokio::test]
async fn status_shows_one_agent_in_detail() {
    let e = start();
    let lead = e.agent("lead").await;
    e.agent("worker").await;
    let sent: SendOk = ok(e.h.call(lead, send(draft("worker", "task.do@1", json!({"goal": "x"})))).await);
    e.h.call(lead, Op::Lease { resource: "docs".into(), ttl_ms: 60_000 }).await.result.unwrap();
    e.h.set_process(
        "worker",
        ProcessStatus { state: ProcessState::Running, pid: Some(42), restarts: 1, last_exit: Some(1) },
    )
    .await;

    let s: StatusOk = ok(e.h.call(lead, Op::Status { agent: Some("worker".into()) }).await);
    let d = s.detail.unwrap();
    assert!(d.connected && !d.in_recv);
    assert_eq!(d.mailbox[0].id, sent.id);
    assert_eq!(d.owes[0].from, "lead");
    assert_eq!(d.process.unwrap().pid, Some(42));
    let s: StatusOk = ok(e.h.call(lead, Op::Status { agent: Some("lead".into()) }).await);
    let d = s.detail.unwrap();
    assert_eq!(d.description.as_deref(), Some("hands out work"));
    assert_eq!((d.awaiting.len(), d.leases[0].resource.as_str()), (1, "docs"));
    let r = e.h.call(lead, Op::Status { agent: Some("ghost".into()) }).await;
    assert_eq!(code(r), ErrorCode::UnknownAgent);
    e.stop().await;
}

#[tokio::test]
async fn contracts_list_only_this_agents_routes() {
    let e = start();
    let worker = e.agent("worker").await;
    let c: ContractsOk = ok(e.h.call(worker, Op::Contracts).await);
    assert_eq!(c.may_send.len(), 1);
    assert_eq!(c.may_send[0].peer, "lead");
    assert_eq!(c.may_receive[0].peer, "lead");
    let names: Vec<String> = c.contracts.iter().map(|x| x.schema.to_string()).collect();
    for n in ["kite.cancel@1", "kite.error@1", "note@1", "task.do@1", "task.done@1"] {
        assert!(names.contains(&n.to_string()), "{n} missing from {names:?}");
    }
    e.stop().await;
}

#[tokio::test]
async fn the_ledger_records_accepted_and_refused_work() {
    let e = start();
    let lead = e.agent("lead").await;
    e.agent("worker").await;
    e.h.call(lead, send(draft("worker", "note@1", json!({"x": 1})))).await.result.unwrap();
    e.h.call(lead, send(draft("worker", "note@1", json!(3)))).await.result.unwrap_err();
    let events = e.stop().await;
    let types: Vec<&str> = events.iter().map(|ev| ev["type"].as_str().unwrap()).collect();
    assert_eq!(types.first(), Some(&"started"));
    assert_eq!(types.last(), Some(&"stopped"));
    assert!(types.contains(&"accepted") && types.contains(&"refused"));
    let accepted = events.iter().find(|ev| ev["type"] == "accepted").unwrap();
    assert_eq!(accepted["envelope"]["payload"], json!({"x": 1}));
}

fn card(op: kite_proto::board::CardOp) -> Op {
    Op::Card { card: op }
}

#[tokio::test]
async fn the_board_survives_a_restart_and_tells_owners_what_changed() {
    use kite_proto::board::*;
    let e = start();
    let lead = e.agent("lead").await;
    let worker = e.agent("worker").await;
    let draft = CardDraft {
        title: "Add a flag".into(),
        acceptance: vec!["flag parses".into()],
        merge_gate: Some("tests pass".into()),
        ..Default::default()
    };
    let created: Card = ok(e.h.call(lead, card(CardOp::Create { card: draft })).await);
    assert_eq!((created.id.as_str(), created.gates.clone()), ("card-001", vec!["tests".to_string()]));

    // Only the manager may assign; the new owner hears about it.
    let assign = |owner: &str| {
        card(CardOp::Update {
            id: "card-001".into(),
            patch: CardPatch { owner: Some(owner.into()), ..Default::default() },
        })
    };
    assert_eq!(code(e.h.call(worker, assign("worker")).await), ErrorCode::NotPermitted);
    e.h.call(lead, assign("worker")).await.result.unwrap();
    let got: RecvOk = ok(e.h.call(worker, recv(0)).await);
    let note = &got.messages[0];
    assert_eq!((note.from.as_str(), note.schema.to_string()), ("kite", "kite.card@1".to_string()));
    assert_eq!(note.payload["action"], "assigned");
    assert_eq!(note.payload["card_id"], "card-001");

    // The status heartbeat and card count reflect the worker's activity.
    let s: StatusOk = ok(e.h.call(lead, Op::Status { agent: Some("worker".into()) }).await);
    assert!(s.agents.iter().find(|a| a.name == "worker").unwrap().last_seen_ms.is_some());
    assert_eq!(s.detail.unwrap().cards, vec!["card-001".to_string()]);

    let (dir, events) = e.stop_keep().await;
    assert_eq!(events.iter().filter(|ev| ev["type"] == "card").count(), 2);

    // A new broker on the same ledger has the same board, readable without registering.
    let e = start_in(dir);
    let observer = e.h.connect();
    let listed = e.h.call(observer, card(CardOp::List { state: None, owner: Some("worker".into()) })).await;
    let cards: Vec<Card> = serde_json::from_value(listed.result.unwrap()["cards"].clone()).unwrap();
    assert_eq!(cards.len(), 1);
    assert_eq!((cards[0].owner.as_deref(), cards[0].state), (Some("worker"), CardState::Backlog));
    let refused =
        e.h.call(
            observer,
            card(CardOp::Move {
                id: "card-001".into(),
                to: CardState::Ready,
                blocker: None,
                reason: None,
                merge_ref: None,
            }),
        )
        .await;
    assert_eq!(code(refused), ErrorCode::NotRegistered);
    let lead = e.agent("lead").await;
    let next: Card = ok(e
        .h
        .call(lead, card(CardOp::Create { card: CardDraft { title: "Next".into(), ..Default::default() } }))
        .await);
    assert_eq!(next.id, "card-002");
    e.stop().await;
}

fn status_of(r: Reply) -> (StatusOk, AgentDetail) {
    let mut s: StatusOk = ok(r);
    let d = s.detail.take().unwrap();
    (s, d)
}

#[tokio::test]
async fn queued_messages_and_open_requests_survive_a_restart() {
    let e = start();
    let lead = e.agent("lead").await;
    let worker = e.agent("worker").await;
    let mut d = draft("worker", "task.do@1", json!({"goal": "build"}));
    d.blocking = true;
    let req: SendOk = ok(e.h.call(lead, send(d)).await);
    let note: SendOk = ok(e.h.call(lead, send(draft("worker", "note@1", json!({"n": 1})))).await);
    // The worker takes both and acknowledges only the note.
    let got: RecvOk = ok(e.h.call(worker, recv(0)).await);
    assert_eq!(got.messages.len(), 2);
    e.h.call(worker, Op::Ack { ids: vec![note.id] }).await.result.unwrap();
    let mut keyed = draft("worker", "note@1", json!({"n": 2}));
    keyed.id = Some(ulid::Ulid::generate());
    let queued: SendOk = ok(e.h.call(lead, send(keyed.clone())).await);
    let before: StatusOk = ok(e.h.call(lead, Op::Status { agent: None }).await);
    let (dir, _) = e.stop_keep().await;

    // Restarting twice without anyone connecting changes nothing more.
    let (dir, _) = start_in(dir).stop_keep().await;
    let e = start_in(dir);
    let observer = e.h.connect();
    let (s, d) = status_of(e.h.call(observer, Op::Status { agent: Some("worker".into()) }).await);
    assert_eq!(s.lc, before.lc);
    assert_eq!(s.waits, vec![("lead".to_string(), "worker".to_string())]);
    let mailbox: Vec<(ulid::Ulid, u32)> = d.mailbox.iter().map(|m| (m.id, m.redelivered)).collect();
    assert_eq!(mailbox, vec![(req.id, 1), (queued.id, 0)]);
    assert_eq!(d.owes.iter().map(|r| r.id).collect::<Vec<_>>(), vec![req.id]);

    // The request handed out before the restart comes back once more and can be answered.
    let worker = e.agent("worker").await;
    let got: RecvOk = ok(e.h.call(worker, recv(0)).await);
    let ids: Vec<(ulid::Ulid, u32)> = got.messages.iter().map(|m| (m.id, m.redelivered)).collect();
    assert_eq!(ids, vec![(req.id, 1), (queued.id, 0)]);
    e.h.call(worker, answer(req.id, json!({"ok": true}))).await.result.unwrap();

    let lead = e.agent("lead").await;
    let AwaitOk::Done { response } = ok(e.h.call(lead, Op::Await { request_id: req.id, wait_ms: 0 }).await) else {
        panic!("expected the worker's answer");
    };
    assert_eq!((response.kind, response.correlation_id), (Kind::Response, Some(req.id)));
    assert!(response.lc > before.lc);

    // Per-pair seq and the logical clock carry on from where they were.
    let next: SendOk = ok(e.h.call(lead, send(draft("worker", "note@1", json!({"n": 3})))).await);
    assert_eq!((req.seq, note.seq, queued.seq, next.seq), (1, 2, 3, 4));
    assert!(next.lc > response.lc);
    // An idempotency key sent before the restart still delivers once.
    let again: SendOk = ok(e.h.call(lead, send(keyed)).await);
    assert!(again.duplicate && again.seq == queued.seq);
    let s: StatusOk = ok(e.h.call(lead, Op::Status { agent: None }).await);
    assert!(s.waits.is_empty());
    let events = e.stop().await;
    assert_eq!(events.iter().filter(|ev| ev["type"] == "restored").count(), 3);
}

#[tokio::test(start_paused = true)]
async fn a_deadline_that_passed_while_the_broker_was_down_answers_timeout() {
    let e = start();
    let lead = e.agent("lead").await;
    let mut d = draft("worker", "task.do@1", json!({"goal": "slow"}));
    d.timeout_ms = Some(20);
    let sent: SendOk = ok(e.h.call(lead, send(d)).await);
    let (dir, _) = e.stop_keep().await;
    // The paused clock kept the first broker from timing it out; let the wall-clock deadline pass.
    let deadline = sent.deadline_ms.unwrap();
    while ledger::now_ms() <= deadline {
        std::thread::sleep(std::time::Duration::from_millis(5));
    }

    let e = start_in(dir);
    let lead = e.agent("lead").await;
    let AwaitOk::Done { response } = ok(e.h.call(lead, Op::Await { request_id: sent.id, wait_ms: 60_000 }).await)
    else {
        panic!("expected the timeout answer");
    };
    assert_eq!((response.kind, response.from.as_str()), (Kind::Error, "kite"));
    assert_eq!(response.payload["code"], "timeout");
    // Nobody had picked the request up, so it is withdrawn from the worker's mailbox.
    let worker = e.agent("worker").await;
    let got: RecvOk = ok(e.h.call(worker, recv(0)).await);
    assert!(got.messages.is_empty());
    let events = e.stop().await;
    let last_start = events.iter().rposition(|ev| ev["type"] == "started").unwrap();
    let closed = events.iter().position(|ev| ev["type"] == "closed" && ev["reason"] == "timeout").unwrap();
    assert!(closed > last_start);
}

#[tokio::test]
async fn a_request_lost_in_too_many_restarts_answers_peer_down() {
    let e = start();
    let lead = e.agent("lead").await;
    let sent: SendOk = ok(e.h.call(lead, send(draft("worker", "task.do@1", json!({"goal": "x"})))).await);
    let mut dir = None;
    let mut e = Some(e);
    // max_redeliveries = 1: the worker holds the request across two restarts.
    for round in 0..2 {
        let env = e.take().unwrap_or_else(|| start_in(dir.take().unwrap()));
        let w = env.agent("worker").await;
        let got: RecvOk = ok(env.h.call(w, recv(0)).await);
        assert_eq!((got.messages[0].id, got.messages[0].redelivered), (sent.id, round));
        dir = Some(env.stop_keep().await.0);
    }
    let e = start_in(dir.unwrap());
    let lead = e.agent("lead").await;
    let AwaitOk::Done { response } = ok(e.h.call(lead, Op::Await { request_id: sent.id, wait_ms: 0 }).await) else {
        panic!("expected peer_down");
    };
    assert_eq!(response.payload["code"], "peer_down");
    let worker = e.agent("worker").await;
    let got: RecvOk = ok(e.h.call(worker, recv(0)).await);
    assert!(got.messages.is_empty());
    let events = e.stop().await;
    assert!(events.iter().any(|ev| ev["type"] == "dead_lettered" && ev["id"] == sent.id.to_string()));
}

#[tokio::test]
async fn a_request_to_an_agent_removed_from_the_config_answers_peer_down() {
    let e = start();
    let lead = e.agent("lead").await;
    let sent: SendOk = ok(e.h.call(lead, send(draft("other", "task.do@1", json!({"goal": "x"})))).await);
    let (dir, _) = e.stop_keep().await;

    let without_other = CONFIG.replace("[agents.other]\n", "").replace(r#"["worker", "other"]"#, r#""worker""#);
    let e = start_with(dir, &without_other);
    let lead = e.agent("lead").await;
    let AwaitOk::Done { response } = ok(e.h.call(lead, Op::Await { request_id: sent.id, wait_ms: 0 }).await) else {
        panic!("expected peer_down");
    };
    assert_eq!(response.payload["code"], "peer_down");
    let err = e.h.call(lead, Op::Cancel { request_id: sent.id }).await.result.unwrap_err();
    assert_eq!(err.detail.unwrap()["closed"], "peer_down");
    e.stop().await;
}

#[tokio::test]
async fn a_torn_ledger_tail_is_repaired_and_recorded_on_the_next_start() {
    let e = start();
    let lead = e.agent("lead").await;
    e.agent("worker").await;
    e.h.call(lead, send(draft("worker", "note@1", json!({"n": 1})))).await.result.unwrap();
    let (dir, _) = e.stop_keep().await;
    // A crash halfway through writing the next record.
    let path = dir.path().join(".kite/ledger.jsonl");
    let torn: &[u8] = b"{\"n\":99,\"ts_ms\":1,\"prev\":\"de";
    let mut f = std::fs::OpenOptions::new().append(true).open(&path).unwrap();
    std::io::Write::write_all(&mut f, torn).unwrap();
    drop(f);

    let e = start_in(dir);
    let worker = e.agent("worker").await;
    let got: RecvOk = ok(e.h.call(worker, recv(0)).await);
    assert_eq!(got.messages[0].payload, json!({"n": 1}), "the note survived the torn tail");
    let events = e.stop().await;
    let recovered = events.iter().find(|ev| ev["type"] == "recovered").expect("a recovered event");
    assert_eq!(recovered["bytes_dropped"], torn.len());
}

#[tokio::test]
async fn registering_needs_the_agents_own_token() {
    use kite_core::tokens;
    let dir = tempfile::tempdir().unwrap();
    let text = CONFIG.replace("require_tokens = false", "require_tokens = true");
    let config = Arc::new(Config::parse(&text, dir.path()).unwrap());
    let tokens_dir = config.tokens_dir();
    tokens::ensure(&tokens_dir, &config.agents.keys().cloned().collect()).unwrap();
    let ledger = Ledger::open(&config.broker.ledger, config.broker.fsync).unwrap();
    let (h, task) = Broker::spawn(config, ledger);
    let e = Env { h, dir, task };
    let reg = |agent: &str, token: Option<&str>| Op::Register {
        agent: agent.into(),
        protocol: PROTOCOL.into(),
        token: token.map(Secret::new),
    };
    let lead_token = tokens::read(&tokens_dir, "lead").unwrap();
    let worker_token = tokens::read(&tokens_dir, "worker").unwrap();

    let c = e.h.connect();
    assert_eq!(code(e.h.call(c, reg("lead", None)).await), ErrorCode::Unauthorized);
    assert_eq!(code(e.h.call(c, reg("lead", Some(&worker_token))).await), ErrorCode::Unauthorized);
    let ok: RegisterOk = ok(e.h.call(c, reg("lead", Some(&lead_token))).await);
    assert_eq!(ok.agent, "lead");

    // A rotated token takes effect on the next registration.
    tokens::rotate(&tokens_dir, "worker").unwrap();
    let w = e.h.connect();
    assert_eq!(code(e.h.call(w, reg("worker", Some(&worker_token))).await), ErrorCode::Unauthorized);
    let fresh = tokens::read(&tokens_dir, "worker").unwrap();
    e.h.call(w, reg("worker", Some(&fresh))).await.result.unwrap();

    // The token never shows up when the operation is formatted or audited.
    assert!(!format!("{:?}", reg("lead", Some(&lead_token))).contains(&lead_token));
    let events = e.stop().await;
    let ledger = serde_json::to_string(&events).unwrap();
    assert!(!ledger.contains(&lead_token) && !ledger.contains(&fresh));
    assert!(events.iter().any(|ev| ev["type"] == "refused" && ev["code"] == "unauthorized"));
}

/// The safety argument for ledger compaction: a snapshot taken just before a
/// `started`, plus every later event, restores exactly what replaying the
/// whole history would. Checked at every cut point of a real two-run history.
#[tokio::test]
async fn a_snapshot_plus_later_events_equals_the_whole_history() {
    use kite_core::restore::Fold;
    use kite_proto::board::*;

    // A history with in-flight, acknowledged and answered messages, a disconnect, and cards.
    let e = start();
    let lead = e.agent("lead").await;
    let worker = e.agent("worker").await;
    for i in 0..3 {
        e.h.call(lead, send(draft("worker", "task.do@1", json!({"goal": format!("g{i}")})))).await.result.unwrap();
    }
    e.h.call(worker, send(draft("lead", "note@1", json!({"n": 1})))).await.result.unwrap();
    let got: RecvOk = ok(e.h.call(worker, Op::Recv { wait_ms: 0, max: 2 }).await);
    e.h.call(worker, answer(got.messages[0].id, json!({"ok": true}))).await.result.unwrap();
    let card = CardDraft {
        title: "t".into(),
        acceptance: vec!["a".into()],
        merge_gate: Some("m".into()),
        ..Default::default()
    };
    e.h.call(lead, Op::Card { card: CardOp::Create { card } }).await.result.unwrap();
    e.h.disconnect(worker).await;
    let (dir, _) = e.stop_keep().await;
    let e = start_in(dir);
    let worker = e.agent("worker").await;
    let got: RecvOk = ok(e.h.call(worker, recv(0)).await);
    e.h.call(worker, Op::Ack { ids: vec![got.messages[0].id] }).await.result.unwrap();
    let events = e.stop().await;
    assert!(events.len() > 10, "a history worth checking, got {} events", events.len());

    let fold = |evs: &[Value]| {
        let mut f = Fold::default();
        evs.iter().for_each(|e| f.apply(e));
        f
    };
    let started = json!({ "type": "started" });
    for k in 0..=events.len() {
        let mut whole = events[..k].to_vec();
        whole.push(started.clone());
        whole.extend_from_slice(&events[k..]);
        let expected = fold(&whole).finish();

        let snapshot = serde_json::to_value(fold(&events[..k]).compacted()).unwrap();
        let mut seeded: Fold = serde_json::from_value(snapshot).unwrap();
        seeded.apply(&started);
        events[k..].iter().for_each(|e| seeded.apply(e));
        assert_eq!(seeded.finish(), expected, "snapshot at event {k} diverges");
    }
}
