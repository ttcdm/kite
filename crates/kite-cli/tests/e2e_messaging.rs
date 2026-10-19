//! Messaging through the real binary: MCP sessions and the CLI talking to `kite up`.

mod common;

use common::Project;
use serde_json::json;

#[tokio::test]
async fn mcp_handshake_negotiates_and_lists_every_tool() {
    let mut p = Project::init("mcp_handshake");
    p.up().await;
    let mut m = p.mcp("lead").await;
    // A version the server does not know is answered with the newest one it speaks.
    let mut old = p.mcp("coder").await;
    let r = old.rpc("initialize", json!({ "protocolVersion": "1999-01-01", "capabilities": {} })).await;
    assert_eq!(r["result"]["protocolVersion"], "2025-06-18");

    let tools = m.rpc("tools/list", json!({})).await;
    let mut names: Vec<&str> =
        tools["result"]["tools"].as_array().unwrap().iter().map(|t| t["name"].as_str().unwrap()).collect();
    names.sort();
    assert_eq!(
        names,
        [
            "kite_await",
            "kite_board",
            "kite_cancel",
            "kite_card_create",
            "kite_card_evidence",
            "kite_card_move",
            "kite_card_update",
            "kite_contracts",
            "kite_lease",
            "kite_recv",
            "kite_release",
            "kite_request",
            "kite_respond",
            "kite_send",
            "kite_status",
        ]
    );
    assert_eq!(m.rpc("ping", json!({})).await["result"], json!({}));
    assert_eq!(m.rpc("no/such", json!({})).await["error"]["code"], -32601);
    m.close().await;
    old.close().await;
    p.stop().await;
}

#[tokio::test]
async fn a_request_round_trip_with_every_payload_checked() {
    let mut p = Project::init("request_round_trip");
    p.up().await;
    let mut lead = p.mcp("lead").await;
    let mut coder = p.mcp("coder").await;

    let contracts = coder.ok("kite_contracts", json!({})).await;
    assert_eq!(contracts["may_receive"][0]["peer"], "lead");

    let bad = json!({ "to": "coder", "schema": "task.assign@1", "payload": { "goal": "" } });
    let e = lead.refused("kite_send", bad, "schema_violation").await;
    assert_eq!(e["detail"]["violations"][0]["path"], "/goal");
    let off_route = json!({ "to": "reviewer", "schema": "status.note@1", "payload": { "text": "hi" } });
    coder.refused("kite_send", off_route, "route_denied").await;

    let task = json!({ "to": "coder", "schema": "task.assign@1", "payload": { "goal": "add a flag" }, "wait_ms": 100 });
    let req = lead.ok("kite_request", task).await;
    assert_eq!(req["answer"]["status"], "pending");
    let rid = req["request_id"].as_str().unwrap().to_string();

    let inbox = coder.ok("kite_recv", json!({ "wait_ms": 5000 })).await;
    assert_eq!(inbox["messages"][0]["id"], rid.as_str());

    // The answer must match task.result@1, which requires files_changed.
    let partial = json!({ "request_id": rid, "result": { "summary": "done" } });
    coder.refused("kite_respond", partial, "schema_violation").await;
    let full = json!({ "request_id": rid, "result": { "summary": "added --flag", "files_changed": ["src/cli.rs"] } });
    coder.ok("kite_respond", full.clone()).await;
    coder.refused("kite_respond", full, "no_such_request").await;

    let answer = lead.ok("kite_await", json!({ "request_id": rid, "wait_ms": 5000 })).await;
    assert_eq!((answer["status"].as_str(), answer["response"]["from"].as_str()), (Some("done"), Some("coder")));
    assert_eq!(answer["response"]["correlation_id"], rid.as_str());

    lead.close().await;
    coder.close().await;
    p.stop().await;
    assert_eq!(p.json(&["trace", "--verify"])["ok"], true);
}

#[tokio::test]
async fn resending_an_id_delivers_once() {
    let mut p = Project::init("idempotent_send");
    p.up().await;
    let mut coder = p.mcp("coder").await;
    let mut lead = p.mcp("lead").await;
    let note = json!({ "to": "lead", "schema": "status.note@1", "payload": { "text": "half way" },
                       "id": "01K00000000000000000000000" });
    assert_eq!(coder.ok("kite_send", note.clone()).await["duplicate"], false);
    assert_eq!(coder.ok("kite_send", note).await["duplicate"], true);
    let inbox = lead.ok("kite_recv", json!({ "wait_ms": 2000 })).await;
    assert_eq!(inbox["messages"].as_array().unwrap().len(), 1);
    coder.close().await;
    lead.close().await;
    p.stop().await;
}

#[tokio::test]
async fn an_unanswered_request_is_answered_with_timeout() {
    let mut p = Project::init("request_timeout");
    p.up().await;
    let mut lead = p.mcp("lead").await;
    // The reviewer never connects, so the broker answers at the deadline.
    let review = json!({ "to": "reviewer", "schema": "review.request@1", "payload": { "summary": "s", "files": ["a.rs"] },
                         "timeout_ms": 300, "wait_ms": 20000 });
    let req = lead.ok("kite_request", review).await;
    let answer = &req["answer"];
    assert_eq!(answer["status"], "done", "{req}");
    let response = &answer["response"];
    assert_eq!((response["kind"].as_str(), response["from"].as_str()), (Some("error"), Some("kite")));
    assert_eq!(response["payload"]["code"], "timeout");
    lead.close().await;
    p.stop().await;
}

#[tokio::test]
async fn a_blocking_request_that_would_deadlock_is_refused() {
    let mut p = Project::init("deadlock_refused");
    p.edit_config(|c| {
        c + r#"
[schemas."help.ask@1"]
kind = "request"
response = "help.answer@1"
schema = { type = "object" }

[schemas."help.answer@1"]
kind = "response"
schema = { type = "object" }

[[routes]]
from = "coder"
to = "lead"
schemas = ["help.ask@1"]
"#
    });
    p.up().await;
    let mut lead = p.mcp("lead").await;
    let mut coder = p.mcp("coder").await;
    // lead now waits on coder ...
    let task = json!({ "to": "coder", "schema": "task.assign@1", "payload": { "goal": "g" }, "wait_ms": 50 });
    assert_eq!(lead.ok("kite_request", task).await["answer"]["status"], "pending");
    // ... so coder waiting on lead would close the cycle.
    let ask = json!({ "to": "lead", "schema": "help.ask@1", "payload": {}, "wait_ms": 50 });
    let e = coder.refused("kite_request", ask, "deadlock").await;
    assert_eq!(e["detail"]["cycle"], json!(["coder", "lead", "coder"]));
    // Sent without waiting it is not a wait, so it goes through.
    coder.ok("kite_send", json!({ "to": "lead", "schema": "help.ask@1", "payload": {} })).await;
    lead.close().await;
    coder.close().await;
    p.stop().await;
}

#[tokio::test]
async fn leases_block_other_agents_edits_through_the_hook() {
    let mut p = Project::init("leases_and_hook");
    p.up().await;
    let mut coder = p.mcp("coder").await;
    let mut lead = p.mcp("lead").await;
    p.write("src/main.rs", "fn main() {}\n");
    let file = p.path("src/main.rs");

    let lease = coder.ok("kite_lease", json!({ "resource": file })).await;
    assert_eq!(lease["resource"], "src/main.rs");
    let e = lead.refused("kite_lease", json!({ "resource": "src/main.rs" }), "lease_conflict").await;
    assert_eq!(e["detail"]["holder"], "coder");
    assert_eq!(p.hook("lead", &file), 2, "the hook blocks the lead's edit");
    assert_eq!(p.hook("coder", &file), 0, "the holder may edit");
    assert_eq!(p.hook("lead", &p.path("src/other.rs")), 0, "unleased files are free");
    assert_eq!(p.json(&["status", "coder"])["detail"]["leases"][0]["resource"], "src/main.rs");

    coder.ok("kite_release", json!({ "resource": "src/main.rs" })).await;
    assert_eq!(p.hook("lead", &file), 0);
    // A lease ends with its holder's connection.
    lead.ok("kite_lease", json!({ "resource": "src/main.rs" })).await;
    lead.close().await;
    p.wait_until("the lead's lease to end with its connection", || {
        p.json(&["lease-check", "src/main.rs"])["lease"].is_null()
    })
    .await;
    coder.close().await;
    p.stop().await;
}

#[tokio::test]
async fn a_shell_agent_works_through_the_cli_alone() {
    let mut p = Project::init("shell_agent_cli");
    p.up().await;
    let sent =
        p.json(&["send", "--as", "lead", "--to", "coder", "--schema", "task.assign@1", "--payload", r#"{"goal":"g"}"#]);
    let rid = sent["id"].as_str().unwrap().to_string();
    let got = p.json(&["recv", "--as", "coder"]);
    assert_eq!(got["messages"][0]["id"], rid.as_str());
    // recv acknowledged it, so it is not handed out again.
    assert_eq!(p.json(&["recv", "--as", "coder"])["messages"], json!([]));
    p.ok(&["respond", "--as", "coder", "--request", &rid, "--result", r#"{"summary":"ok","files_changed":[]}"#]);
    let answer = p.json(&["recv", "--as", "lead"]);
    let m = &answer["messages"][0];
    assert_eq!((m["kind"].as_str(), m["correlation_id"].as_str()), (Some("response"), Some(rid.as_str())));
    let refused = p.kite(&[
        "send",
        "--as",
        "coder",
        "--to",
        "reviewer",
        "--schema",
        "status.note@1",
        "--payload",
        r#"{"text":"x"}"#,
    ]);
    assert_eq!(refused.status.code(), Some(1));
    assert!(String::from_utf8_lossy(&refused.stderr).contains("route_denied"));
    p.stop().await;
}
