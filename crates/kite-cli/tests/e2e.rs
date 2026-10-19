//! Runs the compiled `kite` binary the way a user and two agent hosts would:
//! `kite init`, `kite up`, two `kite mcp` servers speaking JSON-RPC on stdio,
//! the Claude Code lease hook, `kite status`, `kite trace --verify`, and a
//! clean shutdown on SIGTERM.

use serde_json::{Value, json};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::process::{Command as StdCommand, Stdio};
use std::time::Duration;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader, Lines};
use tokio::process::{Child, ChildStdin, ChildStdout, Command};

const KITE: &str = env!("CARGO_BIN_EXE_kite");

fn kite(dir: &Path, args: &[&str]) -> std::process::Output {
    StdCommand::new(KITE).args(args).current_dir(dir).env_remove("KITE_AGENT").output().unwrap()
}

struct Mcp {
    child: Child,
    stdin: ChildStdin,
    lines: Lines<BufReader<ChildStdout>>,
    next: u64,
}

impl Mcp {
    fn start(dir: &Path, agent: &str) -> Mcp {
        let mut child = Command::new(KITE)
            .args(["mcp", "--agent", agent])
            .current_dir(dir)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit())
            .kill_on_drop(true)
            .spawn()
            .unwrap();
        let stdin = child.stdin.take().unwrap();
        let lines = BufReader::new(child.stdout.take().unwrap()).lines();
        Mcp { child, stdin, lines, next: 1 }
    }

    async fn notify(&mut self, method: &str) {
        let line = json!({ "jsonrpc": "2.0", "method": method }).to_string() + "\n";
        self.stdin.write_all(line.as_bytes()).await.unwrap();
    }

    async fn rpc(&mut self, method: &str, params: Value) -> Value {
        let id = self.next;
        self.next += 1;
        let line = json!({ "jsonrpc": "2.0", "id": id, "method": method, "params": params }).to_string() + "\n";
        self.stdin.write_all(line.as_bytes()).await.unwrap();
        loop {
            let reply: Value = serde_json::from_str(&self.lines.next_line().await.unwrap().unwrap()).unwrap();
            if reply["id"] == id {
                return reply;
            }
        }
    }

    /// Calls a tool; returns (is_error, structured content).
    async fn tool(&mut self, name: &str, args: Value) -> (bool, Value) {
        let r = self.rpc("tools/call", json!({ "name": name, "arguments": args })).await;
        let result = &r["result"];
        (result["isError"].as_bool().unwrap(), result["structuredContent"].clone())
    }

    async fn close(mut self) {
        drop(self.stdin);
        let status = tokio::time::timeout(Duration::from_secs(10), self.child.wait()).await.unwrap().unwrap();
        assert!(status.success());
    }
}

fn hook(dir: &Path, agent: &str, file: &Path) -> i32 {
    let mut child = StdCommand::new(KITE)
        .args(["hook", "claude-code", "--agent", agent])
        .current_dir(dir)
        .stdin(Stdio::piped())
        .stderr(Stdio::null())
        .spawn()
        .unwrap();
    let event = json!({ "tool_name": "Edit", "tool_input": { "file_path": file } });
    child.stdin.take().unwrap().write_all(event.to_string().as_bytes()).unwrap();
    child.wait().unwrap().code().unwrap()
}

#[tokio::test]
async fn the_binary_runs_a_two_agent_exchange_end_to_end() {
    let tmp = tempfile::tempdir().unwrap();
    let dir: PathBuf = tmp.path().canonicalize().unwrap();

    let out = kite(&dir, &["init"]);
    assert!(out.status.success(), "{}", String::from_utf8_lossy(&out.stderr));
    assert!(dir.join("kite.toml").exists() && dir.join(".kite/mcp/coder.json").exists());

    let mut up =
        Command::new(KITE).args(["up"]).current_dir(&dir).stderr(Stdio::piped()).kill_on_drop(true).spawn().unwrap();
    let socket = dir.join(".kite/kite.sock");
    for _ in 0..200 {
        if socket.exists() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    assert!(socket.exists(), "kite up did not create its socket");

    let mut lead = Mcp::start(&dir, "lead");
    let mut coder = Mcp::start(&dir, "coder");
    for m in [&mut lead, &mut coder] {
        let init = m
            .rpc(
                "initialize",
                json!({ "protocolVersion": "2025-06-18", "capabilities": {}, "clientInfo": { "name": "test", "version": "0" } }),
            )
            .await;
        assert_eq!(init["result"]["serverInfo"]["name"], "kite");
        assert_eq!(init["result"]["protocolVersion"], "2025-06-18");
        m.notify("notifications/initialized").await;
    }
    let tools = lead.rpc("tools/list", json!({})).await;
    assert_eq!(tools["result"]["tools"].as_array().unwrap().len(), 15);
    let unknown = lead.rpc("no/such", json!({})).await;
    assert_eq!(unknown["error"]["code"], -32601);

    let (err, contracts) = coder.tool("kite_contracts", json!({})).await;
    assert!(!err);
    assert_eq!(contracts["agent"], "coder");
    assert_eq!(contracts["may_receive"][0]["peer"], "lead");

    // A payload that breaks the contract is refused with the failing path.
    let (err, bad) =
        lead.tool("kite_send", json!({ "to": "coder", "schema": "task.assign@1", "payload": { "goal": "" } })).await;
    assert!(err);
    assert_eq!(bad["error"]["code"], "schema_violation");
    assert_eq!(bad["error"]["detail"]["violations"][0]["path"], "/goal");
    // So is a route that kite.toml does not declare.
    let (err, denied) = coder
        .tool("kite_send", json!({ "to": "reviewer", "schema": "status.note@1", "payload": { "text": "hi" } }))
        .await;
    assert!(err);
    assert_eq!(denied["error"]["code"], "route_denied");

    let (err, req) = lead
        .tool(
            "kite_request",
            json!({ "to": "coder", "schema": "task.assign@1", "payload": { "goal": "add a flag" }, "wait_ms": 200 }),
        )
        .await;
    assert!(!err);
    assert_eq!(req["answer"]["status"], "pending");
    let rid = req["request_id"].as_str().unwrap().to_string();

    let (_, inbox) = coder.tool("kite_recv", json!({ "wait_ms": 5000 })).await;
    assert_eq!(inbox["messages"][0]["id"], rid.as_str());
    assert_eq!(inbox["messages"][0]["payload"]["goal"], "add a flag");

    // The coder leases a file; the lease hook then blocks the lead from editing it.
    std::fs::create_dir_all(dir.join("src")).unwrap();
    std::fs::write(dir.join("src/main.rs"), "fn main() {}\n").unwrap();
    let (err, lease) = coder.tool("kite_lease", json!({ "resource": dir.join("src/main.rs") })).await;
    assert!(!err);
    assert_eq!(lease["resource"], "src/main.rs");
    assert_eq!(hook(&dir, "lead", &dir.join("src/main.rs")), 2);
    assert_eq!(hook(&dir, "coder", &dir.join("src/main.rs")), 0);
    assert_eq!(hook(&dir, "lead", &dir.join("src/other.rs")), 0);

    let status = kite(&dir, &["status", "coder", "--json"]);
    let status: Value = serde_json::from_slice(&status.stdout).unwrap();
    assert_eq!(status["detail"]["owes"][0]["id"], rid.as_str());
    assert_eq!(status["detail"]["leases"][0]["resource"], "src/main.rs");
    let table = String::from_utf8(kite(&dir, &["status"]).stdout).unwrap();
    assert!(table.contains("AGENT") && table.contains("coder") && table.contains("reviewer"));

    let (err, bad_answer) =
        coder.tool("kite_respond", json!({ "request_id": rid, "result": { "summary": "done" } })).await;
    assert!(err, "files_changed is required by task.result@1");
    assert_eq!(bad_answer["error"]["code"], "schema_violation");
    let (err, _) = coder
        .tool(
            "kite_respond",
            json!({ "request_id": rid, "result": { "summary": "added --flag", "files_changed": ["src/main.rs"] } }),
        )
        .await;
    assert!(!err);
    let (_, answer) = lead.tool("kite_await", json!({ "request_id": rid, "wait_ms": 5000 })).await;
    assert_eq!(answer["status"], "done");
    assert_eq!(answer["response"]["payload"]["files_changed"][0], "src/main.rs");
    assert_eq!(answer["response"]["from"], "coder");

    let (_, released) = coder.tool("kite_release", json!({ "resource": "src/main.rs" })).await;
    assert_eq!(released["released"], "src/main.rs");

    // Agent Kanban: lead shapes and assigns a card, coder works it, reviewer gates it, lead merges.
    let (err, card) = lead
        .tool(
            "kite_card_create",
            json!({ "title": "Add --flag", "acceptance": ["--flag parses"], "merge_gate": "tests and review pass",
                    "gates": ["tests", "diff_review"], "owner": "coder", "worktree": "wt/coder" }),
        )
        .await;
    assert!(!err, "{card}");
    let id = card["id"].as_str().unwrap().to_string();
    let (_, inbox) = coder.tool("kite_recv", json!({ "wait_ms": 2000 })).await;
    let note = inbox["messages"].as_array().unwrap().iter().find(|m| m["schema"] == "kite.card@1").unwrap().clone();
    assert_eq!(
        (note["payload"]["card_id"].as_str(), note["payload"]["action"].as_str()),
        (Some(id.as_str()), Some("created"))
    );
    let (err, _) = lead.tool("kite_card_move", json!({ "id": id, "to": "ready" })).await;
    assert!(!err);
    let (err, refused) = lead.tool("kite_card_move", json!({ "id": id, "to": "running" })).await;
    assert!(err);
    assert_eq!(refused["error"]["code"], "not_permitted");
    let (err, _) = coder.tool("kite_card_move", json!({ "id": id, "to": "running" })).await;
    assert!(!err);
    std::fs::create_dir_all(dir.join("wt/coder")).unwrap();
    std::fs::write(dir.join("wt/coder/HANDOFF.md"), "Added --flag; tests in tests/flag.rs\n").unwrap();
    coder.tool("kite_card_update", json!({ "id": id, "handoff": "HANDOFF.md" })).await;
    let (err, refused) = coder.tool("kite_card_move", json!({ "id": id, "to": "review" })).await;
    assert!(err);
    assert_eq!(refused["error"]["code"], "transition_refused");
    assert!(refused["error"]["message"].as_str().unwrap().contains("evidence"));
    coder.tool("kite_card_evidence", json!({ "id": id, "kind": "test", "reference": "cargo test" })).await;
    let (err, _) = coder.tool("kite_card_move", json!({ "id": id, "to": "review" })).await;
    assert!(!err);
    for gate in ["tests", "diff_review"] {
        let out = kite(
            &dir,
            &[
                "card", "evidence", "--as", "reviewer", &id, "--kind", "review", "--ref", "git diff", "--gate", gate,
                "--pass",
            ],
        );
        assert!(out.status.success(), "{}", String::from_utf8_lossy(&out.stderr));
    }
    let (err, merged) = lead.tool("kite_card_move", json!({ "id": id, "to": "merged", "merge_ref": "abc1234" })).await;
    assert!(!err, "{merged}");
    assert_eq!(merged["state"], "merged");
    let board = String::from_utf8(kite(&dir, &["board", "--all"]).stdout).unwrap();
    assert!(board.contains("MERGED (1)") && board.contains(&id) && board.contains("abc1234"), "{board}");
    let shown = String::from_utf8(kite(&dir, &["card", "show", &id]).stdout).unwrap();
    assert!(shown.contains("tests ok (reviewer)") && shown.contains("diff_review ok (reviewer)"), "{shown}");

    lead.close().await;
    coder.close().await;

    let pid = up.id().unwrap().to_string();
    assert!(StdCommand::new("kill").args(["-TERM", &pid]).status().unwrap().success());
    let status = tokio::time::timeout(Duration::from_secs(10), up.wait()).await.unwrap().unwrap();
    let mut stderr = String::new();
    tokio::io::AsyncReadExt::read_to_string(&mut up.stderr.take().unwrap(), &mut stderr).await.unwrap();
    assert!(status.success(), "kite up exited with {status}: {stderr}");
    assert!(stderr.contains("kite up: stopped"), "{stderr}");
    assert!(!stderr.contains("panicked"), "{stderr}");
    assert!(!socket.exists(), "the socket is removed on shutdown");

    let verify = kite(&dir, &["trace", "--verify", "--json"]);
    let verify: Value = serde_json::from_slice(&verify.stdout).unwrap();
    assert_eq!(verify["ok"], true);
    let trace = String::from_utf8(kite(&dir, &["trace", "--agent", "coder"]).stdout).unwrap();
    assert!(trace.contains("accepted") && trace.contains("lease_granted") && trace.contains("refused"));
}

#[test]
fn shell_agents_get_clear_errors_without_a_broker() {
    let tmp = tempfile::tempdir().unwrap();
    let dir = tmp.path();
    let none = kite(dir, &["status"]);
    assert_eq!(none.status.code(), Some(1));
    assert!(String::from_utf8_lossy(&none.stderr).contains("no kite.toml"));
    assert!(kite(dir, &["init"]).status.success());
    let down = kite(dir, &["status"]);
    assert_eq!(down.status.code(), Some(1));
    assert!(String::from_utf8_lossy(&down.stderr).contains("is `kite up` running?"));
    let again = kite(dir, &["init"]);
    assert!(String::from_utf8_lossy(&again.stderr).contains("already exists"));
}
