//! The broker's life through the real binary: setup errors, restarts, a
//! supervised agent process, and `kite run` driving it.

mod common;

use common::{KITE, Project};
use serde_json::json;

#[test]
fn without_a_project_or_broker_the_cli_says_what_to_do() {
    let tmp = tempfile::tempdir().unwrap();
    let kite = |args: &[&str]| std::process::Command::new(KITE).args(args).current_dir(tmp.path()).output().unwrap();
    let none = kite(&["status"]);
    assert_eq!(none.status.code(), Some(1));
    assert!(String::from_utf8_lossy(&none.stderr).contains("no kite.toml"));
    assert!(kite(&["init"]).status.success());
    let down = kite(&["status"]);
    assert_eq!(down.status.code(), Some(1));
    assert!(String::from_utf8_lossy(&down.stderr).contains("is `kite up` running?"));
    assert!(String::from_utf8_lossy(&kite(&["init"]).stderr).contains("already exists"));
    let connect = String::from_utf8(kite(&["connect", "coder"]).stdout).unwrap();
    assert!(connect.contains("claude mcp add kite") && connect.contains("[mcp_servers.kite]"), "{connect}");
}

#[tokio::test]
async fn a_second_broker_on_the_same_project_is_refused() {
    let mut p = Project::init("second_broker");
    p.up().await;
    let second = p.kite(&["up"]);
    assert_eq!(second.status.code(), Some(1));
    assert!(String::from_utf8_lossy(&second.stderr).contains("already listening"));
    p.stop().await;
    // The refused broker must not have touched the running broker's ledger.
    let verify = p.kite(&["trace", "--verify"]);
    assert!(verify.status.success(), "ledger damaged: {}", String::from_utf8_lossy(&verify.stderr));
}

#[tokio::test]
async fn queued_work_survives_a_broker_restart() {
    let mut p = Project::init("restart_keeps_work");
    p.up().await;
    let task = ["send", "--as", "lead", "--to", "coder", "--schema", "task.assign@1", "--payload", r#"{"goal":"g"}"#];
    let rid = p.json(&task)["id"].as_str().unwrap().to_string();
    p.stop().await;

    p.up().await;
    let got = p.json(&["recv", "--as", "coder"]);
    assert_eq!(got["messages"][0]["id"], rid.as_str(), "the request is still queued after the restart");
    p.ok(&["respond", "--as", "coder", "--request", &rid, "--result", r#"{"summary":"ok","files_changed":[]}"#]);
    let answer = p.json(&["recv", "--as", "lead"]);
    assert_eq!(answer["messages"][0]["correlation_id"], rid.as_str());
    p.stop().await;
    assert_eq!(p.json(&["trace", "--verify"])["ok"], true);
}

/// A shell agent in Python that only uses the `kite` CLI: it answers every
/// task.assign@1 request it receives with `did <goal>`.
const AGENT: &str = r#"
import json, os, subprocess, sys
kite = os.environ["KITE_BIN"]
def run(*args):
    out = subprocess.run([kite, "--json", *args], capture_output=True, text=True)
    if out.returncode != 0:
        sys.stderr.write(out.stderr)
        sys.exit(1)
    return json.loads(out.stdout)
print("agent", os.environ["KITE_AGENT"], "started", flush=True)
while True:
    for m in run("recv", "--wait", "2", "--max", "8")["messages"]:
        if m["kind"] == "request":
            result = {"summary": "did " + m["payload"]["goal"], "files_changed": []}
            run("respond", "--request", m["id"], "--result", json.dumps(result))
"#;

#[tokio::test]
async fn kite_up_supervises_an_agent_that_kite_run_drives() {
    let mut p = Project::init("supervised_agent_run");
    p.write("agent.py", AGENT);
    p.edit_config(|c| {
        c.replacen(
            "[agents.coder]\n",
            &format!(
                "[agents.coder]\ncommand = [\"python3\", \"{{root}}/agent.py\"]\nrestart = \"on-failure\"\nenv = {{ KITE_BIN = \"{KITE}\" }}\n"
            ),
            1,
        )
    });
    p.write(
        "plan.toml",
        r#"
as = "lead"
max_parallel = 2
[[tasks]]
id = "first"
to = "coder"
schema = "task.assign@1"
payload = { goal = "parse the flag" }
timeout_ms = 20000
[[tasks]]
id = "second"
to = "coder"
schema = "task.assign@1"
after = ["first"]
payload = { goal = "${tasks.first.result.summary} and document it" }
timeout_ms = 20000
"#,
    );
    p.up().await;
    // The socket appears before kite up reports what it launched.
    p.wait_until("kite up to report the launched agent", || p.up_log().contains("1 launched")).await;
    p.wait_until("the supervisor to report coder running", || {
        p.json(&["status", "coder"])["detail"]["process"]["state"] == "running"
    })
    .await;

    let lead_token = p.token_file("lead").display().to_string();
    let report = p.json(&["run", "plan.toml", "--token-file", &lead_token]);
    assert_eq!(report["ok"], true, "{report}");
    assert_eq!(report["tasks"]["second"]["result"]["summary"], json!("did did parse the flag and document it"));

    let log = std::fs::read_to_string(p.path(".kite/logs/coder.log")).unwrap();
    assert!(log.contains("agent coder started"), "{log}");
    p.stop().await;
    let status = p.kite(&["status"]);
    assert_eq!(status.status.code(), Some(1), "the broker is gone after stop");
}

/// An agent that starts a long-running child of its own and records the child's pid.
const PARENT_AGENT: &str = r#"
import subprocess, sys, time
child = subprocess.Popen(["sleep", "1000"])
open(sys.argv[1], "w").write(str(child.pid))
while True:
    time.sleep(1)
"#;

fn alive(pid: &str) -> bool {
    std::process::Command::new("kill").args(["-0", pid]).stderr(std::process::Stdio::null()).status().unwrap().success()
}

fn with_parent_agent(p: &Project) {
    p.write("parent.py", PARENT_AGENT);
    p.edit_config(|c| {
        c.replacen(
            "[agents.coder]\n",
            "[agents.coder]\ncommand = [\"python3\", \"{root}/parent.py\", \"{root}/grandchild.pid\"]\nstop_grace_ms = 2000\n",
            1,
        )
    });
}

#[tokio::test]
async fn stopping_kite_up_ends_everything_an_agent_started() {
    let mut p = Project::init("agent_group_stop");
    with_parent_agent(&p);
    p.up().await;
    let pidfile = p.path("grandchild.pid");
    let written = || std::fs::read_to_string(&pidfile).is_ok_and(|s| !s.trim().is_empty());
    p.wait_until("the agent to record its child's pid", written).await;
    let grandchild = std::fs::read_to_string(&pidfile).unwrap().trim().to_string();
    assert!(alive(&grandchild));
    p.stop().await;
    assert!(!alive(&grandchild), "the agent's own child outlived kite up");
    assert!(!p.path(".kite/run/coder.pid").exists(), "the group record is removed on a clean stop");
}

#[tokio::test]
async fn agents_left_by_a_killed_kite_up_are_reported_on_the_next_start() {
    let mut p = Project::init("agent_group_stale");
    with_parent_agent(&p);
    p.up().await;
    let record = p.path(".kite/run/coder.pid");
    let recorded = || std::fs::read_to_string(&record).is_ok_and(|s| !s.trim().is_empty());
    p.wait_until("the supervisor to record the agent's group", recorded).await;
    let pgid = std::fs::read_to_string(&record).unwrap();
    p.kill_hard().await;
    assert!(alive(&pgid), "after SIGKILL nothing stops the agents");

    p.edit_config(|c| c.replace("command = [\"python3\", \"{root}/parent.py\", \"{root}/grandchild.pid\"]\n", ""));
    p.up().await;
    p.wait_until("kite up to warn about the leftover agents", || p.up_log().contains("still running")).await;
    // Clean up the leftovers the test created.
    std::process::Command::new("kill").args(["-KILL", &format!("-{}", pgid.trim())]).status().unwrap();
    p.stop().await;
    let trace = p.ok(&["trace"]);
    assert!(trace.contains("stale_agent_group"), "{trace}");
}

#[tokio::test]
async fn work_survives_restarts_that_compact_the_ledger() {
    let mut p = Project::init("compaction_restart");
    // Every start finds the live segment too big and compacts it.
    p.edit_config(|c| c.replacen("[broker]\n", "[broker]\nsegment_bytes = 1\n", 1));
    p.up().await;
    let task = ["send", "--as", "lead", "--to", "coder", "--schema", "task.assign@1", "--payload", r#"{"goal":"g"}"#];
    let rid = p.json(&task)["id"].as_str().unwrap().to_string();
    let card = p.json(&["card", "new", "--as", "lead", "--title", "Keep me", "--accept", "survives"]);
    let card_id = card["id"].as_str().unwrap().to_string();
    p.stop().await;
    p.up().await;
    p.stop().await;
    p.up().await;

    let got = p.json(&["recv", "--as", "coder"]);
    // Counted once the broker has answered: the socket appears before the ledger is opened.
    let segments = std::fs::read_dir(p.path(".kite/ledger"))
        .unwrap()
        .filter(|e| e.as_ref().unwrap().path().extension().is_some_and(|x| x == "jsonl"));
    assert_eq!(segments.count(), 3, "each restart started a new segment");
    assert_eq!(got["messages"][0]["id"], rid.as_str(), "the queued request came through two compactions");
    assert_eq!(p.json(&["card", "show", &card_id])["title"], "Keep me");
    p.ok(&["respond", "--as", "coder", "--request", &rid, "--result", r#"{"summary":"ok","files_changed":[]}"#]);
    p.stop().await;
    assert_eq!(p.json(&["trace", "--verify"])["ok"], true, "one chain across all segments");
    assert!(p.ok(&["trace"]).contains("snapshot"));
}
