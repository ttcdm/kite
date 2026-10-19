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

    let report = p.json(&["run", "plan.toml"]);
    assert_eq!(report["ok"], true, "{report}");
    assert_eq!(report["tasks"]["second"]["result"]["summary"], json!("did did parse the flag and document it"));

    let log = std::fs::read_to_string(p.path(".kite/logs/coder.log")).unwrap();
    assert!(log.contains("agent coder started"), "{log}");
    p.stop().await;
    let status = p.kite(&["status"]);
    assert_eq!(status.status.code(), Some(1), "the broker is gone after stop");
}
