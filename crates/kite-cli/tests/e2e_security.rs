//! Agent tokens through the real binary: an agent cannot act as another by
//! passing its name, and tokens never land in files that get committed.

mod common;

use common::Project;
use serde_json::json;

#[tokio::test]
async fn an_agent_cannot_act_as_another_without_its_token() {
    let mut p = Project::init("tokens_required");
    p.up().await;
    let note = ["send", "--as", "lead", "--to", "coder", "--schema", "task.assign@1", "--payload", r#"{"goal":"g"}"#];

    // No token at all.
    let bare = p.kite_bare(&note);
    assert_eq!(bare.status.code(), Some(1));
    let stderr = String::from_utf8_lossy(&bare.stderr).to_string();
    assert!(stderr.contains("unauthorized") && stderr.contains("kite connect lead"), "{stderr}");
    // Someone else's token.
    let coder_token = p.token_file("coder").display().to_string();
    let mut wrong = note.to_vec();
    wrong.extend(["--token-file", &coder_token]);
    assert!(String::from_utf8_lossy(&p.kite_bare(&wrong).stderr).contains("unauthorized"));
    // Its own token.
    assert!(p.kite(&note).status.success());

    // Over MCP the same: a host started without the token cannot act as lead.
    let mut no_token = p.mcp_with("lead", &[]).await;
    let send = json!({ "to": "coder", "schema": "task.assign@1", "payload": { "goal": "g" } });
    no_token.refused("kite_send", send, "unauthorized").await;
    no_token.close().await;
    let mut lead = p.mcp("lead").await;
    lead.ok("kite_contracts", json!({})).await;
    lead.close().await;

    p.stop().await;
    let ledger = p.ok(&["trace", "--json"]);
    let lead_token = std::fs::read_to_string(p.token_file("lead")).unwrap();
    assert!(!ledger.contains(lead_token.trim()), "a token was written to the ledger");
}

#[tokio::test]
async fn tokens_are_rotated_by_the_operator_and_never_printed_into_files() {
    let mut p = Project::init("tokens_rotation");
    p.up().await;
    // Piped output (as an agent's shell would capture it) gets no token.
    let shown = p.kite_bare(&["token", "show", "lead"]);
    assert_eq!(shown.status.code(), Some(1));
    assert!(String::from_utf8_lossy(&shown.stderr).contains("refusing"));

    let old = std::fs::read_to_string(p.token_file("lead")).unwrap();
    p.ok(&["token", "rotate", "lead"]);
    let new = std::fs::read_to_string(p.token_file("lead")).unwrap();
    assert_ne!(old, new);
    let old_file = p.path("old-lead-token");
    std::fs::write(&old_file, &old).unwrap();
    let old_arg = old_file.display().to_string();
    let refused = p.kite_bare(&["recv", "--as", "lead", "--token-file", &old_arg]);
    assert!(String::from_utf8_lossy(&refused.stderr).contains("unauthorized"), "the old token no longer works");
    assert!(p.kite(&["recv", "--as", "lead"]).status.success(), "the new one does");
    assert!(p.kite_bare(&["status"]).status.success(), "reads need no token");
    p.stop().await;

    // Generated host configs point at the token file; none contains a token.
    let mcp_config = std::fs::read_to_string(p.path(".kite/mcp/lead.json")).unwrap();
    assert!(mcp_config.contains("--token-file") && !mcp_config.contains(new.trim()));
    let connect = p.ok(&["connect", "lead"]);
    assert!(connect.contains("--token-file") && connect.contains("--scope local"), "{connect}");
    assert!(!connect.contains(new.trim()));
    assert_eq!(std::fs::read_to_string(p.path(".kite/.gitignore")).unwrap().lines().last(), Some("*"));
}
