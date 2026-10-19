//! The agent Kanban through the real binary: MCP card tools, `kite card`,
//! `kite board`, and git-backed moves in a real repository.

mod common;

use common::{Mcp, Project};
use serde_json::{Value, json};

/// Takes a card from creation to review: lead shapes and assigns it, coder starts it,
/// records evidence and hands it off with `HANDOFF.md` in its worktree.
async fn to_review(p: &Project, lead: &mut Mcp, coder: &mut Mcp, card: Value) -> String {
    let created = lead.ok("kite_card_create", card).await;
    let id = created["id"].as_str().unwrap().to_string();
    let worktree = created["worktree"].as_str().unwrap_or("").to_string();
    lead.ok("kite_card_move", json!({ "id": id, "to": "ready" })).await;
    lead.refused("kite_card_move", json!({ "id": id, "to": "running" }), "not_permitted").await;
    coder.ok("kite_card_move", json!({ "id": id, "to": "running" })).await;
    p.write(&format!("{worktree}/HANDOFF.md"), "Added --flag; see tests/flag.rs\n");
    coder.ok("kite_card_update", json!({ "id": id, "handoff": "HANDOFF.md" })).await;
    let e = coder.refused("kite_card_move", json!({ "id": id, "to": "review" }), "transition_refused").await;
    assert!(e["message"].as_str().unwrap().contains("evidence"), "{e}");
    coder.ok("kite_card_evidence", json!({ "id": id, "kind": "test", "reference": "cargo test" })).await;
    coder.ok("kite_card_move", json!({ "id": id, "to": "review" })).await;
    id
}

/// The reviewer passes every gate from the shell.
fn pass_gates(p: &Project, id: &str) {
    for gate in ["tests", "diff_review", "risk"] {
        p.ok(&[
            "card", "evidence", "--as", "reviewer", id, "--kind", "review", "--ref", "git diff", "--gate", gate,
            "--pass",
        ]);
    }
}

fn flag_card(worktree: &str, branch: Option<&str>) -> Value {
    let mut card = json!({ "title": "Add --flag", "acceptance": ["--flag parses"], "merge_gate": "tests and review pass",
                           "owner": "coder", "worktree": worktree });
    if let Some(b) = branch {
        card["branch"] = json!(b);
    }
    card
}

#[tokio::test]
async fn a_card_goes_from_backlog_to_merged_and_shows_on_the_board() {
    let mut p = Project::init("card_lifecycle");
    p.up().await;
    let mut lead = p.mcp("lead").await;
    let mut coder = p.mcp("coder").await;
    let id = to_review(&p, &mut lead, &mut coder, flag_card("wt/coder", None)).await;

    // The owner was told about the card the lead created for it.
    let inbox = coder.ok("kite_recv", json!({ "wait_ms": 2000, "max": 32 })).await;
    let actions: Vec<&str> = inbox["messages"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|m| m["schema"] == "kite.card@1")
        .map(|m| m["payload"]["action"].as_str().unwrap())
        .collect();
    assert!(actions.contains(&"created"), "{actions:?}");

    let board = p.ok(&["board"]);
    assert!(
        board.contains("REVIEW (1)") && board.contains("MERGE READINESS") && board.contains("gates 0/3"),
        "{board}"
    );
    // Gates pass in order, and the owner alone cannot satisfy independent review.
    let early =
        p.kite(&["card", "evidence", "--as", "reviewer", &id, "--kind", "r", "--ref", "x", "--gate", "risk", "--pass"]);
    assert!(String::from_utf8_lossy(&early.stderr).contains("must pass before risk"));
    pass_gates(&p, &id);
    lead.refused("kite_card_move", json!({ "id": id, "to": "merged" }), "transition_refused").await;
    let merged = lead.ok("kite_card_move", json!({ "id": id, "to": "merged", "merge_ref": "abc1234" })).await;
    assert_eq!(merged["state"], "merged");

    let board = p.ok(&["board", "--all"]);
    assert!(board.contains("MERGED (1)") && board.contains("abc1234"), "{board}");
    let shown = p.ok(&["card", "show", &id]);
    assert!(shown.contains("tests ok (reviewer)") && shown.contains("risk ok (reviewer)"), "{shown}");
    lead.close().await;
    coder.close().await;
    p.stop().await;
}

#[tokio::test]
async fn in_a_git_project_cards_get_worktrees_and_merges_must_be_on_main() {
    let mut p = Project::init("card_git");
    p.git_init();
    p.up().await;
    let mut lead = p.mcp("lead").await;
    let mut coder = p.mcp("coder").await;
    let id = to_review(&p, &mut lead, &mut coder, flag_card("wt/coder", Some("feat/flag"))).await;

    // Starting the card added a real worktree on its branch.
    let worktrees = p.sh("git", &["worktree", "list", "--porcelain"]);
    assert!(worktrees.contains("wt/coder") && worktrees.contains("refs/heads/feat/flag"), "{worktrees}");

    // The coder's work lands as a commit on the branch, in its worktree.
    p.sh("git", &["-C", "wt/coder", "add", "HANDOFF.md"]);
    p.sh("git", &["-C", "wt/coder", "commit", "-q", "-m", "add --flag"]);
    let commit = p.sh("git", &["rev-parse", "feat/flag"]).trim().to_string();
    pass_gates(&p, &id);

    let bogus = json!({ "id": id, "to": "merged", "merge_ref": "no-such-ref" });
    lead.refused("kite_card_move", bogus, "transition_refused").await;
    let e = lead
        .refused("kite_card_move", json!({ "id": id, "to": "merged", "merge_ref": commit }), "transition_refused")
        .await;
    assert!(e["message"].as_str().unwrap().contains("main"), "not on main yet: {e}");

    p.sh("git", &["merge", "-q", "--ff-only", "feat/flag"]);
    let short = &commit[..7];
    let merged = lead.ok("kite_card_move", json!({ "id": id, "to": "merged", "merge_ref": short })).await;
    assert_eq!(merged["merge_ref"], commit.as_str(), "the card keeps the full commit id");
    lead.close().await;
    coder.close().await;
    p.stop().await;
}

#[tokio::test]
async fn the_board_survives_a_broker_restart() {
    let mut p = Project::init("board_restart");
    p.up().await;
    let created = p.json(&["card", "new", "--as", "lead", "--title", "Write docs", "--accept", "README covers it"]);
    let id = created["id"].as_str().unwrap().to_string();
    p.ok(&["card", "set", "--as", "lead", &id, "--merge-gate", "review passes", "--owner", "reviewer"]);
    p.ok(&["card", "move", "--as", "lead", &id, "ready"]);
    p.stop().await;

    p.up().await;
    let card = p.json(&["card", "show", &id]);
    assert_eq!((card["state"].as_str(), card["owner"].as_str()), (Some("ready"), Some("reviewer")));
    let next = p.json(&["card", "new", "--as", "lead", "--title", "Next"]);
    assert_ne!(next["id"], id.as_str(), "numbering continues after the restart");
    p.stop().await;
}
