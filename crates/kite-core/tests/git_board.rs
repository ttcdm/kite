//! The agent Kanban against a real git repository: starting a card adds its
//! worktree or branch, and merging needs a commit on the mainline.

use kite_core::broker::{BrokerHandle, ConnId, Reply};
use kite_core::vcs::Vcs;
use kite_core::{Broker, Ledger};
use kite_proto::board::*;
use kite_proto::wire::*;
use kite_proto::*;
use std::path::Path;
use std::process::Command;
use std::sync::Arc;

const CONFIG: &str = r#"
[agents.lead]
[agents.coder]
[agents.reviewer]

[board]
managers = ["lead"]
default_gates = ["tests"]
"#;

/// Runs git in `dir` with no user or system configuration, and returns its output.
fn git(dir: &Path, args: &[&str]) -> String {
    let out = Command::new("git")
        .arg("-C")
        .arg(dir)
        .args(["-c", "user.name=kite-test", "-c", "user.email=kite@test", "-c", "commit.gpgsign=false"])
        .args(args)
        .env("GIT_CONFIG_GLOBAL", "/dev/null")
        .env("GIT_CONFIG_NOSYSTEM", "1")
        .output()
        .unwrap();
    assert!(out.status.success(), "git {args:?}: {}", String::from_utf8_lossy(&out.stderr));
    String::from_utf8_lossy(&out.stdout).trim().to_string()
}

fn commit(dir: &Path, file: &str) -> String {
    std::fs::write(dir.join(file), file).unwrap();
    git(dir, &["add", file]);
    git(dir, &["commit", "-q", "-m", file]);
    git(dir, &["rev-parse", "HEAD"])
}

struct Env {
    h: BrokerHandle,
    root: std::path::PathBuf,
    _dir: tempfile::TempDir,
}

fn start() -> Env {
    start_with("", None)
}

/// A git project with `extra` appended to [board] and, if given, a stand-in for git.
fn start_with(extra: &str, vcs: Option<Arc<dyn Vcs>>) -> Env {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().canonicalize().unwrap();
    git(&root, &["init", "-q", "-b", "main"]);
    std::fs::write(root.join(".gitignore"), ".kite/\nwt/\n").unwrap();
    commit(&root, "README");
    let config = Arc::new(Config::parse(&format!("{CONFIG}{extra}"), &root).unwrap());
    let ledger = Ledger::open(&config.broker.ledger, config.broker.fsync).unwrap();
    let (h, _task) = match vcs {
        Some(v) => Broker::spawn_with_vcs(config, ledger, v),
        None => Broker::spawn(config, ledger),
    };
    Env { h, root, _dir: dir }
}

impl Env {
    async fn agent(&self, name: &str) -> ConnId {
        let c = self.h.connect();
        self.h.call(c, Op::Register { agent: name.into(), protocol: PROTOCOL.into() }).await.result.unwrap();
        c
    }

    async fn card(&self, who: ConnId, op: CardOp) -> Reply {
        self.h.call(who, Op::Card { card: op }).await
    }

    /// Creates a shaped card owned by coder and marks it ready.
    async fn ready(&self, lead: ConnId, id: &str, branch: Option<&str>, worktree: Option<&str>) {
        let draft = CardDraft {
            id: Some(id.into()),
            title: id.into(),
            owner: Some("coder".into()),
            branch: branch.map(str::to_string),
            worktree: worktree.map(str::to_string),
            acceptance: vec!["it works".into()],
            merge_gate: Some("tests pass".into()),
            ..Default::default()
        };
        self.card(lead, CardOp::Create { card: draft }).await.result.unwrap();
        self.card(lead, mv(id, CardState::Ready, None)).await.result.unwrap();
    }
}

fn mv(id: &str, to: CardState, merge_ref: Option<&str>) -> CardOp {
    CardOp::Move { id: id.into(), to, blocker: None, reason: None, merge_ref: merge_ref.map(str::to_string) }
}

fn refused(r: Reply) -> String {
    let e = r.result.unwrap_err();
    assert_eq!(e.code, ErrorCode::TransitionRefused, "{}", e.message);
    e.message
}

fn card_of(r: Reply) -> Card {
    serde_json::from_value(r.result.unwrap()).unwrap()
}

#[tokio::test]
async fn starting_a_card_adds_its_worktree_or_branch_in_git() {
    let e = start();
    let lead = e.agent("lead").await;
    let coder = e.agent("coder").await;

    // A new branch in a new worktree.
    e.ready(lead, "a", Some("feat/a"), Some("wt/a")).await;
    let c = card_of(e.card(coder, mv("a", CardState::Running, None)).await);
    assert_eq!(c.state, CardState::Running);
    assert_eq!(git(&e.root.join("wt/a"), &["symbolic-ref", "--short", "HEAD"]), "feat/a");

    // A branch that already exists is checked out in the new worktree.
    git(&e.root, &["branch", "feat/c"]);
    e.ready(lead, "c", Some("feat/c"), Some("wt/c")).await;
    e.card(coder, mv("c", CardState::Running, None)).await.result.unwrap();
    assert_eq!(git(&e.root.join("wt/c"), &["symbolic-ref", "--short", "HEAD"]), "feat/c");

    // Without a worktree the branch is created in place, and the root stays on main.
    e.ready(lead, "b", Some("feat/b"), None).await;
    e.card(coder, mv("b", CardState::Running, None)).await.result.unwrap();
    git(&e.root, &["rev-parse", "--verify", "refs/heads/feat/b"]);
    assert_eq!(git(&e.root, &["symbolic-ref", "--short", "HEAD"]), "main");

    // git's refusal is the card's refusal, and the card stays ready.
    e.ready(lead, "bad", Some("bad..name"), Some("wt/bad")).await;
    let m = refused(e.card(coder, mv("bad", CardState::Running, None)).await);
    assert!(m.contains("cannot set up worktree wt/bad on branch bad..name: git check-ref-format"), "{m}");
    assert!(!e.root.join("wt/bad").exists());
    let got = card_of(e.card(coder, CardOp::Get { id: "bad".into() }).await);
    assert_eq!(got.state, CardState::Ready);

    // A branch checked out in another worktree cannot be checked out again.
    e.ready(lead, "dup", Some("feat/a"), Some("wt/dup")).await;
    let m = refused(e.card(coder, mv("dup", CardState::Running, None)).await);
    assert!(m.contains("cannot set up worktree wt/dup on branch feat/a"), "{m}");

    // A worktree needs a branch to be added on.
    e.ready(lead, "nobranch", None, Some("wt/n")).await;
    let m = refused(e.card(coder, mv("nobranch", CardState::Running, None)).await);
    assert!(m.contains("set a branch for worktree wt/n"), "{m}");
}

#[tokio::test]
async fn merging_needs_a_commit_reachable_from_the_mainline() {
    let e = start();
    let lead = e.agent("lead").await;
    let coder = e.agent("coder").await;
    let reviewer = e.agent("reviewer").await;
    e.ready(lead, "a", Some("feat/a"), Some("wt/a")).await;
    e.card(coder, mv("a", CardState::Running, None)).await.result.unwrap();

    let wt = e.root.join("wt/a");
    let work = commit(&wt, "feature.txt");
    std::fs::write(wt.join("HANDOFF.md"), "done").unwrap();
    let handoff = CardPatch { handoff: Some("HANDOFF.md".into()), ..Default::default() };
    e.card(coder, CardOp::Update { id: "a".into(), patch: handoff }).await.result.unwrap();
    let evidence = |gate: Option<&str>| CardOp::Evidence {
        id: "a".into(),
        kind: "test".into(),
        reference: "cargo test".into(),
        note: None,
        gate: gate.map(str::to_string),
        pass: gate.map(|_| true),
    };
    e.card(coder, evidence(None)).await.result.unwrap();
    e.card(coder, mv("a", CardState::Review, None)).await.result.unwrap();
    e.card(reviewer, evidence(Some("tests"))).await.result.unwrap();

    let m = refused(e.card(lead, mv("a", CardState::Merged, Some("no-such-ref"))).await);
    assert!(m.contains("merge_ref no-such-ref is not a commit in this repository"), "{m}");

    // The branch has a commit main does not.
    let m = refused(e.card(lead, mv("a", CardState::Merged, Some("feat/a"))).await);
    assert!(m.contains(&format!("merge_ref feat/a ({work}) is not reachable from main")), "{m}");

    git(&e.root, &["merge", "-q", "--ff-only", "feat/a"]);
    let c = card_of(e.card(lead, mv("a", CardState::Merged, Some("feat/a"))).await);
    assert_eq!((c.state, c.merge_ref), (CardState::Merged, Some(work)));
}

/// Git that blocks every checkout until the test lets it go.
struct HeldGit {
    release: std::sync::Mutex<std::sync::mpsc::Receiver<()>>,
}

impl Vcs for HeldGit {
    fn is_repo(&self) -> bool {
        true
    }
    fn checkout_worktree(&self, _: &Path, _: &str) -> Result<(), String> {
        self.release.lock().unwrap().recv().map_err(|_| "released without a go".to_string())
    }
    fn ensure_branch(&self, _: &str) -> Result<(), String> {
        Ok(())
    }
    fn resolve_commit(&self, rev: &str) -> Result<String, String> {
        Ok(rev.to_string())
    }
    fn reachable_from(&self, _: &str, _: &str) -> Result<bool, String> {
        Ok(true)
    }
}

#[tokio::test]
async fn a_slow_git_holds_only_its_own_card() {
    let (go, release) = std::sync::mpsc::channel();
    let e = start_with("", Some(Arc::new(HeldGit { release: std::sync::Mutex::new(release) })));
    let lead = e.agent("lead").await;
    let coder = e.agent("coder").await;
    e.ready(lead, "slow", Some("feat/slow"), Some("wt/slow")).await;
    e.ready(lead, "other", Some("feat/other"), None).await;

    let start = e.h.submit(coder, Op::Card { card: mv("slow", CardState::Running, None) }).await;
    // The broker keeps serving while git is stuck ...
    let status: StatusOk =
        serde_json::from_value(e.h.call(lead, Op::Status { agent: None }).await.result.unwrap()).unwrap();
    assert_eq!(status.agents.len(), 3);
    let title = CardPatch { title: Some("other, renamed".into()), ..Default::default() };
    e.card(lead, CardOp::Update { id: "other".into(), patch: title }).await.result.unwrap();
    // ... and only the card waiting on git is held.
    let held = CardPatch { title: Some("renamed".into()), ..Default::default() };
    let busy = e.card(lead, CardOp::Update { id: "slow".into(), patch: held }).await.result.unwrap_err();
    assert_eq!(busy.code, ErrorCode::Busy);

    go.send(()).unwrap();
    let started = card_of(start.await.unwrap());
    assert_eq!(started.state, CardState::Running);
}

#[tokio::test]
async fn a_hung_git_hook_is_killed_and_the_start_refused() {
    let e = start_with("git_timeout_ms = 300\n", None);
    let hook = e.root.join(".git/hooks/post-checkout");
    std::fs::write(&hook, "#!/bin/sh\nsleep 60\n").unwrap();
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&hook, std::fs::Permissions::from_mode(0o755)).unwrap();
    }
    let lead = e.agent("lead").await;
    let coder = e.agent("coder").await;
    e.ready(lead, "hung", Some("feat/hung"), Some("wt/hung")).await;
    let why = refused(e.card(coder, mv("hung", CardState::Running, None)).await);
    assert!(why.contains("timed out") && why.contains("git worktree prune"), "{why}");
    let card = card_of(e.card(lead, CardOp::Get { id: "hung".into() }).await);
    assert_eq!(card.state, CardState::Ready, "a refused start leaves the card ready");
}
