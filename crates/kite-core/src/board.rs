//! The agent Kanban state machine. The broker owns one `Board` and applies
//! every card operation to it in order; each change is written to the ledger
//! as a full card snapshot, which is also how the board is rebuilt on start.
//!
//! Exit criteria (refused with `transition_refused` until met):
//!   backlog → ready     acceptance criteria, merge gate and at least one gate (manager)
//!   ready → running     an owner and a branch or worktree; only the owner starts it,
//!                       and no other active card may use the same worktree. In a git
//!                       project the worktree is added on its branch (or the branch is
//!                       created, if the card has no worktree), and a git failure refuses
//!   running → review    the owner's handoff file exists and evidence was recorded
//!   review → merged     every gate passed in order, one of them by someone other
//!                       than the owner (if independent_review), and a merge_ref (manager);
//!                       in a git project merge_ref must be a commit on the mainline
//!
//! Git is reached only through the [`Vcs`] the caller passes in, so the board
//! itself starts no processes.
//!   any → blocked       a blocker with reason, owner and next action
//!   blocked → (back)    only to the state it was blocked from
//!   any → archived      a reason (manager)

use crate::vcs::Vcs;
use kite_proto::board::*;
use kite_proto::config::{BoardSettings, valid_gate_name};
use kite_proto::{ErrorCode, KiteError, KiteResult};
use serde_json::{Value, json};
use std::collections::{BTreeMap, BTreeSet};
use std::path::{Component, Path, PathBuf};

const MAX_TEXT: usize = 2000;
const MAX_ITEMS: usize = 64;

pub enum Applied {
    Read(Value),
    Changed {
        card: Box<Card>,
        action: &'static str,
        /// Agents to send `kite.card@1` to.
        notify: BTreeSet<String>,
    },
}

pub struct Board {
    cards: BTreeMap<String, Card>,
    settings: BoardSettings,
    agents: BTreeSet<String>,
    root: PathBuf,
    next: u64,
}

fn refuse(code: ErrorCode, message: impl Into<String>) -> KiteError {
    KiteError::new(code, message)
}

fn text(field: &str, s: &str) -> KiteResult<String> {
    let t = s.trim();
    if t.is_empty() || t.len() > MAX_TEXT {
        return Err(refuse(ErrorCode::InvalidArgument, format!("{field} must be 1..={MAX_TEXT} characters")));
    }
    Ok(t.to_string())
}

fn texts(field: &str, v: &[String]) -> KiteResult<Vec<String>> {
    if v.len() > MAX_ITEMS {
        return Err(refuse(ErrorCode::InvalidArgument, format!("{field} may hold at most {MAX_ITEMS} items")));
    }
    v.iter().map(|s| text(field, s)).collect()
}

fn gate_list(v: &[String]) -> KiteResult<Vec<String>> {
    let mut seen = BTreeSet::new();
    for g in v {
        if !valid_gate_name(g) || !seen.insert(g) {
            return Err(refuse(
                ErrorCode::InvalidArgument,
                format!("gate {g:?} must be unique and match [a-z][a-z0-9_-]{{0,63}}"),
            ));
        }
    }
    Ok(v.to_vec())
}

/// A path inside the project: relative, no `..`, normalised. `"."` and `""` mean the root.
fn project_path(field: &str, s: &str) -> KiteResult<Option<String>> {
    let p = Path::new(s.trim());
    let mut parts = Vec::new();
    for c in p.components() {
        match c {
            Component::Normal(x) => parts.push(x.to_string_lossy().to_string()),
            Component::CurDir => {}
            _ => {
                return Err(refuse(
                    ErrorCode::InvalidArgument,
                    format!("{field} must be a relative path inside the project, without .."),
                ));
            }
        }
    }
    Ok((!parts.is_empty()).then(|| parts.join("/")))
}

fn valid_card_id(id: &str) -> bool {
    let mut chars = id.chars();
    matches!(chars.next(), Some(c) if c.is_ascii_lowercase() || c.is_ascii_digit())
        && id.len() <= 64
        && chars.all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_' || c == '-')
}

/// Rebuilds the board from ledger events: the last snapshot of each card wins.
pub fn replay(events: impl IntoIterator<Item = Value>) -> BTreeMap<String, Card> {
    let mut cards = BTreeMap::new();
    for e in events {
        if e["type"] == "card"
            && let Ok(card) = serde_json::from_value::<Card>(e["card"].clone())
        {
            cards.insert(card.id.clone(), card);
        }
    }
    cards
}

fn allowed_next(from: CardState) -> Vec<CardState> {
    use CardState::*;
    match from {
        Backlog => vec![Ready, Blocked, Archived],
        Ready => vec![Running, Backlog, Blocked, Archived],
        Running => vec![Review, Blocked, Archived],
        Review => vec![Merged, Running, Blocked, Archived],
        Blocked => vec![Archived],
        Merged | Archived => vec![],
    }
}

impl Board {
    pub fn new(
        settings: BoardSettings,
        agents: BTreeSet<String>,
        root: PathBuf,
        cards: BTreeMap<String, Card>,
    ) -> Board {
        let next = cards
            .keys()
            .filter_map(|id| id.strip_prefix("card-").and_then(|n| n.parse::<u64>().ok()))
            .max()
            .unwrap_or(0)
            + 1;
        Board { cards, settings, agents, root, next }
    }

    pub fn cards(&self) -> impl Iterator<Item = &Card> {
        self.cards.values()
    }

    pub fn active_owned_by<'a>(&'a self, agent: &'a str) -> impl Iterator<Item = &'a Card> + 'a {
        self.cards.values().filter(move |c| c.owner.as_deref() == Some(agent) && !c.state.is_terminal())
    }

    fn manager(&self, by: &str) -> bool {
        self.settings.is_manager(by)
    }

    fn need_manager(&self, by: &str, what: &str) -> KiteResult<()> {
        if self.manager(by) {
            return Ok(());
        }
        Err(refuse(ErrorCode::NotPermitted, format!("only a board manager may {what}"))
            .with_detail(json!({ "managers": self.settings.managers })))
    }

    fn managers_except(&self, by: &str) -> BTreeSet<String> {
        self.settings.managers.iter().filter(|m| *m != by).cloned().collect()
    }

    fn declared(&self, agent: &str) -> KiteResult<()> {
        if self.agents.contains(agent) {
            return Ok(());
        }
        Err(refuse(ErrorCode::UnknownAgent, format!("{agent:?} is not declared under [agents]")))
    }

    fn card(&self, id: &str) -> KiteResult<&Card> {
        self.cards.get(id).ok_or_else(|| refuse(ErrorCode::NoSuchCard, format!("no card {id:?}")))
    }

    /// Whether a card holds its worktree: it is being worked on or waiting on review.
    fn holds_worktree(c: &Card) -> bool {
        matches!(c.state, CardState::Running | CardState::Review)
            || (c.state == CardState::Blocked
                && matches!(c.resume_state, Some(CardState::Running) | Some(CardState::Review)))
    }

    pub fn apply(&mut self, by: &str, op: CardOp, now: u64, vcs: &dyn Vcs) -> KiteResult<Applied> {
        match op {
            CardOp::Get { id } => Ok(Applied::Read(serde_json::to_value(self.card(&id)?).expect("cards serialize"))),
            CardOp::List { state, owner } => {
                let cards: Vec<&Card> = self
                    .cards
                    .values()
                    .filter(|c| state.is_none_or(|s| c.state == s))
                    .filter(|c| owner.as_deref().is_none_or(|o| c.owner.as_deref() == Some(o)))
                    .collect();
                Ok(Applied::Read(json!({ "cards": cards })))
            }
            CardOp::Create { card } => self.create(by, card, now),
            CardOp::Update { id, patch } => self.update(by, &id, patch, now),
            CardOp::Move { id, to, blocker, reason, merge_ref } => {
                self.move_card(by, &id, to, blocker, reason, merge_ref, now, vcs)
            }
            CardOp::Evidence { id, kind, reference, note, gate, pass } => {
                self.evidence(by, &id, kind, reference, note, gate, pass, now)
            }
        }
    }

    fn changed(&mut self, card: Card, action: &'static str, notify: BTreeSet<String>, by: &str) -> KiteResult<Applied> {
        let notify = notify.into_iter().filter(|a| a != by && self.agents.contains(a)).collect();
        self.cards.insert(card.id.clone(), card.clone());
        Ok(Applied::Changed { card: Box::new(card), action, notify })
    }

    fn create(&mut self, by: &str, d: CardDraft, now: u64) -> KiteResult<Applied> {
        let id = match d.id {
            Some(id) => {
                if !valid_card_id(&id) {
                    return Err(refuse(ErrorCode::InvalidArgument, "card id must match [a-z0-9][a-z0-9_-]{0,63}"));
                }
                if self.cards.contains_key(&id) {
                    return Err(refuse(ErrorCode::CardConflict, format!("card {id} already exists")));
                }
                id
            }
            None => loop {
                let id = format!("card-{:03}", self.next);
                self.next += 1;
                if !self.cards.contains_key(&id) {
                    break id;
                }
            },
        };
        if let Some(o) = &d.owner {
            self.need_manager(by, "assign an owner")?;
            self.declared(o)?;
        }
        let card = Card {
            id,
            title: text("title", &d.title)?,
            owner: d.owner.clone(),
            state: CardState::Backlog,
            branch: d.branch.as_deref().map(|b| text("branch", b)).transpose()?,
            worktree: d.worktree.as_deref().map(|w| project_path("worktree", w)).transpose()?.flatten(),
            acceptance: texts("acceptance", &d.acceptance)?,
            merge_gate: d.merge_gate.as_deref().map(|m| text("merge_gate", m)).transpose()?.unwrap_or_default(),
            gates: gate_list(d.gates.as_ref().unwrap_or(&self.settings.default_gates))?,
            handoff: None,
            evidence: Vec::new(),
            blocker: None,
            resume_state: None,
            merge_ref: None,
            archive_reason: None,
            revision: 1,
            created_by: by.to_string(),
            created_at_ms: now,
            updated_at_ms: now,
        };
        let notify = card.owner.iter().cloned().collect();
        self.changed(card, "created", notify, by)
    }

    fn update(&mut self, by: &str, id: &str, p: CardPatch, now: u64) -> KiteResult<Applied> {
        let mut c = self.card(id)?.clone();
        if c.state.is_terminal() {
            return Err(refuse(
                ErrorCode::TransitionRefused,
                format!("card {id} is {} and can no longer change", c.state),
            ));
        }
        let is_owner = c.owner.as_deref() == Some(by);
        if !is_owner && !self.manager(by) {
            return Err(refuse(ErrorCode::NotPermitted, format!("only the owner or a manager may change card {id}")));
        }
        let working = Board::holds_worktree(&c);
        let mut notify = BTreeSet::new();
        let mut action = "updated";
        if let Some(o) = &p.owner {
            self.need_manager(by, "assign an owner")?;
            self.declared(o)?;
            if working {
                return Err(refuse(
                    ErrorCode::TransitionRefused,
                    format!("card {id} is {}; move it back to ready or block it before reassigning", c.state),
                ));
            }
            notify.extend(c.owner.iter().cloned());
            notify.insert(o.clone());
            c.owner = Some(o.clone());
            action = "assigned";
        }
        if p.gates.is_some() {
            self.need_manager(by, "change gates")?;
            if c.state == CardState::Review {
                return Err(refuse(ErrorCode::TransitionRefused, "gates cannot change while the card is in review"));
            }
        }
        if (p.branch.is_some() || p.worktree.is_some()) && working {
            return Err(refuse(
                ErrorCode::TransitionRefused,
                format!("card {id} is {}; its branch and worktree are fixed until it leaves", c.state),
            ));
        }
        if let Some(t) = &p.title {
            c.title = text("title", t)?;
        }
        if let Some(a) = &p.acceptance {
            c.acceptance = texts("acceptance", a)?;
        }
        if let Some(m) = &p.merge_gate {
            c.merge_gate = text("merge_gate", m)?;
        }
        if let Some(g) = &p.gates {
            c.gates = gate_list(g)?;
        }
        if let Some(b) = &p.branch {
            c.branch = Some(text("branch", b)?);
        }
        if let Some(w) = &p.worktree {
            c.worktree = project_path("worktree", w)?;
        }
        if let Some(h) = &p.handoff {
            c.handoff = project_path("handoff", h)?;
        }
        if !is_owner {
            notify.extend(c.owner.iter().cloned());
        }
        c.updated_at_ms = now;
        self.changed(c, action, notify, by)
    }

    fn worktree_dir(&self, c: &Card) -> PathBuf {
        match &c.worktree {
            Some(w) => self.root.join(w),
            None => self.root.clone(),
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn move_card(
        &mut self,
        by: &str,
        id: &str,
        to: CardState,
        blocker: Option<Blocker>,
        reason: Option<String>,
        merge_ref: Option<String>,
        now: u64,
        vcs: &dyn Vcs,
    ) -> KiteResult<Applied> {
        use CardState::*;
        let mut c = self.card(id)?.clone();
        let from = c.state;
        let is_owner = c.owner.as_deref() == Some(by);
        let owner_or_manager = is_owner || self.manager(by);
        let not_met = |what: String| refuse(ErrorCode::TransitionRefused, format!("{from} -> {to}: {what}"));
        let mut notify: BTreeSet<String> = BTreeSet::new();
        let action: &'static str = match (from, to) {
            (Backlog, Ready) => {
                self.need_manager(by, "mark a card ready")?;
                if c.acceptance.is_empty() {
                    return Err(not_met("write acceptance criteria first".into()));
                }
                if c.merge_gate.is_empty() {
                    return Err(not_met("write the merge gate first".into()));
                }
                if c.gates.is_empty() {
                    return Err(not_met("name at least one gate".into()));
                }
                notify.extend(c.owner.iter().cloned());
                "shaped"
            }
            (Ready, Backlog) => {
                self.need_manager(by, "send a card back to the backlog")?;
                "unshaped"
            }
            (Ready, Running) => {
                let Some(owner) = c.owner.clone() else {
                    return Err(not_met("assign an owner first".into()));
                };
                if owner != by {
                    return Err(refuse(
                        ErrorCode::NotPermitted,
                        format!("only the owner, {owner}, can start card {id}"),
                    ));
                }
                if c.branch.is_none() && c.worktree.is_none() {
                    return Err(not_met("set a branch or worktree first".into()));
                }
                let here = c.worktree.clone();
                if let Some(other) =
                    self.cards.values().find(|o| o.id != c.id && Board::holds_worktree(o) && o.worktree == here)
                {
                    return Err(refuse(
                        ErrorCode::CardConflict,
                        format!(
                            "card {} is already working in {}; give this card its own worktree",
                            other.id,
                            here.as_deref().unwrap_or("the project root")
                        ),
                    )
                    .with_detail(json!({ "card": other.id, "worktree": here })));
                }
                // Last, because it changes the repository: every other check has passed.
                if vcs.is_repo() {
                    let git =
                        |what: String, e: String| not_met(format!("{what}: {e}")).with_detail(json!({ "git": e }));
                    match (&c.worktree, &c.branch) {
                        (Some(w), Some(b)) => vcs
                            .checkout_worktree(&self.root.join(w), b)
                            .map_err(|e| git(format!("cannot set up worktree {w} on branch {b}"), e))?,
                        (Some(w), None) => {
                            return Err(not_met(format!("set a branch for worktree {w}: git needs one to add it")));
                        }
                        (None, Some(b)) => {
                            vcs.ensure_branch(b).map_err(|e| git(format!("cannot create branch {b}"), e))?
                        }
                        (None, None) => return Err(not_met("set a branch or worktree first".into())),
                    }
                }
                notify.extend(self.managers_except(by));
                "started"
            }
            (Running, Review) => {
                if !is_owner {
                    return Err(refuse(ErrorCode::NotPermitted, "only the owner can hand a card off for review"));
                }
                let Some(h) = c.handoff.clone() else {
                    return Err(not_met("set the handoff note first".into()));
                };
                let path = self.worktree_dir(&c).join(&h);
                if !path.is_file() {
                    return Err(not_met(format!("handoff note {} does not exist", path.display())));
                }
                if !c.evidence.iter().any(|e| e.revision == c.revision && e.gate.is_none()) {
                    return Err(not_met("record evidence (tests, diff, logs) for this revision first".into()));
                }
                notify.extend(self.managers_except(by));
                "submitted"
            }
            (Review, Running) => {
                if !owner_or_manager {
                    return Err(refuse(ErrorCode::NotPermitted, "only the owner or a manager can reopen a card"));
                }
                c.revision += 1;
                notify.extend(c.owner.iter().cloned());
                "reopened"
            }
            (Review, Merged) => {
                self.need_manager(by, "merge a card")?;
                if let Some(g) = c.gates.get(c.gates_passed()) {
                    return Err(not_met(format!("gate {g} has not passed in revision {}", c.revision))
                        .with_detail(json!({ "gates": c.gates, "passed": c.gates_passed() })));
                }
                let owner = c.owner.clone().unwrap_or_default();
                if self.settings.independent_review
                    && !c.gates.iter().filter_map(|g| c.gate_result(g)).any(|e| e.pass == Some(true) && e.by != owner)
                {
                    return Err(not_met(format!("a gate must be passed by someone other than {owner}")));
                }
                let r = merge_ref.as_deref().map(|r| text("merge_ref", r)).transpose()?;
                let Some(mut r) = r else {
                    return Err(not_met("give merge_ref: the commit or PR it landed in".into()));
                };
                if vcs.is_repo() {
                    let mainline = &self.settings.mainline;
                    let commit = vcs
                        .resolve_commit(&r)
                        .map_err(|e| not_met(format!("merge_ref {r} is not a commit in this repository ({e})")))?;
                    let on = vcs
                        .reachable_from(&commit, mainline)
                        .map_err(|e| not_met(format!("cannot read mainline {mainline} ({e})")))?;
                    if !on {
                        return Err(not_met(format!(
                            "merge_ref {r} ({commit}) is not reachable from {mainline}; merge it into {mainline} first"
                        ))
                        .with_detail(json!({ "commit": commit, "mainline": mainline })));
                    }
                    r = commit;
                }
                c.merge_ref = Some(r);
                notify.extend(c.owner.iter().cloned());
                "merged"
            }
            (Backlog | Ready | Running | Review, Blocked) => {
                if !owner_or_manager {
                    return Err(refuse(ErrorCode::NotPermitted, "only the owner or a manager can block a card"));
                }
                let Some(b) = blocker else {
                    return Err(not_met("give a blocker with reason, owner and next_action".into()));
                };
                c.blocker = Some(Blocker {
                    reason: text("blocker.reason", &b.reason)?,
                    owner: text("blocker.owner", &b.owner)?,
                    next_action: text("blocker.next_action", &b.next_action)?,
                });
                c.resume_state = Some(from);
                notify.extend(self.managers_except(by));
                notify.extend(c.owner.iter().cloned());
                notify.insert(b.owner.clone());
                "blocked"
            }
            (Blocked, back) if Some(back) == c.resume_state => {
                let blocker_owner = c.blocker.as_ref().map(|b| b.owner.as_str()) == Some(by);
                if !owner_or_manager && !blocker_owner {
                    return Err(refuse(
                        ErrorCode::NotPermitted,
                        "only the owner, a manager or the blocker's owner can unblock",
                    ));
                }
                c.blocker = None;
                c.resume_state = None;
                notify.extend(c.owner.iter().cloned());
                "unblocked"
            }
            (s, Archived) if !s.is_terminal() => {
                self.need_manager(by, "archive a card")?;
                let r = reason.as_deref().map(|r| text("reason", r)).transpose()?;
                let Some(r) = r else {
                    return Err(not_met("give a reason".into()));
                };
                c.archive_reason = Some(r);
                notify.extend(c.owner.iter().cloned());
                "archived"
            }
            _ => {
                let mut allowed = allowed_next(from);
                if let (Blocked, Some(back)) = (from, c.resume_state) {
                    allowed.insert(0, back);
                }
                return Err(refuse(ErrorCode::TransitionRefused, format!("a {from} card cannot move to {to}"))
                    .with_detail(json!({ "from": from, "to": to, "allowed": allowed })));
            }
        };
        c.state = to;
        c.updated_at_ms = now;
        self.changed(c, action, notify, by)
    }

    #[allow(clippy::too_many_arguments)]
    fn evidence(
        &mut self,
        by: &str,
        id: &str,
        kind: String,
        reference: String,
        note: Option<String>,
        gate: Option<String>,
        pass: Option<bool>,
        now: u64,
    ) -> KiteResult<Applied> {
        let mut c = self.card(id)?.clone();
        if c.state.is_terminal() {
            return Err(refuse(ErrorCode::TransitionRefused, format!("card {id} is {}", c.state)));
        }
        let mut notify = BTreeSet::new();
        let action = match (&gate, pass) {
            (None, None) => "evidence",
            (None, Some(_)) => return Err(refuse(ErrorCode::InvalidArgument, "pass only applies to a gate result")),
            (Some(_), None) => {
                return Err(refuse(ErrorCode::InvalidArgument, "a gate result needs pass true or false"));
            }
            (Some(g), Some(p)) => {
                let Some(idx) = c.gates.iter().position(|x| x == g) else {
                    return Err(refuse(ErrorCode::InvalidArgument, format!("card {id} has no gate {g}"))
                        .with_detail(json!({ "gates": c.gates })));
                };
                if c.state != CardState::Review {
                    return Err(refuse(
                        ErrorCode::TransitionRefused,
                        format!("gates are recorded in review; card {id} is {}", c.state),
                    ));
                }
                if p && c.gates_passed() < idx {
                    let first = &c.gates[c.gates_passed()];
                    return Err(refuse(ErrorCode::TransitionRefused, format!("gate {first} must pass before {g}"))
                        .with_detail(json!({ "gates": c.gates, "passed": c.gates_passed() })));
                }
                notify.extend(c.owner.iter().cloned());
                if !p {
                    notify.extend(self.managers_except(by));
                }
                if p { "gate_passed" } else { "gate_failed" }
            }
        };
        c.evidence.push(Evidence {
            kind: text("kind", &kind)?,
            reference: text("reference", &reference)?,
            note: note.as_deref().map(|n| text("note", n)).transpose()?,
            gate,
            pass,
            by: by.to_string(),
            at_ms: now,
            revision: c.revision,
        });
        c.updated_at_ms = now;
        self.changed(c, action, notify, by)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::cell::RefCell;

    /// A project that is not a git repository: the board checks nothing in git.
    struct NoGit;

    impl Vcs for NoGit {
        fn is_repo(&self) -> bool {
            false
        }
        fn checkout_worktree(&self, _: &Path, _: &str) -> Result<(), String> {
            unreachable!("not a repository")
        }
        fn ensure_branch(&self, _: &str) -> Result<(), String> {
            unreachable!("not a repository")
        }
        fn resolve_commit(&self, _: &str) -> Result<String, String> {
            unreachable!("not a repository")
        }
        fn reachable_from(&self, _: &str, _: &str) -> Result<bool, String> {
            unreachable!("not a repository")
        }
    }

    /// A git repository in memory: branches, worktrees and which commits are on the mainline.
    #[derive(Default)]
    struct FakeGit {
        branches: RefCell<BTreeSet<String>>,
        worktrees: RefCell<BTreeMap<PathBuf, String>>,
        /// ref → commit id
        refs: BTreeMap<String, String>,
        /// (commit, rev) pairs where the commit is reachable from rev.
        reachable: BTreeSet<(String, String)>,
        fail: Option<String>,
    }

    impl Vcs for FakeGit {
        fn is_repo(&self) -> bool {
            true
        }
        fn checkout_worktree(&self, dir: &Path, branch: &str) -> Result<(), String> {
            if let Some(e) = &self.fail {
                return Err(e.clone());
            }
            self.branches.borrow_mut().insert(branch.to_string());
            self.worktrees.borrow_mut().insert(dir.to_path_buf(), branch.to_string());
            Ok(())
        }
        fn ensure_branch(&self, branch: &str) -> Result<(), String> {
            if let Some(e) = &self.fail {
                return Err(e.clone());
            }
            self.branches.borrow_mut().insert(branch.to_string());
            Ok(())
        }
        fn resolve_commit(&self, rev: &str) -> Result<String, String> {
            self.refs.get(rev).cloned().ok_or_else(|| format!("{rev} does not name a commit"))
        }
        fn reachable_from(&self, commit: &str, rev: &str) -> Result<bool, String> {
            self.resolve_commit(rev)?;
            Ok(self.reachable.contains(&(commit.to_string(), rev.to_string())))
        }
    }

    impl Board {
        fn act(&mut self, by: &str, op: CardOp, now: u64) -> KiteResult<Applied> {
            self.apply(by, op, now, &NoGit)
        }
    }

    struct T {
        board: Board,
        dir: tempfile::TempDir,
    }

    fn setup(managers: &[&str]) -> T {
        let dir = tempfile::tempdir().unwrap();
        let settings = BoardSettings {
            managers: managers.iter().map(|s| s.to_string()).collect(),
            default_gates: vec!["tests".into(), "diff_review".into()],
            independent_review: true,
            mainline: "main".into(),
        };
        let agents = ["lead", "coder", "coder2", "reviewer"].iter().map(|s| s.to_string()).collect();
        T { board: Board::new(settings, agents, dir.path().to_path_buf(), BTreeMap::new()), dir }
    }

    fn changed(r: KiteResult<Applied>) -> (Card, &'static str, BTreeSet<String>) {
        match r.unwrap() {
            Applied::Changed { card, action, notify } => (*card, action, notify),
            Applied::Read(_) => panic!("expected a change"),
        }
    }

    fn code<T>(r: KiteResult<T>) -> ErrorCode {
        match r {
            Err(e) => e.code,
            Ok(_) => panic!("expected an error"),
        }
    }

    fn msg<T>(r: KiteResult<T>) -> String {
        match r {
            Err(e) => e.message,
            Ok(_) => panic!("expected an error"),
        }
    }

    fn mv_id(id: &str, to: CardState) -> CardOp {
        CardOp::Move { id: id.into(), to, blocker: None, reason: None, merge_ref: None }
    }

    fn mv(to: CardState) -> CardOp {
        mv_id("card-001", to)
    }

    fn ev(gate: Option<&str>, pass: Option<bool>) -> CardOp {
        CardOp::Evidence {
            id: "card-001".into(),
            kind: "test".into(),
            reference: "cargo test".into(),
            note: None,
            gate: gate.map(str::to_string),
            pass,
        }
    }

    fn patch(p: CardPatch) -> CardOp {
        CardOp::Update { id: "card-001".into(), patch: p }
    }

    fn shaped(title: &str) -> CardDraft {
        CardDraft {
            title: title.into(),
            acceptance: vec!["flag parses".into()],
            merge_gate: Some("tests and review pass".into()),
            ..Default::default()
        }
    }

    /// Creates card-001 and takes it to running, owned by coder in worktree `wt/a`.
    fn running(t: &mut T) {
        let b = &mut t.board;
        changed(b.act("lead", CardOp::Create { card: shaped("Add a flag") }, 1));
        changed(b.act("lead", mv(CardState::Ready), 2));
        changed(b.act("lead", patch(CardPatch { owner: Some("coder".into()), ..Default::default() }), 3));
        changed(b.act("lead", patch(CardPatch { worktree: Some("./wt/a/".into()), ..Default::default() }), 3));
        changed(b.act("coder", mv(CardState::Running), 4));
    }

    fn to_review(t: &mut T) {
        std::fs::create_dir_all(t.dir.path().join("wt/a")).unwrap();
        std::fs::write(t.dir.path().join("wt/a/HANDOFF.md"), "done").unwrap();
        let b = &mut t.board;
        changed(b.act("coder", patch(CardPatch { handoff: Some("HANDOFF.md".into()), ..Default::default() }), 5));
        changed(b.act("coder", ev(None, None), 5));
        changed(b.act("coder", mv(CardState::Review), 6));
    }

    #[test]
    fn a_card_needs_shaping_before_it_is_ready() {
        let mut t = setup(&["lead"]);
        let b = &mut t.board;
        let draft = CardDraft { title: "x".into(), ..Default::default() };
        let (c, _, _) = changed(b.act("coder", CardOp::Create { card: draft }, 1));
        assert_eq!((c.id.as_str(), c.state), ("card-001", CardState::Backlog));
        assert_eq!(code(b.act("coder", mv(CardState::Ready), 2)), ErrorCode::NotPermitted);
        assert!(msg(b.act("lead", mv(CardState::Ready), 2)).contains("acceptance"));
        changed(b.act("lead", patch(CardPatch { acceptance: Some(vec!["a".into()]), ..Default::default() }), 3));
        assert!(msg(b.act("lead", mv(CardState::Ready), 4)).contains("merge gate"));
        changed(b.act("lead", patch(CardPatch { merge_gate: Some("tests pass".into()), ..Default::default() }), 5));
        let (c, action, _) = changed(b.act("lead", mv(CardState::Ready), 6));
        assert_eq!((c.state, action), (CardState::Ready, "shaped"));
    }

    #[test]
    fn only_the_assigned_owner_starts_and_needs_a_branch_or_worktree() {
        let mut t = setup(&["lead"]);
        let b = &mut t.board;
        let draft = CardDraft { owner: Some("coder".into()), ..shaped("x") };
        let (_, _, notify) = changed(b.act("lead", CardOp::Create { card: draft }, 1));
        assert_eq!(notify, BTreeSet::from(["coder".to_string()]));
        changed(b.act("lead", mv(CardState::Ready), 2));
        assert_eq!(code(b.act("coder2", mv(CardState::Running), 3)), ErrorCode::NotPermitted);
        assert!(msg(b.act("coder", mv(CardState::Running), 3)).contains("branch or worktree"));
        changed(b.act("coder", patch(CardPatch { branch: Some("feat/x".into()), ..Default::default() }), 4));
        let (c, _, notify) = changed(b.act("coder", mv(CardState::Running), 5));
        assert_eq!(c.state, CardState::Running);
        assert_eq!(notify, BTreeSet::from(["lead".to_string()]));
        // A running card keeps its owner and branch until it leaves running.
        let e = b.act("lead", patch(CardPatch { owner: Some("coder2".into()), ..Default::default() }), 6);
        assert_eq!(code(e), ErrorCode::TransitionRefused);
    }

    #[test]
    fn two_active_cards_cannot_share_a_worktree() {
        let mut t = setup(&[]);
        running(&mut t);
        let b = &mut t.board;
        let draft = CardDraft {
            id: Some("other".into()),
            owner: Some("coder2".into()),
            worktree: Some("wt/a".into()),
            ..shaped("y")
        };
        changed(b.act("lead", CardOp::Create { card: draft }, 1));
        changed(b.act("lead", mv_id("other", CardState::Ready), 2));
        let e = b.act("coder2", mv_id("other", CardState::Running), 3).err().unwrap();
        assert_eq!(e.code, ErrorCode::CardConflict);
        assert_eq!(e.detail.unwrap()["card"], "card-001");
        let own = CardPatch { worktree: Some("wt/b".into()), ..Default::default() };
        changed(b.act("coder2", CardOp::Update { id: "other".into(), patch: own }, 4));
        changed(b.act("coder2", mv_id("other", CardState::Running), 5));
    }

    #[test]
    fn in_a_git_project_starting_adds_the_worktree_or_creates_the_branch() {
        let mut t = setup(&["lead"]);
        let root = t.dir.path().to_path_buf();
        let b = &mut t.board;
        let draft = CardDraft { owner: Some("coder".into()), worktree: Some("wt/a".into()), ..shaped("x") };
        changed(b.act("lead", CardOp::Create { card: draft }, 1));
        changed(b.act("lead", mv(CardState::Ready), 2));
        let git = FakeGit::default();
        let e = msg(b.apply("coder", mv(CardState::Running), 3, &git));
        assert!(e.contains("set a branch for worktree wt/a"), "{e}");
        changed(b.act("coder", patch(CardPatch { branch: Some("feat/a".into()), ..Default::default() }), 4));

        // A git failure refuses the move, says why, and leaves the card ready.
        let broken = FakeGit { fail: Some("fatal: 'feat/a' is already checked out".into()), ..Default::default() };
        let e = b.apply("coder", mv(CardState::Running), 5, &broken).err().unwrap();
        assert_eq!(e.code, ErrorCode::TransitionRefused);
        assert!(e.message.contains("cannot set up worktree wt/a on branch feat/a: fatal:"), "{}", e.message);
        assert!(e.detail.unwrap()["git"].as_str().unwrap().contains("already checked out"));
        assert_eq!(b.card("card-001").unwrap().state, CardState::Ready);

        let (c, _, _) = changed(b.apply("coder", mv(CardState::Running), 6, &git));
        assert_eq!(c.state, CardState::Running);
        assert_eq!(git.worktrees.borrow().get(&root.join("wt/a")).map(String::as_str), Some("feat/a"));

        // Without a worktree the branch must exist or be created.
        let draft = CardDraft {
            id: Some("solo".into()),
            owner: Some("coder2".into()),
            branch: Some("feat/b".into()),
            ..shaped("y")
        };
        changed(b.act("lead", CardOp::Create { card: draft }, 7));
        changed(b.act("lead", mv_id("solo", CardState::Ready), 8));
        let e = msg(b.apply("coder2", mv_id("solo", CardState::Running), 9, &broken));
        assert!(e.contains("cannot create branch feat/b"), "{e}");
        changed(b.apply("coder2", mv_id("solo", CardState::Running), 10, &git));
        assert!(git.branches.borrow().contains("feat/b"));
        assert_eq!(git.worktrees.borrow().len(), 1);
    }

    #[test]
    fn in_a_git_project_merge_ref_must_be_a_commit_on_the_mainline() {
        let mut t = setup(&["lead"]);
        running(&mut t);
        to_review(&mut t);
        let b = &mut t.board;
        changed(b.act("reviewer", ev(Some("tests"), Some(true)), 7));
        changed(b.act("reviewer", ev(Some("diff_review"), Some(true)), 7));
        let merge = |r: &str| CardOp::Move {
            id: "card-001".into(),
            to: CardState::Merged,
            blocker: None,
            reason: None,
            merge_ref: Some(r.into()),
        };
        let mut git =
            FakeGit { refs: BTreeMap::from([("feat/a".to_string(), "c1".to_string())]), ..Default::default() };
        let e = msg(b.apply("lead", merge("nope"), 8, &git));
        assert!(e.contains("merge_ref nope is not a commit in this repository"), "{e}");
        let e = msg(b.apply("lead", merge("feat/a"), 8, &git));
        assert!(e.contains("cannot read mainline main"), "{e}");
        git.refs.insert("main".into(), "m1".into());
        let e = b.apply("lead", merge("feat/a"), 9, &git).err().unwrap();
        assert!(
            e.message.contains("feat/a (c1) is not reachable from main; merge it into main first"),
            "{}",
            e.message
        );
        assert_eq!(e.detail.unwrap()["commit"], "c1");
        git.reachable.insert(("c1".into(), "main".into()));
        let (c, _, _) = changed(b.apply("lead", merge("feat/a"), 10, &git));
        // The resolved commit is kept, not the ref, which may move later.
        assert_eq!((c.state, c.merge_ref.as_deref()), (CardState::Merged, Some("c1")));
    }

    #[test]
    fn review_needs_an_existing_handoff_and_evidence() {
        let mut t = setup(&["lead"]);
        running(&mut t);
        let b = &mut t.board;
        assert!(msg(b.act("coder", mv(CardState::Review), 5)).contains("handoff"));
        changed(b.act("coder", patch(CardPatch { handoff: Some("HANDOFF.md".into()), ..Default::default() }), 5));
        assert!(msg(b.act("coder", mv(CardState::Review), 5)).contains("does not exist"));
        std::fs::create_dir_all(t.dir.path().join("wt/a")).unwrap();
        std::fs::write(t.dir.path().join("wt/a/HANDOFF.md"), "notes").unwrap();
        let b = &mut t.board;
        assert!(msg(b.act("coder", mv(CardState::Review), 6)).contains("evidence"));
        changed(b.act("coder", ev(None, None), 6));
        assert_eq!(code(b.act("lead", mv(CardState::Review), 7)), ErrorCode::NotPermitted);
        let (c, action, notify) = changed(b.act("coder", mv(CardState::Review), 7));
        assert_eq!((c.state, action), (CardState::Review, "submitted"));
        assert_eq!(notify, BTreeSet::from(["lead".to_string()]));
        let bad = CardPatch { handoff: Some("../escape.md".into()), ..Default::default() };
        assert_eq!(code(b.act("coder", patch(bad), 8)), ErrorCode::InvalidArgument);
    }

    #[test]
    fn gates_pass_in_order_and_merge_needs_an_independent_pass() {
        let mut t = setup(&["lead"]);
        running(&mut t);
        to_review(&mut t);
        let b = &mut t.board;
        let e = msg(b.act("reviewer", ev(Some("diff_review"), Some(true)), 7));
        assert!(e.contains("tests must pass before diff_review"), "{e}");
        changed(b.act("coder", ev(Some("tests"), Some(true)), 7));
        let merge = |r: Option<&str>| CardOp::Move {
            id: "card-001".into(),
            to: CardState::Merged,
            blocker: None,
            reason: None,
            merge_ref: r.map(str::to_string),
        };
        assert!(msg(b.act("lead", merge(Some("abc123")), 8)).contains("diff_review"));
        changed(b.act("coder", ev(Some("diff_review"), Some(true)), 8));
        // Both gates passed, but only by the owner.
        assert!(msg(b.act("lead", merge(Some("abc123")), 9)).contains("someone other than coder"));
        let (_, action, notify) = changed(b.act("reviewer", ev(Some("diff_review"), Some(true)), 9));
        assert_eq!(action, "gate_passed");
        assert_eq!(notify, BTreeSet::from(["coder".to_string()]));
        assert!(msg(b.act("lead", merge(None), 10)).contains("merge_ref"));
        assert_eq!(code(b.act("coder", merge(Some("abc123")), 10)), ErrorCode::NotPermitted);
        let (c, _, _) = changed(b.act("lead", merge(Some("abc123")), 10));
        assert_eq!((c.state, c.merge_ref.as_deref()), (CardState::Merged, Some("abc123")));
        assert_eq!(code(b.act("lead", mv(CardState::Running), 11)), ErrorCode::TransitionRefused);
        assert_eq!(code(b.act("coder", ev(None, None), 11)), ErrorCode::TransitionRefused);
    }

    #[test]
    fn reopening_starts_a_new_revision_and_clears_gate_results() {
        let mut t = setup(&["lead"]);
        running(&mut t);
        to_review(&mut t);
        let b = &mut t.board;
        changed(b.act("reviewer", ev(Some("tests"), Some(true)), 7));
        let (_, action, notify) = changed(b.act("reviewer", ev(Some("diff_review"), Some(false)), 8));
        assert_eq!(action, "gate_failed");
        assert_eq!(notify, BTreeSet::from(["coder".to_string(), "lead".to_string()]));
        let (c, _, _) = changed(b.act("coder", mv(CardState::Running), 9));
        assert_eq!((c.revision, c.gates_passed()), (2, 0));
        // Revision 2 needs its own evidence before review.
        assert!(msg(b.act("coder", mv(CardState::Review), 10)).contains("evidence"));
    }

    #[test]
    fn blocking_needs_a_full_blocker_and_returns_where_it_came_from() {
        let mut t = setup(&["lead"]);
        running(&mut t);
        let b = &mut t.board;
        assert!(msg(b.act("coder", mv(CardState::Blocked), 5)).contains("blocker"));
        let block = CardOp::Move {
            id: "card-001".into(),
            to: CardState::Blocked,
            blocker: Some(Blocker {
                reason: "needs API key".into(),
                owner: "lead".into(),
                next_action: "add the key".into(),
            }),
            reason: None,
            merge_ref: None,
        };
        let (c, _, _) = changed(b.act("coder", block, 6));
        assert_eq!((c.state, c.resume_state), (CardState::Blocked, Some(CardState::Running)));
        let e = b.act("coder", mv(CardState::Review), 7).err().unwrap();
        assert_eq!(e.detail.unwrap()["allowed"][0], "running");
        let (c, action, _) = changed(b.act("lead", mv(CardState::Running), 8));
        assert_eq!((c.state, action, c.blocker), (CardState::Running, "unblocked", None));
    }

    #[test]
    fn archiving_needs_a_manager_and_a_reason() {
        let mut t = setup(&["lead"]);
        running(&mut t);
        let b = &mut t.board;
        let archive = |r: Option<&str>| CardOp::Move {
            id: "card-001".into(),
            to: CardState::Archived,
            blocker: None,
            reason: r.map(str::to_string),
            merge_ref: None,
        };
        assert_eq!(code(b.act("coder", archive(Some("dup")), 5)), ErrorCode::NotPermitted);
        assert!(msg(b.act("lead", archive(None), 5)).contains("reason"));
        let (c, _, _) = changed(b.act("lead", archive(Some("superseded by card-002")), 6));
        assert_eq!(c.archive_reason.as_deref(), Some("superseded by card-002"));
    }

    #[test]
    fn replay_keeps_the_last_snapshot_and_numbering_continues() {
        let mut t = setup(&[]);
        running(&mut t);
        let snapshot = serde_json::to_value(t.board.cards().next().unwrap()).unwrap();
        let events = vec![json!({"type": "note"}), json!({"type": "card", "action": "started", "card": snapshot})];
        let cards = replay(events);
        assert_eq!(cards["card-001"].state, CardState::Running);
        let agents = BTreeSet::from(["lead".to_string()]);
        let mut b = Board::new(BoardSettings::default(), agents, PathBuf::from("/"), cards);
        let draft = CardDraft { title: "next".into(), ..Default::default() };
        let (c, _, _) = changed(b.act("lead", CardOp::Create { card: draft }, 1));
        assert_eq!(c.id, "card-002");
    }
}
