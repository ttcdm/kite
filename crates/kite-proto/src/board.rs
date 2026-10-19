//! Agent Kanban: work items (cards) with an owner, a scope, acceptance
//! criteria and a merge gate. The broker owns the board and refuses any move
//! whose exit criteria are not met.

use serde::{Deserialize, Serialize};
use std::fmt;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CardState {
    /// Proposed; not yet shaped.
    Backlog,
    /// Shaped (acceptance and merge gate written) and assignable.
    Ready,
    /// The owner is working on it in its branch or worktree.
    Running,
    /// Handed off; waiting on its gates.
    Review,
    /// Waiting on someone; the blocker names who and what next.
    Blocked,
    /// Integrated; `merge_ref` says where.
    Merged,
    /// Dropped; `archive_reason` says why.
    Archived,
}

impl CardState {
    pub const ALL: [CardState; 7] = [
        CardState::Backlog,
        CardState::Ready,
        CardState::Running,
        CardState::Review,
        CardState::Blocked,
        CardState::Merged,
        CardState::Archived,
    ];

    pub fn as_str(self) -> &'static str {
        match self {
            CardState::Backlog => "backlog",
            CardState::Ready => "ready",
            CardState::Running => "running",
            CardState::Review => "review",
            CardState::Blocked => "blocked",
            CardState::Merged => "merged",
            CardState::Archived => "archived",
        }
    }

    pub fn is_terminal(self) -> bool {
        matches!(self, CardState::Merged | CardState::Archived)
    }
}

impl fmt::Display for CardState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl std::str::FromStr for CardState {
    type Err = String;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        CardState::ALL.into_iter().find(|c| c.as_str() == s).ok_or_else(|| {
            format!("unknown card state {s:?}; one of backlog, ready, running, review, blocked, merged, archived")
        })
    }
}

/// Why a card is blocked, who can unblock it, and what they need to do.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Blocker {
    pub reason: String,
    pub owner: String,
    pub next_action: String,
}

/// A piece of evidence: a test run, diff, screenshot, log, review note or
/// eval. With `gate` set it is that gate's result for the card's current
/// revision.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Evidence {
    pub kind: String,
    /// Where it is: a path, URL, commit or command.
    pub reference: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub note: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub gate: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pass: Option<bool>,
    pub by: String,
    pub at_ms: u64,
    pub revision: u32,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Card {
    pub id: String,
    pub title: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub owner: Option<String>,
    pub state: CardState,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub branch: Option<String>,
    /// Directory the owner works in, relative to the project root. Two active
    /// cards may not share one.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub worktree: Option<String>,
    pub acceptance: Vec<String>,
    /// The condition for merging, in words.
    pub merge_gate: String,
    /// Named checks that must pass, in this order, before the card can merge.
    pub gates: Vec<String>,
    /// Path to the owner's handoff note, relative to the worktree.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub handoff: Option<String>,
    pub evidence: Vec<Evidence>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub blocker: Option<Blocker>,
    /// The state a blocked card returns to.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub resume_state: Option<CardState>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub merge_ref: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub archive_reason: Option<String>,
    /// Goes up each time review sends the card back; gate results count only for the current one.
    pub revision: u32,
    pub created_by: String,
    pub created_at_ms: u64,
    pub updated_at_ms: u64,
}

impl Card {
    /// The latest result of `gate` in the current revision.
    pub fn gate_result(&self, gate: &str) -> Option<&Evidence> {
        self.evidence.iter().rev().find(|e| e.revision == self.revision && e.gate.as_deref() == Some(gate))
    }

    /// Gates passed in the current revision, counting only an unbroken run from the first.
    pub fn gates_passed(&self) -> usize {
        self.gates.iter().take_while(|g| self.gate_result(g).and_then(|e| e.pass) == Some(true)).count()
    }
}

#[derive(Debug, Clone, PartialEq, Default, Serialize, Deserialize)]
pub struct CardDraft {
    /// Optional; `card-NNN` is assigned otherwise.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub id: Option<String>,
    pub title: String,
    #[serde(default)]
    pub acceptance: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub merge_gate: Option<String>,
    /// Defaults to `[board] default_gates`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub gates: Option<Vec<String>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub owner: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub branch: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub worktree: Option<String>,
}

/// Fields to change; absent fields stay as they are.
#[derive(Debug, Clone, PartialEq, Default, Serialize, Deserialize)]
pub struct CardPatch {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub title: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub acceptance: Option<Vec<String>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub merge_gate: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub gates: Option<Vec<String>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub owner: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub branch: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub worktree: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub handoff: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "action", rename_all = "snake_case")]
pub enum CardOp {
    Create {
        card: CardDraft,
    },
    Update {
        id: String,
        patch: CardPatch,
    },
    Move {
        id: String,
        to: CardState,
        /// Required when moving to blocked.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        blocker: Option<Blocker>,
        /// Required when moving to archived.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        reason: Option<String>,
        /// Required when moving to merged: the commit or PR it landed in.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        merge_ref: Option<String>,
    },
    Evidence {
        id: String,
        kind: String,
        reference: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        note: Option<String>,
        /// Record this as the result of a gate.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        gate: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        pass: Option<bool>,
    },
    /// Read-only; allowed without registering.
    Get {
        id: String,
    },
    /// Read-only; allowed without registering.
    List {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        state: Option<CardState>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        owner: Option<String>,
    },
}

impl CardOp {
    pub fn is_read_only(&self) -> bool {
        matches!(self, CardOp::Get { .. } | CardOp::List { .. })
    }
}

/// Built-in schema of the event sent to a card's owner and managers when it changes.
pub const CARD_SCHEMA: &str = "kite.card@1";
