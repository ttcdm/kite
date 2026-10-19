//! Human-readable output for `kite status` and `kite trace`.

use kite_core::ledger::now_ms;
use kite_proto::board::{Card, CardState};
use kite_proto::wire::{
    AgentDetail, AgentStatus, MessageSummary, ProcessState, ProcessStatus, RequestSummary, StatusOk,
};
use serde_json::Value;
use std::fmt::Write;

fn table(rows: &[Vec<String>]) -> String {
    let cols = rows.iter().map(Vec::len).max().unwrap_or(0);
    let widths: Vec<usize> =
        (0..cols).map(|c| rows.iter().filter_map(|r| r.get(c)).map(|s| s.chars().count()).max().unwrap_or(0)).collect();
    let mut out = String::new();
    for r in rows {
        let line: Vec<String> = r.iter().enumerate().map(|(i, s)| format!("{s:<w$}", w = widths[i])).collect();
        let _ = writeln!(out, "{}", line.join("  ").trim_end());
    }
    out
}

fn process(p: &Option<ProcessStatus>) -> String {
    let Some(p) = p else { return "-".into() };
    let state = match p.state {
        ProcessState::Starting => "starting",
        ProcessState::Running => "running",
        ProcessState::Restarting => "restarting",
        ProcessState::Exited => "exited",
        ProcessState::GaveUp => "gave up",
    };
    let mut s = state.to_string();
    if let Some(pid) = p.pid {
        let _ = write!(s, " pid {pid}");
    }
    if p.restarts > 0 {
        let _ = write!(s, ", {} restarts", p.restarts);
    }
    if let Some(code) = p.last_exit {
        let _ = write!(s, ", last exit {code}");
    }
    s
}

pub fn status(s: &StatusOk) -> String {
    let mut rows = vec![
        ["AGENT", "CONNECTED", "LAST SEEN", "MAILBOX", "IN-FLIGHT", "AWAITING", "OWES", "LEASES", "CARDS", "PROCESS"]
            .iter()
            .map(|h| h.to_string())
            .collect::<Vec<_>>(),
    ];
    for a in &s.agents {
        rows.push(vec![
            a.name.clone(),
            if a.connected { "yes".into() } else { "no".into() },
            ago(a.last_seen_ms),
            a.mailbox.to_string(),
            a.in_flight.to_string(),
            a.awaiting.to_string(),
            a.owes.to_string(),
            a.leases.to_string(),
            a.cards.to_string(),
            process(&a.process),
        ]);
    }
    let mut out = table(&rows);
    if !s.waits.is_empty() {
        let waits: Vec<String> = s.waits.iter().map(|(a, b)| format!("{a} -> {b}")).collect();
        let _ = writeln!(out, "\nblocking waits: {}", waits.join(", "));
    }
    if let Some(d) = &s.detail {
        out.push('\n');
        out.push_str(&detail(d));
    }
    out
}

fn messages(title: &str, ms: &[MessageSummary], out: &mut String) {
    let _ = writeln!(out, "{title} ({})", ms.len());
    for m in ms {
        let again = if m.redelivered > 0 { format!("  redelivered {}x", m.redelivered) } else { String::new() };
        let _ = writeln!(out, "  {}  {:?} {} from {}  lc {}{again}", m.id, m.kind, m.schema, m.from, m.lc);
    }
}

fn requests(title: &str, rs: &[RequestSummary], peer_is_to: bool, out: &mut String) {
    let _ = writeln!(out, "{title} ({})", rs.len());
    for r in rs {
        let peer = if peer_is_to { format!("to {}", r.to) } else { format!("from {}", r.from) };
        let blocking = if r.blocking { "  blocking" } else { "" };
        let _ = writeln!(out, "  {}  {} {peer}  deadline {}{blocking}", r.id, r.schema, clock(r.deadline_ms));
    }
}

pub fn detail(d: &AgentDetail) -> String {
    let mut out = String::new();
    let _ = writeln!(out, "== {} ==", d.name);
    if let Some(desc) = &d.description {
        let _ = writeln!(out, "{desc}");
    }
    let waiting = match (d.in_recv, d.in_await.len()) {
        (false, 0) => "not waiting".to_string(),
        (true, 0) => "waiting in recv".to_string(),
        (r, n) => format!("{}awaiting {n} answer(s)", if r { "in recv, " } else { "" }),
    };
    let _ = writeln!(
        out,
        "connected: {}   {waiting}   process: {}",
        if d.connected { "yes" } else { "no" },
        process(&d.process)
    );
    messages("mailbox", &d.mailbox, &mut out);
    messages("in flight (handed out, not acknowledged)", &d.in_flight, &mut out);
    requests("awaiting answers", &d.awaiting, true, &mut out);
    requests("owes answers", &d.owes, false, &mut out);
    let _ = writeln!(out, "leases ({})", d.leases.len());
    for l in &d.leases {
        let _ = writeln!(out, "  {}  until {}", l.resource, clock(l.expires_at_ms));
    }
    out
}

/// How long ago a Unix-ms time was, in the largest whole unit.
pub fn ago(ms: Option<u64>) -> String {
    let Some(ms) = ms else { return "never".into() };
    let s = now_ms().saturating_sub(ms) / 1000;
    match s {
        0..60 => format!("{s}s ago"),
        60..3600 => format!("{}m ago", s / 60),
        3600..86400 => format!("{}h ago", s / 3600),
        _ => format!("{}d ago", s / 86400),
    }
}

fn gate_marks(c: &Card) -> String {
    c.gates
        .iter()
        .map(|g| match c.gate_result(g) {
            Some(e) if e.pass == Some(true) => format!("{g} ok ({})", e.by),
            Some(e) => format!("{g} FAILED ({})", e.by),
            None => format!("{g} -"),
        })
        .collect::<Vec<_>>()
        .join(", ")
}

fn place(c: &Card) -> String {
    match (&c.worktree, &c.branch) {
        (Some(w), Some(b)) => format!("{w} @ {b}"),
        (Some(w), None) => w.clone(),
        (None, Some(b)) => format!("@ {b}"),
        (None, None) => "-".into(),
    }
}

/// The control pane: cards by column, who owns what and when they were last
/// seen, blockers by owner, and how close each card in review is to merging.
pub fn board(cards: &[Card], agents: &[AgentStatus], all: bool) -> String {
    let seen = |owner: &Option<String>| {
        owner.as_ref().and_then(|o| agents.iter().find(|a| &a.name == o)).map(|a| ago(a.last_seen_ms))
    };
    let mut out = String::new();
    for state in CardState::ALL {
        let col: Vec<&Card> = cards.iter().filter(|c| c.state == state).collect();
        if state.is_terminal() && !all {
            let _ = writeln!(out, "{} ({})  [--all to list]", state.as_str().to_uppercase(), col.len());
            continue;
        }
        let _ = writeln!(out, "{} ({})", state.as_str().to_uppercase(), col.len());
        let rows: Vec<Vec<String>> = col
            .iter()
            .map(|c| {
                vec![
                    format!("  {}", c.id),
                    c.title.clone(),
                    c.owner.clone().unwrap_or_else(|| "unowned".into()),
                    seen(&c.owner).map(|s| format!("seen {s}")).unwrap_or_default(),
                    place(c),
                    match state {
                        CardState::Review => format!("gates {}/{}", c.gates_passed(), c.gates.len()),
                        CardState::Merged => c.merge_ref.clone().unwrap_or_default(),
                        CardState::Archived => c.archive_reason.clone().unwrap_or_default(),
                        _ => String::new(),
                    },
                ]
            })
            .collect();
        out.push_str(&table(&rows));
    }
    let blocked: Vec<&Card> = cards.iter().filter(|c| c.state == CardState::Blocked).collect();
    if !blocked.is_empty() {
        let _ = writeln!(out, "\nBLOCKERS BY OWNER");
        let mut owners: Vec<&str> =
            blocked.iter().filter_map(|c| c.blocker.as_ref().map(|b| b.owner.as_str())).collect();
        owners.sort();
        owners.dedup();
        for o in owners {
            let _ = writeln!(out, "  {o}");
            for c in blocked.iter().filter(|c| c.blocker.as_ref().is_some_and(|b| b.owner == o)) {
                let b = c.blocker.as_ref().expect("filtered on blocker");
                let _ = writeln!(out, "    {}  {} -> next: {}", c.id, b.reason, b.next_action);
            }
        }
    }
    let review: Vec<&Card> = cards.iter().filter(|c| c.state == CardState::Review).collect();
    if !review.is_empty() {
        let _ = writeln!(out, "\nMERGE READINESS");
        for c in review {
            let ready = c.gates_passed() == c.gates.len();
            let _ = writeln!(
                out,
                "  {}  {}  [{}]  rev {}  handoff {}",
                c.id,
                if ready { "all gates passed" } else { "waiting" },
                gate_marks(c),
                c.revision,
                c.handoff.as_deref().unwrap_or("-")
            );
        }
    }
    out
}

/// Everything on one card.
pub fn card(c: &Card) -> String {
    let mut out = String::new();
    let _ = writeln!(out, "{}  {}  [{}]  rev {}", c.id, c.title, c.state, c.revision);
    let _ = writeln!(out, "owner: {}   where: {}", c.owner.as_deref().unwrap_or("unowned"), place(c));
    let _ = writeln!(out, "merge gate: {}", if c.merge_gate.is_empty() { "-" } else { &c.merge_gate });
    let _ = writeln!(out, "gates: {}", gate_marks(c));
    let _ = writeln!(out, "acceptance:");
    for a in &c.acceptance {
        let _ = writeln!(out, "  - {a}");
    }
    if let Some(h) = &c.handoff {
        let _ = writeln!(out, "handoff: {h}");
    }
    if let Some(b) = &c.blocker {
        let _ = writeln!(out, "blocked: {} (owner {}, next: {})", b.reason, b.owner, b.next_action);
    }
    if let Some(m) = &c.merge_ref {
        let _ = writeln!(out, "merged in: {m}");
    }
    if let Some(r) = &c.archive_reason {
        let _ = writeln!(out, "archived: {r}");
    }
    let _ = writeln!(out, "evidence ({}):", c.evidence.len());
    for e in &c.evidence {
        let gate = match (&e.gate, e.pass) {
            (Some(g), Some(true)) => format!(" gate {g} passed"),
            (Some(g), _) => format!(" gate {g} failed"),
            _ => String::new(),
        };
        let note = e.note.as_deref().map(|n| format!(" ({n})")).unwrap_or_default();
        let _ = writeln!(out, "  rev {} {} {}: {}{gate}{note}", e.revision, e.by, e.kind, e.reference);
    }
    out
}

/// `HH:MM:SS.mmm` UTC.
pub fn clock(ms: u64) -> String {
    let s = ms / 1000;
    format!("{:02}:{:02}:{:02}.{:03}Z", (s / 3600) % 24, (s / 60) % 60, s % 60, ms % 1000)
}

/// Whether a ledger event concerns `agent`.
pub fn mentions(event: &Value, agent: &str) -> bool {
    let is = |v: &Value| v.as_str() == Some(agent);
    is(&event["agent"]) || is(&event["envelope"]["from"]) || is(&event["envelope"]["to"]) || is(&event["holder"])
}

pub fn trace_line(rec: &Value) -> String {
    let e = &rec["event"];
    let ty = e["type"].as_str().unwrap_or("?");
    let s = |v: &Value| v.as_str().map(str::to_string).unwrap_or_else(|| v.to_string());
    let what = match ty {
        "accepted" => {
            let env = &e["envelope"];
            let corr = env["correlation_id"].as_str().map(|c| format!(" re {c}")).unwrap_or_default();
            format!(
                "{} -> {}  {} {}  {}{corr}",
                s(&env["from"]),
                s(&env["to"]),
                s(&env["kind"]),
                s(&env["schema"]),
                s(&env["id"])
            )
        }
        "refused" => format!("{} {}: {} {}", s(&e["agent"]), s(&e["op"]), s(&e["code"]), s(&e["message"])),
        "delivered" | "acked" => format!("{} {}", s(&e["agent"]), e["ids"]),
        "closed" => format!("{} {}", s(&e["request_id"]), s(&e["reason"])),
        "process" => format!("{} {}", s(&e["agent"]), e["status"]),
        "deadlock_prevented" => format!("{} -> {} refused, cycle {}", s(&e["agent"]), s(&e["to"]), e["cycle"]),
        "lease_granted" | "lease_released" | "lease_expired" | "lease_denied" => {
            format!("{} {}", s(&e["agent"]), s(&e["resource"]))
        }
        "registered" | "disconnected" => s(&e["agent"]).to_string(),
        _ => {
            let mut rest = e.clone();
            if let Value::Object(m) = &mut rest {
                m.remove("type");
            }
            rest.to_string()
        }
    };
    format!("{:>6}  {}  {:<18} {what}", rec["n"].as_u64().unwrap_or(0), clock(rec["ts_ms"].as_u64().unwrap_or(0)), ty)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn clock_is_utc_time_of_day() {
        assert_eq!(clock(3_723_004), "01:02:03.004Z");
    }

    #[test]
    fn mentions_checks_sender_recipient_and_actor() {
        let e = json!({"type": "accepted", "envelope": {"from": "a", "to": "b"}});
        assert!(mentions(&e, "a") && mentions(&e, "b") && !mentions(&e, "c"));
        assert!(mentions(&json!({"type": "registered", "agent": "c"}), "c"));
    }
}
