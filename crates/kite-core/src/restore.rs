//! Rebuilds the broker's state from the ledger: queued messages, open
//! requests, the counters that order them, and the board's cards. The ledger
//! is the only store; nothing else is written to disk.
//!
//! [`Fold`] applies one event at a time, so the same code replays a whole
//! ledger or continues from a `snapshot` record (see `ledger.rs`): a snapshot is
//! a serialized `Fold`, and seeding from it then applying the later events
//! gives exactly what replaying everything would.
//!
//! A `started` event means every connection of the run before it is gone, so
//! messages handed out and not acknowledged by then go back to the front of
//! their mailbox with `redelivered` incremented, exactly as on a disconnect.
//! The same happens once more in `finish`, for the broker that is starting now.

use kite_proto::board::Card;
use kite_proto::config::BROKER_AGENT;
use kite_proto::wire::SendOk;
use kite_proto::*;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::{BTreeMap, HashMap, VecDeque};
use ulid::Ulid;

/// How many closed requests the broker remembers (its `closed` window).
pub const CLOSED_WINDOW: usize = 4096;
/// How many sends per agent the broker remembers for idempotent resends.
pub const DEDUP_WINDOW: usize = 4096;

/// A request that was still open when the broker stopped.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct OpenRequest {
    pub id: Ulid,
    pub from: String,
    pub to: String,
    pub schema: SchemaRef,
    /// Wall-clock deadline (Unix ms), as stamped on the envelope.
    pub deadline_ms: u64,
    pub blocking: bool,
    /// The broker clock when it was accepted; open requests are restored in this order.
    pub lc: u64,
}

#[derive(Debug, Default, PartialEq)]
pub struct Restored {
    /// Each agent's mailbox in the order it will be handed out.
    pub mailboxes: BTreeMap<String, Vec<Envelope>>,
    /// Messages that were in flight when the broker stopped and are queued again.
    pub requeued: Vec<Ulid>,
    /// Open requests, in the order they were accepted.
    pub open: Vec<OpenRequest>,
    /// Closed requests and why, oldest first (at most `CLOSED_WINDOW`).
    pub closed: Vec<(Ulid, String)>,
    /// What each agent's sends answered, so a resent idempotency key still delivers once.
    pub sent: Vec<(String, SendOk)>,
    /// The highest logical clock the ledger shows.
    pub lc: u64,
    /// The last `seq` used for each (from, to) pair.
    pub seqs: HashMap<(String, String), u64>,
    /// The board: the latest snapshot of each card.
    pub cards: BTreeMap<String, Card>,
}

#[derive(Debug, Default, Clone, PartialEq, Serialize, Deserialize)]
struct Slot {
    mailbox: VecDeque<Envelope>,
    in_flight: Vec<Envelope>,
}

impl Slot {
    /// Hands everything in flight back to the front of the mailbox, in order.
    fn requeue(&mut self) -> Vec<Ulid> {
        let ids: Vec<Ulid> = self.in_flight.iter().map(|e| e.id).collect();
        for mut env in self.in_flight.drain(..).rev() {
            env.redelivered += 1;
            self.mailbox.push_front(env);
        }
        ids
    }

    fn take(&mut self, id: Ulid) -> Option<Envelope> {
        let pos = self.mailbox.iter().position(|e| e.id == id)?;
        self.mailbox.remove(pos)
    }

    fn drop_id(&mut self, id: Ulid) {
        self.mailbox.retain(|e| e.id != id);
        self.in_flight.retain(|e| e.id != id);
    }
}

/// Replay state. Serializable, so a snapshot of it can stand in for every
/// event before it.
#[derive(Debug, Default, Clone, PartialEq, Serialize, Deserialize)]
pub struct Fold {
    slots: BTreeMap<String, Slot>,
    open: BTreeMap<Ulid, OpenRequest>,
    closed: VecDeque<(Ulid, String)>,
    sent: BTreeMap<String, VecDeque<SendOk>>,
    lc: u64,
    /// `"from>to"` → last seq (agent names cannot contain `>`).
    seqs: BTreeMap<String, u64>,
    cards: BTreeMap<String, Card>,
}

fn ulid(v: &Value) -> Option<Ulid> {
    v.as_str().and_then(|s| Ulid::from_string(s).ok())
}

fn ulids(v: &Value) -> Vec<Ulid> {
    v.as_array().map(|a| a.iter().filter_map(ulid).collect()).unwrap_or_default()
}

impl Fold {
    fn slot(&mut self, agent: &Value) -> &mut Slot {
        self.slots.entry(agent.as_str().unwrap_or_default().to_string()).or_default()
    }

    /// Applies one ledger event.
    pub fn apply(&mut self, e: &Value) {
        match e["type"].as_str().unwrap_or_default() {
            "started" => {
                for s in self.slots.values_mut() {
                    s.requeue();
                }
            }
            "accepted" => {
                let Ok(env) = Envelope::deserialize(&e["envelope"]) else { return };
                self.lc = self.lc.max(env.lc);
                let seq = self.seqs.entry(format!("{}>{}", env.from, env.to)).or_insert(0);
                *seq = (*seq).max(env.seq);
                if matches!(env.kind, Kind::Request | Kind::Event) && env.from != BROKER_AGENT {
                    let ok = SendOk {
                        id: env.id,
                        kind: env.kind,
                        seq: env.seq,
                        lc: env.lc,
                        deadline_ms: env.deadline_ms,
                        duplicate: false,
                    };
                    let sent = self.sent.entry(env.from.clone()).or_default();
                    sent.push_back(ok);
                    if sent.len() > DEDUP_WINDOW {
                        sent.pop_front();
                    }
                }
                if env.kind == Kind::Request
                    && let Some(deadline_ms) = env.deadline_ms
                {
                    let o = OpenRequest {
                        id: env.id,
                        from: env.from.clone(),
                        to: env.to.clone(),
                        schema: env.schema.clone(),
                        deadline_ms,
                        blocking: e["blocking"].as_bool().unwrap_or(false),
                        lc: env.lc,
                    };
                    self.open.insert(env.id, o);
                }
                self.slots.entry(env.to.clone()).or_default().mailbox.push_back(env);
            }
            "delivered" => {
                let via = e["via"].as_str().map(str::to_string);
                let s = self.slot(&e["agent"]);
                for id in ulids(&e["ids"]) {
                    match (s.take(id), via.as_deref()) {
                        (Some(env), Some("recv")) => s.in_flight.push(env),
                        (Some(_), _) => {}
                        // `await` can hand out an answer that a `recv` already had in flight.
                        (None, _) => s.in_flight.retain(|x| x.id != id),
                    }
                }
            }
            "acked" => {
                let ids = ulids(&e["ids"]);
                self.slot(&e["agent"]).in_flight.retain(|x| !ids.contains(&x.id));
            }
            "withdrawn" | "dead_lettered" => {
                if let Some(id) = ulid(&e["id"]) {
                    self.slot(&e["agent"]).drop_id(id);
                }
            }
            "disconnected" => {
                self.slot(&e["agent"]).requeue();
            }
            "closed" => {
                let Some(rid) = ulid(&e["request_id"]) else { return };
                let reason = e["reason"].as_str().unwrap_or_default();
                // An answer takes the request off its recipient without an event of its own.
                if let Some(o) = self.open.remove(&rid)
                    && reason == "answered"
                {
                    self.slots.entry(o.to).or_default().drop_id(rid);
                }
                self.closed.push_back((rid, reason.to_string()));
                if self.closed.len() > CLOSED_WINDOW {
                    self.closed.pop_front();
                }
            }
            "stopped" => self.lc = self.lc.max(e["lc"].as_u64().unwrap_or(0)),
            "card" => {
                if let Ok(card) = serde_json::from_value::<Card>(e["card"].clone()) {
                    self.cards.insert(card.id.clone(), card);
                }
            }
            _ => {}
        }
    }

    /// The state for a broker starting now: everything in flight is requeued.
    pub fn finish(mut self) -> Restored {
        let mut r = Restored::default();
        for (agent, s) in self.slots.iter_mut() {
            r.requeued.extend(s.requeue());
            if !s.mailbox.is_empty() {
                r.mailboxes.insert(agent.clone(), s.mailbox.iter().cloned().collect());
            }
        }
        let mut open: Vec<OpenRequest> = self.open.into_values().collect();
        open.sort_by_key(|o| o.lc);
        r.open = open;
        r.closed = self.closed.into_iter().collect();
        r.sent =
            self.sent.into_iter().flat_map(|(agent, oks)| oks.into_iter().map(move |ok| (agent.clone(), ok))).collect();
        r.sent.sort_by_key(|(_, ok)| ok.lc);
        r.lc = self.lc;
        r.seqs = self
            .seqs
            .into_iter()
            .filter_map(|(k, v)| k.split_once('>').map(|(f, t)| ((f.to_string(), t.to_string()), v)))
            .collect();
        r.cards = self.cards;
        r
    }

    /// The fold that a broker starting now would continue from: `finish` already
    /// requeued what was in flight, so nothing is in flight. Writing this as a
    /// snapshot just before `started` is therefore equivalent to every event
    /// before it.
    pub fn compacted(&self) -> Fold {
        let mut f = self.clone();
        for s in f.slots.values_mut() {
            s.requeue();
        }
        f.slots.retain(|_, s| !s.mailbox.is_empty());
        f
    }
}

/// Replays ledger events, oldest first.
pub fn replay(events: &[Value]) -> Restored {
    let mut f = Fold::default();
    for e in events {
        f.apply(e);
    }
    f.finish()
}
