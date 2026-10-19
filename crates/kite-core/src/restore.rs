//! Rebuilds the broker's messages from the ledger, the way `board::replay`
//! rebuilds the board: queued messages, open requests and the counters that
//! order them. The ledger is the only store; nothing else is written to disk.
//!
//! A `started` event means every connection of the run before it is gone, so
//! messages handed out and not acknowledged by then go back to the front of
//! their mailbox with `redelivered` incremented, exactly as on a disconnect.
//! The same happens once more at the end, for the broker that is starting now.

use kite_proto::config::BROKER_AGENT;
use kite_proto::wire::SendOk;
use kite_proto::*;
use serde::Deserialize;
use serde_json::Value;
use std::collections::{BTreeMap, HashMap, VecDeque};
use ulid::Ulid;

/// A request that was still open when the broker stopped.
#[derive(Debug, Clone, PartialEq)]
pub struct OpenRequest {
    pub id: Ulid,
    pub from: String,
    pub to: String,
    pub schema: SchemaRef,
    /// Wall-clock deadline (Unix ms), as stamped on the envelope.
    pub deadline_ms: u64,
    pub blocking: bool,
}

#[derive(Debug, Default)]
pub struct Restored {
    /// Each agent's mailbox in the order it will be handed out.
    pub mailboxes: BTreeMap<String, Vec<Envelope>>,
    /// Messages that were in flight when the broker stopped and are queued again.
    pub requeued: Vec<Ulid>,
    /// Open requests, in the order they were accepted.
    pub open: Vec<OpenRequest>,
    /// Closed requests and why, oldest first.
    pub closed: Vec<(Ulid, String)>,
    /// What each agent's sends answered, so a resent idempotency key still delivers once.
    pub sent: Vec<(String, SendOk)>,
    /// The highest logical clock the ledger shows.
    pub lc: u64,
    /// The last `seq` used for each (from, to) pair.
    pub seqs: HashMap<(String, String), u64>,
}

#[derive(Default)]
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

fn slot<'a>(slots: &'a mut BTreeMap<String, Slot>, agent: &Value) -> &'a mut Slot {
    slots.entry(agent.as_str().unwrap_or_default().to_string()).or_default()
}

fn ulid(v: &Value) -> Option<Ulid> {
    v.as_str().and_then(|s| Ulid::from_string(s).ok())
}

fn ulids(v: &Value) -> Vec<Ulid> {
    v.as_array().map(|a| a.iter().filter_map(ulid).collect()).unwrap_or_default()
}

/// Replays ledger events, oldest first.
pub fn replay(events: &[Value]) -> Restored {
    let mut slots: BTreeMap<String, Slot> = BTreeMap::new();
    let mut open: BTreeMap<Ulid, (u64, OpenRequest)> = BTreeMap::new();
    let mut r = Restored::default();
    for e in events {
        match e["type"].as_str().unwrap_or_default() {
            "started" => {
                for s in slots.values_mut() {
                    s.requeue();
                }
            }
            "accepted" => {
                let Ok(env) = Envelope::deserialize(&e["envelope"]) else { continue };
                r.lc = r.lc.max(env.lc);
                let seq = r.seqs.entry((env.from.clone(), env.to.clone())).or_insert(0);
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
                    r.sent.push((env.from.clone(), ok));
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
                    };
                    open.insert(env.id, (env.lc, o));
                }
                slots.entry(env.to.clone()).or_default().mailbox.push_back(env);
            }
            "delivered" => {
                let s = slot(&mut slots, &e["agent"]);
                for id in ulids(&e["ids"]) {
                    match (s.take(id), e["via"].as_str()) {
                        (Some(env), Some("recv")) => s.in_flight.push(env),
                        (Some(_), _) => {}
                        // `await` can hand out an answer that a `recv` already had in flight.
                        (None, _) => s.in_flight.retain(|x| x.id != id),
                    }
                }
            }
            "acked" => {
                let ids = ulids(&e["ids"]);
                slot(&mut slots, &e["agent"]).in_flight.retain(|x| !ids.contains(&x.id));
            }
            "withdrawn" | "dead_lettered" => {
                if let Some(id) = ulid(&e["id"]) {
                    slot(&mut slots, &e["agent"]).drop_id(id);
                }
            }
            "disconnected" => {
                slot(&mut slots, &e["agent"]).requeue();
            }
            "closed" => {
                let Some(rid) = ulid(&e["request_id"]) else { continue };
                let reason = e["reason"].as_str().unwrap_or_default();
                // An answer takes the request off its recipient without an event of its own.
                if let Some((_, o)) = open.remove(&rid)
                    && reason == "answered"
                {
                    slots.entry(o.to).or_default().drop_id(rid);
                }
                r.closed.push((rid, reason.to_string()));
            }
            "stopped" => r.lc = r.lc.max(e["lc"].as_u64().unwrap_or(0)),
            _ => {}
        }
    }
    for (agent, mut s) in slots {
        r.requeued.extend(s.requeue());
        if !s.mailbox.is_empty() {
            r.mailboxes.insert(agent, s.mailbox.into());
        }
    }
    let mut open: Vec<(u64, OpenRequest)> = open.into_values().collect();
    open.sort_by_key(|(lc, _)| *lc);
    r.open = open.into_iter().map(|(_, o)| o).collect();
    r
}
